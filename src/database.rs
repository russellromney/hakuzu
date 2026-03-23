//! HaKuzu: dead-simple embedded HA Kuzu/LadybugDB.
//!
//! ```ignore
//! let db = HaKuzu::builder("my-bucket")
//!     .open("/data/graph", "CREATE NODE TABLE IF NOT EXISTS Person(id INT64, name STRING, PRIMARY KEY(id))")
//!     .await?;
//!
//! db.execute("CREATE (p:Person {id: $id, name: $name})", Some(json!({"id": 1, "name": "Alice"}))).await?;
//! let result = db.query("MATCH (p:Person) RETURN p.id, p.name", None).await?;
//! ```

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{anyhow, Result};
use axum::routing::post;
use graphstream::journal::{JournalCommand, JournalSender, PendingEntry};
use hadb::{Coordinator, CoordinatorConfig, LeaseConfig, Role, RoleEvent};
use hadb_s3::S3LeaseStore;

use crate::follower_behavior::KuzuFollowerBehavior;
use crate::forwarding::{self, ForwardingState};
use crate::mutation::is_mutation;
use crate::replicator::KuzuReplicator;
use crate::snapshot;
use crate::values;

const DEFAULT_PREFIX: &str = "hakuzu/";
const DEFAULT_FORWARDING_PORT: u16 = 18080;
const DEFAULT_FORWARD_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_SNAPSHOT_EVERY_N_ENTRIES: u64 = 10_000;

/// Configuration for periodic snapshot creation (leader only).
#[derive(Debug, Clone)]
pub struct SnapshotConfig {
    /// How often to check if a snapshot should be taken.
    pub interval: Duration,
    /// Minimum entries since last snapshot before taking a new one.
    pub every_n_entries: u64,
}

/// Internal snapshot context passed to the role listener.
struct SnapshotContext {
    config: SnapshotConfig,
    s3_client: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,
    db_path: PathBuf,
}

const ROLE_LEADER: u8 = 0;
const ROLE_FOLLOWER: u8 = 1;

/// Query result returned from `HaKuzu::query()`.
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
}

/// Builder for creating an HA Kuzu instance.
pub struct HaKuzuBuilder {
    bucket: String,
    prefix: String,
    endpoint: Option<String>,
    instance_id: Option<String>,
    address: Option<String>,
    forwarding_port: u16,
    forward_timeout: Duration,
    coordinator_config: Option<CoordinatorConfig>,
    secret: Option<String>,
    upload_interval: Option<Duration>,
    snapshot_interval: Option<Duration>,
    snapshot_every_n_entries: Option<u64>,
}

impl HaKuzuBuilder {
    fn new(bucket: &str) -> Self {
        Self {
            bucket: bucket.to_string(),
            prefix: DEFAULT_PREFIX.to_string(),
            endpoint: None,
            instance_id: None,
            address: None,
            forwarding_port: DEFAULT_FORWARDING_PORT,
            forward_timeout: DEFAULT_FORWARD_TIMEOUT,
            coordinator_config: None,
            secret: None,
            upload_interval: None,
            snapshot_interval: None,
            snapshot_every_n_entries: None,
        }
    }

    /// S3 key prefix for all hakuzu data. Default: "hakuzu/".
    pub fn prefix(mut self, prefix: &str) -> Self {
        self.prefix = prefix.to_string();
        self
    }

    /// S3 endpoint URL (for Tigris, MinIO, R2, etc).
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        self.endpoint = Some(endpoint.to_string());
        self
    }

    /// Unique instance ID for this node. Default: FLY_MACHINE_ID env or UUID.
    pub fn instance_id(mut self, id: &str) -> Self {
        self.instance_id = Some(id.to_string());
        self
    }

    /// Network address for this node (how other nodes reach the forwarding server).
    pub fn address(mut self, addr: &str) -> Self {
        self.address = Some(addr.to_string());
        self
    }

    /// Port for the internal write-forwarding HTTP server. Default: 18080.
    pub fn forwarding_port(mut self, port: u16) -> Self {
        self.forwarding_port = port;
        self
    }

    /// Timeout for forwarded write requests. Default: 5s.
    pub fn forward_timeout(mut self, timeout: Duration) -> Self {
        self.forward_timeout = timeout;
        self
    }

    /// Override the coordinator config (lease timing, sync interval, etc).
    pub fn coordinator_config(mut self, config: CoordinatorConfig) -> Self {
        self.coordinator_config = Some(config);
        self
    }

    /// Shared secret for authenticating forwarding requests.
    pub fn secret(mut self, secret: &str) -> Self {
        self.secret = Some(secret.to_string());
        self
    }

    /// Upload interval for journal segments to S3. Default: 10s.
    pub fn upload_interval(mut self, interval: Duration) -> Self {
        self.upload_interval = Some(interval);
        self
    }

    /// How often the leader checks if a snapshot should be taken.
    /// Setting this enables periodic snapshots. Default threshold: 10,000 entries.
    pub fn snapshot_interval(mut self, interval: Duration) -> Self {
        self.snapshot_interval = Some(interval);
        self
    }

    /// Minimum journal entries since last snapshot before taking a new one.
    /// Requires `snapshot_interval` to be set. Default: 10,000.
    pub fn snapshot_every_n_entries(mut self, n: u64) -> Self {
        self.snapshot_every_n_entries = Some(n);
        self
    }

    /// Open the database and join the HA cluster.
    pub async fn open(self, db_path: &str, schema: &str) -> Result<HaKuzu> {
        let db_path = PathBuf::from(db_path);
        let db_name = db_path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("db")
            .to_string();

        let instance_id = self.instance_id.unwrap_or_else(|| {
            std::env::var("FLY_MACHINE_ID")
                .unwrap_or_else(|_| uuid::Uuid::new_v4().to_string())
        });
        let address = self.address.unwrap_or_else(|| {
            detect_address(&instance_id, self.forwarding_port)
        });

        // Build S3 client.
        let s3_config = match &self.endpoint {
            Some(endpoint) => {
                aws_config::defaults(aws_config::BehaviorVersion::latest())
                    .endpoint_url(endpoint)
                    .load()
                    .await
            }
            None => {
                aws_config::defaults(aws_config::BehaviorVersion::latest())
                    .load()
                    .await
            }
        };
        let s3_client = aws_sdk_s3::Client::new(&s3_config);

        // Snapshot bootstrap: if the database doesn't exist locally, try to
        // restore from the latest S3 snapshot for faster cold start.
        let db_exists = db_path.exists()
            && db_path.read_dir().map_or(false, |mut d| d.next().is_some());
        if !db_exists {
            let snap_dest = db_path
                .parent()
                .unwrap_or(&db_path)
                .join("snapshots_tmp");
            match snapshot::download_latest_snapshot(
                &s3_client,
                &self.bucket,
                &self.prefix,
                &db_name,
                &snap_dest,
            )
            .await
            {
                Ok(Some((snap_path, meta))) => {
                    snapshot::extract_snapshot(&snap_path, &db_path)?;

                    // Write recovery.json so journal recovery starts from snapshot seq.
                    let journal_dir = db_path
                        .parent()
                        .unwrap_or(&db_path)
                        .join("journal");
                    std::fs::create_dir_all(&journal_dir)?;
                    let hash_bytes = hex::decode(&meta.chain_hash)
                        .map_err(|e| anyhow!("Invalid chain hash in snapshot: {e}"))?;
                    let mut hash_arr = [0u8; 32];
                    if hash_bytes.len() == 32 {
                        hash_arr.copy_from_slice(&hash_bytes);
                    }
                    graphstream::write_recovery_state(
                        &journal_dir,
                        meta.journal_seq,
                        hash_arr,
                    );

                    tracing::info!(
                        "Bootstrapped from snapshot: seq={}, size={}B",
                        meta.journal_seq,
                        meta.db_size_bytes
                    );

                    let _ = std::fs::remove_dir_all(&snap_dest);
                }
                Ok(None) => {
                    tracing::debug!("No snapshots available, starting fresh");
                }
                Err(e) => {
                    tracing::warn!("Failed to download snapshot, starting fresh: {e}");
                }
            }
        }

        let lease_store = Arc::new(S3LeaseStore::new(s3_client.clone(), self.bucket.clone()));

        // Build replicator.
        let mut replicator = KuzuReplicator::new(self.bucket.clone(), self.prefix.clone());
        if let Some(interval) = self.upload_interval {
            replicator = replicator.with_upload_interval(interval);
        }
        let replicator = Arc::new(replicator);

        // Open database (potentially from snapshot data).
        let db = Arc::new(open_database(&db_path, schema)?);

        // Build follower behavior.
        let follower_s3 = aws_sdk_s3::Client::new(&s3_config);
        let follower_behavior = Arc::new(
            KuzuFollowerBehavior::new(follower_s3, self.bucket.clone())
                .with_shared_db(db.clone()),
        );

        // Build snapshot context for leader.
        let snapshot_ctx = self.snapshot_interval.map(|interval| SnapshotContext {
            config: SnapshotConfig {
                interval,
                every_n_entries: self
                    .snapshot_every_n_entries
                    .unwrap_or(DEFAULT_SNAPSHOT_EVERY_N_ENTRIES),
            },
            s3_client: s3_client.clone(),
            bucket: self.bucket.clone(),
            prefix: self.prefix.clone(),
            db_path: db_path.clone(),
        });

        // Build coordinator.
        let mut config = self.coordinator_config.unwrap_or_default();
        config.lease = Some(LeaseConfig::new(instance_id.clone(), address.clone()));

        let coordinator = Coordinator::new(
            replicator.clone(),
            Some(lease_store as Arc<dyn hadb::LeaseStore>),
            None,
            follower_behavior,
            &self.prefix,
            config,
        );

        open_with_coordinator(
            coordinator,
            replicator,
            db,
            db_path,
            &db_name,
            &address,
            self.forwarding_port,
            self.forward_timeout,
            self.secret,
            snapshot_ctx,
        )
        .await
    }
}

/// HA Kuzu database — transparent write forwarding, local reads, automatic failover.
pub struct HaKuzu {
    inner: Arc<HaKuzuInner>,
    _fwd_handle: tokio::task::JoinHandle<()>,
    _role_handle: tokio::task::JoinHandle<()>,
}

/// Internal state shared between HaKuzu, forwarding handler, and role listener.
pub(crate) struct HaKuzuInner {
    pub(crate) coordinator: Option<Arc<Coordinator>>,
    pub(crate) replicator: Option<Arc<KuzuReplicator>>,
    pub(crate) db: Arc<lbug::Database>,
    pub(crate) db_name: String,
    role: AtomicU8,
    write_mutex: tokio::sync::Mutex<()>,
    read_semaphore: tokio::sync::Semaphore,
    snapshot_lock: Arc<std::sync::RwLock<()>>,
    journal_sender: RwLock<Option<JournalSender>>,
    leader_address: RwLock<String>,
    http_client: reqwest::Client,
    pub(crate) secret: Option<String>,
}

impl HaKuzuInner {
    pub(crate) fn current_role(&self) -> Option<Role> {
        match self.role.load(Ordering::SeqCst) {
            ROLE_LEADER => Some(Role::Leader),
            ROLE_FOLLOWER => Some(Role::Follower),
            _ => None,
        }
    }

    fn set_role(&self, role: Role) {
        self.role.store(
            match role {
                Role::Leader => ROLE_LEADER,
                Role::Follower => ROLE_FOLLOWER,
            },
            Ordering::SeqCst,
        );
    }

    fn leader_addr(&self) -> String {
        self.leader_address.read().unwrap().clone()
    }

    fn set_leader_addr(&self, addr: String) {
        *self.leader_address.write().unwrap() = addr;
    }

    fn set_journal_sender(&self, sender: Option<JournalSender>) {
        *self.journal_sender.write().unwrap() = sender;
    }

    fn get_journal_sender(&self) -> Option<JournalSender> {
        self.journal_sender.read().unwrap().clone()
    }

    /// Execute a write locally (leader path). Used by both HaKuzu::execute and forwarding handler.
    pub(crate) async fn execute_write_local(
        &self,
        cypher: &str,
        params: Option<&serde_json::Value>,
    ) -> Result<()> {
        let _guard = self.write_mutex.lock().await;
        let db = self.db.clone();
        let lock = self.snapshot_lock.clone();
        let cypher_owned = cypher.to_string();
        let params_owned = params.cloned();

        tokio::task::spawn_blocking(move || {
            let conn = lbug::Connection::new(&db)
                .map_err(|e| anyhow!("Failed to create connection: {e}"))?;
            let _snap = lock.read().unwrap_or_else(|e| e.into_inner());

            if let Some(ref p) = params_owned {
                let lbug_params = values::json_params_to_lbug(p)
                    .map_err(|e| anyhow!("Param conversion failed: {e}"))?;
                let refs: Vec<(&str, lbug::Value)> =
                    lbug_params.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
                let mut stmt = conn
                    .prepare(&cypher_owned)
                    .map_err(|e| anyhow!("Prepare failed: {e}"))?;
                conn.execute(&mut stmt, refs)
                    .map_err(|e| anyhow!("Execute failed: {e}"))?;
            } else {
                conn.query(&cypher_owned)
                    .map_err(|e| anyhow!("Query failed: {e}"))?;
            }
            Ok::<(), anyhow::Error>(())
        })
        .await
        .map_err(|e| anyhow!("Task panicked: {e}"))??;

        // Journal on success.
        if let Some(tx) = self.get_journal_sender() {
            let gs_params = match params {
                Some(p) => values::json_params_to_graphstream(p),
                None => vec![],
            };
            let entry = PendingEntry {
                query: cypher.to_string(),
                params: gs_params,
            };
            if tx.send(JournalCommand::Write(entry)).is_err() {
                tracing::error!("Failed to send journal entry — writer may have crashed");
            }
        }

        Ok(())
    }
}

impl HaKuzu {
    /// Start building an HA Kuzu instance.
    pub fn builder(bucket: &str) -> HaKuzuBuilder {
        HaKuzuBuilder::new(bucket)
    }

    /// Open a local-only Kuzu database (no HA, no S3).
    pub fn local(db_path: &str, schema: &str) -> Result<HaKuzu> {
        let db_path = PathBuf::from(db_path);
        let db = Arc::new(open_database(&db_path, schema)?);

        let inner = Arc::new(HaKuzuInner {
            coordinator: None,
            replicator: None,
            db,
            db_name: db_path
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("db")
                .to_string(),
            role: AtomicU8::new(ROLE_LEADER),
            write_mutex: tokio::sync::Mutex::new(()),
            read_semaphore: tokio::sync::Semaphore::new(16),
            snapshot_lock: Arc::new(std::sync::RwLock::new(())),
            journal_sender: RwLock::new(None),
            leader_address: RwLock::new(String::new()),
            http_client: reqwest::Client::new(),
            secret: None,
        });

        let fwd_handle = tokio::spawn(async {});
        let role_handle = tokio::spawn(async {});

        Ok(HaKuzu {
            inner,
            _fwd_handle: fwd_handle,
            _role_handle: role_handle,
        })
    }

    /// Create an HaKuzu instance from a pre-built Coordinator.
    ///
    /// For tests and advanced use cases where you build the Coordinator yourself.
    pub async fn from_coordinator(
        coordinator: Arc<Coordinator>,
        replicator: Arc<KuzuReplicator>,
        db: Arc<lbug::Database>,
        db_path: &str,
        db_name: &str,
        forwarding_port: u16,
        forward_timeout: Duration,
    ) -> Result<HaKuzu> {
        Self::from_coordinator_with_secret(
            coordinator,
            replicator,
            db,
            db_path,
            db_name,
            forwarding_port,
            forward_timeout,
            None,
        )
        .await
    }

    /// Like `from_coordinator`, but with an optional shared secret for auth.
    pub async fn from_coordinator_with_secret(
        coordinator: Arc<Coordinator>,
        replicator: Arc<KuzuReplicator>,
        db: Arc<lbug::Database>,
        db_path: &str,
        db_name: &str,
        forwarding_port: u16,
        forward_timeout: Duration,
        secret: Option<String>,
    ) -> Result<HaKuzu> {
        let db_path = PathBuf::from(db_path);
        let address = format!("http://localhost:{}", forwarding_port);

        open_with_coordinator(
            coordinator,
            replicator,
            db,
            db_path,
            db_name,
            &address,
            forwarding_port,
            forward_timeout,
            secret,
            None, // no snapshot config via from_coordinator
        )
        .await
    }

    /// Execute a Cypher statement. Auto-routes reads vs writes.
    ///
    /// - Mutations: leader executes locally + journals; follower forwards to leader.
    /// - Reads: always execute locally (bounded by semaphore).
    pub async fn execute(
        &self,
        cypher: &str,
        params: Option<serde_json::Value>,
    ) -> Result<()> {
        if is_mutation(cypher) {
            let role = self.inner.current_role();
            match role {
                Some(Role::Leader) | None => {
                    self.inner
                        .execute_write_local(cypher, params.as_ref())
                        .await
                }
                Some(Role::Follower) => {
                    self.execute_forwarded(cypher, params.as_ref()).await
                }
            }
        } else {
            // Read — just execute locally via query(), ignore result
            self.query(cypher, params).await?;
            Ok(())
        }
    }

    /// Execute a Cypher query and return results. Always executes locally.
    pub async fn query(
        &self,
        cypher: &str,
        params: Option<serde_json::Value>,
    ) -> Result<QueryResult> {
        let _permit = self
            .inner
            .read_semaphore
            .acquire()
            .await
            .map_err(|_| anyhow!("Engine closed"))?;

        let db = self.inner.db.clone();
        let lock = self.inner.snapshot_lock.clone();
        let cypher = cypher.to_string();
        let params_owned = params;

        tokio::task::spawn_blocking(move || {
            let conn = lbug::Connection::new(&db)
                .map_err(|e| anyhow!("Failed to create connection: {e}"))?;
            let _snap = lock.read().unwrap_or_else(|e| e.into_inner());

            let mut result = if let Some(ref p) = params_owned {
                let lbug_params = values::json_params_to_lbug(p)
                    .map_err(|e| anyhow!("Param conversion failed: {e}"))?;
                let refs: Vec<(&str, lbug::Value)> =
                    lbug_params.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
                let mut stmt = conn
                    .prepare(&cypher)
                    .map_err(|e| anyhow!("Prepare failed: {e}"))?;
                conn.execute(&mut stmt, refs)
                    .map_err(|e| anyhow!("Execute failed: {e}"))?
            } else {
                conn.query(&cypher)
                    .map_err(|e| anyhow!("Query failed: {e}"))?
            };

            let columns = result.get_column_names();
            let mut rows = Vec::new();
            while let Some(row) = result.next() {
                let json_row: Vec<serde_json::Value> =
                    row.iter().map(values::lbug_to_json).collect();
                rows.push(json_row);
            }

            Ok(QueryResult { columns, rows })
        })
        .await
        .map_err(|e| anyhow!("Task panicked: {e}"))?
    }

    /// Get the current role of this node.
    pub fn role(&self) -> Option<Role> {
        self.inner.current_role()
    }

    /// Subscribe to role change events.
    pub fn role_events(&self) -> Option<tokio::sync::broadcast::Receiver<RoleEvent>> {
        self.inner.coordinator.as_ref().map(|c| c.role_events())
    }

    /// Access the underlying Coordinator.
    pub fn coordinator(&self) -> Option<&Arc<Coordinator>> {
        self.inner.coordinator.as_ref()
    }

    /// Get metrics in Prometheus exposition format.
    pub fn prometheus_metrics(&self) -> Option<String> {
        self.inner
            .coordinator
            .as_ref()
            .map(|c| c.metrics().snapshot().to_prometheus())
    }

    /// Graceful leader handoff with drain barrier.
    ///
    /// 1. Acquires write_mutex — blocks until any in-flight write completes,
    ///    prevents new writes from starting.
    /// 2. Seals the journal segment via replicator.sync() — ensures all entries
    ///    are in a sealed segment ready for upload.
    /// 3. Releases mutex, then delegates to coordinator.handoff() for lease
    ///    release and role demotion.
    pub async fn handoff(&self) -> Result<bool> {
        let coordinator = match &self.inner.coordinator {
            Some(c) => c,
            None => return Ok(false),
        };

        // Drain: wait for in-flight writes to finish + prevent new ones.
        {
            let _guard = self.inner.write_mutex.lock().await;

            // Seal journal segment so all entries are durable before we step down.
            if let Some(ref replicator) = self.inner.replicator {
                use hadb::Replicator;
                replicator.sync(&self.inner.db_name).await?;
            }
        } // write_mutex released — coordinator handoff may take time

        coordinator.handoff(&self.inner.db_name).await
    }

    /// Cleanly shut down: drain writes, seal journal, leave cluster, stop tasks.
    pub async fn close(self) -> Result<()> {
        if let Some(ref coordinator) = self.inner.coordinator {
            // Drain: wait for in-flight writes + seal journal before leaving.
            {
                let _guard = self.inner.write_mutex.lock().await;
                if let Some(ref replicator) = self.inner.replicator {
                    use hadb::Replicator;
                    if let Err(e) = replicator.sync(&self.inner.db_name).await {
                        tracing::error!("Failed to seal journal on close: {e}");
                    }
                }
            }
            coordinator.leave(&self.inner.db_name).await?;
        }

        self._fwd_handle.abort();
        self._role_handle.abort();

        // Wait for tasks to finish so their Arc<HaKuzuInner> refs are released
        // before the Database is dropped. Otherwise the lbug::Database destructor
        // can race with TempDir cleanup in tests (or filesystem removal in prod).
        let _ = self._fwd_handle.await;
        let _ = self._role_handle.await;

        Ok(())
    }

    // ========================================================================
    // Internal
    // ========================================================================

    async fn execute_forwarded(
        &self,
        cypher: &str,
        params: Option<&serde_json::Value>,
    ) -> Result<()> {
        let leader_addr = self.inner.leader_addr();
        if leader_addr.is_empty() {
            return Err(anyhow!(
                "No leader address available — cannot forward write"
            ));
        }

        let url = format!("{}/hakuzu/execute", leader_addr);
        let body = forwarding::ForwardedExecute {
            cypher: cypher.to_string(),
            params: params.cloned(),
        };

        let mut req = self.inner.http_client.post(&url).json(&body);
        if let Some(ref secret) = self.inner.secret {
            req = req.bearer_auth(secret);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| anyhow!("Failed to forward write to leader: {}", e))?;

        if !resp.status().is_success() {
            return Err(anyhow!(
                "Leader returned error: {} {}",
                resp.status(),
                resp.text().await.unwrap_or_default()
            ));
        }

        Ok(())
    }
}

// ============================================================================
// Shared open logic
// ============================================================================

async fn open_with_coordinator(
    coordinator: Arc<Coordinator>,
    replicator: Arc<KuzuReplicator>,
    db: Arc<lbug::Database>,
    db_path: PathBuf,
    db_name: &str,
    address: &str,
    forwarding_port: u16,
    forward_timeout: Duration,
    secret: Option<String>,
    snapshot_ctx: Option<SnapshotContext>,
) -> Result<HaKuzu> {
    // Subscribe to role events BEFORE join.
    let role_rx = coordinator.role_events();

    // Join the HA cluster.
    let initial_role = coordinator.join(db_name, &db_path).await?;

    let leader_addr = if initial_role == Role::Leader {
        address.to_string()
    } else {
        coordinator
            .leader_address(db_name)
            .await
            .unwrap_or_default()
    };

    let http_client = reqwest::Client::builder()
        .timeout(forward_timeout)
        .build()?;

    let inner = Arc::new(HaKuzuInner {
        coordinator: Some(coordinator),
        replicator: Some(replicator.clone()),
        db,
        db_name: db_name.to_string(),
        role: AtomicU8::new(match initial_role {
            Role::Leader => ROLE_LEADER,
            Role::Follower => ROLE_FOLLOWER,
        }),
        write_mutex: tokio::sync::Mutex::new(()),
        read_semaphore: tokio::sync::Semaphore::new(16),
        snapshot_lock: Arc::new(std::sync::RwLock::new(())),
        journal_sender: RwLock::new(None),
        leader_address: RwLock::new(leader_addr),
        http_client,
        secret: secret.clone(),
    });

    // If leader, grab the journal sender from the replicator.
    if initial_role == Role::Leader {
        if let Some(tx) = replicator.journal_sender(db_name).await {
            inner.set_journal_sender(Some(tx));
        }
    }

    // Spawn forwarding server.
    let fwd_state = Arc::new(ForwardingState {
        inner: inner.clone(),
    });
    let fwd_app = axum::Router::new()
        .route(
            "/hakuzu/execute",
            post(forwarding::handle_forwarded_execute),
        )
        .with_state(fwd_state);
    let fwd_listener =
        tokio::net::TcpListener::bind(format!("0.0.0.0:{}", forwarding_port)).await?;
    let fwd_handle = tokio::spawn(async move {
        if let Err(e) = axum::serve(fwd_listener, fwd_app).await {
            tracing::error!("Forwarding server error: {}", e);
        }
    });

    // Spawn role event listener.
    let role_inner = inner.clone();
    let role_address = address.to_string();
    let role_replicator = replicator;
    let role_db_name = db_name.to_string();
    let role_handle = tokio::spawn(async move {
        run_role_listener(
            role_rx,
            role_inner,
            role_address,
            role_replicator,
            role_db_name,
            snapshot_ctx,
        )
        .await;
    });

    Ok(HaKuzu {
        inner,
        _fwd_handle: fwd_handle,
        _role_handle: role_handle,
    })
}

// ============================================================================
// Role event listener
// ============================================================================

async fn run_role_listener(
    mut role_rx: tokio::sync::broadcast::Receiver<RoleEvent>,
    inner: Arc<HaKuzuInner>,
    self_address: String,
    replicator: Arc<KuzuReplicator>,
    db_name: String,
    snapshot_ctx: Option<SnapshotContext>,
) {
    let mut snapshot_shutdown: Option<tokio::sync::watch::Sender<bool>> = None;
    let mut snapshot_handle: Option<tokio::task::JoinHandle<()>> = None;

    loop {
        match role_rx.recv().await {
            Ok(RoleEvent::Promoted { db_name: name }) => {
                tracing::info!("HaKuzu: promoted to leader for '{}'", name);
                inner.set_leader_addr(self_address.clone());
                inner.set_role(Role::Leader);

                // Grab journal sender from replicator.
                if let Some(tx) = replicator.journal_sender(&db_name).await {
                    inner.set_journal_sender(Some(tx));
                }

                // Start snapshot loop if configured.
                if let Some(ref ctx) = snapshot_ctx {
                    let (tx, rx) = tokio::sync::watch::channel(false);
                    let handle = tokio::spawn(run_snapshot_loop(
                        ctx.config.clone(),
                        inner.clone(),
                        replicator.clone(),
                        ctx.s3_client.clone(),
                        ctx.bucket.clone(),
                        ctx.prefix.clone(),
                        name.clone(),
                        ctx.db_path.clone(),
                        rx,
                    ));
                    snapshot_shutdown = Some(tx);
                    snapshot_handle = Some(handle);
                }
            }
            Ok(RoleEvent::Demoted { db_name: name }) => {
                tracing::error!("HaKuzu: demoted from leader for '{}'", name);
                inner.set_role(Role::Follower);
                inner.set_journal_sender(None);
                stop_snapshot_loop(&mut snapshot_shutdown, &mut snapshot_handle);
            }
            Ok(RoleEvent::Fenced { db_name: name }) => {
                tracing::error!("HaKuzu: fenced for '{}' — stopping writes", name);
                inner.set_role(Role::Follower);
                inner.set_journal_sender(None);
                stop_snapshot_loop(&mut snapshot_shutdown, &mut snapshot_handle);
            }
            Ok(RoleEvent::Sleeping { db_name: name }) => {
                tracing::info!("HaKuzu: sleeping signal for '{}'", name);
                inner.set_journal_sender(None);
                stop_snapshot_loop(&mut snapshot_shutdown, &mut snapshot_handle);
            }
            Ok(RoleEvent::Joined { .. }) => {}
            Err(_) => {
                tracing::error!("HaKuzu: role event channel closed");
                stop_snapshot_loop(&mut snapshot_shutdown, &mut snapshot_handle);
                break;
            }
        }
    }
}

fn stop_snapshot_loop(
    shutdown: &mut Option<tokio::sync::watch::Sender<bool>>,
    handle: &mut Option<tokio::task::JoinHandle<()>>,
) {
    if let Some(tx) = shutdown.take() {
        let _ = tx.send(true);
    }
    if let Some(h) = handle.take() {
        h.abort();
    }
}

// ============================================================================
// Snapshot loop (leader only)
// ============================================================================

async fn run_snapshot_loop(
    config: SnapshotConfig,
    inner: Arc<HaKuzuInner>,
    replicator: Arc<KuzuReplicator>,
    s3_client: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,
    db_name: String,
    db_path: PathBuf,
    mut cancel_rx: tokio::sync::watch::Receiver<bool>,
) {
    let mut last_snapshot_seq: u64 = 0;
    let mut interval = tokio::time::interval(config.interval);

    // Skip the first tick (fires immediately).
    interval.tick().await;

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let state = match replicator.journal_state(&db_name).await {
                    Some(s) => s,
                    None => continue,
                };
                let current_seq = state.sequence.load(Ordering::SeqCst);

                if current_seq.saturating_sub(last_snapshot_seq) < config.every_n_entries {
                    continue;
                }

                // 1. Drain writes + seal journal for a consistent snapshot.
                let _write_guard = inner.write_mutex.lock().await;

                {
                    use hadb::Replicator;
                    if let Err(e) = replicator.sync(&db_name).await {
                        tracing::error!("Snapshot seal failed: {e}");
                        continue;
                    }
                }

                // Re-read state after seal (seq may have advanced).
                let snap_seq = state.sequence.load(Ordering::SeqCst);
                let snap_hash = hex::encode(&*state.chain_hash.lock().unwrap());

                // 2. Checkpoint + create snapshot under snapshot_lock.
                let snap_dir = db_path.parent().unwrap_or(&db_path).join("snapshots_tmp");
                let snap_path = snap_dir.join("snapshot.tar.zst");

                let db = inner.db.clone();
                let lock = inner.snapshot_lock.clone();
                let db_path_clone = db_path.clone();
                let snap_path_clone = snap_path.clone();

                let snap_result = tokio::task::spawn_blocking(move || {
                    let _snap_guard = lock.write().unwrap_or_else(|e| e.into_inner());
                    let conn = lbug::Connection::new(&db)
                        .map_err(|e| anyhow!("Snapshot connection: {e}"))?;
                    conn.query("CHECKPOINT")
                        .map_err(|e| anyhow!("Snapshot CHECKPOINT: {e}"))?;
                    snapshot::create_snapshot(&db_path_clone, &snap_path_clone)
                })
                .await;

                // Release write_mutex — upload can happen while writes resume.
                drop(_write_guard);

                let snap_size = match snap_result {
                    Ok(Ok(size)) => size,
                    Ok(Err(e)) => {
                        tracing::error!("Snapshot creation failed: {e}");
                        continue;
                    }
                    Err(e) => {
                        tracing::error!("Snapshot task panicked: {e}");
                        continue;
                    }
                };

                // 3. Upload to S3.
                let meta = snapshot::SnapshotMeta {
                    journal_seq: snap_seq,
                    chain_hash: snap_hash,
                    timestamp_ms: graphstream::current_timestamp_ms(),
                    db_size_bytes: snap_size,
                };

                if let Err(e) = snapshot::upload_snapshot(
                    &s3_client, &bucket, &prefix, &db_name, &snap_path, &meta,
                )
                .await
                {
                    tracing::error!("Snapshot upload failed: {e}");
                    continue;
                }

                last_snapshot_seq = snap_seq;
                tracing::info!(
                    "Snapshot complete: seq={}, size={}B",
                    snap_seq, snap_size
                );

                // Clean up local snapshot file.
                let _ = std::fs::remove_dir_all(&snap_dir);
            }
            _ = cancel_rx.changed() => {
                tracing::info!("Snapshot loop cancelled for '{}'", db_name);
                return;
            }
        }
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Open a Kuzu database and apply schema statements.
fn open_database(db_path: &Path, schema: &str) -> Result<lbug::Database> {
    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let db = lbug::Database::new(db_path, lbug::SystemConfig::default())
        .map_err(|e| anyhow!("Failed to open database: {e}"))?;

    // Apply schema — split on ';' and execute each statement.
    {
        let conn = lbug::Connection::new(&db)
            .map_err(|e| anyhow!("Failed to create connection for schema: {e}"))?;
        for stmt in schema.split(';') {
            let stmt = stmt.trim();
            if stmt.is_empty() {
                continue;
            }
            conn.query(stmt)
                .map_err(|e| anyhow!("Schema statement failed: {e}"))?;
        }
    } // conn dropped here, releasing borrow on db

    Ok(db)
}

/// Auto-detect this node's network address for the forwarding server.
fn detect_address(instance_id: &str, port: u16) -> String {
    if let Ok(app_name) = std::env::var("FLY_APP_NAME") {
        return format!(
            "http://{}.vm.{}.internal:{}",
            instance_id, app_name, port
        );
    }
    let hostname = hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .unwrap_or_else(|| "localhost".to_string());
    format!("http://{}:{}", hostname, port)
}
