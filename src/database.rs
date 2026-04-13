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
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{anyhow, Result};
use axum::routing::post;
use graphstream::journal::{JournalCommand, JournalSender, PendingEntry};
use hadb::{Coordinator, JoinResult, Role, RoleEvent};

use crate::builder::HaKuzuBuilder;
use crate::error::HakuzuError;
use crate::forwarding::{self, ForwardingState};
use crate::metrics;
use crate::mode::Durability;
use crate::mutation::is_mutation;
use crate::replicator::KuzuReplicator;
use crate::rewriter;
use crate::snapshot_loop::{self, SnapshotContext};
use crate::values;

pub(crate) const ROLE_LEADER: u8 = 0;
pub(crate) const ROLE_FOLLOWER: u8 = 1;

/// Configuration for periodic snapshot creation (leader only).
#[derive(Debug, Clone)]
pub struct SnapshotConfig {
    /// How often to check if a snapshot should be taken.
    pub interval: Duration,
    /// Minimum entries since last snapshot before taking a new one.
    pub every_n_entries: u64,
}

/// Query result returned from `HaKuzu::query()`.
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
}

/// HA Kuzu database — transparent write forwarding, local reads, automatic failover.
pub struct HaKuzu {
    pub(crate) inner: Arc<HaKuzuInner>,
    _fwd_handle: tokio::task::JoinHandle<()>,
    _role_handle: tokio::task::JoinHandle<()>,
}

/// Internal state shared between HaKuzu, forwarding handler, and role listener.
pub(crate) struct HaKuzuInner {
    pub(crate) coordinator: Option<Arc<Coordinator>>,
    pub(crate) replicator: Option<Arc<dyn hadb::Replicator>>,
    pub(crate) durability: Durability,
    pub(crate) db: Arc<lbug::Database>,
    pub(crate) db_name: String,
    role: AtomicU8,
    pub(crate) write_mutex: Arc<tokio::sync::Mutex<()>>,
    read_semaphore: tokio::sync::Semaphore,
    pub(crate) snapshot_lock: Arc<std::sync::RwLock<()>>,
    journal_sender: RwLock<Option<JournalSender>>,
    leader_address: RwLock<String>,
    http_client: reqwest::Client,
    pub(crate) secret: Option<String>,
    pub(crate) metrics: Arc<metrics::HakuzuMetrics>,
    /// Follower readiness — true when last poll found no new segments and
    /// replay is complete. Leader always returns true.
    follower_caught_up: Arc<AtomicBool>,
    /// Last successfully replayed sequence number (follower only).
    follower_replay_position: Arc<AtomicU64>,
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
        self.leader_address.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    fn set_leader_addr(&self, addr: String) {
        *self.leader_address.write().unwrap_or_else(|e| e.into_inner()) = addr;
    }

    pub(crate) fn set_journal_sender(&self, sender: Option<JournalSender>) {
        *self.journal_sender.write().unwrap_or_else(|e| e.into_inner()) = sender;
    }

    pub(crate) fn get_journal_sender(&self) -> Option<JournalSender> {
        self.journal_sender.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Execute a write locally (leader path). Used by both HaKuzu::execute and forwarding handler.
    ///
    /// Rewrites non-deterministic functions (gen_random_uuid, current_timestamp, etc.)
    /// to parameter references with concrete values before execution. Journals the
    /// **rewritten** query + **merged** params so followers replay deterministically.
    pub(crate) async fn execute_write_local(
        &self,
        cypher: &str,
        params: Option<&serde_json::Value>,
    ) -> Result<()> {
        let write_start = std::time::Instant::now();
        // write_mutex serializes all writes. Also held during snapshot seal
        // (see snapshot_loop.rs) to prevent writes during checkpoint.
        // Phase GraphMeridian: turbograph_sync() also runs under this lock.
        let _guard = self.write_mutex.lock().await;

        // Rewrite non-deterministic functions before execution.
        let rewrite = rewriter::rewrite_query(cypher);
        let merged_params = rewriter::merge_params(params, &rewrite.generated_params);

        let db = self.db.clone();
        let lock = self.snapshot_lock.clone();
        let rewritten_query = rewrite.query.clone();
        let exec_params = merged_params.clone();

        tokio::task::spawn_blocking(move || {
            let conn = lbug::Connection::new(&db)
                .map_err(|e| anyhow!("Failed to create connection: {e}"))?;
            let _snap = lock.read().unwrap_or_else(|e| e.into_inner());

            if let Some(ref p) = exec_params {
                let lbug_params = values::json_params_to_lbug(p)
                    .map_err(|e| anyhow!("Param conversion failed: {e}"))?;
                let refs: Vec<(&str, lbug::Value)> =
                    lbug_params.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
                let mut stmt = conn
                    .prepare(&rewritten_query)
                    .map_err(|e| anyhow!("Prepare failed: {e}"))?;
                conn.execute(&mut stmt, refs)
                    .map_err(|e| anyhow!("Execute failed: {e}"))?;
            } else {
                conn.query(&rewritten_query)
                    .map_err(|e| anyhow!("Query failed: {e}"))?;
            }
            Ok::<(), anyhow::Error>(())
        })
        .await
        .map_err(|e| anyhow!("Task panicked: {e}"))??;

        // Make the write durable based on the durability mode.
        match self.durability {
            Durability::Replicated => {
                // Journal the rewritten query + merged params (not the original).
                // If journaling fails, the write already executed locally — this is
                // a critical consistency violation (local diverged from journal).
                if let Some(tx) = self.get_journal_sender() {
                    let gs_params = match &merged_params {
                        Some(p) => values::json_params_to_graphstream(p),
                        None => vec![],
                    };
                    let entry = PendingEntry {
                        query: rewrite.query,
                        params: gs_params,
                    };
                    if tx.send(JournalCommand::Write(entry)).is_err() {
                        return Err(anyhow!(
                            "Failed to send journal entry — writer may have crashed"
                        ));
                    }
                }
            }
            Durability::Synchronous => {
                // RPO=0: flush dirty pages to S3 via turbograph_sync() after every
                // write. The write is only considered durable once S3 confirms.
                // The write_mutex is still held, so no concurrent writes can interleave.
                if let Some(ref replicator) = self.replicator {
                    replicator
                        .sync(&self.db_name)
                        .await
                        .map_err(|e| anyhow!("turbograph_sync after write failed: {e}"))?;
                }
            }
            Durability::Eventual => unreachable!("hakuzu does not support Eventual durability"),
        }

        self.metrics.inc(&self.metrics.writes_total);
        self.metrics.record_duration(&self.metrics.last_write_duration_us, write_start);
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
            durability: Durability::Replicated,
            db,
            db_name: db_path
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("db")
                .to_string(),
            role: AtomicU8::new(ROLE_LEADER),
            write_mutex: Arc::new(tokio::sync::Mutex::new(())),
            read_semaphore: tokio::sync::Semaphore::new(crate::builder::DEFAULT_READ_CONCURRENCY),
            snapshot_lock: Arc::new(std::sync::RwLock::new(())),
            journal_sender: RwLock::new(None),
            leader_address: RwLock::new(String::new()),
            http_client: reqwest::Client::new(),
            secret: None,
            metrics: Arc::new(metrics::HakuzuMetrics::new()),
            follower_caught_up: Arc::new(AtomicBool::new(true)), // local mode = always leader = always caught up
            follower_replay_position: Arc::new(AtomicU64::new(0)),
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

        let replicator_trait: Arc<dyn hadb::Replicator> = replicator.clone();
        open_with_coordinator(
            coordinator,
            replicator_trait,
            Some(replicator),
            Durability::Replicated,
            db,
            db_path,
            db_name,
            &address,
            forwarding_port,
            forward_timeout,
            secret,
            None,   // no snapshot config via from_coordinator
            None,   // no pre-created locks
            crate::builder::DEFAULT_READ_CONCURRENCY,
            None,   // no manifest wakeup (Replicated mode)
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
    ) -> crate::error::Result<()> {
        if is_mutation(cypher) {
            let role = self.inner.current_role();
            match role {
                Some(Role::Leader) | None => {
                    self.inner
                        .execute_write_local(cypher, params.as_ref())
                        .await
                        .map_err(|e| {
                            let msg = e.to_string();
                            if msg.contains("journal") {
                                HakuzuError::JournalError(msg)
                            } else {
                                HakuzuError::DatabaseError(msg)
                            }
                        })
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
    ) -> crate::error::Result<QueryResult> {
        let read_start = std::time::Instant::now();
        let _permit = self
            .inner
            .read_semaphore
            .acquire()
            .await
            .map_err(|_| HakuzuError::EngineClosed)?;

        let db = self.inner.db.clone();
        let lock = self.inner.snapshot_lock.clone();
        let cypher = cypher.to_string();
        let params_owned = params;

        let qr = tokio::task::spawn_blocking(move || {
            let conn = lbug::Connection::new(&db)
                .map_err(|e| HakuzuError::DatabaseError(format!("Failed to create connection: {e}")))?;
            let _snap = lock.read().unwrap_or_else(|e| e.into_inner());

            let mut result = if let Some(ref p) = params_owned {
                let lbug_params = values::json_params_to_lbug(p)
                    .map_err(|e| HakuzuError::DatabaseError(format!("Param conversion failed: {e}")))?;
                let refs: Vec<(&str, lbug::Value)> =
                    lbug_params.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
                let mut stmt = conn
                    .prepare(&cypher)
                    .map_err(|e| HakuzuError::DatabaseError(format!("Prepare failed: {e}")))?;
                conn.execute(&mut stmt, refs)
                    .map_err(|e| HakuzuError::DatabaseError(format!("Execute failed: {e}")))?
            } else {
                conn.query(&cypher)
                    .map_err(|e| HakuzuError::DatabaseError(format!("Query failed: {e}")))?
            };

            let columns = result.get_column_names();
            let mut rows = Vec::new();
            while let Some(row) = result.next() {
                let json_row: Vec<serde_json::Value> =
                    row.iter().map(values::lbug_to_json).collect();
                rows.push(json_row);
            }

            Ok::<QueryResult, HakuzuError>(QueryResult { columns, rows })
        })
        .await
        .map_err(|e| HakuzuError::DatabaseError(format!("Task panicked: {e}")))?
        ?;

        self.inner.metrics.inc(&self.inner.metrics.reads_total);
        self.inner.metrics.record_duration(&self.inner.metrics.last_read_duration_us, read_start);
        Ok(qr)
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

    /// Whether this node is ready to serve reads.
    ///
    /// - **Leader**: always returns `true`.
    /// - **Follower**: returns `true` when the last poll found no new segments
    ///   and all downloaded entries have been replayed.
    /// - **Local mode**: always returns `true`.
    ///
    /// Use this for load balancer health checks — don't route reads to a
    /// follower that hasn't finished catching up.
    pub fn is_caught_up(&self) -> bool {
        match self.inner.current_role() {
            Some(Role::Leader) | None => true,
            Some(Role::Follower) => self.inner.follower_caught_up.load(Ordering::SeqCst),
        }
    }

    /// The last successfully replayed journal sequence number (follower only).
    /// Returns 0 for leaders and local mode.
    pub fn replay_position(&self) -> u64 {
        self.inner.follower_replay_position.load(Ordering::SeqCst)
    }

    /// Get metrics in Prometheus exposition format.
    /// Concatenates hadb coordinator metrics + hakuzu operation metrics + readiness.
    pub fn prometheus_metrics(&self) -> String {
        let mut out = String::new();
        if let Some(c) = self.inner.coordinator.as_ref() {
            out.push_str(&c.metrics().snapshot().to_prometheus());
        }
        out.push_str(&self.inner.metrics.snapshot().to_prometheus());
        // Readiness metrics.
        let caught_up = if self.is_caught_up() { 1 } else { 0 };
        out.push_str(&format!(
            "# HELP hakuzu_follower_caught_up Whether this node is caught up and ready for reads\n\
             # TYPE hakuzu_follower_caught_up gauge\n\
             hakuzu_follower_caught_up {}\n",
            caught_up
        ));
        out.push_str(&format!(
            "# HELP hakuzu_replay_position Last replayed journal sequence number\n\
             # TYPE hakuzu_replay_position gauge\n\
             hakuzu_replay_position {}\n",
            self.replay_position()
        ));
        out
    }

    /// Graceful leader handoff with drain barrier.
    ///
    /// 1. Acquires write_mutex — blocks until any in-flight write completes,
    ///    prevents new writes from starting.
    /// 2. Seals the journal segment via replicator.sync() — ensures all entries
    ///    are in a sealed segment ready for upload.
    /// 3. Releases mutex, then delegates to coordinator.handoff() for lease
    ///    release and role demotion.
    pub async fn handoff(&self) -> crate::error::Result<bool> {
        let coordinator = match &self.inner.coordinator {
            Some(c) => c,
            None => return Ok(false),
        };

        // Drain: wait for in-flight writes to finish + prevent new ones.
        {
            let _guard = self.inner.write_mutex.lock().await;

            // Seal journal segment so all entries are durable before we step down.
            if let Some(ref replicator) = self.inner.replicator {
                replicator
                    .sync(&self.inner.db_name)
                    .await
                    .map_err(|e| HakuzuError::JournalError(e.to_string()))?;
            }
        } // write_mutex released — coordinator handoff may take time

        coordinator
            .handoff(&self.inner.db_name)
            .await
            .map_err(|e| HakuzuError::CoordinatorError(e.to_string()))
    }

    /// Cleanly shut down: stop accepting new ops, drain in-flight work, seal journal,
    /// leave cluster, stop background tasks.
    ///
    /// After close() returns, all in-flight reads and writes have completed.
    /// New operations started concurrently will get `EngineClosed`.
    pub async fn close(self) -> crate::error::Result<()> {
        // Close semaphore — new reads immediately get EngineClosed.
        // In-flight reads continue until they release their permits.
        self.inner.read_semaphore.close();

        if let Some(ref coordinator) = self.inner.coordinator {
            // Drain in-flight writes + seal journal before leaving.
            {
                let _guard = self.inner.write_mutex.lock().await;
                if let Some(ref replicator) = self.inner.replicator {
                    if let Err(e) = replicator.sync(&self.inner.db_name).await {
                        tracing::error!("Failed to seal journal on close: {e}");
                    }
                }
            }
            coordinator
                .leave(&self.inner.db_name)
                .await
                .map_err(|e| HakuzuError::CoordinatorError(e.to_string()))?;
        }

        // Abort the forwarding server (stops accepting forwarded writes from followers)
        // and the role listener. In-flight spawn_blocking tasks (reads/writes) hold
        // Arc<HaKuzuInner> clones and will complete independently.
        self._fwd_handle.abort();
        self._role_handle.abort();

        // Wait for background tasks to finish so their Arc<HaKuzuInner> refs are
        // released before the Database is dropped.
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
    ) -> crate::error::Result<()> {
        let start = std::time::Instant::now();
        let leader_addr = self.inner.leader_addr();
        if leader_addr.is_empty() {
            return Err(HakuzuError::NotLeader);
        }

        let url = format!("{}/hakuzu/execute", leader_addr);
        let body = forwarding::ForwardedExecute {
            cypher: cypher.to_string(),
            params: params.cloned(),
        };

        // Retry with exponential backoff: 100ms, 400ms, 1600ms.
        let backoffs = [
            Duration::from_millis(100),
            Duration::from_millis(400),
            Duration::from_millis(1600),
        ];
        let mut last_err = None;

        for (attempt, backoff) in std::iter::once(&Duration::ZERO)
            .chain(backoffs.iter())
            .enumerate()
        {
            if attempt > 0 {
                tokio::time::sleep(*backoff).await;
            }

            let mut req = self.inner.http_client.post(&url).json(&body);
            if let Some(ref secret) = self.inner.secret {
                req = req.bearer_auth(secret);
            }

            match req.send().await {
                Ok(resp) if resp.status().is_success() => {
                    self.inner.metrics.inc(&self.inner.metrics.writes_forwarded);
                    self.inner.metrics.record_duration(
                        &self.inner.metrics.last_forward_duration_us,
                        start,
                    );
                    return Ok(());
                }
                Ok(resp) => {
                    let status = resp.status();
                    let body_text = resp.text().await.unwrap_or_default();
                    // Don't retry client errors (4xx) — they won't succeed on retry.
                    if status.is_client_error() {
                        self.inner.metrics.inc(&self.inner.metrics.forwarding_errors);
                        self.inner.metrics.record_duration(
                            &self.inner.metrics.last_forward_duration_us,
                            start,
                        );
                        return Err(HakuzuError::LeaderUnavailable(format!(
                            "Leader returned error: {} {}",
                            status, body_text
                        )));
                    }
                    last_err = Some(format!("Leader returned error: {} {}", status, body_text));
                }
                Err(e) => {
                    last_err = Some(format!("Failed to forward write: {e}"));
                }
            }

            if attempt < backoffs.len() {
                tracing::warn!(
                    attempt = attempt + 1,
                    "Forwarding write failed, retrying in {:?}",
                    backoffs.get(attempt).unwrap_or(&Duration::ZERO)
                );
            }
        }

        self.inner.metrics.inc(&self.inner.metrics.forwarding_errors);
        self.inner.metrics.record_duration(
            &self.inner.metrics.last_forward_duration_us,
            start,
        );
        Err(HakuzuError::LeaderUnavailable(
            last_err.unwrap_or_else(|| "Forwarding failed after retries".into()),
        ))
    }
}

// ============================================================================
// Shared open logic
// ============================================================================

/// Open an HaKuzu instance with a pre-configured coordinator.
///
/// Shared by `HaKuzuBuilder::open()` and `HaKuzu::from_coordinator*()`.
/// Subscribes to role events, joins the cluster, starts the forwarding server
/// and role listener tasks.
///
/// `manifest_wakeup`: for Synchronous durability mode, a Notify shared with
/// `TurbographFollowerBehavior` so `ManifestChanged` events immediately wake
/// the follower loop without waiting for the poll interval.
pub(crate) async fn open_with_coordinator(
    coordinator: Arc<Coordinator>,
    replicator: Arc<dyn hadb::Replicator>,
    kuzu_replicator: Option<Arc<KuzuReplicator>>,
    durability: Durability,
    db: Arc<lbug::Database>,
    db_path: PathBuf,
    db_name: &str,
    address: &str,
    forwarding_port: u16,
    forward_timeout: Duration,
    secret: Option<String>,
    snapshot_ctx: Option<SnapshotContext>,
    locks: Option<(Arc<tokio::sync::Mutex<()>>, Arc<std::sync::RwLock<()>>)>,
    read_concurrency: usize,
    manifest_wakeup: Option<Arc<tokio::sync::Notify>>,
) -> Result<HaKuzu> {
    // Subscribe to role events BEFORE join.
    let role_rx = coordinator.role_events();

    // Join the HA cluster. JoinResult contains Arc refs to the coordinator's
    // per-database atomics for zero-overhead health checks.
    let JoinResult { role: initial_role, caught_up: follower_caught_up, position: follower_replay_position } =
        coordinator.join(db_name, &db_path).await?;

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

    // Use pre-created locks if provided (shared with follower_behavior),
    // otherwise create new ones.
    let (write_mutex, snapshot_lock) = locks.unwrap_or_else(|| {
        (
            Arc::new(tokio::sync::Mutex::new(())),
            Arc::new(std::sync::RwLock::new(())),
        )
    });

    let inner = Arc::new(HaKuzuInner {
        coordinator: Some(coordinator),
        replicator: Some(replicator),
        durability,
        db,
        db_name: db_name.to_string(),
        role: AtomicU8::new(match initial_role {
            Role::Leader => ROLE_LEADER,
            Role::Follower => ROLE_FOLLOWER,
        }),
        write_mutex,
        read_semaphore: tokio::sync::Semaphore::new(read_concurrency),
        snapshot_lock,
        journal_sender: RwLock::new(None),
        leader_address: RwLock::new(leader_addr),
        http_client,
        secret: secret.clone(),
        metrics: Arc::new(metrics::HakuzuMetrics::new()),
        follower_caught_up,
        follower_replay_position,
    });

    // If leader, grab the journal sender from the KuzuReplicator (Replicated durability only).
    if initial_role == Role::Leader {
        if let Some(ref kr) = kuzu_replicator {
            if let Some(tx) = kr.journal_sender(db_name).await {
                inner.set_journal_sender(Some(tx));
            }
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
    let role_kuzu_replicator = kuzu_replicator;
    let role_db_name = db_name.to_string();
    let role_handle = tokio::spawn(async move {
        run_role_listener(
            role_rx,
            role_inner,
            role_address,
            role_kuzu_replicator,
            role_db_name,
            snapshot_ctx,
            manifest_wakeup,
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
    kuzu_replicator: Option<Arc<KuzuReplicator>>,
    db_name: String,
    snapshot_ctx: Option<SnapshotContext>,
    // For Synchronous durability: notify the TurbographFollowerBehavior loop
    // immediately when a new manifest is available, bypassing the poll interval.
    manifest_wakeup: Option<Arc<tokio::sync::Notify>>,
) {
    let mut snapshot_shutdown: Option<tokio::sync::watch::Sender<bool>> = None;
    let mut snapshot_handle: Option<tokio::task::JoinHandle<()>> = None;

    loop {
        match role_rx.recv().await {
            Ok(RoleEvent::Promoted { db_name: name }) => {
                tracing::info!("HaKuzu: promoted to leader for '{}'", name);
                inner.follower_caught_up.store(true, Ordering::SeqCst);
                inner.set_leader_addr(self_address.clone());
                inner.set_role(Role::Leader);

                // Grab journal sender from KuzuReplicator (Replicated durability only).
                if let Some(ref kr) = kuzu_replicator {
                    if let Some(tx) = kr.journal_sender(&db_name).await {
                        inner.set_journal_sender(Some(tx));
                    }
                }

                // Start snapshot loop if configured (Replicated durability only).
                if let (Some(ref ctx), Some(ref kr)) = (&snapshot_ctx, &kuzu_replicator) {
                    let (tx, rx) = tokio::sync::watch::channel(false);
                    let handle = tokio::spawn(snapshot_loop::run_snapshot_loop(
                        ctx.config.clone(),
                        inner.clone(),
                        kr.clone(),
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
                inner.follower_caught_up.store(false, Ordering::SeqCst);
                inner.set_journal_sender(None);
                snapshot_loop::stop_snapshot_loop(&mut snapshot_shutdown, &mut snapshot_handle);
            }
            Ok(RoleEvent::Fenced { db_name: name }) => {
                tracing::error!("HaKuzu: fenced for '{}' — stopping writes", name);
                inner.set_role(Role::Follower);
                inner.follower_caught_up.store(false, Ordering::SeqCst);
                inner.set_journal_sender(None);
                snapshot_loop::stop_snapshot_loop(&mut snapshot_shutdown, &mut snapshot_handle);
            }
            Ok(RoleEvent::Sleeping { db_name: name }) => {
                tracing::info!("HaKuzu: sleeping signal for '{}'", name);
                inner.set_role(Role::Follower);
                inner.follower_caught_up.store(false, Ordering::SeqCst);
                inner.set_journal_sender(None);
                snapshot_loop::stop_snapshot_loop(&mut snapshot_shutdown, &mut snapshot_handle);
            }
            Ok(RoleEvent::Joined { .. }) => {}
            Ok(RoleEvent::ManifestChanged { db_name: name, version }) => {
                // For Synchronous durability: wake the TurbographFollowerBehavior loop
                // immediately so it applies the new manifest without polling delay.
                if let Some(ref notify) = manifest_wakeup {
                    tracing::info!(
                        "HaKuzu: manifest changed for '{}' v{}, waking follower loop",
                        name,
                        version,
                    );
                    notify.notify_one();
                } else {
                    tracing::debug!(
                        "HaKuzu: manifest changed for '{}' v{} (Replicated mode, no action)",
                        name,
                        version,
                    );
                }
            }
            Err(_) => {
                tracing::error!("HaKuzu: role event channel closed");
                snapshot_loop::stop_snapshot_loop(&mut snapshot_shutdown, &mut snapshot_handle);
                break;
            }
        }
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Open a Kuzu database and apply schema statements.
pub(crate) fn open_database(db_path: &Path, schema: &str) -> Result<lbug::Database> {
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
