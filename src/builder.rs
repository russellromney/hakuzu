//! HaKuzuBuilder: fluent builder for HaKuzu instances.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use hadb::{Coordinator, CoordinatorConfig, LeaseConfig};
use hadb_lease_s3::S3LeaseStore;

use crate::database::{HaKuzu, SnapshotConfig, open_database, open_with_coordinator};
use crate::follower_behavior::KuzuFollowerBehavior;
use crate::mode::{self, Durability, HaMode};
use crate::replicator::KuzuReplicator;
use crate::snapshot;
use crate::snapshot_loop::SnapshotContext;
use crate::turbograph_follower_behavior::TurbographFollowerBehavior;
use crate::turbograph_replicator::TurbographReplicator;

const DEFAULT_PREFIX: &str = "hakuzu/";
const DEFAULT_FORWARDING_PORT: u16 = 18080;
const DEFAULT_FORWARD_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_SNAPSHOT_EVERY_N_ENTRIES: u64 = 10_000;
pub(crate) const DEFAULT_READ_CONCURRENCY: usize = 8;

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
    read_concurrency: usize,
    custom_lease_store: Option<Arc<dyn hadb::LeaseStore>>,
    external_db: Option<Arc<lbug::Database>>,
    external_locks: Option<(Arc<tokio::sync::Mutex<()>>, Arc<std::sync::RwLock<()>>)>,
    ha_mode: HaMode,
    durability: Durability,
    manifest_store: Option<Arc<dyn hadb::ManifestStore>>,
}

impl HaKuzuBuilder {
    pub(crate) fn new(bucket: &str) -> Self {
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
            read_concurrency: DEFAULT_READ_CONCURRENCY,
            custom_lease_store: None,
            external_db: None,
            external_locks: None,
            ha_mode: HaMode::default(),
            durability: Durability::default(),
            manifest_store: None,
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

    /// Maximum number of concurrent read operations. Default: 8.
    /// Benchmarking shows 8 permits is optimal for most workloads.
    pub fn read_concurrency(mut self, n: usize) -> Self {
        self.read_concurrency = n;
        self
    }

    /// Use a custom LeaseStore instead of the default S3LeaseStore.
    ///
    /// Works with any `LeaseStore` implementation: NATS, Redis, in-memory, etc.
    /// When set, the builder skips S3LeaseStore construction in `open()`.
    pub fn lease_store(mut self, store: Arc<dyn hadb::LeaseStore>) -> Self {
        self.custom_lease_store = Some(store);
        self
    }

    /// Use an externally-created lbug::Database instead of opening one internally.
    ///
    /// When set, the builder skips database creation and snapshot bootstrap in
    /// `open()`. The caller owns the database lifecycle; `HaKuzu::close()` will
    /// drain the journal and leave the cluster, but will not close the database.
    ///
    /// This is how cinch-cloud passes in a graphd-engine Engine's underlying
    /// database with sandbox config, FTS extensions, and custom memory limits
    /// already applied.
    pub fn database(mut self, db: Arc<lbug::Database>) -> Self {
        self.external_db = Some(db);
        self
    }

    /// Provide pre-created write mutex and snapshot lock for the external database.
    ///
    /// When using `.database()` with an external engine that already has its own
    /// concurrency primitives (write mutex, snapshot lock), pass them here so
    /// hakuzu shares the same locks instead of creating duplicates.
    pub fn locks(
        mut self,
        write_mutex: Arc<tokio::sync::Mutex<()>>,
        snapshot_lock: Arc<std::sync::RwLock<()>>,
    ) -> Self {
        self.external_locks = Some((write_mutex, snapshot_lock));
        self
    }

    /// Set the HA topology mode. Default: Dedicated.
    pub fn mode(mut self, mode: HaMode) -> Self {
        self.ha_mode = mode;
        self
    }

    /// Set the durability mode. Default: Replicated.
    pub fn durability(mut self, durability: Durability) -> Self {
        self.durability = durability;
        self
    }

    /// Set the ManifestStore for HA manifest coordination.
    ///
    /// Required for Durability::Synchronous follower catch-up and for
    /// TurbographReplicator pull operations.
    pub fn manifest_store(mut self, store: Arc<dyn hadb::ManifestStore>) -> Self {
        self.manifest_store = Some(store);
        self
    }

    /// Open the database and join the HA cluster.
    pub async fn open(self, db_path: &str, schema: &str) -> Result<HaKuzu> {
        // Validate mode + durability combination (hadb-level rules).
        mode::validate_mode_durability(self.ha_mode, self.durability)
            .map_err(|e| anyhow!("{e}"))?;

        // Hakuzu-specific: Shared mode not yet implemented.
        if matches!(self.ha_mode, HaMode::Shared) {
            return Err(anyhow!("Shared mode not yet implemented in hakuzu"));
        }

        if matches!(self.durability, Durability::Eventual) {
            return Err(anyhow!(
                "Durability::Eventual is not supported by hakuzu \
                 (graph databases use Replicated or Synchronous)"
            ));
        }

        // External database requires external locks to prevent data races.
        // Without shared locks, hakuzu creates independent locks, meaning two lock
        // systems protect the same database (the caller's and hakuzu's).
        if self.external_db.is_some() && self.external_locks.is_none() {
            return Err(anyhow!(
                "external database requires external locks via .locks() to prevent data races"
            ));
        }

        // Synchronous durability requires a ManifestStore for follower catch-up.
        if matches!(self.durability, Durability::Synchronous) && self.manifest_store.is_none() {
            return Err(anyhow!(
                "Durability::Synchronous requires a ManifestStore via .manifest_store()"
            ));
        }

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

        // Always build S3 client. Even when lease store and database are both
        // external, the KuzuReplicator (Replicated mode) needs it for journal uploads.
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

        // Snapshot bootstrap: skip when using an external database (caller
        // is responsible for the database state).
        if self.external_db.is_none() {
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
                        let hash_arr: [u8; 32] = hash_bytes.try_into().map_err(|v: Vec<u8>| {
                            anyhow!("chain_hash in snapshot must be 32 bytes, got {}", v.len())
                        })?;
                        graphstream::write_recovery_state(
                            &journal_dir,
                            meta.journal_seq,
                            hash_arr,
                        )?;

                        tracing::info!(
                            "Bootstrapped from snapshot: seq={}, size={}B",
                            meta.journal_seq,
                            meta.db_size_bytes,
                        );

                        if let Err(e) = std::fs::remove_dir_all(&snap_dest) {
                            tracing::error!("Failed to clean snapshot staging dir: {e}");
                        }
                    }
                    Ok(None) => {
                        tracing::debug!("No snapshots available, starting fresh");
                    }
                    Err(e) => {
                        tracing::error!("Failed to download snapshot, starting fresh: {e}");
                    }
                }
            }
        }

        // Resolve lease store: explicit custom store, or build S3LeaseStore.
        let lease_store: Arc<dyn hadb::LeaseStore> = match self.custom_lease_store {
            Some(store) => store,
            None => Arc::new(S3LeaseStore::new(s3_client.clone(), self.bucket.clone())),
        };

        // Build shared ObjectStore for replicator + follower behavior.
        let object_store: Arc<dyn hadb_io::ObjectStore> = Arc::new(
            hadb_io::S3Backend::new(s3_client.clone(), self.bucket.clone()),
        );

        // Open database: use external database if provided, otherwise open locally.
        // NOTE: database must be opened before TurbographReplicator (needs Arc<lbug::Database>).
        let db = match self.external_db {
            Some(ext_db) => ext_db,
            None => Arc::new(open_database(&db_path, schema)?),
        };

        // Build replicator based on durability mode.
        let (replicator, kuzu_replicator): (Arc<dyn hadb::Replicator>, Option<Arc<KuzuReplicator>>) =
            match self.durability {
                Durability::Replicated => {
                    let mut kr = KuzuReplicator::new(object_store.clone(), self.prefix.clone());
                    if let Some(interval) = self.upload_interval {
                        kr = kr.with_upload_interval(interval);
                    }
                    let kr = Arc::new(kr);
                    (kr.clone(), Some(kr))
                }
                Durability::Synchronous => {
                    let mut tr = TurbographReplicator::new(db.clone());
                    if let Some(store) = self.manifest_store.clone() {
                        tr = tr.with_manifest_store(store);
                    }
                    (Arc::new(tr), None)
                }
                Durability::Eventual => unreachable!("rejected above"),
            };

        // Create or use pre-provided locks.
        let (write_mutex, snapshot_lock) = match self.external_locks {
            Some(locks) => locks,
            None => (
                Arc::new(tokio::sync::Mutex::new(())),
                Arc::new(std::sync::RwLock::new(())),
            ),
        };

        // Build follower behavior based on durability mode.
        //
        // Replicated: replay graphstream journal entries (existing behavior).
        // Synchronous: fetch turbograph manifest from ManifestStore and apply via UDF.
        //   A Notify is shared with the role listener so ManifestChanged events
        //   immediately wake the follower loop without waiting for the poll interval.
        let (follower_behavior, manifest_wakeup): (Arc<dyn hadb::FollowerBehavior>, _) =
            match self.durability {
                Durability::Replicated => {
                    let fb = Arc::new(
                        KuzuFollowerBehavior::new(object_store.clone())
                            .with_shared_db(db.clone())
                            .with_locks(write_mutex.clone(), snapshot_lock.clone()),
                    );
                    (fb, None)
                }
                Durability::Synchronous => {
                    // manifest_store is validated above; safe to unwrap.
                    let store = self.manifest_store.clone().expect("manifest_store required");
                    let notify = Arc::new(tokio::sync::Notify::new());
                    let fb = Arc::new(
                        TurbographFollowerBehavior::new(store, db.clone())
                            .with_locks(write_mutex.clone(), snapshot_lock.clone())
                            .with_wakeup(notify.clone()),
                    );
                    (fb, Some(notify))
                }
                Durability::Eventual => unreachable!("rejected above"),
            };

        // Snapshot loop only makes sense for Replicated durability.
        // In Synchronous mode, every write is already durable in S3 via turbograph_sync.
        let snapshot_ctx = match self.durability {
            Durability::Replicated => {
                self.snapshot_interval.map(|interval| SnapshotContext {
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
                })
            }
            Durability::Synchronous => None,
            Durability::Eventual => unreachable!("rejected above"),
        };

        // Build coordinator.
        let mut config = self.coordinator_config.unwrap_or_default();
        config.lease = Some(LeaseConfig::new(instance_id.clone(), address.clone()));

        let coordinator = Coordinator::new(
            replicator.clone(),
            Some(lease_store),
            None,
            None, // node_registry
            follower_behavior,
            &self.prefix,
            config,
        );

        open_with_coordinator(
            coordinator,
            replicator,
            kuzu_replicator,
            self.durability,
            db,
            db_path,
            &db_name,
            &address,
            self.forwarding_port,
            self.forward_timeout,
            self.secret,
            snapshot_ctx,
            Some((write_mutex, snapshot_lock)),
            self.read_concurrency,
            manifest_wakeup,
        )
        .await
    }
}

/// Auto-detect this node's network address for the forwarding server.
pub(crate) fn detect_address(instance_id: &str, port: u16) -> String {
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
