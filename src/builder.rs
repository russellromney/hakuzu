//! HaKuzuBuilder: fluent builder for HaKuzu instances.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use hadb::{Coordinator, CoordinatorConfig, LeaseConfig};
use hadb_storage::StorageBackend;

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
///
/// The caller is responsible for supplying both trust-rooted dependencies
/// via [`Self::lease_store`] and [`Self::storage`] — hakuzu will not pick
/// an S3 bucket / endpoint for you. See the setter doc comments for
/// guidance on which backend fits which deployment.
pub struct HaKuzuBuilder {
    prefix: String,
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
    custom_storage: Option<Arc<dyn StorageBackend>>,
    external_db: Option<Arc<lbug::Database>>,
    external_locks: Option<(Arc<tokio::sync::Mutex<()>>, Arc<std::sync::RwLock<()>>)>,
    ha_mode: HaMode,
    durability: Durability,
    manifest_store: Option<Arc<dyn hadb::ManifestStore>>,
    // Lease timing knobs. `None` means "use the LeaseConfig default, or
    // whatever the caller put in `coordinator_config`."
    lease_ttl: Option<u64>,
    lease_renew_interval: Option<Duration>,
    lease_follower_poll_interval: Option<Duration>,
}

impl HaKuzuBuilder {
    pub(crate) fn new() -> Self {
        Self {
            prefix: DEFAULT_PREFIX.to_string(),
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
            custom_storage: None,
            external_db: None,
            external_locks: None,
            ha_mode: HaMode::default(),
            durability: Durability::default(),
            manifest_store: None,
            lease_ttl: None,
            lease_renew_interval: None,
            lease_follower_poll_interval: None,
        }
    }

    /// Lease TTL in seconds. Overrides whatever `coordinator_config.lease`
    /// carried. Default: `LeaseConfig::new`'s default (5s).
    pub fn lease_ttl(mut self, ttl_secs: u64) -> Self {
        self.lease_ttl = Some(ttl_secs);
        self
    }

    /// How often the Leader renews its lease. Default: 2s.
    pub fn lease_renew_interval(mut self, interval: Duration) -> Self {
        self.lease_renew_interval = Some(interval);
        self
    }

    /// How often a Follower polls the lease looking for Leader death.
    /// Default: 1s.
    pub fn lease_follower_poll_interval(mut self, interval: Duration) -> Self {
        self.lease_follower_poll_interval = Some(interval);
        self
    }

    /// Configure the [`StorageBackend`] used for journal segment and
    /// snapshot I/O.
    ///
    /// Required. `HaKuzuBuilder::open()` returns an error when this is not
    /// set — hakuzu won't silently build an `S3Storage` for you. Pick the
    /// backend that fits your deployment:
    ///
    /// - `hadb_storage_s3::S3Storage` — AWS, Tigris, MinIO, RustFS, R2,
    ///   Wasabi, or any other S3-compatible service. Caller supplies
    ///   bucket, credentials, and optional endpoint via an
    ///   `aws_sdk_s3::Client`.
    /// - `hadb_storage_cinch::CinchHttpStorage` — cinch-hosted deployments
    ///   (fence-token-gated writes).
    /// - Any other `StorageBackend` impl — in-process mocks for tests, etc.
    ///
    /// Tests pointing at a local RustFS pass standard AWS env vars
    /// (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL`)
    /// when they build the `S3Storage` — hakuzu doesn't need to know.
    pub fn storage(mut self, storage: Arc<dyn StorageBackend>) -> Self {
        self.custom_storage = Some(storage);
        self
    }

    /// Storage key prefix for all hakuzu data. Default: "hakuzu/".
    pub fn prefix(mut self, prefix: &str) -> Self {
        self.prefix = prefix.to_string();
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

    /// Configure the [`hadb::LeaseStore`] used for leader election.
    ///
    /// Required. `HaKuzuBuilder::open()` returns an error when this is not
    /// set — leader election semantics depend on the CAS atomicity of the
    /// backend, which hakuzu won't pick for you. Supported choices:
    ///
    /// - `hadb_lease_cinch::CinchLeaseStore` — cinch-hosted deployments
    ///   (NATS KV behind the grabby API).
    /// - `hadb_lease_nats::NatsKvLeaseStore` — self-hosted against Tigris,
    ///   MinIO, RustFS, or any other object store without atomic CAS.
    /// - `hadb_lease_s3::S3LeaseStore` — AWS S3 only. **Not safe on Tigris**
    ///   (atomic conditional PUTs are not enforced for concurrent writes).
    /// - `hadb::InMemoryLeaseStore` — in-process tests only.
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

        // No silent fallbacks for the two trust-rooted dependencies. Lease
        // store CAS atomicity controls leader election safety; storage
        // backend choice controls where bytes land. hakuzu won't pick
        // either for you. See `.lease_store()` / `.storage()` doc comments
        // for the supported choices.
        if self.custom_lease_store.is_none() {
            return Err(anyhow!(
                "HaKuzuBuilder requires an explicit lease store via .lease_store(...). \
                 Leader election semantics depend on the CAS atomicity of the backend, \
                 which hakuzu cannot pick for you. Supported choices: \
                 hadb_lease_cinch::CinchLeaseStore (cinch-hosted), \
                 hadb_lease_nats::NatsKvLeaseStore (self-hosted Tigris/MinIO/RustFS), \
                 hadb_lease_s3::S3LeaseStore (AWS S3 only — NOT safe on Tigris), \
                 or hadb::InMemoryLeaseStore (in-process tests only)."
            ));
        }
        if self.custom_storage.is_none() {
            return Err(anyhow!(
                "HaKuzuBuilder requires an explicit storage backend via .storage(...). \
                 hakuzu won't silently build an S3Storage from `bucket`/`endpoint`. \
                 Supported choices: hadb_storage_s3::S3Storage (AWS / RustFS / MinIO / \
                 Tigris — caller picks bucket + creds), hadb_storage_cinch::CinchHttpStorage \
                 (cinch-hosted, fenced), or any other StorageBackend impl."
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

        // Storage is caller-provided (validated above — the `None` arm is
        // unreachable here).
        let storage: Arc<dyn StorageBackend> = self
            .custom_storage
            .expect("storage validated above");

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
                    &*storage,
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

        // Lease store is caller-provided (validated above).
        let lease_store: Arc<dyn hadb::LeaseStore> = self
            .custom_lease_store
            .expect("lease_store validated above");

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
                    let mut kr = KuzuReplicator::new(storage.clone(), self.prefix.clone());
                    if let Some(interval) = self.upload_interval {
                        kr = kr.with_upload_interval(interval);
                    }
                    let kr = Arc::new(kr);
                    (kr.clone(), Some(kr))
                }
                Durability::Synchronous => {
                    // NOTE: Synchronous requires the turbograph extension loaded in lbug.
                    // Not yet supported via crates.io lbug (Phase GraphMeridian-c).
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
                        KuzuFollowerBehavior::new(storage.clone())
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
                    storage: storage.clone(),
                    prefix: self.prefix.clone(),
                    db_path: db_path.clone(),
                })
            }
            Durability::Synchronous => None,
            Durability::Eventual => unreachable!("rejected above"),
        };

        // Build coordinator. The lease store lives inside `LeaseConfig`
        // (hadb Phase Fjord) — store + policy travel together. If the caller
        // passed a `coordinator_config` with a pre-built `lease`, preserve
        // its timing policy but patch in the real wiring (store, instance_id,
        // address). Explicit builder timing setters override both defaults
        // and caller-supplied timing. Mirrors haqlite `src/database.rs`.
        let mut config = self.coordinator_config.unwrap_or_default();
        let mut lease_cfg = match config.lease.take() {
            Some(mut existing) => {
                existing.store = lease_store.clone();
                existing.instance_id = instance_id.clone();
                existing.address = address.clone();
                existing
            }
            None => LeaseConfig::new(
                lease_store.clone(),
                instance_id.clone(),
                address.clone(),
            ),
        };
        if let Some(ttl) = self.lease_ttl {
            lease_cfg.ttl_secs = ttl;
        }
        if let Some(d) = self.lease_renew_interval {
            lease_cfg.renew_interval = d;
        }
        if let Some(d) = self.lease_follower_poll_interval {
            lease_cfg.follower_poll_interval = d;
        }
        config.lease = Some(lease_cfg);

        let coordinator = Coordinator::new(
            replicator.clone(),
            None, // manifest_store: TurbographFollowerBehavior owns its own manifest polling
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
