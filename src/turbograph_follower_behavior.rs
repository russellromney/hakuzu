//! TurbographFollowerBehavior: follower catch-up for Synchronous durability.
//!
//! Instead of replaying a graphstream journal, Synchronous followers catch up
//! by fetching the latest turbograph manifest from the ManifestStore and
//! applying it via the `turbograph_set_manifest()` UDF.
//!
//! The manifest IS the durable state in Synchronous mode: each manifest version
//! represents a complete checkpoint already uploaded to S3. Followers never need
//! to replay journal entries, they just apply the latest manifest and open.
//!
//! **Requires the turbograph extension to be loaded in the lbug database.**
//! See turbograph_replicator.rs for details on extension loading status.
//!
//! # Fast-path wakeup
//!
//! The role listener can trigger a wakeup via `Arc<Notify>` when it receives a
//! `ManifestChanged` event from the coordinator. This avoids waiting the full
//! poll interval after a new manifest is published.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use tokio::sync::watch;

use hadb::{FollowerBehavior, HaMetrics, Replicator};

/// Follower behavior for Synchronous durability mode.
///
/// Polls ManifestStore for new turbograph manifest versions and applies them
/// via `turbograph_set_manifest()`. Used when `Durability::Synchronous` is set
/// in `HaKuzuBuilder`.
pub struct TurbographFollowerBehavior {
    manifest_store: Arc<dyn hadb::ManifestStore>,
    db: Arc<lbug::Database>,
    write_mutex: Option<Arc<tokio::sync::Mutex<()>>>,
    snapshot_lock: Option<Arc<std::sync::RwLock<()>>>,
    /// Wakeup notifier: triggered by the role listener on `ManifestChanged`
    /// events so the follower loop applies the new manifest without delay.
    wakeup: Option<Arc<tokio::sync::Notify>>,
}

impl TurbographFollowerBehavior {
    pub fn new(manifest_store: Arc<dyn hadb::ManifestStore>, db: Arc<lbug::Database>) -> Self {
        Self {
            manifest_store,
            db,
            write_mutex: None,
            snapshot_lock: None,
            wakeup: None,
        }
    }

    /// Set write mutex and snapshot lock for replay serialization.
    ///
    /// Must share the same locks as `HaKuzuInner` so manifest apply and writes
    /// are mutually exclusive.
    pub fn with_locks(
        mut self,
        write_mutex: Arc<tokio::sync::Mutex<()>>,
        snapshot_lock: Arc<std::sync::RwLock<()>>,
    ) -> Self {
        self.write_mutex = Some(write_mutex);
        self.snapshot_lock = Some(snapshot_lock);
        self
    }

    /// Set a wakeup notifier for fast-path catch-up on ManifestChanged events.
    pub fn with_wakeup(mut self, notify: Arc<tokio::sync::Notify>) -> Self {
        self.wakeup = Some(notify);
        self
    }

    /// Apply a pre-fetched manifest via turbograph_set_manifest().
    ///
    /// Returns the manifest version applied. Called with write_mutex already
    /// held by the caller. Avoids double-fetching: caller fetches once, passes
    /// the manifest directly.
    async fn apply_manifest(&self, db_name: &str, manifest: &hadb::HaManifest) -> Result<u64> {
        let version = manifest.version;

        // Reconstruct turbograph's internal JSON from the structured fields.
        // This is the format that Manifest::fromJSON() expects on the C++ side.
        let json = crate::turbograph_manifest_json::to_turbograph_json(&manifest.storage)?;

        let db = self.db.clone();
        let snapshot_lock = self.snapshot_lock.clone();
        let json_escaped = json.replace('\'', "''");

        tokio::task::spawn_blocking(move || {
            // Snapshot lock prevents a snapshot from racing with manifest apply.
            let _snap = snapshot_lock
                .as_ref()
                .map(|l| l.read().unwrap_or_else(|e| e.into_inner()));

            let conn = lbug::Connection::new(&db)
                .map_err(|e| anyhow!("Connection failed: {e}"))?;

            // turbograph_set_manifest(json) applies the remote manifest and
            // invalidates stale cache entries. Returns 0 on success.
            let query = format!("RETURN turbograph_set_manifest('{json_escaped}')");
            conn.query(&query)
                .map_err(|e| anyhow!("turbograph_set_manifest() failed: {e}"))?;

            Ok::<(), anyhow::Error>(())
        })
        .await
        .map_err(|e| anyhow!("apply_manifest task panicked: {e}"))??;

        tracing::info!(
            "TurbographFollowerBehavior '{}': applied manifest version {}",
            db_name,
            version,
        );
        Ok(version)
    }

    /// Fetch the latest manifest from the store and apply it.
    ///
    /// Returns the manifest version applied, or 0 if no manifest exists yet.
    /// Convenience method for promotion catchup and other contexts where the
    /// manifest hasn't been pre-fetched.
    async fn apply_latest_manifest(&self, db_name: &str) -> Result<u64> {
        let manifest = self.manifest_store.get(db_name).await?;
        match manifest {
            Some(m) => self.apply_manifest(db_name, &m).await,
            None => {
                tracing::debug!(
                    "TurbographFollowerBehavior '{}': no manifest in store yet",
                    db_name,
                );
                Ok(0)
            }
        }
    }
}

#[async_trait]
impl FollowerBehavior for TurbographFollowerBehavior {
    /// Continuously poll the ManifestStore for new manifest versions.
    ///
    /// - On new version: acquire write_mutex, apply via turbograph_set_manifest().
    /// - On ManifestChanged wakeup: immediately poll without waiting for interval.
    /// - On cancel: drain and return.
    async fn run_follower_loop(
        &self,
        _replicator: Arc<dyn Replicator>,
        _prefix: &str,
        db_name: &str,
        _db_path: &PathBuf,
        poll_interval: Duration,
        position: Arc<AtomicU64>,
        caught_up: Arc<AtomicBool>,
        mut cancel_rx: watch::Receiver<bool>,
        metrics: Arc<HaMetrics>,
    ) -> Result<()> {
        let mut interval = tokio::time::interval(poll_interval);

        loop {
            // Wait for poll interval, wakeup signal, or cancellation.
            let do_poll = if let Some(ref notify) = self.wakeup {
                tokio::select! {
                    _ = interval.tick() => true,
                    _ = notify.notified() => true,
                    _ = cancel_rx.changed() => false,
                }
            } else {
                tokio::select! {
                    _ = interval.tick() => true,
                    _ = cancel_rx.changed() => false,
                }
            };

            if !do_poll {
                let current_version = position.load(Ordering::SeqCst);
                tracing::info!(
                    "TurbographFollower '{}': cancelled at manifest v{}",
                    db_name,
                    current_version,
                );
                return Ok(());
            }

            let current_version = position.load(Ordering::SeqCst);

            // Cheap version check via meta() (HeadObject on S3, no body download).
            // Only fetch the full manifest via get() when the version actually changed.
            let meta = self.manifest_store.meta(db_name).await;
            let new_version = match &meta {
                Ok(Some(m)) if m.version > current_version => m.version,
                Ok(_) => {
                    // Already on latest version (or no manifest yet).
                    caught_up.store(true, Ordering::SeqCst);
                    metrics.follower_caught_up.store(1, Ordering::Relaxed);
                    metrics.inc(&metrics.follower_pulls_no_new_data);
                    continue;
                }
                Err(e) => {
                    tracing::error!(
                        "TurbographFollower '{}': manifest store meta error: {}",
                        db_name,
                        e,
                    );
                    metrics.inc(&metrics.follower_pulls_failed);
                    continue;
                }
            };

            // New version detected. Fetch the full manifest for apply.
            caught_up.store(false, Ordering::SeqCst);
            metrics.follower_caught_up.store(0, Ordering::Relaxed);

            let manifest = match self.manifest_store.get(db_name).await {
                Ok(Some(m)) => m,
                Ok(None) => {
                    // Race: meta() saw a version but get() returned None.
                    // Shouldn't happen in practice; retry on next poll.
                    tracing::warn!(
                        "TurbographFollower '{}': meta returned v{} but get() returned None",
                        db_name,
                        new_version,
                    );
                    continue;
                }
                Err(e) => {
                    tracing::error!(
                        "TurbographFollower '{}': manifest store get error: {}",
                        db_name,
                        e,
                    );
                    metrics.inc(&metrics.follower_pulls_failed);
                    continue;
                }
            };

            // Hold write_mutex during apply to prevent concurrent reads
            // from seeing partially-applied manifest state.
            let _write_guard = match &self.write_mutex {
                Some(wm) => Some(wm.lock().await),
                None => None,
            };

            match self.apply_manifest(db_name, &manifest).await {
                Ok(v) => {
                    position.store(v, Ordering::SeqCst);
                    metrics.follower_replay_position.store(v, Ordering::Relaxed);
                    metrics.inc(&metrics.follower_pulls_succeeded);
                    caught_up.store(true, Ordering::SeqCst);
                    metrics.follower_caught_up.store(1, Ordering::Relaxed);
                    tracing::debug!(
                        "TurbographFollower '{}': manifest v{} -> v{}",
                        db_name,
                        current_version,
                        v,
                    );
                }
                Err(e) => {
                    tracing::error!(
                        "TurbographFollower '{}': apply_manifest failed: {}",
                        db_name,
                        e,
                    );
                    metrics.inc(&metrics.follower_pulls_failed);
                }
            }
        }
    }

    /// Catch up on promotion by applying the latest manifest immediately.
    ///
    /// Called by the coordinator when this follower is promoted to leader.
    /// Non-fatal: if apply fails, the node promotes anyway and the data it
    /// has is consistent (just behind by at most one writer checkpoint).
    async fn catchup_on_promotion(
        &self,
        _prefix: &str,
        db_name: &str,
        _db_path: &PathBuf,
        _position: u64,
    ) -> Result<()> {
        let _write_guard = match &self.write_mutex {
            Some(m) => Some(m.lock().await),
            None => None,
        };

        match self.apply_latest_manifest(db_name).await {
            Ok(v) if v > 0 => {
                tracing::info!(
                    "TurbographFollower '{}': promotion catchup applied manifest v{}",
                    db_name,
                    v,
                );
            }
            Ok(_) => {
                tracing::debug!(
                    "TurbographFollower '{}': no manifest during promotion catchup",
                    db_name,
                );
            }
            Err(e) => {
                // Non-fatal: log and continue. The promoted leader's state is still
                // consistent — just one checkpoint behind the last leader.
                tracing::warn!(
                    "TurbographFollower '{}': promotion catchup failed (promoting anyway): {}",
                    db_name,
                    e,
                );
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hadb::{HaManifest, StorageManifest};

    struct NullManifestStore;

    #[async_trait]
    impl hadb::ManifestStore for NullManifestStore {
        async fn get(&self, _name: &str) -> Result<Option<HaManifest>> {
            Ok(None)
        }
        async fn put(
            &self,
            _name: &str,
            _manifest: &HaManifest,
            _expected_version: Option<u64>,
        ) -> Result<hadb::CasResult> {
            Ok(hadb::CasResult { success: true, etag: None })
        }
        async fn meta(&self, _name: &str) -> Result<Option<hadb::ManifestMeta>> {
            Ok(None)
        }
    }

    struct FixedManifestStore(HaManifest);

    #[async_trait]
    impl hadb::ManifestStore for FixedManifestStore {
        async fn get(&self, _name: &str) -> Result<Option<HaManifest>> {
            Ok(Some(self.0.clone()))
        }
        async fn put(
            &self,
            _name: &str,
            _manifest: &HaManifest,
            _expected_version: Option<u64>,
        ) -> Result<hadb::CasResult> {
            Ok(hadb::CasResult { success: true, etag: None })
        }
        async fn meta(&self, _name: &str) -> Result<Option<hadb::ManifestMeta>> {
            Ok(Some(hadb::ManifestMeta {
                version: self.0.version,
                writer_id: self.0.writer_id.clone(),
                lease_epoch: self.0.lease_epoch,
            }))
        }
    }

    fn make_db(dir: &std::path::Path) -> Arc<lbug::Database> {
        Arc::new(
            lbug::Database::new(
                dir.join("db").to_str().unwrap(),
                lbug::SystemConfig::default(),
            )
            .unwrap(),
        )
    }

    fn turbo_manifest(version: u64) -> HaManifest {
        HaManifest {
            version,
            writer_id: "test".into(),
            lease_epoch: 0,
            timestamp_ms: 0,
            storage: StorageManifest::Turbograph {
                turbograph_version: version,
                page_count: 0,
                page_size: 0,
                pages_per_group: 0,
                sub_pages_per_frame: 0,
                page_group_keys: vec![],
                frame_tables: vec![],
                subframe_overrides: vec![],
                encrypted: false,
                journal_seq: 0,
            },
        }
    }

    #[test]
    fn construction() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db = make_db(tmp.path());
        let store = Arc::new(NullManifestStore);
        let behavior = TurbographFollowerBehavior::new(store, db);
        assert!(behavior.write_mutex.is_none());
        assert!(behavior.wakeup.is_none());
    }

    #[test]
    fn with_locks_sets_fields() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db = make_db(tmp.path());
        let store = Arc::new(NullManifestStore);
        let wm = Arc::new(tokio::sync::Mutex::new(()));
        let sl = Arc::new(std::sync::RwLock::new(()));
        let behavior = TurbographFollowerBehavior::new(store, db).with_locks(wm, sl);
        assert!(behavior.write_mutex.is_some());
        assert!(behavior.snapshot_lock.is_some());
    }

    #[test]
    fn with_wakeup_sets_field() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db = make_db(tmp.path());
        let store = Arc::new(NullManifestStore);
        let notify = Arc::new(tokio::sync::Notify::new());
        let behavior = TurbographFollowerBehavior::new(store, db).with_wakeup(notify);
        assert!(behavior.wakeup.is_some());
    }

    /// No manifest in store -> apply returns 0 (not an error).
    #[tokio::test]
    async fn apply_manifest_no_manifest_is_ok() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db = make_db(tmp.path());
        let store = Arc::new(NullManifestStore);
        let behavior = TurbographFollowerBehavior::new(store, db);
        let v = behavior.apply_latest_manifest("db").await.unwrap();
        assert_eq!(v, 0);
    }

    /// When extension is not loaded, apply_manifest errors on the UDF call.
    /// The structured fields get reconstructed into turbograph JSON, but the
    /// UDF isn't available so the query fails.
    #[tokio::test]
    async fn apply_manifest_without_extension_errors() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db = make_db(tmp.path());
        let store = Arc::new(FixedManifestStore(turbo_manifest(5)));
        let behavior = TurbographFollowerBehavior::new(store, db);
        let result = behavior.apply_latest_manifest("db").await;
        assert!(result.is_err(), "should fail without turbograph extension loaded");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("turbograph_set_manifest") || err.contains("failed"),
            "error should mention the UDF: {err}"
        );
    }

    /// catchup_on_promotion with no manifest is non-fatal.
    #[tokio::test]
    async fn catchup_on_promotion_no_manifest_ok() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db = make_db(tmp.path());
        let store = Arc::new(NullManifestStore);
        let behavior = TurbographFollowerBehavior::new(store, db);
        // Should not error even though no manifest exists.
        behavior
            .catchup_on_promotion("prefix", "db", &PathBuf::from("/tmp"), 0)
            .await
            .unwrap();
    }

    /// catchup_on_promotion failure (no extension) is non-fatal.
    #[tokio::test]
    async fn catchup_on_promotion_failure_is_nonfatal() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db = make_db(tmp.path());
        let store = Arc::new(FixedManifestStore(turbo_manifest(3)));
        let behavior = TurbographFollowerBehavior::new(store, db);
        // Extension not loaded => turbograph_set_manifest fails. But catchup_on_promotion
        // should still return Ok (it warns and continues).
        behavior
            .catchup_on_promotion("prefix", "db", &PathBuf::from("/tmp"), 0)
            .await
            .unwrap();
    }

    /// Wakeup channel is notifiable without error.
    #[tokio::test]
    async fn wakeup_notify_works() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db = make_db(tmp.path());
        let store = Arc::new(NullManifestStore);
        let notify = Arc::new(tokio::sync::Notify::new());
        let notify2 = notify.clone();
        let _behavior = TurbographFollowerBehavior::new(store, db).with_wakeup(notify);
        // Triggering the notify should not panic.
        notify2.notify_one();
    }

    // ========================================================================
    // Follower loop tests
    // ========================================================================

    /// Follower loop exits cleanly on cancellation.
    #[tokio::test]
    async fn follower_loop_cancellation() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db = make_db(tmp.path());
        let store: Arc<dyn hadb::ManifestStore> = Arc::new(NullManifestStore);
        let behavior = Arc::new(TurbographFollowerBehavior::new(store.clone(), db));

        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        let position = Arc::new(AtomicU64::new(0));
        let caught_up = Arc::new(AtomicBool::new(false));
        let metrics = Arc::new(hadb::HaMetrics::default());

        // Dummy replicator (never used by TurbographFollowerBehavior).
        let dummy_repl: Arc<dyn hadb::Replicator> = Arc::new(NullReplicator);

        let behavior2 = behavior.clone();
        let position2 = position.clone();
        let caught_up2 = caught_up.clone();
        let metrics2 = metrics.clone();
        let handle = tokio::spawn(async move {
            behavior2
                .run_follower_loop(
                    dummy_repl,
                    "prefix",
                    "db",
                    &PathBuf::from("/tmp"),
                    Duration::from_millis(50),
                    position2,
                    caught_up2,
                    cancel_rx,
                    metrics2,
                )
                .await
        });

        // Let the loop run for a bit, then cancel.
        tokio::time::sleep(Duration::from_millis(120)).await;
        let _ = cancel_tx.send(true);

        let result = handle.await.expect("task should not panic");
        assert!(result.is_ok(), "follower loop should exit cleanly: {:?}", result);
        // With NullManifestStore (no manifest), should be caught up.
        assert!(caught_up.load(Ordering::SeqCst));
    }

    /// Manifest store returning errors doesn't crash the follower loop.
    #[tokio::test]
    async fn follower_loop_handles_store_errors() {
        struct ErrorManifestStore;

        #[async_trait]
        impl hadb::ManifestStore for ErrorManifestStore {
            async fn get(&self, _name: &str) -> Result<Option<HaManifest>> {
                Err(anyhow!("simulated store error"))
            }
            async fn put(
                &self,
                _name: &str,
                _manifest: &HaManifest,
                _expected: Option<u64>,
            ) -> Result<hadb::CasResult> {
                Err(anyhow!("simulated store error"))
            }
            async fn meta(&self, _name: &str) -> Result<Option<hadb::ManifestMeta>> {
                Err(anyhow!("simulated store error"))
            }
        }

        let tmp = tempfile::TempDir::new().unwrap();
        let db = make_db(tmp.path());
        let store: Arc<dyn hadb::ManifestStore> = Arc::new(ErrorManifestStore);
        let behavior = Arc::new(TurbographFollowerBehavior::new(store, db));

        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        let position = Arc::new(AtomicU64::new(0));
        let caught_up = Arc::new(AtomicBool::new(false));
        let metrics = Arc::new(hadb::HaMetrics::default());
        let dummy_repl: Arc<dyn hadb::Replicator> = Arc::new(NullReplicator);

        let behavior2 = behavior.clone();
        let handle = tokio::spawn(async move {
            behavior2
                .run_follower_loop(
                    dummy_repl,
                    "prefix",
                    "db",
                    &PathBuf::from("/tmp"),
                    Duration::from_millis(50),
                    position,
                    caught_up,
                    cancel_rx,
                    metrics,
                )
                .await
        });

        // Let a few error cycles run.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let _ = cancel_tx.send(true);

        let result = handle.await.expect("task should not panic");
        assert!(result.is_ok(), "follower loop should handle errors gracefully");
    }

    /// Follower loop with wakeup: notify wakes the loop immediately.
    #[tokio::test]
    async fn follower_loop_wakeup_triggers_immediate_poll() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db = make_db(tmp.path());
        // Empty store -- returns None, which means caught_up = true.
        let store: Arc<dyn hadb::ManifestStore> = Arc::new(NullManifestStore);
        let notify = Arc::new(tokio::sync::Notify::new());
        let behavior = Arc::new(
            TurbographFollowerBehavior::new(store, db).with_wakeup(notify.clone()),
        );

        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        let position = Arc::new(AtomicU64::new(0));
        let caught_up = Arc::new(AtomicBool::new(false));
        let metrics = Arc::new(hadb::HaMetrics::default());
        let dummy_repl: Arc<dyn hadb::Replicator> = Arc::new(NullReplicator);

        let caught_up2 = caught_up.clone();
        let metrics2 = metrics.clone();
        let handle = tokio::spawn(async move {
            behavior
                .run_follower_loop(
                    dummy_repl,
                    "prefix",
                    "db",
                    &PathBuf::from("/tmp"),
                    Duration::from_secs(60), // very long poll interval
                    position,
                    caught_up2,
                    cancel_rx,
                    metrics2,
                )
                .await
        });

        // With 60s poll interval, the loop won't fire for a long time.
        // But notify should wake it immediately.
        tokio::time::sleep(Duration::from_millis(50)).await;
        notify.notify_one();
        tokio::time::sleep(Duration::from_millis(50)).await;

        // After wakeup, follower should be caught up (NullManifestStore = no manifest).
        assert!(
            caught_up.load(Ordering::SeqCst),
            "follower should be caught up after wakeup"
        );

        let _ = cancel_tx.send(true);
        let _ = handle.await;
    }

    // ========================================================================
    // Helpers for follower loop tests
    // ========================================================================

    /// Minimal no-op replicator (the follower loop needs one but never calls it).
    struct NullReplicator;

    #[async_trait]
    impl hadb::Replicator for NullReplicator {
        async fn add(&self, _: &str, _: &std::path::Path) -> Result<()> {
            Ok(())
        }
        async fn pull(&self, _: &str, _: &std::path::Path) -> Result<()> {
            Ok(())
        }
        async fn remove(&self, _: &str) -> Result<()> {
            Ok(())
        }
        async fn add_continuing(&self, _: &str, _: &std::path::Path) -> Result<()> {
            Ok(())
        }
        async fn sync(&self, _: &str) -> Result<()> {
            Ok(())
        }
    }
}
