//! Kuzu-specific follower behavior implementation.
//!
//! Polls S3 for new journal segments, downloads them, reads entries via
//! graphstream JournalReader, and replays against local Kuzu via replay_entries.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use lbug;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::watch;

use hadb::{FollowerBehavior, HaMetrics, Replicator};

use crate::replay;

/// Kuzu-specific follower behavior.
///
/// Tracks journal sequence numbers for incremental journal replication
/// using graphstream. Holds write_mutex + snapshot_lock during replay
/// to prevent concurrent reads from seeing partial replay state.
pub struct KuzuFollowerBehavior {
    s3_client: aws_sdk_s3::Client,
    bucket: String,
    shared_db: Option<Arc<lbug::Database>>,
    /// Write mutex — held during replay to serialize with leader writes.
    write_mutex: Option<Arc<tokio::sync::Mutex<()>>>,
    /// Snapshot lock — held (read) during replay to prevent snapshots mid-replay.
    snapshot_lock: Option<Arc<std::sync::RwLock<()>>>,
    /// Shared with HaKuzuInner — true when last poll found no new segments.
    caught_up: Arc<AtomicBool>,
    /// Shared with HaKuzuInner — last successfully replayed sequence number.
    replay_position: Arc<AtomicU64>,
}

impl KuzuFollowerBehavior {
    pub fn new(s3_client: aws_sdk_s3::Client, bucket: String) -> Self {
        Self {
            s3_client,
            bucket,
            shared_db: None,
            write_mutex: None,
            snapshot_lock: None,
            caught_up: Arc::new(AtomicBool::new(false)),
            replay_position: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Get shared references for threading into HaKuzuInner.
    pub fn readiness_state(&self) -> (Arc<AtomicBool>, Arc<AtomicU64>) {
        (self.caught_up.clone(), self.replay_position.clone())
    }

    /// Use a shared Database for replay instead of creating a new one per replay.
    ///
    /// Required when the experiment binary also serves HTTP reads from the same DB.
    /// Without this, two Database instances on the same file would conflict.
    pub fn with_shared_db(mut self, db: Arc<lbug::Database>) -> Self {
        self.shared_db = Some(db);
        self
    }

    /// Set the write mutex and snapshot lock for replay serialization.
    pub fn with_locks(
        mut self,
        write_mutex: Arc<tokio::sync::Mutex<()>>,
        snapshot_lock: Arc<std::sync::RwLock<()>>,
    ) -> Self {
        self.write_mutex = Some(write_mutex);
        self.snapshot_lock = Some(snapshot_lock);
        self
    }
}

#[async_trait]
impl FollowerBehavior for KuzuFollowerBehavior {
    async fn run_follower_loop(
        &self,
        _replicator: Arc<dyn Replicator>,
        prefix: &str,
        db_name: &str,
        db_path: &PathBuf,
        poll_interval: Duration,
        position: Arc<AtomicU64>,
        mut cancel_rx: watch::Receiver<bool>,
        metrics: Arc<HaMetrics>,
    ) -> Result<()> {
        let mut interval = tokio::time::interval(poll_interval);
        let journal_dir = db_path.parent().unwrap_or(db_path.as_path()).join("journal");
        let db_prefix = format!("{}{}/", prefix, db_name);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let current_seq = position.load(Ordering::SeqCst);

                    // 1. Download new segments from S3.
                    let downloaded = graphstream::download_new_segments(
                        &self.s3_client,
                        &self.bucket,
                        &db_prefix,
                        &journal_dir,
                        current_seq,
                    ).await;

                    match downloaded {
                        Ok(segments) if segments.is_empty() => {
                            self.caught_up.store(true, Ordering::SeqCst);
                            metrics.inc(&metrics.follower_pulls_no_new_data);
                        }
                        Ok(_segments) => {
                            self.caught_up.store(false, Ordering::SeqCst);
                            // 2. Replay entries against local Kuzu.
                            // Hold write_mutex + snapshot_lock during replay to prevent
                            // concurrent reads from seeing partial replay state.
                            let _write_guard = match &self.write_mutex {
                                Some(m) => Some(m.lock().await),
                                None => None,
                            };
                            let journal_dir_clone = journal_dir.clone();
                            let db_path_clone = db_path.clone();
                            let shared_db_clone = self.shared_db.clone();
                            let snapshot_lock_clone = self.snapshot_lock.clone();
                            let replay_result = tokio::task::spawn_blocking(move || {
                                let _snap = snapshot_lock_clone
                                    .as_ref()
                                    .map(|l| l.read().unwrap_or_else(|e| e.into_inner()));
                                if let Some(db) = shared_db_clone {
                                    let conn = lbug::Connection::new(&*db)
                                        .map_err(|e| format!("Connection failed: {e}"))?;
                                    replay::replay_entries(&conn, &journal_dir_clone, current_seq)
                                } else {
                                    let db = lbug::Database::new(
                                        &db_path_clone,
                                        lbug::SystemConfig::default(),
                                    ).map_err(|e| format!("Open DB failed: {e}"))?;
                                    let conn = lbug::Connection::new(&db)
                                        .map_err(|e| format!("Connection failed: {e}"))?;
                                    replay::replay_entries(&conn, &journal_dir_clone, current_seq)
                                }
                            }).await;

                            match replay_result {
                                Ok(Ok(new_seq)) => {
                                    if new_seq > current_seq {
                                        tracing::debug!(
                                            "Follower '{}': replayed seq {} → {}",
                                            db_name, current_seq, new_seq
                                        );
                                        position.store(new_seq, Ordering::SeqCst);
                                        self.replay_position.store(new_seq, Ordering::SeqCst);
                                        metrics.inc(&metrics.follower_pulls_succeeded);
                                    }
                                    // Always mark caught up after successful replay,
                                    // even if new_seq == current_seq (already-replayed
                                    // segments after crash recovery).
                                    self.caught_up.store(true, Ordering::SeqCst);
                                }
                                Ok(Err(e)) => {
                                    tracing::error!("Follower '{}': replay failed: {}", db_name, e);
                                    metrics.inc(&metrics.follower_pulls_failed);
                                }
                                Err(e) => {
                                    tracing::error!("Follower '{}': replay task panic: {}", db_name, e);
                                    metrics.inc(&metrics.follower_pulls_failed);
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!("Follower '{}': download failed: {}", db_name, e);
                            metrics.inc(&metrics.follower_pulls_failed);
                        }
                    }
                }
                _ = cancel_rx.changed() => {
                    let current_seq = position.load(Ordering::SeqCst);
                    tracing::info!("Follower '{}': cancelled at seq {}", db_name, current_seq);
                    return Ok(());
                }
            }
        }
    }

    async fn catchup_on_promotion(
        &self,
        prefix: &str,
        db_name: &str,
        db_path: &PathBuf,
        position: u64,
    ) -> Result<()> {
        let journal_dir = db_path.parent().unwrap_or(db_path.as_path()).join("journal");
        let db_prefix = format!("{}{}/", prefix, db_name);

        // Note: snapshot-based bootstrap happens in HaKuzuBuilder::open() on cold
        // start. Here the database is already open, so we can only replay journal
        // entries. The snapshot accelerates cold start, not promotion catchup.

        // Download latest segments.
        graphstream::download_new_segments(
            &self.s3_client,
            &self.bucket,
            &db_prefix,
            &journal_dir,
            position,
        )
        .await
        .map_err(|e| anyhow::anyhow!("Catchup download failed: {e}"))?;

        // Replay entries — hold locks during catchup to prevent partial state.
        let _write_guard = match &self.write_mutex {
            Some(m) => Some(m.lock().await),
            None => None,
        };
        let journal_dir_clone = journal_dir.clone();
        let db_path_clone = db_path.clone();
        let shared_db_clone = self.shared_db.clone();
        let snapshot_lock_clone = self.snapshot_lock.clone();
        let new_seq = tokio::task::spawn_blocking(move || {
            let _snap = snapshot_lock_clone
                .as_ref()
                .map(|l| l.read().unwrap_or_else(|e| e.into_inner()));
            if let Some(db) = shared_db_clone {
                let conn = lbug::Connection::new(&*db)
                    .map_err(|e| format!("Connection failed: {e}"))?;
                replay::replay_entries(&conn, &journal_dir_clone, position)
            } else {
                let db = lbug::Database::new(&db_path_clone, lbug::SystemConfig::default())
                    .map_err(|e| format!("Open DB failed: {e}"))?;
                let conn = lbug::Connection::new(&db)
                    .map_err(|e| format!("Connection failed: {e}"))?;
                replay::replay_entries(&conn, &journal_dir_clone, position)
            }
        })
        .await
        .map_err(|e| anyhow::anyhow!("Catchup task panic: {e}"))?
        .map_err(|e| anyhow::anyhow!("Catchup replay failed: {e}"))?;

        if new_seq > position {
            self.replay_position.store(new_seq, Ordering::SeqCst);
        }

        tracing::info!(
            "KuzuFollowerBehavior '{}': caught up seq {} → {}",
            db_name, position, new_seq
        );

        Ok(())
    }
}
