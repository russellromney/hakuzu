//! Kuzu-specific follower behavior implementation.
//!
//! Polls S3 for new journal segments, downloads them, reads entries via
//! graphstream JournalReader, and replays against local Kuzu via replay_entries.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
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
/// using graphstream.
pub struct KuzuFollowerBehavior {
    s3_client: aws_sdk_s3::Client,
    bucket: String,
    shared_db: Option<Arc<lbug::Database>>,
}

impl KuzuFollowerBehavior {
    pub fn new(s3_client: aws_sdk_s3::Client, bucket: String) -> Self {
        Self {
            s3_client,
            bucket,
            shared_db: None,
        }
    }

    /// Use a shared Database for replay instead of creating a new one per replay.
    ///
    /// Required when the experiment binary also serves HTTP reads from the same DB.
    /// Without this, two Database instances on the same file would conflict.
    pub fn with_shared_db(mut self, db: Arc<lbug::Database>) -> Self {
        self.shared_db = Some(db);
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
                            metrics.inc(&metrics.follower_pulls_no_new_data);
                        }
                        Ok(_segments) => {
                            // 2. Replay entries against local Kuzu.
                            let journal_dir_clone = journal_dir.clone();
                            let db_path_clone = db_path.clone();
                            let shared_db_clone = self.shared_db.clone();
                            let replay_result = tokio::task::spawn_blocking(move || {
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
                                        metrics.inc(&metrics.follower_pulls_succeeded);
                                    }
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

        // Replay entries.
        let journal_dir_clone = journal_dir.clone();
        let db_path_clone = db_path.clone();
        let shared_db_clone = self.shared_db.clone();
        let new_seq = tokio::task::spawn_blocking(move || {
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

        tracing::info!(
            "KuzuFollowerBehavior '{}': caught up seq {} → {}",
            db_name, position, new_seq
        );

        Ok(())
    }
}
