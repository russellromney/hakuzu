//! Periodic snapshot loop for leader nodes.
//!
//! Extracted from database.rs — runs as a background task that periodically
//! checks journal progress and creates/uploads snapshots to S3.

use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use anyhow::anyhow;

use crate::database::{HaKuzuInner, SnapshotConfig};
use crate::replicator::KuzuReplicator;
use crate::snapshot;

/// Internal snapshot context passed to the role listener.
pub(crate) struct SnapshotContext {
    pub(crate) config: SnapshotConfig,
    pub(crate) s3_client: aws_sdk_s3::Client,
    pub(crate) bucket: String,
    pub(crate) prefix: String,
    pub(crate) db_path: PathBuf,
}

pub(crate) fn stop_snapshot_loop(
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

pub(crate) async fn run_snapshot_loop(
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
                        let _ = std::fs::remove_dir_all(&snap_dir);
                        continue;
                    }
                    Err(e) => {
                        tracing::error!("Snapshot task panicked: {e}");
                        let _ = std::fs::remove_dir_all(&snap_dir);
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
                    let _ = std::fs::remove_dir_all(&snap_dir);
                    continue;
                }

                last_snapshot_seq = snap_seq;
                tracing::info!(
                    seq = snap_seq,
                    size_bytes = snap_size,
                    "Snapshot complete"
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
