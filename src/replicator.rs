//! Kuzu replicator implementation using graphstream.
//!
//! Manages journal writer + S3 uploader per database. Mirrors haqlite's
//! SqliteReplicator, using graphstream (logical journal replication) instead
//! of walrust (physical WAL replication).

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use graphstream::journal::{self, JournalCommand, JournalSender, JournalState};
use graphstream::uploader::{spawn_journal_uploader, UploadMessage};
use hadb::Replicator;

/// Per-database replication state.
struct KuzuDbState {
    journal_tx: JournalSender,
    journal_state: Arc<JournalState>,
    upload_tx: tokio::sync::mpsc::Sender<UploadMessage>,
    uploader_handle: JoinHandle<()>,
    uploader_shutdown: tokio::sync::watch::Sender<bool>,
}

/// Kuzu replicator wrapping graphstream.
///
/// Handles journal replication via .graphj files (Kuzu-specific logical format).
pub struct KuzuReplicator {
    prefix: String,
    /// ObjectStore for upload + pull operations.
    object_store: Arc<dyn hadb_io::ObjectStore>,
    /// Segment max bytes before rotation (default 4MB).
    segment_max_bytes: u64,
    /// Fsync interval in ms (default 100ms).
    fsync_ms: u64,
    /// Upload interval (default 10s).
    upload_interval: Duration,
    databases: Mutex<HashMap<String, KuzuDbState>>,
}

impl KuzuReplicator {
    pub fn new(object_store: Arc<dyn hadb_io::ObjectStore>, prefix: String) -> Self {
        Self {
            prefix,
            object_store,
            segment_max_bytes: 4 * 1024 * 1024, // 4MB
            fsync_ms: 100,
            upload_interval: Duration::from_secs(10),
            databases: Mutex::new(HashMap::new()),
        }
    }

    pub fn with_segment_max_bytes(mut self, bytes: u64) -> Self {
        self.segment_max_bytes = bytes;
        self
    }

    pub fn with_fsync_ms(mut self, ms: u64) -> Self {
        self.fsync_ms = ms;
        self
    }

    pub fn with_upload_interval(mut self, interval: Duration) -> Self {
        self.upload_interval = interval;
        self
    }

    /// Get the journal sender for a database (for writing entries from the outside).
    pub async fn journal_sender(&self, name: &str) -> Option<JournalSender> {
        let dbs = self.databases.lock().await;
        dbs.get(name).map(|state| state.journal_tx.clone())
    }

    /// Get the journal state for a database.
    pub async fn journal_state(&self, name: &str) -> Option<Arc<JournalState>> {
        let dbs = self.databases.lock().await;
        dbs.get(name).map(|state| state.journal_state.clone())
    }
}

#[async_trait]
impl Replicator for KuzuReplicator {
    async fn add(&self, name: &str, path: &Path) -> Result<()> {
        let journal_dir = path.parent().unwrap_or(path).join("journal");
        std::fs::create_dir_all(&journal_dir)
            .map_err(|e| anyhow::anyhow!("Failed to create journal dir: {e}"))?;

        // Recover journal state (continue chain if exists).
        let (seq, hash) = journal::recover_journal_state(&journal_dir)
            .map_err(|e| anyhow::anyhow!("Failed to recover journal state: {e}"))?;

        let state = Arc::new(JournalState::with_sequence_and_hash(seq, hash));

        // Spawn journal writer.
        let journal_tx = journal::spawn_journal_writer(
            journal_dir.clone(),
            self.segment_max_bytes,
            self.fsync_ms,
            state.clone(),
        );

        // Spawn uploader (concurrent, returns sender + handle).
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let db_prefix = format!("{}{}/", self.prefix, name);
        let (upload_tx, uploader_handle) = spawn_journal_uploader(
            journal_tx.clone(),
            journal_dir,
            self.object_store.clone(),
            db_prefix,
            self.upload_interval,
            shutdown_rx,
        );

        let db_state = KuzuDbState {
            journal_tx,
            journal_state: state,
            upload_tx,
            uploader_handle,
            uploader_shutdown: shutdown_tx,
        };

        self.databases.lock().await.insert(name.to_string(), db_state);

        tracing::info!("KuzuReplicator: added database '{}'", name);
        Ok(())
    }

    async fn pull(&self, name: &str, path: &Path) -> Result<()> {
        let journal_dir = path.parent().unwrap_or(path).join("journal");
        std::fs::create_dir_all(&journal_dir)
            .map_err(|e| anyhow::anyhow!("Failed to create journal dir: {e}"))?;

        // Download all journal segments from object store.
        let db_prefix = format!("{}{}/", self.prefix, name);

        match graphstream::download_new_segments(&*self.object_store, &db_prefix, &journal_dir, 0).await {
            Ok(segments) => {
                tracing::info!("KuzuReplicator: pulled {} journal segments for '{}'", segments.len(), name);
            }
            Err(e) => {
                // Pull is called during coordinator.join() for followers. Failing here
                // prevents the follower from starting. Log at error level — the follower
                // loop will retry the download and catch up. This is acceptable because
                // the snapshot bootstrap (in database.rs open()) already populated the DB
                // if a snapshot was available, so reads won't be empty.
                tracing::error!("KuzuReplicator: pull for '{}' failed (follower loop will retry): {}", name, e);
            }
        }
        Ok(())
    }

    async fn remove(&self, name: &str) -> Result<()> {
        if let Some(state) = self.databases.lock().await.remove(name) {
            // Signal uploader shutdown via watch (stops seal timer loop).
            let _ = state.uploader_shutdown.send(true);

            // Send journal shutdown.
            let tx = state.journal_tx.clone();
            tokio::task::spawn_blocking(move || {
                let _ = tx.send(JournalCommand::Shutdown);
            })
            .await?;

            // Wait for uploader to finish (drains in-flight uploads).
            let _ = state.uploader_handle.await;

            tracing::info!("KuzuReplicator: removed database '{}'", name);
        }

        Ok(())
    }

    async fn sync(&self, name: &str) -> Result<()> {
        let dbs = self.databases.lock().await;
        if let Some(state) = dbs.get(name) {
            // Seal current segment.
            let tx = state.journal_tx.clone();
            let sealed = tokio::task::spawn_blocking(move || {
                let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
                if tx.send(JournalCommand::SealForUpload(ack_tx)).is_err() {
                    return None;
                }
                ack_rx.recv().ok().flatten()
            })
            .await?;

            if let Some(path) = &sealed {
                tracing::info!("KuzuReplicator: synced (sealed {})", path.display());
                // Trigger immediate upload and wait for confirmation.
                let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
                if state
                    .upload_tx
                    .send(UploadMessage::UploadWithAck(path.clone(), ack_tx))
                    .await
                    .is_err()
                {
                    tracing::error!("KuzuReplicator: upload channel closed for '{}'", name);
                } else {
                    // Wait for upload to complete.
                    match ack_rx.await {
                        Ok(Ok(())) => {
                            tracing::info!(
                                "KuzuReplicator: sync upload confirmed for '{}'",
                                name
                            );
                        }
                        Ok(Err(e)) => {
                            tracing::error!(
                                "KuzuReplicator: sync upload failed for '{}': {}",
                                name,
                                e
                            );
                            return Err(e);
                        }
                        Err(_) => {
                            tracing::error!(
                                "KuzuReplicator: upload ack channel dropped for '{}'",
                                name
                            );
                            return Err(anyhow::anyhow!("Upload ack channel dropped"));
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
