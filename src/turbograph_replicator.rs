//! TurbographReplicator: S3 page-level tiering via turbograph UDFs.
//!
//! Wraps turbograph's three UDFs (turbograph_sync, turbograph_get_manifest_version,
//! turbograph_set_manifest) via lbug::Connection::query(). Implements hadb::Replicator
//! so it can be used as a drop-in replacement for KuzuReplicator when
//! Durability::Synchronous is selected.
//!
//! RPO = 0: every sync() call checkpoints + uploads pages to S3 before returning.

use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use hadb::Replicator;

/// Replicator that uses turbograph's S3-backed page-level tiering.
///
/// Calls turbograph UDFs via lbug connections:
/// - `turbograph_sync()` -> checkpoint + upload to S3, returns manifest version
/// - `turbograph_get_manifest_version()` -> current manifest version
/// - `turbograph_set_manifest(json)` -> apply remote manifest (follower pull)
pub struct TurbographReplicator {
    db: Arc<lbug::Database>,
    /// ManifestStore for publishing HA manifests (used in Shared mode).
    manifest_store: Option<Arc<dyn hadb::ManifestStore>>,
}

impl TurbographReplicator {
    /// Create a new TurbographReplicator.
    ///
    /// The lbug::Database must already have the turbograph extension loaded.
    pub fn new(db: Arc<lbug::Database>) -> Self {
        Self {
            db,
            manifest_store: None,
        }
    }

    /// Set a ManifestStore for publishing manifests after sync (Shared mode).
    pub fn with_manifest_store(mut self, store: Arc<dyn hadb::ManifestStore>) -> Self {
        self.manifest_store = Some(store);
        self
    }

    /// Call turbograph_sync() via lbug connection.
    ///
    /// Triggers a checkpoint, uploads dirty pages to S3, and returns the
    /// new manifest version number.
    fn call_sync(&self) -> Result<i64> {
        let conn = lbug::Connection::new(&self.db)
            .map_err(|e| anyhow!("turbograph_replicator: failed to create connection: {e}"))?;
        let mut result = conn
            .query("RETURN turbograph_sync()")
            .map_err(|e| anyhow!("turbograph_sync() failed: {e}"))?;

        parse_int64_result(&mut result, "turbograph_sync")
    }

    /// Call turbograph_get_manifest_version() via lbug connection.
    pub fn get_manifest_version(&self) -> Result<i64> {
        let conn = lbug::Connection::new(&self.db)
            .map_err(|e| anyhow!("turbograph_replicator: failed to create connection: {e}"))?;
        let mut result = conn
            .query("RETURN turbograph_get_manifest_version()")
            .map_err(|e| anyhow!("turbograph_get_manifest_version() failed: {e}"))?;

        parse_int64_result(&mut result, "turbograph_get_manifest_version")
    }

    /// Call turbograph_set_manifest(json) via lbug connection.
    fn call_set_manifest(&self, manifest_json: &str) -> Result<i64> {
        let conn = lbug::Connection::new(&self.db)
            .map_err(|e| anyhow!("turbograph_replicator: failed to create connection: {e}"))?;

        let query = format!("RETURN turbograph_set_manifest('{}')", manifest_json.replace('\'', "''"));
        let mut result = conn
            .query(&query)
            .map_err(|e| anyhow!("turbograph_set_manifest() failed: {e}"))?;

        parse_int64_result(&mut result, "turbograph_set_manifest")
    }

    /// Call turbograph_get_manifest() via lbug connection (Phase GraphBridge).
    ///
    /// Returns the current manifest as a JSON string in turbograph's internal
    /// format. This is the opaque blob that gets stored in manifest_json and
    /// passed to turbograph_set_manifest() on followers.
    fn call_get_manifest(&self) -> Result<String> {
        let conn = lbug::Connection::new(&self.db)
            .map_err(|e| anyhow!("turbograph_replicator: failed to create connection: {e}"))?;
        let mut result = conn
            .query("RETURN turbograph_get_manifest()")
            .map_err(|e| anyhow!("turbograph_get_manifest() failed: {e}"))?;

        parse_string_result(&mut result, "turbograph_get_manifest")
    }
}

#[async_trait]
impl Replicator for TurbographReplicator {
    /// No-op: turbograph VFS is already registered when the extension is loaded.
    async fn add(&self, _name: &str, _path: &Path) -> Result<()> {
        tracing::info!("TurbographReplicator: add (no-op, extension already loaded)");
        Ok(())
    }

    /// Pull the latest manifest from ManifestStore and apply via turbograph_set_manifest.
    ///
    /// Extracts the `manifest_json` field from StorageManifest::Turbograph (the raw
    /// turbograph-internal JSON) and passes it directly to the UDF. This is the
    /// correct format that Manifest::fromJSON() expects on the C++ side.
    async fn pull(&self, name: &str, _path: &Path) -> Result<()> {
        let store = self.manifest_store.as_ref().ok_or_else(|| {
            anyhow!("TurbographReplicator: pull requires a ManifestStore (set via with_manifest_store)")
        })?;

        let manifest = store.get(name).await?;
        let manifest = manifest.ok_or_else(|| {
            anyhow!("TurbographReplicator: no manifest found for '{name}'")
        })?;

        // Extract the raw turbograph manifest JSON from the HaManifest.
        let json = match &manifest.storage {
            hadb::StorageManifest::Turbograph { manifest_json, .. }
                if !manifest_json.is_empty() =>
            {
                manifest_json.clone()
            }
            _ => {
                return Err(anyhow!(
                    "TurbographReplicator: manifest_json is empty for '{}' v{} \
                     (manifest was published before Phase GraphBridge)",
                    name,
                    manifest.version,
                ));
            }
        };

        let db = self.db.clone();
        let version = tokio::task::spawn_blocking(move || {
            let replicator = TurbographReplicator { db, manifest_store: None };
            replicator.call_set_manifest(&json)
        })
        .await
        .map_err(|e| anyhow!("TurbographReplicator pull task panicked: {e}"))??;

        tracing::info!(
            "TurbographReplicator: pull for '{}' applied manifest version {}",
            name,
            version,
        );
        Ok(())
    }

    /// No-op: turbograph cleanup handled by VFS destructor.
    async fn remove(&self, _name: &str) -> Result<()> {
        tracing::info!("TurbographReplicator: remove (no-op, VFS handles cleanup)");
        Ok(())
    }

    /// No-op: same as add, turbograph continues from manifest state.
    async fn add_continuing(&self, _name: &str, _path: &Path) -> Result<()> {
        tracing::info!("TurbographReplicator: add_continuing (no-op, extension already loaded)");
        Ok(())
    }

    /// Checkpoint + upload to S3 via turbograph_sync(). RPO = 0.
    ///
    /// After sync, reads the full manifest JSON via turbograph_get_manifest()
    /// and publishes it to the ManifestStore so followers can apply it.
    async fn sync(&self, name: &str) -> Result<()> {
        let db = self.db.clone();
        let (version, manifest_json) = tokio::task::spawn_blocking(move || {
            let replicator = TurbographReplicator { db, manifest_store: None };
            let version = replicator.call_sync()?;
            // Phase GraphBridge: read the full manifest JSON after sync.
            // If the UDF isn't available (extension too old), fall back to empty.
            let json = replicator.call_get_manifest().unwrap_or_default();
            Ok::<_, anyhow::Error>((version, json))
        })
        .await
        .map_err(|e| anyhow!("TurbographReplicator sync task panicked: {e}"))??;

        tracing::info!(
            "TurbographReplicator: sync for '{}' completed, manifest version = {}, json_len = {}",
            name,
            version,
            manifest_json.len(),
        );

        // Publish the manifest to the ManifestStore so followers can discover it.
        if let Some(ref store) = self.manifest_store {
            let current = store.get(name).await?;
            let expected_version = current.as_ref().map(|m| m.version);

            // Parse simple scalars from the turbograph JSON for monitoring.
            // The manifest_json blob is the source of truth for followers.
            let parsed: serde_json::Value = serde_json::from_str(&manifest_json)
                .unwrap_or_default();

            let manifest = hadb::HaManifest {
                version: version as u64,
                writer_id: String::new(),
                lease_epoch: 0,
                timestamp_ms: 0,
                storage: hadb::StorageManifest::Turbograph {
                    turbograph_version: version as u64,
                    page_count: parsed["page_count"].as_u64().unwrap_or(0),
                    page_size: parsed["page_size"].as_u64().unwrap_or(0) as u32,
                    pages_per_group: parsed["pages_per_group"].as_u64().unwrap_or(0) as u32,
                    sub_pages_per_frame: parsed["sub_pages_per_frame"].as_u64().unwrap_or(0) as u32,
                    page_group_keys: parsed["page_group_keys"]
                        .as_array()
                        .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                        .unwrap_or_default(),
                    frame_tables: vec![],        // populated in manifest_json blob
                    subframe_overrides: vec![],  // populated in manifest_json blob
                    encrypted: parsed["encrypted"].as_bool().unwrap_or(false),
                    journal_seq: parsed["journal_seq"].as_u64().unwrap_or(0),
                    manifest_json,
                },
            };

            let cas_result = store.put(name, &manifest, expected_version).await?;
            if !cas_result.success {
                tracing::warn!(
                    "TurbographReplicator: manifest CAS failed for '{}' (expected version {:?})",
                    name,
                    expected_version,
                );
            }
        }

        Ok(())
    }
}

/// Parse a single INT64 result from a lbug query result.
fn parse_int64_result(result: &mut lbug::QueryResult, fn_name: &str) -> Result<i64> {
    let row = result.next().ok_or_else(|| {
        anyhow!("{fn_name}() returned no rows")
    })?;

    if row.is_empty() {
        return Err(anyhow!("{fn_name}() returned empty row"));
    }

    match &row[0] {
        lbug::Value::Int64(v) => Ok(*v),
        other => Err(anyhow!(
            "{fn_name}() returned unexpected type: {:?} (expected INT64)",
            other
        )),
    }
}

/// Parse a single STRING result from a lbug query result.
fn parse_string_result(result: &mut lbug::QueryResult, fn_name: &str) -> Result<String> {
    let row = result.next().ok_or_else(|| {
        anyhow!("{fn_name}() returned no rows")
    })?;

    if row.is_empty() {
        return Err(anyhow!("{fn_name}() returned empty row"));
    }

    match &row[0] {
        lbug::Value::String(v) => Ok(v.clone()),
        other => Err(anyhow!(
            "{fn_name}() returned unexpected type: {:?} (expected STRING)",
            other
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// TurbographReplicator can be constructed with just a database.
    #[test]
    fn construction() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db_path = tmp.path().join("db");
        let db = Arc::new(
            lbug::Database::new(db_path.to_str().unwrap(), lbug::SystemConfig::default())
                .unwrap(),
        );
        let replicator = TurbographReplicator::new(db);
        assert!(replicator.manifest_store.is_none());
    }

    /// Calling sync without turbograph extension loaded returns a clear error.
    #[tokio::test]
    async fn sync_without_extension_errors() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db_path = tmp.path().join("db");
        let db = Arc::new(
            lbug::Database::new(db_path.to_str().unwrap(), lbug::SystemConfig::default())
                .unwrap(),
        );
        let replicator = TurbographReplicator::new(db);

        let result = replicator.sync("test-db").await;
        assert!(result.is_err(), "sync should fail without turbograph extension");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("turbograph_sync") || err.contains("failed"),
            "error should mention the UDF: {err}"
        );
    }

    /// Calling pull without manifest store returns a clear error.
    #[tokio::test]
    async fn pull_without_manifest_store_errors() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db_path = tmp.path().join("db");
        let db = Arc::new(
            lbug::Database::new(db_path.to_str().unwrap(), lbug::SystemConfig::default())
                .unwrap(),
        );
        let replicator = TurbographReplicator::new(db);

        let result = replicator.pull("test-db", Path::new("/tmp")).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("ManifestStore"),
            "error should mention ManifestStore: {err}"
        );
    }

    /// add() and remove() are no-ops and always succeed.
    #[tokio::test]
    async fn add_remove_are_noops() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db_path = tmp.path().join("db");
        let db = Arc::new(
            lbug::Database::new(db_path.to_str().unwrap(), lbug::SystemConfig::default())
                .unwrap(),
        );
        let replicator = TurbographReplicator::new(db);

        replicator.add("test", Path::new("/tmp")).await.unwrap();
        replicator.add_continuing("test", Path::new("/tmp")).await.unwrap();
        replicator.remove("test").await.unwrap();
    }

    /// get_manifest_version without extension returns a clear error.
    #[test]
    fn get_manifest_version_without_extension() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db_path = tmp.path().join("db");
        let db = Arc::new(
            lbug::Database::new(db_path.to_str().unwrap(), lbug::SystemConfig::default())
                .unwrap(),
        );
        let replicator = TurbographReplicator::new(db);

        let result = replicator.get_manifest_version();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("turbograph_get_manifest_version"),
            "error should mention the UDF: {err}"
        );
    }
}
