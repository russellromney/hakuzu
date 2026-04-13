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
    /// format. Parsed into structured StorageManifest::Turbograph fields by the
    /// leader, then reconstructed to JSON on the follower for turbograph_set_manifest().
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
    /// Reconstructs turbograph's internal JSON from the structured
    /// StorageManifest::Turbograph fields and passes it to the UDF.
    async fn pull(&self, name: &str, _path: &Path) -> Result<()> {
        let store = self.manifest_store.as_ref().ok_or_else(|| {
            anyhow!("TurbographReplicator: pull requires a ManifestStore (set via with_manifest_store)")
        })?;

        let manifest = store.get(name).await?;
        let manifest = manifest.ok_or_else(|| {
            anyhow!("TurbographReplicator: no manifest found for '{name}'")
        })?;

        // Reconstruct turbograph's internal JSON from the structured fields.
        let json = crate::turbograph_manifest_json::to_turbograph_json(&manifest.storage)?;

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
            // Read the full manifest JSON after sync so we can populate structured fields.
            // If the UDF isn't available (extension too old), returns Err.
            let json = replicator.call_get_manifest();
            Ok::<_, anyhow::Error>((version, json))
        })
        .await
        .map_err(|e| anyhow!("TurbographReplicator sync task panicked: {e}"))??;

        tracing::info!(
            "TurbographReplicator: sync for '{}' completed, manifest version = {}",
            name,
            version,
        );

        // Publish the manifest to the ManifestStore so followers can discover it.
        // Parse the turbograph JSON into structured fields. Followers reconstruct
        // the JSON from these fields (no opaque blob stored in msgpack).
        if let Some(ref store) = self.manifest_store {
            let manifest_json = match manifest_json {
                Ok(json) => json,
                Err(e) => {
                    // turbograph_get_manifest() unavailable (extension too old).
                    // The sync itself succeeded (data is durable in S3). Skip
                    // ManifestStore publication; followers can't discover this
                    // version until the extension is upgraded.
                    tracing::error!(
                        "TurbographReplicator: turbograph_get_manifest() failed for '{}': {}. \
                         Skipping ManifestStore publication (data is still durable in S3).",
                        name, e,
                    );
                    return Ok(());
                }
            };

            let current = store.get(name).await?;
            let expected_version = current.as_ref().map(|m| m.version);

            let manifest = crate::turbograph_manifest_json::parse_turbograph_json_to_ha_manifest(
                version as u64,
                &manifest_json,
            )?;

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
