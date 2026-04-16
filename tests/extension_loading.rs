//! Tests for turbograph extension UDFs (static or dynamic loading).
//!
//! **Static (recommended):** Build lbug with the extension baked in:
//!   LBUG_STATIC_EXTENSIONS=turbograph TURBOGRAPH_DIR=~/turbograph \
//!     cargo test --test extension_loading -- --ignored
//!
//! **Dynamic (Linux only):** Build the .lbug_extension and point to it:
//!   TURBOGRAPH_EXTENSION_PATH=/path/to/libturbograph.lbug_extension \
//!     cargo test --test extension_loading -- --ignored

use std::sync::Arc;

/// Open a database with the turbograph extension loaded.
///
/// Two modes:
/// - **Static** (LBUG_STATIC_EXTENSIONS=turbograph at build time): auto-loads on db init.
/// - **Dynamic** (TURBOGRAPH_EXTENSION_PATH env var): loads via LOAD EXTENSION SQL.
fn open_db_with_extension(dir: &std::path::Path) -> Arc<lbug::Database> {
    let db_path = dir.join("db");
    let db = lbug::Database::new(&db_path, lbug::SystemConfig::default())
        .expect("failed to open database");
    {
        let conn = lbug::Connection::new(&db).expect("failed to create connection");

        // Dynamic loading (if env var is set).
        if let Ok(path) = std::env::var("TURBOGRAPH_EXTENSION_PATH") {
            conn.query(&format!("LOAD EXTENSION '{path}'"))
                .expect("failed to load turbograph extension");
        }
        // Static: extension auto-loaded at database init, nothing to do.

        conn.query("CREATE NODE TABLE IF NOT EXISTS Person(id INT64, name STRING, PRIMARY KEY(id))")
            .expect("schema failed");
    }
    Arc::new(db)
}

// ============================================================================
// Extension loading + UDF smoke tests
// ============================================================================

#[test]
#[ignore]
fn extension_loads_successfully() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db = open_db_with_extension(tmp.path());
    let conn = lbug::Connection::new(&db).unwrap();

    // turbograph_get_manifest_version() should return NULL (no tiered file open).
    let mut result = conn
        .query("RETURN turbograph_get_manifest_version()")
        .expect("UDF should be registered");
    let row = result.next().expect("should have one row");
    // NULL result is fine (no active tiered file).
    println!("turbograph_get_manifest_version() = {:?}", row);
}

#[test]
#[ignore]
fn get_manifest_udf_registered() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db = open_db_with_extension(tmp.path());
    let conn = lbug::Connection::new(&db).unwrap();

    // Phase GraphBridge: turbograph_get_manifest() should be registered.
    let mut result = conn
        .query("RETURN turbograph_get_manifest()")
        .expect("turbograph_get_manifest UDF should be registered");
    let row = result.next().expect("should have one row");
    println!("turbograph_get_manifest() = {:?}", row);
}

#[test]
#[ignore]
fn sync_udf_registered() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db = open_db_with_extension(tmp.path());
    let conn = lbug::Connection::new(&db).unwrap();

    // turbograph_sync() should be registered (returns NULL when no tiered file).
    let mut result = conn
        .query("RETURN turbograph_sync()")
        .expect("turbograph_sync UDF should be registered");
    let row = result.next().expect("should have one row");
    println!("turbograph_sync() = {:?}", row);
}

#[test]
#[ignore]
fn set_manifest_udf_registered() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db = open_db_with_extension(tmp.path());
    let conn = lbug::Connection::new(&db).unwrap();

    // turbograph_set_manifest() should be registered.
    let mut result = conn
        .query("RETURN turbograph_set_manifest('{\"version\":0,\"page_count\":0,\"page_size\":4096,\"pages_per_group\":4096,\"page_group_keys\":[]}')")
        .expect("turbograph_set_manifest UDF should be registered");
    let row = result.next().expect("should have one row");
    println!("turbograph_set_manifest() = {:?}", row);
}

// ============================================================================
// TurbographReplicator with real extension
// ============================================================================

#[tokio::test]
#[ignore]
async fn replicator_call_get_manifest_with_extension() {
    use hakuzu::TurbographReplicator;

    let tmp = tempfile::TempDir::new().unwrap();
    let db = open_db_with_extension(tmp.path());
    let replicator = TurbographReplicator::new(db);

    // get_manifest_version should work (returns version, possibly 0 or NULL-mapped).
    let version = replicator.get_manifest_version();
    println!("get_manifest_version result: {:?}", version);
    // Don't assert success -- may return NULL (no tiered file active).
    // The point is it doesn't panic.
}
