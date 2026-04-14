//! Tests that require the turbograph extension shared library.
//!
//! Set TURBOGRAPH_EXTENSION_PATH to the .lbug_extension file path:
//!   TURBOGRAPH_EXTENSION_PATH=/path/to/libturbograph.lbug_extension \
//!     cargo test --test extension_loading -- --ignored
//!
//! Build the extension from the ladybug tree:
//!   cd ~/Documents/Github/ladybug
//!   cmake -B build/release -DCMAKE_BUILD_TYPE=Release \
//!     -DBUILD_EXTENSIONS="turbograph" \
//!     -DTURBOGRAPH_DIR=~/Documents/Github/turbograph
//!   cmake --build build/release --target lbug_turbograph_extension

use std::sync::Arc;

fn extension_path() -> String {
    std::env::var("TURBOGRAPH_EXTENSION_PATH")
        .expect("TURBOGRAPH_EXTENSION_PATH env var required for extension tests")
}

fn load_extension(conn: &lbug::Connection) {
    let path = extension_path();
    conn.query(&format!("LOAD EXTENSION '{path}'"))
        .expect("failed to load turbograph extension");
}

fn open_db_with_extension(dir: &std::path::Path) -> Arc<lbug::Database> {
    let db_path = dir.join("db");
    let db = lbug::Database::new(&db_path, lbug::SystemConfig::default())
        .expect("failed to open database");
    {
        let conn = lbug::Connection::new(&db).expect("failed to create connection");
        load_extension(&conn);
        conn.query("CREATE NODE TABLE IF NOT EXISTS Person(id INT64, name STRING, PRIMARY KEY(id))")
            .expect("schema failed");
    } // conn dropped, releasing borrow on db
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
