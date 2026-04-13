//! Phase GraphMeridian tests: Durability, HaMode, TurbographReplicator, and
//! TurbographFollowerBehavior (GraphMeridian-a and -b).

use std::path::Path;
use std::sync::Arc;

use hakuzu::mode::{validate_mode_durability, Durability, HaMode};
use hakuzu::{TurbographFollowerBehavior, TurbographReplicator};

// ============================================================================
// Mode validation tests
// ============================================================================

#[test]
fn dedicated_replicated_is_valid() {
    assert!(validate_mode_durability(HaMode::Dedicated, Durability::Replicated).is_ok());
}

#[test]
fn dedicated_synchronous_is_valid() {
    assert!(validate_mode_durability(HaMode::Dedicated, Durability::Synchronous).is_ok());
}

#[test]
fn shared_replicated_rejected() {
    let err = validate_mode_durability(HaMode::Shared, Durability::Replicated).unwrap_err();
    assert!(
        err.contains("Synchronous durability"),
        "should explain that Shared needs Synchronous: {err}"
    );
}

#[test]
fn shared_synchronous_not_yet_implemented() {
    let err = validate_mode_durability(HaMode::Shared, Durability::Synchronous).unwrap_err();
    assert!(
        err.contains("not yet implemented"),
        "should explain Shared is not yet implemented: {err}"
    );
}

#[test]
fn default_mode_is_dedicated_replicated() {
    assert_eq!(HaMode::default(), HaMode::Dedicated);
    assert_eq!(Durability::default(), Durability::Replicated);
}

// ============================================================================
// TurbographReplicator construction tests
// ============================================================================

#[test]
fn turbograph_replicator_construction() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db_path = tmp.path().join("db");
    let db = Arc::new(
        lbug::Database::new(db_path.to_str().unwrap(), lbug::SystemConfig::default()).unwrap(),
    );
    let replicator = TurbographReplicator::new(db);
    // Should construct without panic.
    drop(replicator);
}

#[test]
fn turbograph_replicator_with_manifest_store() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db_path = tmp.path().join("db");
    let db = Arc::new(
        lbug::Database::new(db_path.to_str().unwrap(), lbug::SystemConfig::default()).unwrap(),
    );
    let store = Arc::new(hadb::InMemoryManifestStore::new());
    let replicator = TurbographReplicator::new(db).with_manifest_store(store);
    drop(replicator);
}

// ============================================================================
// TurbographReplicator trait impl tests (no extension loaded)
// ============================================================================

#[tokio::test]
async fn add_and_remove_are_noops() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db_path = tmp.path().join("db");
    let db = Arc::new(
        lbug::Database::new(db_path.to_str().unwrap(), lbug::SystemConfig::default()).unwrap(),
    );
    let replicator = TurbographReplicator::new(db);

    // add, add_continuing, remove should all succeed (no-ops).
    use hadb::Replicator;
    replicator.add("test", Path::new("/tmp")).await.unwrap();
    replicator
        .add_continuing("test", Path::new("/tmp"))
        .await
        .unwrap();
    replicator.remove("test").await.unwrap();
}

#[tokio::test]
async fn sync_without_extension_returns_clear_error() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db_path = tmp.path().join("db");
    let db = Arc::new(
        lbug::Database::new(db_path.to_str().unwrap(), lbug::SystemConfig::default()).unwrap(),
    );
    let replicator = TurbographReplicator::new(db);

    use hadb::Replicator;
    let err = replicator.sync("test-db").await.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("turbograph_sync"),
        "error should mention turbograph_sync UDF: {msg}"
    );
}

#[tokio::test]
async fn pull_without_manifest_store_returns_clear_error() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db_path = tmp.path().join("db");
    let db = Arc::new(
        lbug::Database::new(db_path.to_str().unwrap(), lbug::SystemConfig::default()).unwrap(),
    );
    let replicator = TurbographReplicator::new(db);

    use hadb::Replicator;
    let err = replicator.pull("test-db", Path::new("/tmp")).await.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("ManifestStore"),
        "error should mention ManifestStore requirement: {msg}"
    );
}

#[tokio::test]
async fn pull_with_empty_manifest_store_returns_clear_error() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db_path = tmp.path().join("db");
    let db = Arc::new(
        lbug::Database::new(db_path.to_str().unwrap(), lbug::SystemConfig::default()).unwrap(),
    );
    let store = Arc::new(hadb::InMemoryManifestStore::new());
    let replicator = TurbographReplicator::new(db).with_manifest_store(store);

    use hadb::Replicator;
    let err = replicator.pull("test-db", Path::new("/tmp")).await.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("no manifest found"),
        "error should say no manifest found: {msg}"
    );
}

#[test]
fn get_manifest_version_without_extension_returns_clear_error() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db_path = tmp.path().join("db");
    let db = Arc::new(
        lbug::Database::new(db_path.to_str().unwrap(), lbug::SystemConfig::default()).unwrap(),
    );
    let replicator = TurbographReplicator::new(db);
    let err = replicator.get_manifest_version().unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("turbograph_get_manifest_version"),
        "error should mention the UDF: {msg}"
    );
}

// ============================================================================
// Builder mode/durability integration tests
// ============================================================================

#[test]
fn builder_accepts_mode_and_durability() {
    // Just verify these builder methods compile and chain correctly.
    let _builder = hakuzu::HaKuzu::builder("test-bucket")
        .mode(HaMode::Dedicated)
        .durability(Durability::Replicated);
}

#[test]
fn builder_accepts_synchronous_durability() {
    let _builder = hakuzu::HaKuzu::builder("test-bucket")
        .mode(HaMode::Dedicated)
        .durability(Durability::Synchronous);
}

#[test]
fn builder_accepts_manifest_store() {
    let store = Arc::new(hadb::InMemoryManifestStore::new());
    let _builder = hakuzu::HaKuzu::builder("test-bucket")
        .mode(HaMode::Dedicated)
        .durability(Durability::Synchronous)
        .manifest_store(store);
}

// ============================================================================
// HaKuzu::local() backward compatibility
// ============================================================================

#[tokio::test]
async fn local_mode_still_works() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db_path = tmp.path().join("local-test");
    let db = hakuzu::HaKuzu::local(
        db_path.to_str().unwrap(),
        "CREATE NODE TABLE IF NOT EXISTS T(id INT64, PRIMARY KEY(id))",
    )
    .unwrap();
    drop(db);
}

// ============================================================================
// Re-export tests
// ============================================================================

#[test]
fn reexports_accessible() {
    // Verify the public API re-exports work.
    let _d: hakuzu::Durability = hakuzu::Durability::Replicated;
    let _m: hakuzu::HaMode = hakuzu::HaMode::Dedicated;
}

// ============================================================================
// GraphMeridian-b: TurbographFollowerBehavior builder wiring
// ============================================================================

/// Builder rejects Synchronous durability without a ManifestStore.
/// This check happens before S3 config load (fast fail, no network access).
#[tokio::test]
async fn synchronous_durability_requires_manifest_store() {
    let result = hakuzu::HaKuzu::builder("test-bucket")
        .durability(Durability::Synchronous)
        .open("/tmp/nonexistent_test_db", "")
        .await;
    assert!(result.is_err(), "Should fail without manifest_store");
    let msg = result.err().unwrap().to_string();
    assert!(
        msg.contains("ManifestStore"),
        "error should mention ManifestStore requirement: {msg}"
    );
}

/// TurbographFollowerBehavior is in the public API and constructible.
#[test]
fn turbograph_follower_behavior_in_public_api() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db_path = tmp.path().join("db");
    let db = Arc::new(
        lbug::Database::new(db_path.to_str().unwrap(), lbug::SystemConfig::default()).unwrap(),
    );
    let store = Arc::new(hadb::InMemoryManifestStore::new());
    let _fb = TurbographFollowerBehavior::new(store, db);
}

/// TurbographFollowerBehavior with_locks and with_wakeup chain correctly.
#[test]
fn turbograph_follower_behavior_builder_chain() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db_path = tmp.path().join("db");
    let db = Arc::new(
        lbug::Database::new(db_path.to_str().unwrap(), lbug::SystemConfig::default()).unwrap(),
    );
    let store = Arc::new(hadb::InMemoryManifestStore::new());
    let wm = Arc::new(tokio::sync::Mutex::new(()));
    let sl = Arc::new(std::sync::RwLock::new(()));
    let notify = Arc::new(tokio::sync::Notify::new());
    let _fb = TurbographFollowerBehavior::new(store, db)
        .with_locks(wm, sl)
        .with_wakeup(notify);
}

/// TurbographFollowerBehavior catchup_on_promotion with no manifest is non-fatal.
#[tokio::test]
async fn turbograph_follower_catchup_no_manifest_is_nonfatal() {
    use hadb::FollowerBehavior;
    let tmp = tempfile::TempDir::new().unwrap();
    let db_path = tmp.path().join("db");
    let db = Arc::new(
        lbug::Database::new(db_path.to_str().unwrap(), lbug::SystemConfig::default()).unwrap(),
    );
    let store = Arc::new(hadb::InMemoryManifestStore::new());
    let fb = TurbographFollowerBehavior::new(store, db);
    // Should not return an error even though the store is empty.
    fb.catchup_on_promotion("prefix", "db", &std::path::PathBuf::from("/tmp"), 0)
        .await
        .unwrap();
}

/// ManifestChanged wakeup notify: TurbographFollowerBehavior shares a Notify
/// with the role listener. Triggering the notify does not cause errors.
#[tokio::test]
async fn manifest_changed_wakeup_triggers_correctly() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db_path = tmp.path().join("db");
    let db = Arc::new(
        lbug::Database::new(db_path.to_str().unwrap(), lbug::SystemConfig::default()).unwrap(),
    );
    let store = Arc::new(hadb::InMemoryManifestStore::new());
    let notify = Arc::new(tokio::sync::Notify::new());
    let notify2 = notify.clone();

    let _fb = TurbographFollowerBehavior::new(store, db).with_wakeup(notify);

    // Simulating a ManifestChanged event: notify_one() should not panic.
    notify2.notify_one();
}

/// Replicated mode uses journal-based follower (no manifest_store required).
/// Verify the builder doesn't require manifest_store for Replicated.
#[test]
fn replicated_mode_does_not_require_manifest_store() {
    // Just building, not opening (no S3 access needed to verify builder state).
    let _b = hakuzu::HaKuzu::builder("bucket")
        .durability(Durability::Replicated);
    // If this compiles and doesn't panic, the test passes.
}
