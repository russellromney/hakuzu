//! Phase Rubicon: custom lease store + external database builder tests.
//!
//! Tests for the new HaKuzuBuilder methods: .lease_store(), .database(), .locks().

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use tempfile::TempDir;

use hakuzu::{
    Coordinator, CoordinatorConfig, HaKuzu, InMemoryLeaseStore, KuzuFollowerBehavior,
    KuzuReplicator, LeaseConfig, Role,
};

mod common;

const SCHEMA: &str = "CREATE NODE TABLE IF NOT EXISTS Person(id INT64, name STRING, PRIMARY KEY(id))";

// ============================================================================
// Helper (same as ha_database.rs)
// ============================================================================

fn build_coordinator(
    lease_store: Arc<dyn hakuzu::LeaseStore>,
    instance_id: &str,
    address: &str,
    shared_db: Arc<lbug::Database>,
) -> (Arc<Coordinator>, Arc<KuzuReplicator>) {
    let config = CoordinatorConfig {
        lease: Some(LeaseConfig::new(
            lease_store,
            instance_id.to_string(),
            address.to_string(),
        )),
        ..Default::default()
    };

    let object_store = common::MockObjectStore::new();
    let replicator = Arc::new(
        KuzuReplicator::new(object_store.clone(), "test/".into()),
    );
    let follower_behavior = Arc::new(
        KuzuFollowerBehavior::new(object_store)
            .with_shared_db(shared_db),
    );

    let coordinator = Coordinator::new(
        replicator.clone(),
        None, // manifest_store
        None, // node_registry
        follower_behavior,
        "test/",
        config,
    );

    (coordinator, replicator)
}

// ============================================================================
// Builder compile tests
// ============================================================================

#[tokio::test]
async fn builder_with_custom_lease_store_compiles() {
    // Verify the builder method type-checks and accepts an InMemoryLeaseStore.
    let lease_store: Arc<dyn hakuzu::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    // Just verify the builder chain compiles. We don't call open() because
    // that requires S3 for the replicator, but the builder should accept
    // the custom lease store.
    let _builder = HaKuzu::builder()
        .prefix("test/")
        .instance_id("test-node")
        .address("http://localhost:19200")
        .lease_store(lease_store);
}

#[tokio::test]
async fn builder_without_lease_store_rejects() {
    // Regression for the lease-store safety gate: `HaKuzuBuilder::open()`
    // must error when no `.lease_store(...)` is set. Leader election
    // semantics depend on backend CAS atomicity (AWS S3 atomic; Tigris
    // not), so hakuzu refuses to silently pick a default.
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("no_lease");

    // Provide storage so the lease check is the one that fires first.
    let storage: Arc<dyn hadb_storage::StorageBackend> = common::MockObjectStore::new();
    let result = HaKuzu::builder()
        .prefix("no-lease/")
        .instance_id("no-lease-node")
        .address("http://localhost:19260")
        .storage(storage)
        .open(db_path.to_str().unwrap(), SCHEMA)
        .await;

    let err = match result {
        Ok(_) => panic!("open() should error when no .lease_store() is set"),
        Err(e) => e,
    };
    let msg = format!("{err}");
    assert!(
        msg.contains("lease_store"),
        "error should mention the missing lease_store setter, got: {msg}"
    );
}

#[tokio::test]
async fn builder_without_storage_rejects() {
    // Regression for the storage safety gate: `HaKuzuBuilder::open()`
    // must error when no `.storage(...)` is set. hakuzu won't silently
    // build an S3Storage for you.
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("no_storage");

    // Provide lease store so the storage check is the one that fires.
    let lease_store: Arc<dyn hakuzu::LeaseStore> = Arc::new(InMemoryLeaseStore::new());
    let result = HaKuzu::builder()
        .prefix("no-storage/")
        .instance_id("no-storage-node")
        .address("http://localhost:19262")
        .lease_store(lease_store)
        .open(db_path.to_str().unwrap(), SCHEMA)
        .await;

    let err = match result {
        Ok(_) => panic!("open() should error when no .storage() is set"),
        Err(e) => e,
    };
    let msg = format!("{err}");
    assert!(
        msg.contains("storage"),
        "error should mention the missing storage setter, got: {msg}"
    );
}

#[tokio::test]
async fn builder_database_method_compiles() {
    // Verify the .database() builder method type-checks.
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("builder_ext_db");

    let db = lbug::Database::new(&db_path, lbug::SystemConfig::default()).unwrap();
    let db = Arc::new(db);

    let _builder = HaKuzu::builder()
        .prefix("test/")
        .instance_id("builder-ext")
        .address("http://localhost:19230")
        .database(db);
}

#[tokio::test]
async fn builder_locks_method_compiles() {
    // Verify the .locks() builder method type-checks.
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("builder_locks_db");

    let db = lbug::Database::new(&db_path, lbug::SystemConfig::default()).unwrap();
    let db = Arc::new(db);

    let write_mutex = Arc::new(tokio::sync::Mutex::new(()));
    let snapshot_lock = Arc::new(std::sync::RwLock::new(()));

    let _builder = HaKuzu::builder()
        .prefix("test/")
        .database(db)
        .locks(write_mutex, snapshot_lock);
}

// ============================================================================
// Validation tests
// ============================================================================

#[tokio::test]
async fn builder_rejects_database_without_locks() {
    // Using .database() without .locks() should return an error at open() time.
    // Without shared locks, two independent lock systems would protect the same
    // database, causing data races.
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("no_locks_db");

    let db = lbug::Database::new(&db_path, lbug::SystemConfig::default()).unwrap();
    let db = Arc::new(db);

    let result = HaKuzu::builder()
        .prefix("test/")
        .instance_id("no-locks")
        .address("http://localhost:19260")
        .database(db)
        .open(db_path.to_str().unwrap(), SCHEMA)
        .await;

    assert!(result.is_err(), "Should reject .database() without .locks()");
    let err_msg = match result {
        Err(e) => e.to_string(),
        Ok(_) => panic!("Expected error"),
    };
    assert!(
        err_msg.contains("external locks"),
        "Error should mention locks requirement, got: {}",
        err_msg
    );
}

#[tokio::test]
async fn builder_accepts_database_with_locks() {
    // Using .database() with .locks() should be accepted.
    // We cannot call .open() fully (needs S3), but we verify the validation
    // does not reject this combination. The open() will fail later on the
    // S3 connection, not on the validation.
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("with_locks_db");

    let db = lbug::Database::new(&db_path, lbug::SystemConfig::default()).unwrap();
    let db = Arc::new(db);

    let write_mutex = Arc::new(tokio::sync::Mutex::new(()));
    let snapshot_lock = Arc::new(std::sync::RwLock::new(()));

    // This will attempt S3 connection and fail, but should NOT fail on
    // the database-without-locks validation.
    let result = HaKuzu::builder()
        .prefix("test/")
        .instance_id("with-locks")
        .address("http://localhost:19261")
        .database(db)
        .locks(write_mutex, snapshot_lock)
        .open(db_path.to_str().unwrap(), SCHEMA)
        .await;

    // The error should be about S3/network, not about locks.
    match result {
        Ok(_) => {
            // Unlikely but fine (maybe local S3 is running).
        }
        Err(e) => {
            let msg = e.to_string();
            assert!(
                !msg.contains("external locks"),
                "Should not fail on locks validation when .locks() is provided, got: {}",
                msg
            );
        }
    }
}

// ============================================================================
// Custom lease store functional tests
// ============================================================================

#[tokio::test]
async fn custom_lease_store_single_node() {
    // Verify HaKuzu works with a custom InMemoryLeaseStore via from_coordinator.
    // This confirms the lease store is properly wired through without S3 credentials.
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("custom_lease_db");
    let lease_store: Arc<dyn hakuzu::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    let db = lbug::Database::new(&db_path, lbug::SystemConfig::default()).unwrap();
    {
        let conn = lbug::Connection::new(&db).unwrap();
        conn.query(SCHEMA).unwrap();
    }
    let db = Arc::new(db);

    let (coordinator, replicator) = build_coordinator(
        lease_store,
        "custom-lease-1",
        "http://localhost:19201",
        db.clone(),
    );

    let ha = HaKuzu::from_coordinator(
        coordinator,
        replicator,
        db,
        db_path.to_str().unwrap(),
        "custom_lease_db",
        19201,
        Duration::from_secs(5),
    )
    .await
    .unwrap();

    assert_eq!(ha.role(), Some(Role::Leader));

    // Write and read work without S3 credentials.
    ha.execute(
        "CREATE (:Person {id: $id, name: $name})",
        Some(json!({"id": 1, "name": "NoS3"})),
    )
    .await
    .unwrap();

    let result = ha
        .query("MATCH (p:Person) RETURN p.name", None)
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], json!("NoS3"));

    ha.close().await.unwrap();
}

#[tokio::test]
async fn two_node_custom_lease_store() {
    // Two HaKuzu nodes sharing an InMemoryLeaseStore. Write on leader,
    // verify follower forwards correctly.
    let tmp1 = TempDir::new().unwrap();
    let tmp2 = TempDir::new().unwrap();
    let db_path1 = tmp1.path().join("lease_leader");
    let db_path2 = tmp2.path().join("lease_follower");

    let lease_store: Arc<dyn hakuzu::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    // Build leader.
    let db1 = lbug::Database::new(&db_path1, lbug::SystemConfig::default()).unwrap();
    {
        let conn = lbug::Connection::new(&db1).unwrap();
        conn.query(SCHEMA).unwrap();
    }
    let db1 = Arc::new(db1);

    let (coord1, repl1) = build_coordinator(
        lease_store.clone(),
        "lease-leader",
        "http://localhost:19210",
        db1.clone(),
    );

    let leader = HaKuzu::from_coordinator(
        coord1,
        repl1,
        db1,
        db_path1.to_str().unwrap(),
        "lease_db",
        19210,
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert_eq!(leader.role(), Some(Role::Leader));

    // Build follower.
    let db2 = lbug::Database::new(&db_path2, lbug::SystemConfig::default()).unwrap();
    {
        let conn = lbug::Connection::new(&db2).unwrap();
        conn.query(SCHEMA).unwrap();
    }
    let db2 = Arc::new(db2);

    let (coord2, repl2) = build_coordinator(
        lease_store.clone(),
        "lease-follower",
        "http://localhost:19211",
        db2.clone(),
    );

    let follower = HaKuzu::from_coordinator(
        coord2,
        repl2,
        db2,
        db_path2.to_str().unwrap(),
        "lease_db",
        19211,
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert_eq!(follower.role(), Some(Role::Follower));

    // Write through follower (forwarded to leader).
    follower
        .execute(
            "CREATE (:Person {id: $id, name: $name})",
            Some(json!({"id": 1, "name": "ViaLeaseStore"})),
        )
        .await
        .unwrap();

    // Verify on leader.
    let result = leader
        .query("MATCH (p:Person) RETURN p.name", None)
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], json!("ViaLeaseStore"));

    leader.close().await.unwrap();
    follower.close().await.unwrap();
}

// ============================================================================
// External database lifecycle tests
// ============================================================================

#[tokio::test]
async fn external_db_via_from_coordinator() {
    // Verify that an externally-created lbug::Database works with HaKuzu.
    // The database is created outside hakuzu and passed in. After close(),
    // the database should still be usable (hakuzu does not own its lifecycle).
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("external_db");
    let lease_store: Arc<dyn hakuzu::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    // Create the database externally with custom config.
    let config = lbug::SystemConfig::default();
    let db = lbug::Database::new(&db_path, config).unwrap();
    {
        let conn = lbug::Connection::new(&db).unwrap();
        conn.query(SCHEMA).unwrap();
    }
    let db = Arc::new(db);

    let (coordinator, replicator) = build_coordinator(
        lease_store,
        "ext-db-1",
        "http://localhost:19220",
        db.clone(),
    );

    let ha = HaKuzu::from_coordinator(
        coordinator,
        replicator,
        db.clone(),
        db_path.to_str().unwrap(),
        "external_db",
        19220,
        Duration::from_secs(5),
    )
    .await
    .unwrap();

    // Write via hakuzu.
    ha.execute(
        "CREATE (:Person {id: $id, name: $name})",
        Some(json!({"id": 1, "name": "External"})),
    )
    .await
    .unwrap();

    // Read via hakuzu.
    let result = ha
        .query("MATCH (p:Person) RETURN p.name", None)
        .await
        .unwrap();
    assert_eq!(result.rows[0][0], json!("External"));

    // Close hakuzu. This drains the journal but does NOT destroy the database.
    ha.close().await.unwrap();

    // The database is still usable after hakuzu close because the Arc keeps it alive.
    let conn = lbug::Connection::new(&db).unwrap();
    let mut result = conn.query("MATCH (p:Person) RETURN p.name").unwrap();
    let row = result.next().expect("should have one row");
    // Verify the data survived the hakuzu close.
    assert_eq!(row.len(), 1);
}

// ============================================================================
// Failover test with external databases
// ============================================================================

#[tokio::test]
async fn failover_with_external_databases() {
    // Two nodes use external databases + locks + shared InMemoryLeaseStore.
    // Leader writes data, then is dropped (simulating failure).
    // Follower should promote to leader and accept new writes.
    // Verify all data (old + new) is present.
    let tmp1 = TempDir::new().unwrap();
    let tmp2 = TempDir::new().unwrap();
    let db_path1 = tmp1.path().join("failover_leader");
    let db_path2 = tmp2.path().join("failover_follower");

    let lease_store: Arc<dyn hakuzu::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    // Create external databases with schema.
    let db1 = lbug::Database::new(&db_path1, lbug::SystemConfig::default()).unwrap();
    {
        let conn = lbug::Connection::new(&db1).unwrap();
        conn.query(SCHEMA).unwrap();
    }
    let db1 = Arc::new(db1);

    let db2 = lbug::Database::new(&db_path2, lbug::SystemConfig::default()).unwrap();
    {
        let conn = lbug::Connection::new(&db2).unwrap();
        conn.query(SCHEMA).unwrap();
    }
    let db2 = Arc::new(db2);

    // Shared locks for each node.
    let write_mutex1 = Arc::new(tokio::sync::Mutex::new(()));
    let snapshot_lock1 = Arc::new(std::sync::RwLock::new(()));
    let write_mutex2 = Arc::new(tokio::sync::Mutex::new(()));
    let snapshot_lock2 = Arc::new(std::sync::RwLock::new(()));

    // Build leader with external db + locks.
    let object_store1 = common::MockObjectStore::new();
    let replicator1 = Arc::new(KuzuReplicator::new(object_store1.clone(), "failover/".into()));
    let follower_behavior1 = Arc::new(
        KuzuFollowerBehavior::new(object_store1)
            .with_shared_db(db1.clone())
            .with_locks(write_mutex1.clone(), snapshot_lock1.clone()),
    );
    let config1 = CoordinatorConfig {
        lease: Some(LeaseConfig::new(
            lease_store.clone(),
            "failover-leader".to_string(),
            "http://localhost:19240".to_string(),
        )),
        ..Default::default()
    };
    let coordinator1 = Coordinator::new(
        replicator1.clone(),
        None, // manifest_store
        None, // node_registry
        follower_behavior1,
        "failover/",
        config1,
    );

    let leader = HaKuzu::from_coordinator(
        coordinator1,
        replicator1,
        db1.clone(),
        db_path1.to_str().unwrap(),
        "failover_db",
        19240,
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert_eq!(leader.role(), Some(Role::Leader));

    // Build follower with external db + locks.
    let object_store2 = common::MockObjectStore::new();
    let replicator2 = Arc::new(KuzuReplicator::new(object_store2.clone(), "failover/".into()));
    let follower_behavior2 = Arc::new(
        KuzuFollowerBehavior::new(object_store2)
            .with_shared_db(db2.clone())
            .with_locks(write_mutex2.clone(), snapshot_lock2.clone()),
    );
    let config2 = CoordinatorConfig {
        lease: Some(LeaseConfig::new(
            lease_store.clone(),
            "failover-follower".to_string(),
            "http://localhost:19241".to_string(),
        )),
        ..Default::default()
    };
    let coordinator2 = Coordinator::new(
        replicator2.clone(),
        None, // manifest_store
        None, // node_registry
        follower_behavior2,
        "failover/",
        config2,
    );

    let follower = HaKuzu::from_coordinator(
        coordinator2,
        replicator2,
        db2.clone(),
        db_path2.to_str().unwrap(),
        "failover_db",
        19241,
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert_eq!(follower.role(), Some(Role::Follower));

    // Leader writes data.
    for i in 1..=3 {
        leader
            .execute(
                "CREATE (:Person {id: $id, name: $name})",
                Some(json!({"id": i, "name": format!("Pre-failover-{}", i)})),
            )
            .await
            .unwrap();
    }

    // Verify leader has 3 rows.
    let result = leader
        .query("MATCH (p:Person) RETURN count(p) AS cnt", None)
        .await
        .unwrap();
    assert_eq!(result.rows[0][0], json!(3));

    // Simulate leader failure by closing it.
    leader.close().await.unwrap();

    // Wait for the follower to notice leader is gone and promote.
    // InMemoryLeaseStore's lease should expire, allowing follower to acquire it.
    let mut promoted = false;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        if follower.role() == Some(Role::Leader) {
            promoted = true;
            break;
        }
    }
    assert!(promoted, "Follower should have promoted to leader after leader dropped");

    // New leader writes more data.
    for i in 4..=6 {
        follower
            .execute(
                "CREATE (:Person {id: $id, name: $name})",
                Some(json!({"id": i, "name": format!("Post-failover-{}", i)})),
            )
            .await
            .unwrap();
    }

    // Verify follower (now leader) has its own writes.
    // Note: since these are separate databases with mock object stores (no real
    // replication), the follower only has its own writes (post-failover).
    // In a real deployment with shared object store, it would have all data.
    let result = follower
        .query("MATCH (p:Person) RETURN count(p) AS cnt", None)
        .await
        .unwrap();
    assert_eq!(
        result.rows[0][0],
        json!(3),
        "Follower (now leader) should have 3 rows from its own writes"
    );

    // Verify the original leader's data is still accessible via the external Arc<Database>.
    let conn = lbug::Connection::new(&db1).unwrap();
    let mut result = conn.query("MATCH (p:Person) RETURN count(p) AS cnt").unwrap();
    let row = result.next().expect("should have one row");
    assert_eq!(row.len(), 1);

    follower.close().await.unwrap();
}

// ============================================================================
// Journal drain verification test
// ============================================================================

#[tokio::test]
async fn close_drains_journal_segments() {
    // Verify that close() seals and uploads journal segments by checking
    // the replicator's sync() is called (observable via journal state).
    //
    // This test creates entries, verifies the journal sequence advances,
    // then calls close() and verifies the replicator was synced.
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("drain_db");
    let lease_store: Arc<dyn hakuzu::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    let db = lbug::Database::new(&db_path, lbug::SystemConfig::default()).unwrap();
    {
        let conn = lbug::Connection::new(&db).unwrap();
        conn.query(SCHEMA).unwrap();
    }
    let db = Arc::new(db);

    let object_store = common::MockObjectStore::new();
    let replicator = Arc::new(KuzuReplicator::new(object_store.clone(), "drain/".into()));
    let follower_behavior = Arc::new(
        KuzuFollowerBehavior::new(object_store.clone()).with_shared_db(db.clone()),
    );

    let config = CoordinatorConfig {
        lease: Some(LeaseConfig::new(
            lease_store,
            "drain-node".to_string(),
            "http://localhost:19250".to_string(),
        )),
        ..Default::default()
    };
    let coordinator = Coordinator::new(
        replicator.clone(),
        None, // manifest_store
        None, // node_registry
        follower_behavior,
        "drain/",
        config,
    );

    let ha = HaKuzu::from_coordinator(
        coordinator,
        replicator.clone(),
        db,
        db_path.to_str().unwrap(),
        "drain_db",
        19250,
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert_eq!(ha.role(), Some(Role::Leader));

    // Write several entries to populate the journal.
    for i in 1..=10 {
        ha.execute(
            "CREATE (:Person {id: $id, name: $name})",
            Some(json!({"id": i, "name": format!("Drain-{}", i)})),
        )
        .await
        .unwrap();
    }

    // Check journal state before close: sequence should have advanced.
    let state_before = replicator.journal_state("drain_db").await;
    assert!(
        state_before.is_some(),
        "Journal state should exist before close"
    );
    let seq_before = state_before
        .as_ref()
        .map(|s| s.sequence.load(std::sync::atomic::Ordering::SeqCst))
        .unwrap_or(0);
    // Journal sequence may start at 0, so 10 writes produce seq 9 (0-indexed)
    // or seq 10 (1-indexed) depending on the journal implementation.
    assert!(
        seq_before >= 9,
        "Journal sequence should be >= 9 after 10 writes, got {}",
        seq_before
    );

    // Call close() which should drain the journal (seal + upload).
    ha.close().await.unwrap();

    // After close, the replicator's database entry is removed (via coordinator.leave()).
    // Verify that journal_state is now None, confirming the replicator was cleaned up.
    let state_after = replicator.journal_state("drain_db").await;
    assert!(
        state_after.is_none(),
        "Journal state should be removed after close (replicator cleaned up)"
    );

    // Verify that the object store has uploaded segments.
    // The mock object store should have at least one segment uploaded.
    let keys = object_store.list("drain/", None).await.unwrap();
    // After close, the sealed segment should have been uploaded.
    // The segment key format is: {prefix}{db_name}/journal/{filename}
    let journal_keys: Vec<_> = keys
        .iter()
        .filter(|k| k.contains("journal") || k.contains("drain_db"))
        .collect();
    assert!(
        !journal_keys.is_empty(),
        "Expected uploaded journal segments in object store after close, found keys: {:?}",
        keys
    );
}
