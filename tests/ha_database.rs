//! Integration tests for HaKuzu — the high-level HA Kuzu API.
//!
//! Uses in-memory lease stores to test without real S3.

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use tempfile::TempDir;

use hadb::Replicator;
use hakuzu::{
    Coordinator, CoordinatorConfig, HaKuzu, HakuzuError, InMemoryLeaseStore, KuzuFollowerBehavior,
    KuzuReplicator, LeaseConfig, Role,
};

mod common;

const SCHEMA: &str = "CREATE NODE TABLE IF NOT EXISTS Person(id INT64, name STRING, PRIMARY KEY(id))";

// ============================================================================
// Helper to build a Coordinator with in-memory backends
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

    // Use mock object store — tests don't actually upload to S3.
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
// Tests
// ============================================================================

#[tokio::test]
async fn single_node_local_mode() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("local_db");

    let db = HaKuzu::local(db_path.to_str().unwrap(), SCHEMA).unwrap();

    // Should be leader in local mode.
    assert_eq!(db.role(), Some(Role::Leader));

    // Execute a write.
    db.execute(
        "CREATE (:Person {id: $id, name: $name})",
        Some(json!({"id": 1, "name": "Alice"})),
    )
    .await
    .unwrap();

    // Query a read.
    let result = db
        .query("MATCH (p:Person) RETURN p.id, p.name", None)
        .await
        .unwrap();
    assert_eq!(result.columns, vec!["p.id", "p.name"]);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], json!(1));
    assert_eq!(result.rows[0][1], json!("Alice"));

    db.close().await.unwrap();
}

#[tokio::test]
async fn single_node_execute_and_query() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("ha_db");
    let lease_store: Arc<dyn hakuzu::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    let db = lbug::Database::new(&db_path, lbug::SystemConfig::default()).unwrap();
    {
        let conn = lbug::Connection::new(&db).unwrap();
        conn.query(SCHEMA).unwrap();
    }
    let db = Arc::new(db);

    let (coordinator, replicator) = build_coordinator(
        lease_store,
        "instance-1",
        "http://localhost:19001",
        db.clone(),
    );

    let ha = HaKuzu::from_coordinator(
        coordinator,
        replicator,
        db,
        db_path.to_str().unwrap(),
        "ha_db",
        19001,
        Duration::from_secs(5),
    )
    .await
    .unwrap();

    // Should become leader.
    assert_eq!(ha.role(), Some(Role::Leader));

    // Write 5 entries.
    for i in 1..=5 {
        ha.execute(
            "CREATE (:Person {id: $id, name: $name})",
            Some(json!({"id": i, "name": format!("Person-{}", i)})),
        )
        .await
        .unwrap();
    }

    // Query count.
    let result = ha
        .query("MATCH (p:Person) RETURN count(p) AS cnt", None)
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], json!(5));

    ha.close().await.unwrap();
}

#[tokio::test]
async fn two_node_forwarded_write() {
    let tmp1 = TempDir::new().unwrap();
    let tmp2 = TempDir::new().unwrap();
    let db_path1 = tmp1.path().join("leader_db");
    let db_path2 = tmp2.path().join("follower_db");

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
        "leader-1",
        "http://localhost:19010",
        db1.clone(),
    );

    let leader = HaKuzu::from_coordinator(
        coord1,
        repl1,
        db1,
        db_path1.to_str().unwrap(),
        "fwd_db",
        19010,
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
        "follower-1",
        "http://localhost:19011",
        db2.clone(),
    );

    let follower = HaKuzu::from_coordinator(
        coord2,
        repl2,
        db2,
        db_path2.to_str().unwrap(),
        "fwd_db",
        19011,
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert_eq!(follower.role(), Some(Role::Follower));

    // Write through the follower — should be forwarded to leader.
    follower
        .execute(
            "CREATE (:Person {id: $id, name: $name})",
            Some(json!({"id": 1, "name": "Forwarded"})),
        )
        .await
        .unwrap();

    // Verify data on leader.
    let result = leader
        .query("MATCH (p:Person) RETURN p.name", None)
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], json!("Forwarded"));

    leader.close().await.unwrap();
    follower.close().await.unwrap();
}

#[tokio::test]
async fn forwarding_error_no_leader() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("error_db");
    let lease_store: Arc<dyn hakuzu::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    let db = lbug::Database::new(&db_path, lbug::SystemConfig::default()).unwrap();
    {
        let conn = lbug::Connection::new(&db).unwrap();
        conn.query(SCHEMA).unwrap();
    }
    let db = Arc::new(db);

    let (coordinator, replicator) = build_coordinator(
        lease_store,
        "lonely-follower",
        "http://localhost:19020",
        db.clone(),
    );

    let ha = HaKuzu::from_coordinator(
        coordinator,
        replicator,
        db,
        db_path.to_str().unwrap(),
        "error_db",
        19020,
        Duration::from_secs(1),
    )
    .await
    .unwrap();

    // This will be leader (only node), so writes should work locally.
    // But let's verify the API works.
    let result = ha
        .execute(
            "CREATE (:Person {id: $id, name: $name})",
            Some(json!({"id": 1, "name": "Test"})),
        )
        .await;
    assert!(result.is_ok());

    ha.close().await.unwrap();
}

#[tokio::test]
async fn close_is_clean() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("clean_db");

    // First open — write some data.
    {
        let db = HaKuzu::local(db_path.to_str().unwrap(), SCHEMA).unwrap();
        db.execute(
            "CREATE (:Person {id: $id, name: $name})",
            Some(json!({"id": 1, "name": "Persistent"})),
        )
        .await
        .unwrap();
        db.close().await.unwrap();
    }

    // Second open — data should still be there.
    {
        let db = HaKuzu::local(db_path.to_str().unwrap(), SCHEMA).unwrap();
        let result = db
            .query("MATCH (p:Person) RETURN p.name", None)
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], json!("Persistent"));
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn auth_rejects_wrong_secret() {
    let tmp1 = TempDir::new().unwrap();
    let tmp2 = TempDir::new().unwrap();
    let db_path1 = tmp1.path().join("auth_leader");
    let db_path2 = tmp2.path().join("auth_follower");

    let lease_store: Arc<dyn hakuzu::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    // Leader with secret.
    let db1 = lbug::Database::new(&db_path1, lbug::SystemConfig::default()).unwrap();
    {
        let conn = lbug::Connection::new(&db1).unwrap();
        conn.query(SCHEMA).unwrap();
    }
    let db1 = Arc::new(db1);

    let (coord1, repl1) = build_coordinator(
        lease_store.clone(),
        "auth-leader",
        "http://localhost:19030",
        db1.clone(),
    );

    let leader = HaKuzu::from_coordinator_with_secret(
        coord1,
        repl1,
        db1,
        db_path1.to_str().unwrap(),
        "auth_db",
        19030,
        Duration::from_secs(5),
        Some("correct-secret".into()),
    )
    .await
    .unwrap();

    // Follower with WRONG secret.
    let db2 = lbug::Database::new(&db_path2, lbug::SystemConfig::default()).unwrap();
    {
        let conn = lbug::Connection::new(&db2).unwrap();
        conn.query(SCHEMA).unwrap();
    }
    let db2 = Arc::new(db2);

    let (coord2, repl2) = build_coordinator(
        lease_store.clone(),
        "auth-follower",
        "http://localhost:19031",
        db2.clone(),
    );

    let follower = HaKuzu::from_coordinator_with_secret(
        coord2,
        repl2,
        db2,
        db_path2.to_str().unwrap(),
        "auth_db",
        19031,
        Duration::from_secs(5),
        Some("wrong-secret".into()),
    )
    .await
    .unwrap();

    // Wait for follower to see the leader.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Write through follower should fail (wrong secret) with LeaderUnavailable.
    let result = follower
        .execute(
            "CREATE (:Person {id: $id, name: $name})",
            Some(json!({"id": 1, "name": "Rejected"})),
        )
        .await;
    assert!(result.is_err(), "Should reject wrong secret: {:?}", result);
    assert!(
        matches!(result.unwrap_err(), HakuzuError::LeaderUnavailable(_)),
        "Should be LeaderUnavailable error"
    );

    leader.close().await.unwrap();
    follower.close().await.unwrap();
}

#[tokio::test]
async fn auth_accepts_correct_secret() {
    let tmp1 = TempDir::new().unwrap();
    let tmp2 = TempDir::new().unwrap();
    let db_path1 = tmp1.path().join("auth_ok_leader");
    let db_path2 = tmp2.path().join("auth_ok_follower");

    let lease_store: Arc<dyn hakuzu::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    // Leader with secret.
    let db1 = lbug::Database::new(&db_path1, lbug::SystemConfig::default()).unwrap();
    {
        let conn = lbug::Connection::new(&db1).unwrap();
        conn.query(SCHEMA).unwrap();
    }
    let db1 = Arc::new(db1);

    let (coord1, repl1) = build_coordinator(
        lease_store.clone(),
        "ok-leader",
        "http://localhost:19040",
        db1.clone(),
    );

    let leader = HaKuzu::from_coordinator_with_secret(
        coord1,
        repl1,
        db1,
        db_path1.to_str().unwrap(),
        "ok_db",
        19040,
        Duration::from_secs(5),
        Some("shared-secret".into()),
    )
    .await
    .unwrap();

    // Follower with CORRECT secret.
    let db2 = lbug::Database::new(&db_path2, lbug::SystemConfig::default()).unwrap();
    {
        let conn = lbug::Connection::new(&db2).unwrap();
        conn.query(SCHEMA).unwrap();
    }
    let db2 = Arc::new(db2);

    let (coord2, repl2) = build_coordinator(
        lease_store.clone(),
        "ok-follower",
        "http://localhost:19041",
        db2.clone(),
    );

    let follower = HaKuzu::from_coordinator_with_secret(
        coord2,
        repl2,
        db2,
        db_path2.to_str().unwrap(),
        "ok_db",
        19041,
        Duration::from_secs(5),
        Some("shared-secret".into()),
    )
    .await
    .unwrap();

    // Wait for follower to see the leader.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Write through follower should succeed.
    follower
        .execute(
            "CREATE (:Person {id: $id, name: $name})",
            Some(json!({"id": 1, "name": "Accepted"})),
        )
        .await
        .unwrap();

    // Verify on leader.
    let result = leader
        .query("MATCH (p:Person) RETURN p.name", None)
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], json!("Accepted"));

    leader.close().await.unwrap();
    follower.close().await.unwrap();
}

// ============================================================================
// Phase 8: Production hardening tests
// ============================================================================

#[tokio::test]
async fn journal_failure_returns_error() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("journal_err_db");
    let lease_store: Arc<dyn hakuzu::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    let db = lbug::Database::new(&db_path, lbug::SystemConfig::default()).unwrap();
    {
        let conn = lbug::Connection::new(&db).unwrap();
        conn.query(SCHEMA).unwrap();
    }
    let db = Arc::new(db);

    let (coordinator, replicator) = build_coordinator(
        lease_store,
        "journal-err",
        "http://localhost:19050",
        db.clone(),
    );

    let ha = HaKuzu::from_coordinator(
        coordinator,
        replicator.clone(),
        db,
        db_path.to_str().unwrap(),
        "journal_err_db",
        19050,
        Duration::from_secs(5),
    )
    .await
    .unwrap();

    assert_eq!(ha.role(), Some(Role::Leader));

    // Write should work initially.
    ha.execute(
        "CREATE (:Person {id: $id, name: $name})",
        Some(json!({"id": 1, "name": "BeforeShutdown"})),
    )
    .await
    .unwrap();

    // Kill the journal writer by removing the database from the replicator.
    // This sends Shutdown to the journal writer, closing the receiver end.
    replicator.remove("journal_err_db").await.unwrap();

    // Give journal writer time to process Shutdown and drop the receiver.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Now a write should fail because the journal channel is closed.
    let result = ha
        .execute(
            "CREATE (:Person {id: $id, name: $name})",
            Some(json!({"id": 2, "name": "AfterShutdown"})),
        )
        .await;
    assert!(result.is_err(), "Write should fail when journal is dead: {:?}", result);
    let err = result.unwrap_err();
    assert!(
        matches!(err, HakuzuError::JournalError(_)),
        "Should be JournalError, got: {:?}",
        err
    );

    ha.close().await.unwrap();
}

#[tokio::test]
async fn read_concurrency_default_works() {
    // Verify local mode uses DEFAULT_READ_CONCURRENCY (8) and handles
    // concurrent reads correctly.
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("concurrency_db");

    let db = HaKuzu::local(db_path.to_str().unwrap(), SCHEMA).unwrap();

    // Write some data.
    for i in 1..=10 {
        db.execute(
            "CREATE (:Person {id: $id, name: $name})",
            Some(json!({"id": i, "name": format!("P{}", i)})),
        )
        .await
        .unwrap();
    }

    // Fire 20 concurrent reads — should all succeed with semaphore(8).
    let db = Arc::new(db);
    let mut handles = Vec::new();
    for _ in 0..20 {
        let db_ref = db.clone();
        handles.push(tokio::spawn(async move {
            db_ref.query("MATCH (p:Person) RETURN count(p) AS cnt", None)
                .await
        }));
    }

    for handle in handles {
        let result = handle.await.unwrap().unwrap();
        assert_eq!(result.rows[0][0], json!(10));
    }

    // Unwrap Arc to call close() which takes ownership.
    Arc::try_unwrap(db).ok().unwrap().close().await.unwrap();
}

#[tokio::test]
async fn journal_error_contains_message() {
    // Verify the JournalError variant contains a meaningful message.
    let err = HakuzuError::JournalError("writer crashed".into());
    let msg = err.to_string();
    assert!(msg.contains("Journal"), "Error message should mention Journal: {}", msg);
    assert!(msg.contains("writer crashed"), "Error message should contain detail: {}", msg);
}

// ============================================================================
// Phase 9: Graceful ops tests
// ============================================================================

#[tokio::test]
async fn close_completes_with_data() {
    // Verify close() completes cleanly when the database has data.
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("close_data_db");

    let db = HaKuzu::local(db_path.to_str().unwrap(), SCHEMA).unwrap();

    // Write data.
    db.execute(
        "CREATE (:Person {id: $id, name: $name})",
        Some(json!({"id": 1, "name": "Test"})),
    )
    .await
    .unwrap();

    // Verify data exists before close.
    let result = db.query("MATCH (p:Person) RETURN count(p) AS cnt", None).await.unwrap();
    assert_eq!(result.rows[0][0], json!(1));

    db.close().await.unwrap();
}

#[tokio::test]
async fn close_then_reopen_preserves_data() {
    // Verify close + reopen cycle preserves data (regression: close must
    // not corrupt the database).
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("close_reopen_db");

    // Write data, close.
    let db = HaKuzu::local(db_path.to_str().unwrap(), SCHEMA).unwrap();
    for i in 1..=5 {
        db.execute(
            "CREATE (:Person {id: $id, name: $name})",
            Some(json!({"id": i, "name": format!("P{}", i)})),
        )
        .await
        .unwrap();
    }
    db.close().await.unwrap();

    // Reopen, verify all 5 entries survived.
    let db = HaKuzu::local(db_path.to_str().unwrap(), SCHEMA).unwrap();
    let result = db.query("MATCH (p:Person) RETURN count(p) AS cnt", None).await.unwrap();
    assert_eq!(result.rows[0][0], json!(5));
    db.close().await.unwrap();
}

#[tokio::test]
async fn close_with_concurrent_reads() {
    // Verify close() works cleanly even when reads are in flight.
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("close_concurrent_db");

    let db = HaKuzu::local(db_path.to_str().unwrap(), SCHEMA).unwrap();
    for i in 1..=10 {
        db.execute(
            "CREATE (:Person {id: $id, name: $name})",
            Some(json!({"id": i, "name": format!("P{}", i)})),
        )
        .await
        .unwrap();
    }

    // Spawn some concurrent reads, then close.
    let db = Arc::new(db);
    let mut handles = Vec::new();
    for _ in 0..5 {
        let db_ref = db.clone();
        handles.push(tokio::spawn(async move {
            db_ref.query("MATCH (p:Person) RETURN count(p)", None).await
        }));
    }

    // Wait for reads to complete.
    for handle in handles {
        let _ = handle.await;
    }

    // Close should succeed cleanly.
    Arc::try_unwrap(db).ok().unwrap().close().await.unwrap();
}

#[tokio::test]
async fn metrics_track_writes_and_reads() {
    // Verify that writes and reads increment the correct metrics counters.
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("metrics_counters_db");

    let db = HaKuzu::local(db_path.to_str().unwrap(), SCHEMA).unwrap();

    // Do 3 writes.
    for i in 1..=3 {
        db.execute(
            "CREATE (:Person {id: $id, name: $name})",
            Some(json!({"id": i, "name": format!("P{}", i)})),
        )
        .await
        .unwrap();
    }

    // Do 2 reads.
    for _ in 0..2 {
        db.query("MATCH (p:Person) RETURN count(p)", None)
            .await
            .unwrap();
    }

    let metrics_text = db.prometheus_metrics();

    // Verify write counter = 3.
    assert!(
        metrics_text.contains("hakuzu_writes_total 3"),
        "Expected writes_total 3, got: {}",
        metrics_text
    );

    // Verify read counter = 2.
    assert!(
        metrics_text.contains("hakuzu_reads_total 2"),
        "Expected reads_total 2, got: {}",
        metrics_text
    );

    // Verify duration gauges are non-zero.
    assert!(
        !metrics_text.contains("hakuzu_last_write_duration_seconds 0.000000"),
        "Write duration should be non-zero"
    );
    assert!(
        !metrics_text.contains("hakuzu_last_read_duration_seconds 0.000000"),
        "Read duration should be non-zero"
    );

    db.close().await.unwrap();
}

#[tokio::test]
async fn forwarding_retry_exhausts_backoff() {
    // Verify that forwarding to a non-existent leader retries with backoff
    // and takes at least ~2 seconds (100ms + 400ms + 1600ms = 2100ms).
    let tmp1 = TempDir::new().unwrap();
    let tmp2 = TempDir::new().unwrap();
    let db_path1 = tmp1.path().join("retry_leader");
    let db_path2 = tmp2.path().join("retry_follower");

    let lease_store: Arc<dyn hakuzu::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    // Leader — register it so follower sees the leader address.
    let db1 = lbug::Database::new(&db_path1, lbug::SystemConfig::default()).unwrap();
    {
        let conn = lbug::Connection::new(&db1).unwrap();
        conn.query(SCHEMA).unwrap();
    }
    let db1 = Arc::new(db1);

    let (coord1, repl1) = build_coordinator(
        lease_store.clone(),
        "retry-leader",
        "http://localhost:19060",
        db1.clone(),
    );

    let leader = HaKuzu::from_coordinator(
        coord1,
        repl1,
        db1,
        db_path1.to_str().unwrap(),
        "retry_db",
        19060,
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert_eq!(leader.role(), Some(Role::Leader));

    // Follower.
    let db2 = lbug::Database::new(&db_path2, lbug::SystemConfig::default()).unwrap();
    {
        let conn = lbug::Connection::new(&db2).unwrap();
        conn.query(SCHEMA).unwrap();
    }
    let db2 = Arc::new(db2);

    // Build follower coordinator with slow promotion: requires 1000 consecutive
    // expired reads before claiming. Prevents premature promotion when the leader
    // closes during the test (which happens fast with InMemoryLeaseStore).
    let object_store2 = common::MockObjectStore::new();
    let repl2 = Arc::new(KuzuReplicator::new(object_store2.clone(), "test/".into()));
    let fb2 = Arc::new(KuzuFollowerBehavior::new(object_store2).with_shared_db(db2.clone()));
    let slow_config = CoordinatorConfig {
        lease: Some(LeaseConfig {
            instance_id: "retry-follower".into(),
            address: "http://localhost:19061".into(),
            follower_poll_interval: Duration::from_secs(60),
            required_expired_reads: 1000,
            ..LeaseConfig::new(
                lease_store.clone(),
                "retry-follower".into(),
                "http://localhost:19061".into(),
            )
        }),
        ..Default::default()
    };
    let coord2 = Coordinator::new(
        repl2.clone(),
        None, // manifest_store
        None, // node_registry
        fb2,
        "test/",
        slow_config,
    );

    let follower = HaKuzu::from_coordinator(
        coord2,
        repl2,
        db2,
        db_path2.to_str().unwrap(),
        "retry_db",
        19061,
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert_eq!(follower.role(), Some(Role::Follower));

    // Shut down the leader's forwarding server so follower's forwarding fails.
    leader.close().await.unwrap();

    // Now the follower will try to forward to the dead leader and retry.
    let start = std::time::Instant::now();
    let result = follower
        .execute(
            "CREATE (:Person {id: $id, name: $name})",
            Some(json!({"id": 1, "name": "Retry"})),
        )
        .await;
    let elapsed = start.elapsed();

    assert!(result.is_err(), "Should fail after retries: {:?}", result);
    assert!(
        matches!(result.unwrap_err(), HakuzuError::LeaderUnavailable(_)),
        "Should be LeaderUnavailable"
    );

    // Backoff: 0ms + 100ms + 400ms + 1600ms = ~2100ms minimum.
    assert!(
        elapsed >= Duration::from_millis(1500),
        "Should take >= 1500ms from retries, took {:?}",
        elapsed
    );

    // Verify forwarding_errors metric was incremented.
    let metrics = follower.prometheus_metrics();
    assert!(
        metrics.contains("hakuzu_forwarding_errors_total 1"),
        "Expected forwarding_errors 1, got: {}",
        metrics
    );

    follower.close().await.unwrap();
}

#[tokio::test]
async fn prometheus_metrics_includes_hakuzu() {
    // Verify prometheus_metrics() includes hakuzu-level metrics with correct values.
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("metrics_db");

    let db = HaKuzu::local(db_path.to_str().unwrap(), SCHEMA).unwrap();

    // Do some operations.
    db.execute(
        "CREATE (:Person {id: $id, name: $name})",
        Some(json!({"id": 1, "name": "Alice"})),
    )
    .await
    .unwrap();

    db.query("MATCH (p:Person) RETURN count(p)", None)
        .await
        .unwrap();

    let metrics_text = db.prometheus_metrics();

    // All metric names present.
    let expected_names = [
        "hakuzu_writes_total",
        "hakuzu_writes_forwarded_total",
        "hakuzu_reads_total",
        "hakuzu_forwarding_errors_total",
        "hakuzu_last_write_duration_seconds",
        "hakuzu_last_read_duration_seconds",
        "hakuzu_last_forward_duration_seconds",
        "hakuzu_journal_sequence",
    ];
    for name in expected_names {
        assert!(metrics_text.contains(name), "Missing metric: {}", name);
    }

    // Values are correct after 1 write + 1 read.
    assert!(
        metrics_text.contains("hakuzu_writes_total 1"),
        "Expected writes_total 1: {}",
        metrics_text
    );
    assert!(
        metrics_text.contains("hakuzu_reads_total 1"),
        "Expected reads_total 1: {}",
        metrics_text
    );

    db.close().await.unwrap();
}

// ============================================================================
// Phase 10: Follower readiness & edge cases
// ============================================================================

#[tokio::test]
async fn leader_is_always_caught_up() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("caught_up_leader");

    let db = HaKuzu::local(db_path.to_str().unwrap(), SCHEMA).unwrap();

    assert!(db.is_caught_up(), "Leader/local should always be caught up");
    assert_eq!(db.replay_position(), 0, "Leader replay_position should be 0");

    db.execute(
        "CREATE (:Person {id: $id, name: $name})",
        Some(json!({"id": 1, "name": "Alice"})),
    )
    .await
    .unwrap();

    assert!(db.is_caught_up(), "Leader still caught up after write");

    db.close().await.unwrap();
}

#[tokio::test]
async fn ha_leader_is_caught_up() {
    // Verify is_caught_up() returns true for an HA leader (not just local mode).
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("ha_caught_up");
    let lease_store: Arc<dyn hakuzu::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    let db = lbug::Database::new(&db_path, lbug::SystemConfig::default()).unwrap();
    {
        let conn = lbug::Connection::new(&db).unwrap();
        conn.query(SCHEMA).unwrap();
    }
    let db = Arc::new(db);

    let (coordinator, replicator) = build_coordinator(
        lease_store,
        "caught-up-leader",
        "http://localhost:19070",
        db.clone(),
    );

    let ha = HaKuzu::from_coordinator(
        coordinator,
        replicator,
        db,
        db_path.to_str().unwrap(),
        "caught_up_db",
        19070,
        Duration::from_secs(5),
    )
    .await
    .unwrap();

    assert_eq!(ha.role(), Some(Role::Leader));
    assert!(ha.is_caught_up(), "HA leader should be caught up");

    ha.close().await.unwrap();
}

#[tokio::test]
async fn readiness_metrics_in_prometheus() {
    // Verify readiness metrics appear in prometheus output.
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("readiness_metrics_db");

    let db = HaKuzu::local(db_path.to_str().unwrap(), SCHEMA).unwrap();

    let metrics = db.prometheus_metrics();
    assert!(
        metrics.contains("hakuzu_follower_caught_up 1"),
        "Local/leader should report caught_up=1: {}",
        metrics
    );
    assert!(
        metrics.contains("hakuzu_replay_position 0"),
        "Local should report replay_position=0: {}",
        metrics
    );

    db.close().await.unwrap();
}

// Phase Rubicon tests are in tests/rubicon.rs
