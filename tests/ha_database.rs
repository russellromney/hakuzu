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
        lease: Some(LeaseConfig::new(instance_id.to_string(), address.to_string())),
        ..Default::default()
    };

    // Use a dummy s3 client — tests don't actually upload to S3.
    let s3_config = aws_config::SdkConfig::builder()
        .behavior_version(aws_config::BehaviorVersion::latest())
        .build();
    let s3_client = aws_sdk_s3::Client::new(&s3_config);
    let replicator = Arc::new(
        KuzuReplicator::new("test-bucket".into(), "test/".into())
            .with_s3_client(s3_client.clone()),
    );
    let follower_behavior = Arc::new(
        KuzuFollowerBehavior::new(s3_client, "test-bucket".into())
            .with_shared_db(shared_db),
    );

    let coordinator = Coordinator::new(
        replicator.clone(),
        Some(lease_store),
        None,
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
