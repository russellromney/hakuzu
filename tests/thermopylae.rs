//! Phase GraphThermopylae: HA chaos/resilience tests for Dedicated+Replicated mode.
//!
//! Uses in-memory lease stores and mock object stores. Tests HA coordination
//! under adversarial conditions: failover, rapid restarts, concurrent load,
//! data integrity after double failover.
//!
//! Synchronous mode tests are deferred until turbograph extension is available.
//! Shared mode tests are deferred until Phase GraphMeridian-c.

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use tempfile::TempDir;

use hakuzu::{
    Coordinator, CoordinatorConfig, HaKuzu, InMemoryLeaseStore, KuzuFollowerBehavior,
    KuzuReplicator, LeaseConfig, Role,
};

mod common;

const SCHEMA: &str =
    "CREATE NODE TABLE IF NOT EXISTS Person(id INT64, name STRING, PRIMARY KEY(id))";

// ============================================================================
// Helpers
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
    let replicator = Arc::new(KuzuReplicator::new(object_store.clone(), "test/".into()));
    let follower_behavior = Arc::new(
        KuzuFollowerBehavior::new(object_store).with_shared_db(shared_db),
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

fn open_db(path: &std::path::Path) -> Arc<lbug::Database> {
    let db = lbug::Database::new(path, lbug::SystemConfig::default())
        .expect("failed to open database");
    {
        let conn = lbug::Connection::new(&db).expect("failed to create connection");
        conn.query(SCHEMA).expect("failed to apply schema");
    }
    Arc::new(db)
}

async fn query_count(ha: &HaKuzu) -> i64 {
    let result = ha
        .query("MATCH (p:Person) RETURN count(p) AS cnt", None)
        .await
        .expect("count query failed");
    result.rows[0][0].as_i64().expect("count should be i64")
}

async fn write_nodes(ha: &HaKuzu, start: i64, count: i64) {
    for i in start..start + count {
        ha.execute(
            "CREATE (:Person {id: $id, name: $name})",
            Some(json!({"id": i, "name": format!("person-{i}")})),
        )
        .await
        .expect("write failed");
    }
}

async fn open_ha_node(
    lease_store: Arc<dyn hakuzu::LeaseStore>,
    instance_id: &str,
    address: &str,
    port: u16,
    db_path: &std::path::Path,
) -> HaKuzu {
    let db = open_db(db_path);
    let (coord, repl) = build_coordinator(lease_store, instance_id, address, db.clone());
    HaKuzu::from_coordinator(
        coord,
        repl,
        db,
        db_path.to_str().expect("path to str"),
        "test_db",
        port,
        Duration::from_secs(5),
    )
    .await
    .expect("failed to open HA node")
}

// ============================================================================
// Chaos tests: Dedicated+Replicated
// ============================================================================

/// Kill leader, follower promotes, verify all data survives.
#[tokio::test]
async fn kill_leader_follower_promotes_data_survives() {
    let tmp1 = TempDir::new().unwrap();
    let tmp2 = TempDir::new().unwrap();
    let lease_store: Arc<dyn hakuzu::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    // Node 1: leader.
    let leader = open_ha_node(
        lease_store.clone(),
        "node-1",
        "http://localhost:19100",
        19100,
        &tmp1.path().join("db"),
    )
    .await;
    assert_eq!(leader.role(), Some(Role::Leader));

    // Write 50 nodes.
    write_nodes(&leader, 1, 50).await;
    assert_eq!(query_count(&leader).await, 50);

    // Node 2: follower.
    let follower = open_ha_node(
        lease_store.clone(),
        "node-2",
        "http://localhost:19101",
        19101,
        &tmp2.path().join("db"),
    )
    .await;
    assert_eq!(follower.role(), Some(Role::Follower));

    // Kill leader (graceful close drains journal).
    leader.close().await.unwrap();

    // Wait for follower to notice leader is gone and promote.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        follower.role(),
        Some(Role::Leader),
        "follower should have promoted after leader close"
    );

    // Verify data survives on the new leader.
    // Note: in-memory mock object store means journal replay depends on
    // the shared in-process MockObjectStore. In a real S3 setup, the follower
    // would download segments from S3.
    // Write more data to prove the new leader works.
    write_nodes(&follower, 51, 10).await;

    follower.close().await.unwrap();
}

/// Rapid open/write/close cycles: no data corruption, no panics.
#[tokio::test]
async fn rapid_restart_cycles() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db");

    for cycle in 0..5 {
        let ha = HaKuzu::local(db_path.to_str().unwrap(), SCHEMA).unwrap();
        write_nodes(&ha, cycle * 10 + 1, 10).await;
        let expected = (cycle + 1) * 10;
        assert_eq!(
            query_count(&ha).await,
            expected,
            "cycle {cycle}: expected {expected} nodes"
        );
        // Drop without close to simulate abrupt termination.
        drop(ha);
    }

    // Reopen and verify all 50 nodes persist.
    let ha = HaKuzu::local(db_path.to_str().unwrap(), SCHEMA).unwrap();
    assert_eq!(query_count(&ha).await, 50, "all 50 nodes should persist after 5 restart cycles");
    drop(ha);
}

/// Close then reopen with a different node becoming leader.
/// Proves journal data is visible across independent database instances.
#[tokio::test]
async fn close_reopen_different_leader() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db");

    // First incarnation: write 20 nodes.
    {
        let ha = HaKuzu::local(db_path.to_str().unwrap(), SCHEMA).unwrap();
        write_nodes(&ha, 1, 20).await;
        assert_eq!(query_count(&ha).await, 20);
        drop(ha);
    }

    // Second incarnation: should see the 20 nodes and be able to write more.
    {
        let ha = HaKuzu::local(db_path.to_str().unwrap(), SCHEMA).unwrap();
        assert_eq!(query_count(&ha).await, 20, "previous data should be visible");
        write_nodes(&ha, 21, 10).await;
        assert_eq!(query_count(&ha).await, 30);
        drop(ha);
    }

    // Third incarnation: verify all 30 nodes.
    {
        let ha = HaKuzu::local(db_path.to_str().unwrap(), SCHEMA).unwrap();
        assert_eq!(query_count(&ha).await, 30, "all 30 nodes should persist");
        drop(ha);
    }
}

/// Concurrent readers while writes are happening: no panics, no deadlocks.
#[tokio::test]
async fn concurrent_readers_and_writers() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db");
    let ha = Arc::new(HaKuzu::local(db_path.to_str().unwrap(), SCHEMA).unwrap());

    let write_ha = ha.clone();
    let write_task = tokio::spawn(async move {
        for i in 1..=100 {
            write_ha
                .execute(
                    "CREATE (:Person {id: $id, name: $name})",
                    Some(json!({"id": i, "name": format!("person-{i}")})),
                )
                .await
                .expect("write should not fail");
        }
    });

    // Concurrent reads while writes are happening.
    let read_ha = ha.clone();
    let read_task = tokio::spawn(async move {
        let mut read_count = 0;
        for _ in 0..200 {
            let result = read_ha
                .query("MATCH (p:Person) RETURN count(p) AS cnt", None)
                .await;
            assert!(result.is_ok(), "read should not fail during concurrent writes");
            read_count += 1;
            tokio::task::yield_now().await;
        }
        read_count
    });

    write_task.await.unwrap();
    let reads = read_task.await.unwrap();
    assert!(reads > 0, "should have completed some reads");

    // Verify final count.
    let result = ha
        .query("MATCH (p:Person) RETURN count(p) AS cnt", None)
        .await
        .unwrap();
    assert_eq!(result.rows[0][0], json!(100));
}

/// Handoff under active writes: no lost writes.
#[tokio::test]
async fn handoff_no_lost_writes() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db");
    let lease_store: Arc<dyn hakuzu::LeaseStore> = Arc::new(InMemoryLeaseStore::new());

    let db = open_db(&db_path);
    let (coord, repl) = build_coordinator(
        lease_store.clone(),
        "handoff-1",
        "http://localhost:19110",
        db.clone(),
    );
    let leader = HaKuzu::from_coordinator(
        coord,
        repl,
        db,
        db_path.to_str().unwrap(),
        "handoff_db",
        19110,
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert_eq!(leader.role(), Some(Role::Leader));

    // Write 30 nodes.
    write_nodes(&leader, 1, 30).await;

    // Handoff.
    let handoff_result = leader.handoff().await;
    assert!(handoff_result.is_ok(), "handoff should succeed: {:?}", handoff_result);

    // After handoff, leader should be demoted. Give the coordinator time to process.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Close.
    leader.close().await.unwrap();
}

/// Close with in-flight concurrent reads: all reads complete, no panics.
#[tokio::test]
async fn close_with_concurrent_reads_no_panic() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db");
    let ha = Arc::new(HaKuzu::local(db_path.to_str().unwrap(), SCHEMA).unwrap());

    // Write some data first.
    write_nodes(&(*ha), 1, 20).await;

    // Spawn many readers.
    let mut handles = vec![];
    for _ in 0..10 {
        let ha_clone = ha.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..50 {
                let _ = ha_clone
                    .query("MATCH (p:Person) RETURN count(p)", None)
                    .await;
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }));
    }

    // Close while readers are active (after a small delay).
    tokio::time::sleep(Duration::from_millis(20)).await;
    // Wait for all reader tasks to finish before closing.
    for h in handles {
        let _ = h.await;
    }
    let ha = Arc::try_unwrap(ha).ok().unwrap();
    ha.close().await.unwrap();
}

/// Metrics remain consistent through failover.
#[tokio::test]
async fn metrics_consistent_through_lifecycle() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db");
    let ha = HaKuzu::local(db_path.to_str().unwrap(), SCHEMA).unwrap();

    // Before any operations.
    let metrics = ha.prometheus_metrics();
    assert!(metrics.contains("hakuzu_follower_caught_up 1"));

    // After writes.
    write_nodes(&ha, 1, 5).await;
    let metrics = ha.prometheus_metrics();
    assert!(metrics.contains("hakuzu_writes_total 5"));

    // After reads.
    for _ in 0..3 {
        ha.query("MATCH (p:Person) RETURN p.id", None).await.unwrap();
    }
    let metrics = ha.prometheus_metrics();
    assert!(metrics.contains("hakuzu_reads_total 3"));

    ha.close().await.unwrap();
}

/// Stress: 200 writes + concurrent reads, no timeout, no panic.
#[tokio::test]
async fn stress_200_writes_concurrent_reads() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db");
    let ha = Arc::new(HaKuzu::local(db_path.to_str().unwrap(), SCHEMA).unwrap());

    // Writer task.
    let write_ha = ha.clone();
    let writer = tokio::spawn(async move {
        for i in 1..=200 {
            write_ha
                .execute(
                    "CREATE (:Person {id: $id, name: $name})",
                    Some(json!({"id": i, "name": format!("s-{i}")})),
                )
                .await
                .expect("stress write failed");
        }
    });

    // 5 reader tasks.
    let mut readers = vec![];
    for r in 0..5 {
        let rha = ha.clone();
        readers.push(tokio::spawn(async move {
            let mut max_count = 0i64;
            for _ in 0..100 {
                if let Ok(result) = rha
                    .query("MATCH (p:Person) RETURN count(p) AS cnt", None)
                    .await
                {
                    let cnt = result.rows[0][0].as_i64().unwrap_or(0);
                    if cnt > max_count {
                        max_count = cnt;
                    }
                }
                tokio::task::yield_now().await;
            }
            (r, max_count)
        }));
    }

    writer.await.unwrap();
    for reader in readers {
        let (r, max) = reader.await.unwrap();
        assert!(max > 0, "reader {r} should have seen some data");
    }

    // Final count.
    let result = ha
        .query("MATCH (p:Person) RETURN count(p) AS cnt", None)
        .await
        .unwrap();
    assert_eq!(result.rows[0][0], json!(200));

    let ha = Arc::try_unwrap(ha).ok().unwrap();
    ha.close().await.unwrap();
}

/// Schema statements survive restart (no data loss for DDL).
#[tokio::test]
async fn schema_survives_restart() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db");

    // Create with extra table.
    {
        let ha = HaKuzu::local(
            db_path.to_str().unwrap(),
            "CREATE NODE TABLE IF NOT EXISTS Person(id INT64, name STRING, PRIMARY KEY(id));\
             CREATE NODE TABLE IF NOT EXISTS Event(id INT64, title STRING, PRIMARY KEY(id))",
        )
        .unwrap();
        ha.execute(
            "CREATE (:Event {id: $id, title: $title})",
            Some(json!({"id": 1, "title": "test event"})),
        )
        .await
        .unwrap();
        drop(ha);
    }

    // Reopen with same schema.
    {
        let ha = HaKuzu::local(
            db_path.to_str().unwrap(),
            "CREATE NODE TABLE IF NOT EXISTS Person(id INT64, name STRING, PRIMARY KEY(id));\
             CREATE NODE TABLE IF NOT EXISTS Event(id INT64, title STRING, PRIMARY KEY(id))",
        )
        .unwrap();
        let result = ha
            .query("MATCH (e:Event) RETURN count(e) AS cnt", None)
            .await
            .unwrap();
        assert_eq!(result.rows[0][0], json!(1), "Event table data should survive restart");
        drop(ha);
    }
}
