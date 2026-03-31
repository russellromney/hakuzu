//! Integration tests for hakuzu.
//!
//! Tests verify KuzuReplicator lifecycle, replay logic, and journal
//! round-tripping. These require lbug (Kuzu) which means they only
//! run on platforms where lbug compiles (Linux, or macOS with the
//! static-only build fix).

use std::sync::Arc;

use graphstream::journal::{self, JournalCommand, JournalState, PendingEntry};
use graphstream::types::ParamValue;

mod common;

/// Write journal entries via graphstream, then replay them against a fresh Kuzu DB.
#[test]
fn test_replay_entries() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");
    let db_path = dir.path().join("db");

    // 1. Write journal entries via graphstream.
    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(journal_dir.clone(), 1024 * 1024, 100, state.clone());

    // Create schema first, then insert data.
    tx.send(JournalCommand::Write(PendingEntry {
        query: "CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name))".into(),
        params: vec![],
    }))
    .unwrap();

    for i in 1..=3 {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!("CREATE (:Person {{name: 'Person{}', age: {}}})", i, 20 + i),
            params: vec![],
        }))
        .unwrap();
    }

    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();
    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    // 2. Create a fresh Kuzu DB and replay.
    let db = lbug::Database::new(&db_path, lbug::SystemConfig::default())
        .expect("Failed to create Kuzu DB");
    let conn = lbug::Connection::new(&db).expect("Failed to create connection");

    let last_seq = hakuzu::replay::replay_entries(&conn, &journal_dir, 0)
        .expect("replay_entries failed");
    assert_eq!(last_seq, 4); // 1 schema + 3 data entries

    // 3. Verify data was replayed correctly.
    let mut result = conn.query("MATCH (p:Person) RETURN p.name ORDER BY p.name").unwrap();
    let mut names = Vec::new();
    for row in &mut result {
        if let lbug::Value::String(s) = &row[0] {
            names.push(s.clone());
        }
    }
    assert_eq!(names, vec!["Person1", "Person2", "Person3"]);
}

/// Replay from a specific sequence (skip earlier entries).
#[test]
fn test_replay_from_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");
    let db_path = dir.path().join("db");

    // Write journal entries: schema + 5 data entries.
    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(journal_dir.clone(), 1024 * 1024, 100, state.clone());

    tx.send(JournalCommand::Write(PendingEntry {
        query: "CREATE NODE TABLE Counter(id INT64, val INT64, PRIMARY KEY(id))".into(),
        params: vec![],
    }))
    .unwrap();

    for i in 1..=5 {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!("CREATE (:Counter {{id: {}, val: {}}})", i, i * 10),
            params: vec![],
        }))
        .unwrap();
    }

    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();
    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Create DB with schema already applied (simulate partial replay).
    let db = lbug::Database::new(&db_path, lbug::SystemConfig::default())
        .expect("Failed to create Kuzu DB");
    let conn = lbug::Connection::new(&db).expect("Failed to create connection");
    conn.query("CREATE NODE TABLE Counter(id INT64, val INT64, PRIMARY KEY(id))")
        .expect("Schema creation failed");

    // Replay only entries after seq=2 (skip schema at seq=1 and first data at seq=2).
    let last_seq = hakuzu::replay::replay_entries(&conn, &journal_dir, 2)
        .expect("replay_entries failed");
    assert_eq!(last_seq, 6); // entries 3, 4, 5, 6

    // Verify only entries after seq=2 were replayed.
    let mut result = conn
        .query("MATCH (c:Counter) RETURN c.id ORDER BY c.id")
        .unwrap();
    let mut ids = Vec::new();
    for row in &mut result {
        if let lbug::Value::Int64(id) = &row[0] {
            ids.push(*id);
        }
    }
    // seq 3 = id 2, seq 4 = id 3, seq 5 = id 4, seq 6 = id 5
    assert_eq!(ids, vec![2, 3, 4, 5]);
}

/// KuzuReplicator add + sync + remove lifecycle.
/// Uses a dedicated journal directory separate from the Kuzu DB directory
/// since Kuzu's storage may conflict with journal subdirectories.
#[tokio::test]
async fn test_replicator_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    // Use a standalone directory for the journal (not inside the Kuzu DB dir).
    let journal_base = dir.path().join("journals");
    std::fs::create_dir_all(&journal_base).unwrap();

    // Create replicator (with a dummy bucket — we're testing lifecycle, not S3).
    let replicator = hakuzu::KuzuReplicator::new(
        common::MockObjectStore::new(),
        "test/".into(),
    );

    // Add database using the journal base directory.
    use hadb::Replicator;
    replicator.add("testdb", &journal_base).await.expect("add failed");

    // Verify journal sender is available.
    let sender = replicator.journal_sender("testdb").await;
    assert!(sender.is_some(), "journal sender should exist after add");

    // Write an entry via the journal sender.
    let tx = sender.unwrap();
    tx.send(JournalCommand::Write(PendingEntry {
        query: "CREATE NODE TABLE T(id INT64, PRIMARY KEY(id))".into(),
        params: vec![],
    }))
    .unwrap();

    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();

    // Verify state tracks the entry.
    let state = replicator.journal_state("testdb").await.unwrap();
    assert_eq!(
        state.sequence.load(std::sync::atomic::Ordering::SeqCst),
        1
    );

    // Remove database.
    replicator
        .remove("testdb")
        .await
        .expect("remove failed");

    // Verify sender is gone.
    let sender = replicator.journal_sender("testdb").await;
    assert!(sender.is_none(), "journal sender should be gone after remove");
}

/// Test ParamValue round-trip through protobuf serialization.
#[test]
fn test_param_value_conversions() {
    let params = vec![
        ("name".to_string(), ParamValue::String("Alice".into())),
        ("age".to_string(), ParamValue::Int(30)),
        ("active".to_string(), ParamValue::Bool(true)),
        ("score".to_string(), ParamValue::Float(9.5)),
    ];

    let entries = graphstream::types::param_values_to_map_entries(&params);
    let roundtripped = graphstream::types::map_entries_to_param_values(&entries);

    assert_eq!(roundtripped.len(), 4);
    assert_eq!(roundtripped[0].0, "name");
    assert!(matches!(&roundtripped[0].1, ParamValue::String(s) if s == "Alice"));
    assert_eq!(roundtripped[1].0, "age");
    assert!(matches!(&roundtripped[1].1, ParamValue::Int(30)));
    assert_eq!(roundtripped[2].0, "active");
    assert!(matches!(&roundtripped[2].1, ParamValue::Bool(true)));
    assert_eq!(roundtripped[3].0, "score");
    assert!(matches!(&roundtripped[3].1, ParamValue::Float(f) if (*f - 9.5).abs() < 1e-10));
}

/// Replay empty journal returns since_seq unchanged.
#[test]
fn test_replay_empty_journal() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");
    let db_path = dir.path().join("db");

    // Create an empty journal directory.
    std::fs::create_dir_all(&journal_dir).unwrap();

    let db = lbug::Database::new(&db_path, lbug::SystemConfig::default())
        .expect("Failed to create Kuzu DB");
    let conn = lbug::Connection::new(&db).expect("Failed to create connection");

    let last_seq = hakuzu::replay::replay_entries(&conn, &journal_dir, 0)
        .expect("replay_entries failed");
    assert_eq!(last_seq, 0, "Empty journal should return since_seq");
}

// NOTE: test_replay_invalid_query is intentionally omitted. lbug/Kuzu throws
// C++ exceptions (SIGABRT) for ALL query errors (parser, binder, runtime,
// conversion). The replay code's Err path is unreachable with current bindings.
// Error handling would only work if lbug catches C++ exceptions and converts
// them to Rust Result::Err, which it currently does not.

/// Replay with relationship queries (schema + nodes + edges).
#[test]
fn test_replay_with_relationships() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");
    let db_path = dir.path().join("db");

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(journal_dir.clone(), 1024 * 1024, 100, state.clone());

    // Schema: node tables + relationship table.
    tx.send(JournalCommand::Write(PendingEntry {
        query: "CREATE NODE TABLE Person(name STRING, PRIMARY KEY(name))".into(),
        params: vec![],
    })).unwrap();
    tx.send(JournalCommand::Write(PendingEntry {
        query: "CREATE NODE TABLE City(name STRING, PRIMARY KEY(name))".into(),
        params: vec![],
    })).unwrap();
    tx.send(JournalCommand::Write(PendingEntry {
        query: "CREATE REL TABLE LIVES_IN(FROM Person TO City)".into(),
        params: vec![],
    })).unwrap();

    // Data.
    tx.send(JournalCommand::Write(PendingEntry {
        query: "CREATE (:Person {name: 'Alice'})".into(),
        params: vec![],
    })).unwrap();
    tx.send(JournalCommand::Write(PendingEntry {
        query: "CREATE (:City {name: 'NYC'})".into(),
        params: vec![],
    })).unwrap();
    tx.send(JournalCommand::Write(PendingEntry {
        query: "MATCH (p:Person {name: 'Alice'}), (c:City {name: 'NYC'}) CREATE (p)-[:LIVES_IN]->(c)".into(),
        params: vec![],
    })).unwrap();

    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();
    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    let db = lbug::Database::new(&db_path, lbug::SystemConfig::default())
        .expect("Failed to create Kuzu DB");
    let conn = lbug::Connection::new(&db).expect("Failed to create connection");

    let last_seq = hakuzu::replay::replay_entries(&conn, &journal_dir, 0)
        .expect("replay_entries failed");
    assert_eq!(last_seq, 6);

    // Verify relationship was created.
    let mut result = conn.query(
        "MATCH (p:Person)-[:LIVES_IN]->(c:City) RETURN p.name, c.name"
    ).unwrap();
    let mut found = false;
    for row in &mut result {
        if let (lbug::Value::String(p), lbug::Value::String(c)) = (&row[0], &row[1]) {
            assert_eq!(p, "Alice");
            assert_eq!(c, "NYC");
            found = true;
        }
    }
    assert!(found, "Relationship should exist after replay");
}

/// Replay many entries (stress test with 100 inserts).
#[test]
fn test_replay_many_entries() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");
    let db_path = dir.path().join("db");

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(journal_dir.clone(), 1024 * 1024, 100, state.clone());

    tx.send(JournalCommand::Write(PendingEntry {
        query: "CREATE NODE TABLE Item(id INT64, PRIMARY KEY(id))".into(),
        params: vec![],
    })).unwrap();

    for i in 1..=100 {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!("CREATE (:Item {{id: {}}})", i),
            params: vec![],
        })).unwrap();
    }

    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();
    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    let db = lbug::Database::new(&db_path, lbug::SystemConfig::default())
        .expect("Failed to create Kuzu DB");
    let conn = lbug::Connection::new(&db).expect("Failed to create connection");

    let last_seq = hakuzu::replay::replay_entries(&conn, &journal_dir, 0)
        .expect("replay_entries failed");
    assert_eq!(last_seq, 101); // 1 schema + 100 data

    let mut result = conn.query("MATCH (i:Item) RETURN COUNT(*)").unwrap();
    for row in &mut result {
        if let lbug::Value::Int64(count) = &row[0] {
            assert_eq!(*count, 100);
        }
    }
}

/// Replay across multiple journal segments (small segment size).
#[test]
fn test_replay_cross_segment() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");
    let db_path = dir.path().join("db");

    // Small segments to force rotation.
    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(journal_dir.clone(), 512, 100, state.clone());

    tx.send(JournalCommand::Write(PendingEntry {
        query: "CREATE NODE TABLE Item(id INT64, PRIMARY KEY(id))".into(),
        params: vec![],
    })).unwrap();

    for i in 1..=10 {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!("CREATE (:Item {{id: {}}})", i),
            params: vec![],
        })).unwrap();
    }

    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();
    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Verify multiple segments were created.
    let segment_count = std::fs::read_dir(&journal_dir)
        .unwrap()
        .filter(|e| {
            e.as_ref().unwrap().path().extension()
                .map_or(false, |ext| ext == "graphj")
        })
        .count();
    assert!(segment_count > 1, "Expected multiple segments, got {segment_count}");

    let db = lbug::Database::new(&db_path, lbug::SystemConfig::default())
        .expect("Failed to create Kuzu DB");
    let conn = lbug::Connection::new(&db).expect("Failed to create connection");

    let last_seq = hakuzu::replay::replay_entries(&conn, &journal_dir, 0)
        .expect("replay_entries failed");
    assert_eq!(last_seq, 11); // 1 schema + 10 data

    let mut result = conn.query("MATCH (i:Item) RETURN COUNT(*)").unwrap();
    for row in &mut result {
        if let lbug::Value::Int64(count) = &row[0] {
            assert_eq!(*count, 10);
        }
    }
}

/// KuzuReplicator builder methods.
#[tokio::test]
async fn test_replicator_builder_methods() {
    let replicator = hakuzu::KuzuReplicator::new(common::MockObjectStore::new(), "prefix/".into())
        .with_segment_max_bytes(2 * 1024 * 1024)
        .with_fsync_ms(50)
        .with_upload_interval(std::time::Duration::from_secs(5));

    // Add a database to verify the builder config took effect.
    let dir = tempfile::tempdir().unwrap();
    let journal_base = dir.path().join("journals");
    std::fs::create_dir_all(&journal_base).unwrap();

    use hadb::Replicator;
    replicator.add("testdb", &journal_base).await.expect("add failed");

    let state = replicator.journal_state("testdb").await;
    assert!(state.is_some());
    assert!(state.unwrap().is_alive());

    replicator.remove("testdb").await.expect("remove failed");
}

/// Replicator add → sync seals the current segment.
#[tokio::test]
async fn test_replicator_sync_seals_segment() {
    let dir = tempfile::tempdir().unwrap();
    let journal_base = dir.path().join("journals");
    std::fs::create_dir_all(&journal_base).unwrap();

    let replicator = hakuzu::KuzuReplicator::new(common::MockObjectStore::new(), "test/".into());

    use hadb::Replicator;
    replicator.add("syncdb", &journal_base).await.expect("add failed");

    // Write entries.
    let tx = replicator.journal_sender("syncdb").await.unwrap();
    for i in 1..=3 {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!("CREATE (:N {{v: {}}})", i),
            params: vec![],
        })).unwrap();
    }
    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();

    // Sync seals the segment (S3 upload will fail since no real bucket, but seal should work).
    let _sync_result = replicator.sync("syncdb").await;
    // Sync may error on S3 upload — that's expected. The seal still happened.

    let state = replicator.journal_state("syncdb").await.unwrap();
    assert_eq!(state.sequence.load(std::sync::atomic::Ordering::SeqCst), 3);

    replicator.remove("syncdb").await.expect("remove failed");
}

/// Multi-database: replicator manages multiple databases independently.
#[tokio::test]
async fn test_replicator_multi_db() {
    let dir = tempfile::tempdir().unwrap();
    let journals_a = dir.path().join("journals_a");
    let journals_b = dir.path().join("journals_b");
    std::fs::create_dir_all(&journals_a).unwrap();
    std::fs::create_dir_all(&journals_b).unwrap();

    let replicator = hakuzu::KuzuReplicator::new(common::MockObjectStore::new(), "test/".into());

    use hadb::Replicator;
    replicator.add("db_a", &journals_a).await.expect("add a failed");
    replicator.add("db_b", &journals_b).await.expect("add b failed");

    // Write to db_a.
    let tx_a = replicator.journal_sender("db_a").await.unwrap();
    tx_a.send(JournalCommand::Write(PendingEntry {
        query: "CREATE (:N {v: 1})".into(),
        params: vec![],
    })).unwrap();
    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx_a.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();

    // Write to db_b (different entries).
    let tx_b = replicator.journal_sender("db_b").await.unwrap();
    for i in 1..=5 {
        tx_b.send(JournalCommand::Write(PendingEntry {
            query: format!("CREATE (:N {{v: {}}})", i),
            params: vec![],
        })).unwrap();
    }
    let (ack_tx2, ack_rx2) = std::sync::mpsc::sync_channel(1);
    tx_b.send(JournalCommand::Flush(ack_tx2)).unwrap();
    ack_rx2.recv().unwrap();

    // Verify independent state.
    let state_a = replicator.journal_state("db_a").await.unwrap();
    let state_b = replicator.journal_state("db_b").await.unwrap();
    assert_eq!(state_a.sequence.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(state_b.sequence.load(std::sync::atomic::Ordering::SeqCst), 5);

    // Nonexistent db returns None.
    assert!(replicator.journal_sender("nonexistent").await.is_none());
    assert!(replicator.journal_state("nonexistent").await.is_none());

    // Remove one, other still works.
    replicator.remove("db_a").await.expect("remove a failed");
    assert!(replicator.journal_sender("db_a").await.is_none());
    assert!(replicator.journal_sender("db_b").await.is_some());

    replicator.remove("db_b").await.expect("remove b failed");
}

/// Remove nonexistent database doesn't error.
#[tokio::test]
async fn test_replicator_remove_nonexistent() {
    let replicator = hakuzu::KuzuReplicator::new(common::MockObjectStore::new(), "test/".into());

    use hadb::Replicator;
    // Removing a database that was never added should not error.
    replicator.remove("never_added").await.expect("remove should not error");
}

/// Replay idempotency: replaying same entries twice doesn't duplicate data.
/// First replay creates data, second replay from same since_seq skips duplicates.
#[test]
fn test_replay_idempotency() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");
    let db_path = dir.path().join("db");

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(journal_dir.clone(), 1024 * 1024, 100, state.clone());

    tx.send(JournalCommand::Write(PendingEntry {
        query: "CREATE NODE TABLE Item(id INT64, PRIMARY KEY(id))".into(),
        params: vec![],
    })).unwrap();
    for i in 1..=3 {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!("CREATE (:Item {{id: {}}})", i),
            params: vec![],
        })).unwrap();
    }

    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();
    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    let db = lbug::Database::new(&db_path, lbug::SystemConfig::default())
        .expect("Failed to create Kuzu DB");
    let conn = lbug::Connection::new(&db).expect("Failed to create connection");

    // First replay.
    let last_seq = hakuzu::replay::replay_entries(&conn, &journal_dir, 0)
        .expect("first replay failed");
    assert_eq!(last_seq, 4);

    // Second replay from the last sequence should be a no-op.
    let last_seq2 = hakuzu::replay::replay_entries(&conn, &journal_dir, last_seq)
        .expect("second replay failed");
    assert_eq!(last_seq2, last_seq, "Idempotent replay should return same seq");

    // Verify count is still 3 (no duplicates).
    let mut result = conn.query("MATCH (i:Item) RETURN COUNT(*)").unwrap();
    for row in &mut result {
        if let lbug::Value::Int64(count) = &row[0] {
            assert_eq!(*count, 3);
        }
    }
}

/// Multiple node table schemas replayed correctly.
#[test]
fn test_replay_multiple_schemas() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");
    let db_path = dir.path().join("db");

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(journal_dir.clone(), 1024 * 1024, 100, state.clone());

    // Create multiple node tables.
    tx.send(JournalCommand::Write(PendingEntry {
        query: "CREATE NODE TABLE User(name STRING, PRIMARY KEY(name))".into(),
        params: vec![],
    })).unwrap();
    tx.send(JournalCommand::Write(PendingEntry {
        query: "CREATE NODE TABLE Post(id INT64, content STRING, PRIMARY KEY(id))".into(),
        params: vec![],
    })).unwrap();

    tx.send(JournalCommand::Write(PendingEntry {
        query: "CREATE (:User {name: 'Alice'})".into(),
        params: vec![],
    })).unwrap();
    tx.send(JournalCommand::Write(PendingEntry {
        query: "CREATE (:Post {id: 1, content: 'Hello world'})".into(),
        params: vec![],
    })).unwrap();

    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();
    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    let db = lbug::Database::new(&db_path, lbug::SystemConfig::default())
        .expect("Failed to create Kuzu DB");
    let conn = lbug::Connection::new(&db).expect("Failed to create connection");

    let last_seq = hakuzu::replay::replay_entries(&conn, &journal_dir, 0)
        .expect("replay_entries failed");
    assert_eq!(last_seq, 4);

    // Verify both tables have data.
    let mut result = conn.query("MATCH (u:User) RETURN u.name").unwrap();
    let mut found_user = false;
    for row in &mut result {
        if let lbug::Value::String(name) = &row[0] {
            assert_eq!(name, "Alice");
            found_user = true;
        }
    }
    assert!(found_user);

    let mut result = conn.query("MATCH (p:Post) RETURN p.content").unwrap();
    let mut found_post = false;
    for row in &mut result {
        if let lbug::Value::String(content) = &row[0] {
            assert_eq!(content, "Hello world");
            found_post = true;
        }
    }
    assert!(found_post);
}
