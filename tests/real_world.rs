//! Real-world integration tests for hakuzu.
//!
//! These tests close the critical gaps in test coverage:
//! - Rewriter execution: verify rewritten queries actually execute against Kuzu
//! - Deterministic replay: write on "leader" DB, journal, replay on "follower" DB, compare
//! - Complex Cypher: MERGE, DELETE, relationships, typed params through full pipeline
//! - Concurrent writes: stress test single-writer serialization

use std::sync::Arc;

use graphstream::journal::{self, JournalCommand, JournalState, PendingEntry};

// ============================================================================
// Helpers
// ============================================================================

/// Execute a query on leader via rewriter (simulates execute_write_local path).
/// Returns the journal entry that would be written (rewritten query + merged params).
fn execute_rewritten(
    conn: &lbug::Connection,
    query: &str,
    params: Option<&serde_json::Value>,
) -> PendingEntry {
    let rewrite = hakuzu::rewriter::rewrite_query(query);
    let merged = hakuzu::rewriter::merge_params(params, &rewrite.generated_params);

    // Execute against Kuzu (same path as execute_write_local).
    if let Some(ref p) = merged {
        let lbug_params = hakuzu::values::json_params_to_lbug(p).expect("param conversion");
        let refs: Vec<(&str, lbug::Value)> = lbug_params
            .iter()
            .map(|(k, v)| (k.as_str(), v.clone()))
            .collect();
        let mut stmt = conn.prepare(&rewrite.query).expect("prepare failed");
        conn.execute(&mut stmt, refs).expect("execute failed");
    } else {
        conn.query(&rewrite.query).expect("query failed");
    }

    // Build the journal entry (same as execute_write_local).
    let gs_params = match &merged {
        Some(p) => hakuzu::values::json_params_to_graphstream(p),
        None => vec![],
    };
    PendingEntry {
        query: rewrite.query,
        params: gs_params,
    }
}

/// Write journal entries, flush, shutdown, return journal_dir.
fn write_journal_entries(
    base_dir: &std::path::Path,
    entries: Vec<PendingEntry>,
) -> std::path::PathBuf {
    let journal_dir = base_dir.join("journal");
    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(journal_dir.clone(), 1024 * 1024, 100, state);

    for entry in entries {
        tx.send(JournalCommand::Write(entry)).unwrap();
    }
    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();
    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    journal_dir
}

/// Query a single string column from Kuzu, return sorted results.
fn query_strings(conn: &lbug::Connection, cypher: &str) -> Vec<String> {
    let mut result = conn.query(cypher).expect("query failed");
    let mut values = Vec::new();
    for row in &mut result {
        if let lbug::Value::String(s) = &row[0] {
            values.push(s.clone());
        }
    }
    values.sort();
    values
}

/// Query a single i64 column, return sorted.
fn query_ints(conn: &lbug::Connection, cypher: &str) -> Vec<i64> {
    let mut result = conn.query(cypher).expect("query failed");
    let mut values = Vec::new();
    for row in &mut result {
        if let lbug::Value::Int64(v) = &row[0] {
            values.push(*v);
        }
    }
    values.sort();
    values
}

/// Query count(*) and return it.
fn query_count(conn: &lbug::Connection, cypher: &str) -> i64 {
    let mut result = conn.query(cypher).expect("query failed");
    for row in &mut result {
        if let lbug::Value::Int64(v) = &row[0] {
            return *v;
        }
    }
    panic!("No count returned from: {cypher}");
}

// ============================================================================
// 7a: Rewriter execution tests
// ============================================================================

/// gen_random_uuid() rewrite executes and stores a valid UUID.
#[test]
fn test_rewrite_uuid_executes() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("db");
    let db = lbug::Database::new(&db_path, lbug::SystemConfig::default()).unwrap();
    let conn = lbug::Connection::new(&db).unwrap();

    conn.query("CREATE NODE TABLE T(id STRING, PRIMARY KEY(id))").unwrap();

    execute_rewritten(&conn, "CREATE (:T {id: gen_random_uuid()})", None);

    let ids = query_strings(&conn, "MATCH (n:T) RETURN n.id");
    assert_eq!(ids.len(), 1);
    let id = &ids[0];
    assert_eq!(id.len(), 36, "UUID should be 36 chars: {id}");
    assert_eq!(&id[8..9], "-");
    assert_eq!(&id[13..14], "-");
    assert_eq!(&id[14..15], "4", "UUID v4 version nibble should be '4': {id}");
}

/// current_timestamp() rewrite executes and stores ISO 8601 timestamp.
#[test]
fn test_rewrite_timestamp_executes() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("db");
    let db = lbug::Database::new(&db_path, lbug::SystemConfig::default()).unwrap();
    let conn = lbug::Connection::new(&db).unwrap();

    conn.query("CREATE NODE TABLE T(id INT64, ts STRING, PRIMARY KEY(id))").unwrap();

    execute_rewritten(&conn, "CREATE (:T {id: 1, ts: current_timestamp()})", None);

    let timestamps = query_strings(&conn, "MATCH (n:T) RETURN n.ts");
    assert_eq!(timestamps.len(), 1);
    let ts = &timestamps[0];
    assert!(ts.ends_with('Z'), "Timestamp should end with Z: {ts}");
    assert!(ts.contains('T'), "Timestamp should contain T separator: {ts}");
    assert!(ts.len() >= 20, "Timestamp too short: {ts}");
}

/// current_date() rewrite executes and stores ISO 8601 date.
#[test]
fn test_rewrite_date_executes() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("db");
    let db = lbug::Database::new(&db_path, lbug::SystemConfig::default()).unwrap();
    let conn = lbug::Connection::new(&db).unwrap();

    conn.query("CREATE NODE TABLE T(id INT64, d STRING, PRIMARY KEY(id))").unwrap();

    execute_rewritten(&conn, "CREATE (:T {id: 1, d: current_date()})", None);

    let dates = query_strings(&conn, "MATCH (n:T) RETURN n.d");
    assert_eq!(dates.len(), 1);
    let d = &dates[0];
    assert_eq!(d.len(), 10, "Date should be 10 chars: {d}");
    assert_eq!(&d[4..5], "-");
    assert_eq!(&d[7..8], "-");
}

/// REMOVE n.prop rewrite executes as SET n.prop = NULL.
#[test]
fn test_rewrite_remove_executes() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("db");
    let db = lbug::Database::new(&db_path, lbug::SystemConfig::default()).unwrap();
    let conn = lbug::Connection::new(&db).unwrap();

    conn.query("CREATE NODE TABLE T(id INT64, val STRING, PRIMARY KEY(id))").unwrap();
    conn.query("CREATE (:T {id: 1, val: 'hello'})").unwrap();

    let vals = query_strings(&conn, "MATCH (n:T {id: 1}) RETURN n.val");
    assert_eq!(vals, vec!["hello"]);

    execute_rewritten(&conn, "MATCH (n:T {id: 1}) REMOVE n.val", None);

    let mut result = conn.query("MATCH (n:T {id: 1}) RETURN n.val").unwrap();
    for row in &mut result {
        match &row[0] {
            lbug::Value::Null(_) => {}
            lbug::Value::String(s) if s.is_empty() => {}
            other => panic!("Expected NULL after REMOVE, got: {:?}", other),
        }
    }
}

/// Multi-property REMOVE executes correctly.
#[test]
fn test_rewrite_remove_multi_executes() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("db");
    let db = lbug::Database::new(&db_path, lbug::SystemConfig::default()).unwrap();
    let conn = lbug::Connection::new(&db).unwrap();

    conn.query("CREATE NODE TABLE T(id INT64, a STRING, b STRING, PRIMARY KEY(id))").unwrap();
    conn.query("CREATE (:T {id: 1, a: 'x', b: 'y'})").unwrap();

    execute_rewritten(&conn, "MATCH (n:T {id: 1}) REMOVE n.a, n.b", None);

    let mut result = conn.query("MATCH (n:T {id: 1}) RETURN n.a, n.b").unwrap();
    for row in &mut result {
        for val in row.iter() {
            match val {
                lbug::Value::Null(_) => {}
                lbug::Value::String(s) if s.is_empty() => {}
                other => panic!("Expected NULL after multi-REMOVE, got: {:?}", other),
            }
        }
    }
}

/// Mixed: UUID + timestamp in same query, both params bind and execute.
#[test]
fn test_rewrite_mixed_functions_execute() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("db");
    let db = lbug::Database::new(&db_path, lbug::SystemConfig::default()).unwrap();
    let conn = lbug::Connection::new(&db).unwrap();

    conn.query("CREATE NODE TABLE T(id STRING, ts STRING, PRIMARY KEY(id))").unwrap();

    execute_rewritten(
        &conn,
        "CREATE (:T {id: gen_random_uuid(), ts: current_timestamp()})",
        None,
    );

    let mut result = conn.query("MATCH (n:T) RETURN n.id, n.ts").unwrap();
    let mut found = false;
    for row in &mut result {
        if let (lbug::Value::String(id), lbug::Value::String(ts)) = (&row[0], &row[1]) {
            assert_eq!(id.len(), 36, "UUID should be 36 chars");
            assert!(ts.ends_with('Z'), "Timestamp should end with Z");
            found = true;
        }
    }
    assert!(found, "Should find row with UUID + timestamp");
}

/// User params + generated params coexist correctly.
#[test]
fn test_rewrite_with_user_params_executes() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("db");
    let db = lbug::Database::new(&db_path, lbug::SystemConfig::default()).unwrap();
    let conn = lbug::Connection::new(&db).unwrap();

    conn.query("CREATE NODE TABLE T(id STRING, name STRING, ts STRING, PRIMARY KEY(id))").unwrap();

    let params = serde_json::json!({"name": "Alice"});
    execute_rewritten(
        &conn,
        "CREATE (:T {id: gen_random_uuid(), name: $name, ts: current_timestamp()})",
        Some(&params),
    );

    let names = query_strings(&conn, "MATCH (n:T) RETURN n.name");
    assert_eq!(names, vec!["Alice"]);

    let ids = query_strings(&conn, "MATCH (n:T) RETURN n.id");
    assert_eq!(ids.len(), 1);
    assert_eq!(ids[0].len(), 36);
}

// ============================================================================
// 7b: Leader/follower deterministic replay verification
// ============================================================================

/// gen_random_uuid(): leader and follower have identical UUIDs after replay.
#[test]
fn test_replay_deterministic_uuid() {
    let dir = tempfile::tempdir().unwrap();
    let leader_path = dir.path().join("leader");
    let follower_path = dir.path().join("follower");

    let schema = "CREATE NODE TABLE T(id STRING, PRIMARY KEY(id))";
    let leader_db = lbug::Database::new(&leader_path, lbug::SystemConfig::default()).unwrap();
    let follower_db = lbug::Database::new(&follower_path, lbug::SystemConfig::default()).unwrap();
    let leader = lbug::Connection::new(&leader_db).unwrap();
    leader.query(schema).unwrap();

    let mut entries = Vec::new();
    for _ in 0..3 {
        entries.push(execute_rewritten(&leader, "CREATE (:T {id: gen_random_uuid()})", None));
    }

    // Journal + replay on follower.
    let mut all = vec![PendingEntry { query: schema.into(), params: vec![] }];
    all.extend(entries);
    let journal_dir = write_journal_entries(dir.path(), all);

    let follower = lbug::Connection::new(&follower_db).unwrap();
    hakuzu::replay::replay_entries(&follower, &journal_dir, 0).unwrap();

    let leader_ids = query_strings(&leader, "MATCH (n:T) RETURN n.id");
    let follower_ids = query_strings(&follower, "MATCH (n:T) RETURN n.id");

    assert_eq!(leader_ids.len(), 3);
    assert_eq!(leader_ids, follower_ids, "UUIDs must match between leader and follower");
}

/// current_timestamp(): leader and follower have identical timestamps.
#[test]
fn test_replay_deterministic_timestamp() {
    let dir = tempfile::tempdir().unwrap();
    let leader_path = dir.path().join("leader");
    let follower_path = dir.path().join("follower");

    let schema = "CREATE NODE TABLE T(id INT64, ts STRING, PRIMARY KEY(id))";
    let leader_db = lbug::Database::new(&leader_path, lbug::SystemConfig::default()).unwrap();
    let follower_db = lbug::Database::new(&follower_path, lbug::SystemConfig::default()).unwrap();
    let leader = lbug::Connection::new(&leader_db).unwrap();
    leader.query(schema).unwrap();

    let entries = vec![
        execute_rewritten(&leader, "CREATE (:T {id: 1, ts: current_timestamp()})", None),
        execute_rewritten(&leader, "CREATE (:T {id: 2, ts: current_timestamp()})", None),
    ];

    let mut all = vec![PendingEntry { query: schema.into(), params: vec![] }];
    all.extend(entries);
    let journal_dir = write_journal_entries(dir.path(), all);

    let follower = lbug::Connection::new(&follower_db).unwrap();
    hakuzu::replay::replay_entries(&follower, &journal_dir, 0).unwrap();

    let leader_ts = query_strings(&leader, "MATCH (n:T) RETURN n.ts ORDER BY n.id");
    let follower_ts = query_strings(&follower, "MATCH (n:T) RETURN n.ts ORDER BY n.id");

    assert_eq!(leader_ts.len(), 2);
    assert_eq!(leader_ts, follower_ts, "Timestamps must match between leader and follower");
    for ts in &leader_ts {
        assert!(ts.ends_with('Z'));
    }
}

/// current_date(): leader and follower have identical dates.
#[test]
fn test_replay_deterministic_date() {
    let dir = tempfile::tempdir().unwrap();
    let leader_path = dir.path().join("leader");
    let follower_path = dir.path().join("follower");

    let schema = "CREATE NODE TABLE T(id INT64, d STRING, PRIMARY KEY(id))";
    let leader_db = lbug::Database::new(&leader_path, lbug::SystemConfig::default()).unwrap();
    let follower_db = lbug::Database::new(&follower_path, lbug::SystemConfig::default()).unwrap();
    let leader = lbug::Connection::new(&leader_db).unwrap();
    leader.query(schema).unwrap();

    let entries = vec![execute_rewritten(&leader, "CREATE (:T {id: 1, d: current_date()})", None)];

    let mut all = vec![PendingEntry { query: schema.into(), params: vec![] }];
    all.extend(entries);
    let journal_dir = write_journal_entries(dir.path(), all);

    let follower = lbug::Connection::new(&follower_db).unwrap();
    hakuzu::replay::replay_entries(&follower, &journal_dir, 0).unwrap();

    let leader_dates = query_strings(&leader, "MATCH (n:T) RETURN n.d");
    let follower_dates = query_strings(&follower, "MATCH (n:T) RETURN n.d");
    assert_eq!(leader_dates, follower_dates, "Dates must match");
    assert_eq!(leader_dates[0].len(), 10);
}

/// REMOVE rewrite: leader removes property, follower replays SET NULL, both NULL.
#[test]
fn test_replay_deterministic_remove() {
    let dir = tempfile::tempdir().unwrap();
    let leader_path = dir.path().join("leader");
    let follower_path = dir.path().join("follower");

    let schema = "CREATE NODE TABLE T(id INT64, val STRING, PRIMARY KEY(id))";
    let leader_db = lbug::Database::new(&leader_path, lbug::SystemConfig::default()).unwrap();
    let follower_db = lbug::Database::new(&follower_path, lbug::SystemConfig::default()).unwrap();
    let leader = lbug::Connection::new(&leader_db).unwrap();
    leader.query(schema).unwrap();

    let entries = vec![
        execute_rewritten(&leader, "CREATE (:T {id: 1, val: 'hello'})", None),
        execute_rewritten(&leader, "MATCH (n:T {id: 1}) REMOVE n.val", None),
    ];

    let mut all = vec![PendingEntry { query: schema.into(), params: vec![] }];
    all.extend(entries);
    let journal_dir = write_journal_entries(dir.path(), all);

    let follower = lbug::Connection::new(&follower_db).unwrap();
    hakuzu::replay::replay_entries(&follower, &journal_dir, 0).unwrap();

    for (label, conn) in [("leader", &leader), ("follower", &follower)] {
        let mut result = conn.query("MATCH (n:T {id: 1}) RETURN n.val").unwrap();
        for row in &mut result {
            match &row[0] {
                lbug::Value::Null(_) => {}
                lbug::Value::String(s) if s.is_empty() => {}
                other => panic!("{label}: Expected NULL after REMOVE, got: {:?}", other),
            }
        }
    }
}

/// Mixed sequence: plain writes + rewritten writes, full state comparison.
#[test]
fn test_replay_deterministic_mixed() {
    let dir = tempfile::tempdir().unwrap();
    let leader_path = dir.path().join("leader");
    let follower_path = dir.path().join("follower");

    let schema = "CREATE NODE TABLE T(id INT64, name STRING, uid STRING, ts STRING, PRIMARY KEY(id))";
    let leader_db = lbug::Database::new(&leader_path, lbug::SystemConfig::default()).unwrap();
    let follower_db = lbug::Database::new(&follower_path, lbug::SystemConfig::default()).unwrap();
    let leader = lbug::Connection::new(&leader_db).unwrap();
    leader.query(schema).unwrap();

    let p1 = serde_json::json!({"id": 1, "name": "Alice"});
    let p2 = serde_json::json!({"id": 2, "name": "Bob"});
    let p3 = serde_json::json!({"id": 3, "name": "Charlie"});
    let entries = vec![
        execute_rewritten(&leader, "CREATE (:T {id: $id, name: $name})", Some(&p1)),
        execute_rewritten(
            &leader,
            "CREATE (:T {id: $id, name: $name, uid: gen_random_uuid(), ts: current_timestamp()})",
            Some(&p2),
        ),
        execute_rewritten(&leader, "CREATE (:T {id: $id, name: $name})", Some(&p3)),
    ];

    let mut all = vec![PendingEntry { query: schema.into(), params: vec![] }];
    all.extend(entries);
    let journal_dir = write_journal_entries(dir.path(), all);

    let follower = lbug::Connection::new(&follower_db).unwrap();
    hakuzu::replay::replay_entries(&follower, &journal_dir, 0).unwrap();

    // Compare full state — use string queries for simplicity.
    for query in [
        "MATCH (n:T) RETURN n.name ORDER BY n.id",
        "MATCH (n:T) WHERE n.uid IS NOT NULL RETURN n.uid",
        "MATCH (n:T) WHERE n.ts IS NOT NULL RETURN n.ts",
    ] {
        let leader_result = query_strings(&leader, query);
        let follower_result = query_strings(&follower, query);
        assert_eq!(leader_result, follower_result, "Mismatch for query: {query}");
    }

    // Verify counts.
    assert_eq!(query_count(&leader, "MATCH (n:T) RETURN COUNT(*)"), 3);
    assert_eq!(query_count(&follower, "MATCH (n:T) RETURN COUNT(*)"), 3);
}

// ============================================================================
// 7c: Complex Cypher coverage
// ============================================================================

/// MERGE: create-or-match through rewriter + replay.
#[test]
fn test_complex_merge() {
    let dir = tempfile::tempdir().unwrap();
    let leader_path = dir.path().join("leader");
    let follower_path = dir.path().join("follower");

    let schema = "CREATE NODE TABLE T(id INT64, name STRING, PRIMARY KEY(id))";
    let leader_db = lbug::Database::new(&leader_path, lbug::SystemConfig::default()).unwrap();
    let follower_db = lbug::Database::new(&follower_path, lbug::SystemConfig::default()).unwrap();
    let leader = lbug::Connection::new(&leader_db).unwrap();
    leader.query(schema).unwrap();

    let p1 = serde_json::json!({"id": 1, "name": "Alice"});
    let p2 = serde_json::json!({"id": 1, "name": "Alice Updated"});
    let p3 = serde_json::json!({"id": 2, "name": "Bob"});

    let entries = vec![
        execute_rewritten(&leader, "MERGE (n:T {id: $id}) SET n.name = $name", Some(&p1)),
        execute_rewritten(&leader, "MERGE (n:T {id: $id}) SET n.name = $name", Some(&p2)),
        execute_rewritten(&leader, "MERGE (n:T {id: $id}) SET n.name = $name", Some(&p3)),
    ];

    let mut all = vec![PendingEntry { query: schema.into(), params: vec![] }];
    all.extend(entries);
    let journal_dir = write_journal_entries(dir.path(), all);

    let follower = lbug::Connection::new(&follower_db).unwrap();
    hakuzu::replay::replay_entries(&follower, &journal_dir, 0).unwrap();

    let leader_names = query_strings(&leader, "MATCH (n:T) RETURN n.name ORDER BY n.id");
    let follower_names = query_strings(&follower, "MATCH (n:T) RETURN n.name ORDER BY n.id");
    assert_eq!(leader_names, vec!["Alice Updated", "Bob"]);
    assert_eq!(leader_names, follower_names);
}

/// SET with multiple properties through replay.
#[test]
fn test_complex_set_multiple_properties() {
    let dir = tempfile::tempdir().unwrap();
    let leader_path = dir.path().join("leader");
    let follower_path = dir.path().join("follower");

    let schema = "CREATE NODE TABLE T(id INT64, a STRING, b INT64, c DOUBLE, PRIMARY KEY(id))";
    let leader_db = lbug::Database::new(&leader_path, lbug::SystemConfig::default()).unwrap();
    let follower_db = lbug::Database::new(&follower_path, lbug::SystemConfig::default()).unwrap();
    let leader = lbug::Connection::new(&leader_db).unwrap();
    leader.query(schema).unwrap();

    let p1 = serde_json::json!({"id": 1});
    let p2 = serde_json::json!({"a": "hello", "b": 42, "c": 3.14});
    let entries = vec![
        execute_rewritten(&leader, "CREATE (:T {id: $id})", Some(&p1)),
        execute_rewritten(&leader, "MATCH (n:T {id: 1}) SET n.a = $a, n.b = $b, n.c = $c", Some(&p2)),
    ];

    let mut all = vec![PendingEntry { query: schema.into(), params: vec![] }];
    all.extend(entries);
    let journal_dir = write_journal_entries(dir.path(), all);

    let follower = lbug::Connection::new(&follower_db).unwrap();
    hakuzu::replay::replay_entries(&follower, &journal_dir, 0).unwrap();

    for (label, conn) in [("leader", &leader), ("follower", &follower)] {
        let mut result = conn.query("MATCH (n:T {id: 1}) RETURN n.a, n.b, n.c").unwrap();
        for row in &mut result {
            if let lbug::Value::String(a) = &row[0] {
                assert_eq!(a, "hello", "{label}: wrong a");
            }
            if let lbug::Value::Int64(b) = &row[1] {
                assert_eq!(*b, 42, "{label}: wrong b");
            }
            if let lbug::Value::Double(c) = &row[2] {
                assert!((*c - 3.14).abs() < 0.001, "{label}: wrong c: {c}");
            }
        }
    }
}

/// DELETE nodes through rewriter + replay.
#[test]
fn test_complex_delete() {
    let dir = tempfile::tempdir().unwrap();
    let leader_path = dir.path().join("leader");
    let follower_path = dir.path().join("follower");

    let schema = "CREATE NODE TABLE T(id INT64, PRIMARY KEY(id))";
    let leader_db = lbug::Database::new(&leader_path, lbug::SystemConfig::default()).unwrap();
    let follower_db = lbug::Database::new(&follower_path, lbug::SystemConfig::default()).unwrap();
    let leader = lbug::Connection::new(&leader_db).unwrap();
    leader.query(schema).unwrap();

    let mut entries = Vec::new();
    for i in 1..=5 {
        let p = serde_json::json!({"id": i});
        entries.push(execute_rewritten(&leader, "CREATE (:T {id: $id})", Some(&p)));
    }
    entries.push(execute_rewritten(&leader, "MATCH (n:T {id: 3}) DELETE n", None));

    let mut all = vec![PendingEntry { query: schema.into(), params: vec![] }];
    all.extend(entries);
    let journal_dir = write_journal_entries(dir.path(), all);

    let follower = lbug::Connection::new(&follower_db).unwrap();
    hakuzu::replay::replay_entries(&follower, &journal_dir, 0).unwrap();

    let leader_ids = query_ints(&leader, "MATCH (n:T) RETURN n.id ORDER BY n.id");
    let follower_ids = query_ints(&follower, "MATCH (n:T) RETURN n.id ORDER BY n.id");
    assert_eq!(leader_ids, vec![1, 2, 4, 5]);
    assert_eq!(leader_ids, follower_ids);
}

/// Relationships: CREATE REL TABLE + edges through rewriter + replay.
#[test]
fn test_complex_relationships() {
    let dir = tempfile::tempdir().unwrap();
    let leader_path = dir.path().join("leader");
    let follower_path = dir.path().join("follower");

    let schemas = [
        "CREATE NODE TABLE Person(name STRING, PRIMARY KEY(name))",
        "CREATE NODE TABLE City(name STRING, PRIMARY KEY(name))",
        "CREATE REL TABLE LIVES_IN(FROM Person TO City, since INT64)",
    ];

    let leader_db = lbug::Database::new(&leader_path, lbug::SystemConfig::default()).unwrap();
    let follower_db = lbug::Database::new(&follower_path, lbug::SystemConfig::default()).unwrap();
    let leader = lbug::Connection::new(&leader_db).unwrap();
    for s in &schemas {
        leader.query(s).unwrap();
    }

    let mut entries = Vec::new();
    entries.push(execute_rewritten(&leader, "CREATE (:Person {name: 'Alice'})", None));
    entries.push(execute_rewritten(&leader, "CREATE (:Person {name: 'Bob'})", None));
    entries.push(execute_rewritten(&leader, "CREATE (:City {name: 'NYC'})", None));
    entries.push(execute_rewritten(&leader, "CREATE (:City {name: 'SF'})", None));

    let p1 = serde_json::json!({"since": 2020});
    entries.push(execute_rewritten(
        &leader,
        "MATCH (p:Person {name: 'Alice'}), (c:City {name: 'NYC'}) CREATE (p)-[:LIVES_IN {since: $since}]->(c)",
        Some(&p1),
    ));
    let p2 = serde_json::json!({"since": 2023});
    entries.push(execute_rewritten(
        &leader,
        "MATCH (p:Person {name: 'Bob'}), (c:City {name: 'SF'}) CREATE (p)-[:LIVES_IN {since: $since}]->(c)",
        Some(&p2),
    ));

    let mut all: Vec<PendingEntry> = schemas
        .iter()
        .map(|s| PendingEntry { query: s.to_string(), params: vec![] })
        .collect();
    all.extend(entries);
    let journal_dir = write_journal_entries(dir.path(), all);

    let follower = lbug::Connection::new(&follower_db).unwrap();
    hakuzu::replay::replay_entries(&follower, &journal_dir, 0).unwrap();

    let query = "MATCH (p:Person)-[r:LIVES_IN]->(c:City) RETURN p.name, c.name, r.since ORDER BY p.name";
    for (label, conn) in [("leader", &leader), ("follower", &follower)] {
        let mut result = conn.query(query).unwrap();
        let mut rows = Vec::new();
        for row in &mut result {
            if let (lbug::Value::String(p), lbug::Value::String(c), lbug::Value::Int64(s)) =
                (&row[0], &row[1], &row[2])
            {
                rows.push((p.clone(), c.clone(), *s));
            }
        }
        assert_eq!(
            rows,
            vec![("Alice".into(), "NYC".into(), 2020), ("Bob".into(), "SF".into(), 2023)],
            "{label}: relationship data mismatch"
        );
    }
}

/// DELETE relationships through rewriter + replay.
#[test]
fn test_complex_delete_relationship() {
    let dir = tempfile::tempdir().unwrap();
    let leader_path = dir.path().join("leader");
    let follower_path = dir.path().join("follower");

    let schemas = [
        "CREATE NODE TABLE Person(name STRING, PRIMARY KEY(name))",
        "CREATE NODE TABLE City(name STRING, PRIMARY KEY(name))",
        "CREATE REL TABLE LIVES_IN(FROM Person TO City)",
    ];

    let leader_db = lbug::Database::new(&leader_path, lbug::SystemConfig::default()).unwrap();
    let follower_db = lbug::Database::new(&follower_path, lbug::SystemConfig::default()).unwrap();
    let leader = lbug::Connection::new(&leader_db).unwrap();
    for s in &schemas {
        leader.query(s).unwrap();
    }

    let mut entries = Vec::new();
    entries.push(execute_rewritten(&leader, "CREATE (:Person {name: 'Alice'})", None));
    entries.push(execute_rewritten(&leader, "CREATE (:City {name: 'NYC'})", None));
    entries.push(execute_rewritten(
        &leader,
        "MATCH (p:Person {name: 'Alice'}), (c:City {name: 'NYC'}) CREATE (p)-[:LIVES_IN]->(c)",
        None,
    ));
    entries.push(execute_rewritten(
        &leader,
        "MATCH (p:Person {name: 'Alice'})-[r:LIVES_IN]->(c:City) DELETE r",
        None,
    ));

    let mut all: Vec<PendingEntry> = schemas
        .iter()
        .map(|s| PendingEntry { query: s.to_string(), params: vec![] })
        .collect();
    all.extend(entries);
    let journal_dir = write_journal_entries(dir.path(), all);

    let follower = lbug::Connection::new(&follower_db).unwrap();
    hakuzu::replay::replay_entries(&follower, &journal_dir, 0).unwrap();

    for (label, conn) in [("leader", &leader), ("follower", &follower)] {
        let count = query_count(conn, "MATCH ()-[r:LIVES_IN]->() RETURN COUNT(*)");
        assert_eq!(count, 0, "{label}: relationship should be deleted");
        let count = query_count(conn, "MATCH (n:Person) RETURN COUNT(*)");
        assert_eq!(count, 1, "{label}: person should still exist");
    }
}

/// Parameterized queries with all supported types through replay.
#[test]
fn test_complex_all_param_types() {
    let dir = tempfile::tempdir().unwrap();
    let leader_path = dir.path().join("leader");
    let follower_path = dir.path().join("follower");

    let schema = "CREATE NODE TABLE T(id INT64, s STRING, b BOOLEAN, d DOUBLE, PRIMARY KEY(id))";
    let leader_db = lbug::Database::new(&leader_path, lbug::SystemConfig::default()).unwrap();
    let follower_db = lbug::Database::new(&follower_path, lbug::SystemConfig::default()).unwrap();
    let leader = lbug::Connection::new(&leader_db).unwrap();
    leader.query(schema).unwrap();

    let p = serde_json::json!({"id": 1, "s": "hello world", "b": true, "d": 2.718281828});
    let entries = vec![execute_rewritten(&leader, "CREATE (:T {id: $id, s: $s, b: $b, d: $d})", Some(&p))];

    let mut all = vec![PendingEntry { query: schema.into(), params: vec![] }];
    all.extend(entries);
    let journal_dir = write_journal_entries(dir.path(), all);

    let follower = lbug::Connection::new(&follower_db).unwrap();
    hakuzu::replay::replay_entries(&follower, &journal_dir, 0).unwrap();

    for (label, conn) in [("leader", &leader), ("follower", &follower)] {
        let mut result = conn.query("MATCH (n:T {id: 1}) RETURN n.s, n.b, n.d").unwrap();
        for row in &mut result {
            if let lbug::Value::String(s) = &row[0] {
                assert_eq!(s, "hello world", "{label}: string mismatch");
            }
            if let lbug::Value::Bool(b) = &row[1] {
                assert!(*b, "{label}: bool mismatch");
            }
            if let lbug::Value::Double(d) = &row[2] {
                assert!((*d - 2.718281828).abs() < 1e-6, "{label}: double mismatch: {d}");
            }
        }
    }
}

// ============================================================================
// 7d: Concurrent write stress tests
// ============================================================================

/// N concurrent writes via HaKuzu::local, verify all succeed and count is correct.
#[tokio::test]
async fn test_concurrent_writes() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("concurrent_db");

    let db = Arc::new(
        hakuzu::HaKuzu::local(
            db_path.to_str().unwrap(),
            "CREATE NODE TABLE IF NOT EXISTS T(id INT64, PRIMARY KEY(id))",
        )
        .unwrap(),
    );

    let mut handles = Vec::new();
    let n: i64 = 50;

    for i in 0..n {
        let db = db.clone();
        handles.push(tokio::spawn(async move {
            db.execute("CREATE (:T {id: $id})", Some(serde_json::json!({"id": i})))
                .await
                .expect(&format!("Write {i} failed"));
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let result = db.query("MATCH (n:T) RETURN COUNT(*)", None).await.unwrap();
    assert_eq!(result.rows[0][0], serde_json::json!(n), "All writes should succeed");
}

/// Concurrent writes with gen_random_uuid() — all get unique UUIDs.
#[tokio::test]
async fn test_concurrent_uuid_writes() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("uuid_db");

    let db = Arc::new(
        hakuzu::HaKuzu::local(
            db_path.to_str().unwrap(),
            "CREATE NODE TABLE IF NOT EXISTS T(id STRING, seq INT64, PRIMARY KEY(id))",
        )
        .unwrap(),
    );

    let mut handles = Vec::new();
    let n = 30;

    for i in 0..n {
        let db = db.clone();
        handles.push(tokio::spawn(async move {
            db.execute(
                "CREATE (:T {id: gen_random_uuid(), seq: $seq})",
                Some(serde_json::json!({"seq": i})),
            )
            .await
            .expect(&format!("UUID write {i} failed"));
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let result = db.query("MATCH (n:T) RETURN n.id ORDER BY n.id", None).await.unwrap();
    assert_eq!(result.rows.len(), n as usize, "All UUID writes should succeed");

    let ids: Vec<&str> = result.rows.iter().map(|r| r[0].as_str().unwrap()).collect();
    let unique: std::collections::HashSet<&str> = ids.iter().copied().collect();
    assert_eq!(ids.len(), unique.len(), "All UUIDs should be unique");
}

/// Interleaved reads + writes — reads see monotonically non-decreasing counts.
#[tokio::test]
async fn test_concurrent_reads_during_writes() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("rw_db");

    let db = Arc::new(
        hakuzu::HaKuzu::local(
            db_path.to_str().unwrap(),
            "CREATE NODE TABLE IF NOT EXISTS T(id INT64, PRIMARY KEY(id))",
        )
        .unwrap(),
    );

    let n_writes: i64 = 50;
    let n_readers = 10;

    let mut write_handles = Vec::new();
    for i in 0..n_writes {
        let db = db.clone();
        write_handles.push(tokio::spawn(async move {
            db.execute("CREATE (:T {id: $id})", Some(serde_json::json!({"id": i})))
                .await
                .unwrap();
        }));
    }

    let mut read_handles = Vec::new();
    for _ in 0..n_readers {
        let db = db.clone();
        read_handles.push(tokio::spawn(async move {
            let mut counts = Vec::new();
            for _ in 0..5 {
                let result = db.query("MATCH (n:T) RETURN COUNT(*)", None).await.unwrap();
                let count = result.rows[0][0].as_i64().unwrap();
                counts.push(count);
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
            for window in counts.windows(2) {
                assert!(window[1] >= window[0], "Count should never decrease: {:?}", counts);
            }
        }));
    }

    for h in write_handles {
        h.await.unwrap();
    }
    for h in read_handles {
        h.await.unwrap();
    }

    let result = db.query("MATCH (n:T) RETURN COUNT(*)", None).await.unwrap();
    assert_eq!(result.rows[0][0], serde_json::json!(n_writes));
}

/// Stress test: 1000 sequential writes, verify count and sample IDs.
#[tokio::test]
async fn test_stress_1000_writes() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("stress_db");

    let db = hakuzu::HaKuzu::local(
        db_path.to_str().unwrap(),
        "CREATE NODE TABLE IF NOT EXISTS T(id INT64, PRIMARY KEY(id))",
    )
    .unwrap();

    let n: i64 = 1000;
    for i in 0..n {
        db.execute("CREATE (:T {id: $id})", Some(serde_json::json!({"id": i})))
            .await
            .unwrap();
    }

    let result = db.query("MATCH (n:T) RETURN COUNT(*)", None).await.unwrap();
    assert_eq!(result.rows[0][0], serde_json::json!(n));

    let result = db
        .query(
            "MATCH (n:T) WHERE n.id = 0 OR n.id = 499 OR n.id = 999 RETURN n.id ORDER BY n.id",
            None,
        )
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0][0], serde_json::json!(0));
    assert_eq!(result.rows[1][0], serde_json::json!(499));
    assert_eq!(result.rows[2][0], serde_json::json!(999));

    db.close().await.unwrap();
}
