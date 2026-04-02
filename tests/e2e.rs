//! End-to-end tests for hakuzu S3 integration.
//!
//! These tests require S3 credentials, a test bucket, and lbug (Kuzu).
//! Run with:
//!   soup run -p hadb -e development -- \
//!     CC=/opt/homebrew/opt/llvm/bin/clang \
//!     CXX=/opt/homebrew/opt/llvm/bin/clang++ \
//!     RUSTFLAGS="-L /opt/homebrew/opt/llvm/lib/c++" \
//!     cargo test --test e2e -- --ignored

use graphstream::journal::{self, JournalCommand, JournalState, PendingEntry};
use hadb_changeset::journal::{decode_header, HADBJ_MAGIC, HEADER_SIZE};
use hadb_changeset::storage::{format_key, ChangesetKind, GENERATION_INCREMENTAL};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

/// Create S3 client + ObjectStore + bucket + unique prefix from env vars.
async fn s3_setup() -> (aws_sdk_s3::Client, Arc<dyn hadb_io::ObjectStore>, String, String) {
    let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let client = aws_sdk_s3::Client::new(&config);
    let bucket =
        std::env::var("S3_TEST_BUCKET").expect("S3_TEST_BUCKET env var required for e2e tests");
    let object_store: Arc<dyn hadb_io::ObjectStore> = Arc::new(
        hadb_io::S3Backend::new(client.clone(), bucket.clone()),
    );
    let prefix = format!("hakuzu-e2e-{}/", uuid::Uuid::new_v4());
    (client, object_store, bucket, prefix)
}

/// Manually upload all sealed .hadbj files to S3 using format_key layout.
async fn upload_sealed_files(
    client: &aws_sdk_s3::Client,
    journal_dir: &std::path::Path,
    bucket: &str,
    prefix: &str,
    db_name: &str,
) {
    for entry in std::fs::read_dir(journal_dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let name = path.file_name().unwrap().to_str().unwrap();
        if !name.ends_with(".hadbj") {
            continue;
        }
        let bytes = std::fs::read(&path).unwrap();
        if bytes.len() < HEADER_SIZE || bytes[0..5] != HADBJ_MAGIC {
            continue;
        }
        let header = decode_header(&bytes).unwrap();
        if !header.is_sealed() {
            continue;
        }
        let key = format_key(prefix, db_name, GENERATION_INCREMENTAL, header.first_seq, ChangesetKind::Journal);
        client
            .put_object()
            .bucket(bucket)
            .key(&key)
            .body(bytes.into())
            .send()
            .await
            .unwrap();
    }
}

// ============================================================================
// Tests
// ============================================================================

/// KuzuReplicator add → write entries → sync → verify on S3.
#[tokio::test]
#[ignore]
async fn test_replicator_uploads_to_s3() {
    let (client, object_store, bucket, prefix) = s3_setup().await;
    let dir = tempfile::tempdir().unwrap();
    let journal_base = dir.path().join("journals");

    // Create replicator with short upload interval.
    let replicator = hakuzu::KuzuReplicator::new(object_store.clone(), prefix.clone())
        .with_upload_interval(Duration::from_secs(1))
        .with_segment_max_bytes(1024 * 1024);

    // Add database.
    use hadb::Replicator;
    replicator.add("graph", &journal_base).await.unwrap();

    // Write entries via journal sender.
    let tx = replicator.journal_sender("graph").await.unwrap();
    for i in 1..=10 {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!(
                "CREATE (:TestData {{id: {}, value: 'row-{}'}})",
                i, i
            ),
            params: vec![],
        }))
        .unwrap();
    }

    // Flush.
    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();

    // Wait for uploader to upload (interval is 1s, wait 3s).
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify segments on S3 (format_key layout: {prefix}{db_name}/0000/*.hadbj).
    let journal_prefix = format!("{}graph/", prefix);
    let list_resp = client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix(&journal_prefix)
        .send()
        .await
        .unwrap();

    let keys: Vec<_> = list_resp
        .contents()
        .iter()
        .filter_map(|o| o.key())
        .collect();
    assert!(
        !keys.is_empty(),
        "Expected uploaded journal segments on S3 at {}",
        journal_prefix
    );

    // Verify journal state advanced.
    let state = replicator.journal_state("graph").await.unwrap();
    assert_eq!(state.sequence.load(Ordering::SeqCst), 10);

    // Cleanup.
    replicator.remove("graph").await.unwrap();
}

/// Full pipeline: leader writes → S3 upload → follower downloads → replays into Kuzu.
#[tokio::test]
#[ignore]
async fn test_follower_downloads_and_replays() {
    let (client, object_store, bucket, prefix) = s3_setup().await;
    let dir = tempfile::tempdir().unwrap();

    // === Leader side: write journal entries and upload to S3 ===
    let leader_journal = dir.path().join("leader/journal");
    std::fs::create_dir_all(&leader_journal).unwrap();

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(
        leader_journal.clone(),
        1024 * 1024,
        50,
        state.clone(),
    );

    // Write schema + data as Cypher entries.
    tx.send(JournalCommand::Write(PendingEntry {
        query: "CREATE NODE TABLE IF NOT EXISTS TestData(id INT64, value STRING, PRIMARY KEY(id))"
            .into(),
        params: vec![],
    }))
    .unwrap();

    for i in 1..=5 {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!(
                "CREATE (:TestData {{id: {}, value: 'follower-row-{}'}})",
                i, i
            ),
            params: vec![],
        }))
        .unwrap();
    }

    // Flush and seal.
    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();

    let (seal_tx, seal_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::SealForUpload(seal_tx)).unwrap();
    seal_rx.recv().unwrap();

    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(Duration::from_millis(100));

    // Upload leader segments to S3 under the db-specific prefix.
    upload_sealed_files(&client, &leader_journal, &bucket, &prefix, "graph").await;

    // === Follower side: download from S3 and replay against Kuzu ===
    // Keep journal separate from DB path (Kuzu doesn't like extra dirs inside its DB).
    let follower_base = dir.path().join("follower");
    std::fs::create_dir_all(&follower_base).unwrap();
    let follower_db_path = follower_base.join("db");
    let follower_journal = follower_base.join("journal");

    // Create follower Kuzu DB (let lbug create the directory).
    let follower_db =
        lbug::Database::new(&follower_db_path, lbug::SystemConfig::default()).unwrap();
    let follower_conn = lbug::Connection::new(&follower_db).unwrap();
    follower_conn
        .query("CREATE NODE TABLE IF NOT EXISTS TestData(id INT64, value STRING, PRIMARY KEY(id))")
        .unwrap();

    // Download segments from S3.
    let downloaded = graphstream::download_new_segments(
        &*object_store,
        &prefix,
        "graph",
        &follower_journal,
        0,
    )
    .await
    .unwrap();
    assert!(!downloaded.is_empty(), "Expected downloaded segments");

    // Replay entries against follower Kuzu.
    let new_seq = hakuzu::replay::replay_entries(&follower_conn, &follower_journal, 0).unwrap();
    assert_eq!(new_seq, 6, "Expected 6 entries replayed (1 schema + 5 data)");

    // Verify data in follower Kuzu.
    let mut result = follower_conn
        .query("MATCH (t:TestData) RETURN COUNT(*)")
        .unwrap();
    let mut count: i64 = 0;
    for row in &mut result {
        if let lbug::Value::Int64(c) = &row[0] {
            count = *c;
        }
    }
    assert_eq!(count, 5, "Expected 5 TestData nodes in follower");
}

/// KuzuFollowerBehavior runs a polling loop that downloads + replays from S3.
#[tokio::test]
#[ignore]
async fn test_follower_behavior_loop() {
    let (client, object_store, bucket, prefix) = s3_setup().await;
    let dir = tempfile::tempdir().unwrap();

    // === Leader: write entries and upload to S3 ===
    let leader_journal = dir.path().join("leader/journal");
    std::fs::create_dir_all(&leader_journal).unwrap();

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(
        leader_journal.clone(),
        1024 * 1024,
        50,
        state.clone(),
    );

    tx.send(JournalCommand::Write(PendingEntry {
        query: "CREATE NODE TABLE IF NOT EXISTS TestData(id INT64, value STRING, PRIMARY KEY(id))"
            .into(),
        params: vec![],
    }))
    .unwrap();

    for i in 1..=3 {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!(
                "CREATE (:TestData {{id: {}, value: 'loop-row-{}'}})",
                i, i
            ),
            params: vec![],
        }))
        .unwrap();
    }

    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();

    let (seal_tx, seal_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::SealForUpload(seal_tx)).unwrap();
    seal_rx.recv().unwrap();

    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(Duration::from_millis(100));

    upload_sealed_files(&client, &leader_journal, &bucket, &prefix, "graph").await;

    // === Follower: set up Kuzu DB + run follower behavior ===
    // Note: catchup_on_promotion creates journal at db_path.join("journal"),
    // so the DB dir must be created by lbug FIRST (before any subdirs are added).
    let follower_base = dir.path().join("follower");
    std::fs::create_dir_all(&follower_base).unwrap();
    let follower_path = follower_base.join("db");

    // Create empty DB with schema (let lbug create the directory).
    {
        let db = lbug::Database::new(&follower_path, lbug::SystemConfig::default()).unwrap();
        let conn = lbug::Connection::new(&db).unwrap();
        conn.query(
            "CREATE NODE TABLE IF NOT EXISTS TestData(id INT64, value STRING, PRIMARY KEY(id))",
        )
        .unwrap();
    }

    // Create follower behavior and run one poll iteration via catchup_on_promotion.
    let follower = hakuzu::KuzuFollowerBehavior::new(object_store.clone());

    use hadb::FollowerBehavior;
    follower
        .catchup_on_promotion(&prefix, "graph", &follower_path, 0)
        .await
        .unwrap();

    // Verify data was replayed.
    let db = lbug::Database::new(&follower_path, lbug::SystemConfig::default()).unwrap();
    let conn = lbug::Connection::new(&db).unwrap();
    let mut result = conn.query("MATCH (t:TestData) RETURN COUNT(*)").unwrap();
    let mut count: i64 = 0;
    for row in &mut result {
        if let lbug::Value::Int64(c) = &row[0] {
            count = *c;
        }
    }
    assert_eq!(count, 3, "Expected 3 TestData nodes after catchup");
}

/// Replicator pull downloads journal segments from S3.
#[tokio::test]
#[ignore]
async fn test_replicator_pull_from_s3() {
    let (client, object_store, bucket, prefix) = s3_setup().await;
    let dir = tempfile::tempdir().unwrap();

    // Upload journal segments to S3 first.
    let leader_journal = dir.path().join("leader/journal");
    std::fs::create_dir_all(&leader_journal).unwrap();

    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx = journal::spawn_journal_writer(
        leader_journal.clone(),
        1024 * 1024,
        50,
        state.clone(),
    );

    for i in 1..=5 {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!("CREATE (:Node {{id: {}}})", i),
            params: vec![],
        }))
        .unwrap();
    }

    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();

    let (seal_tx, seal_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::SealForUpload(seal_tx)).unwrap();
    seal_rx.recv().unwrap();

    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(Duration::from_millis(100));

    upload_sealed_files(&client, &leader_journal, &bucket, &prefix, "graph").await;

    // Use replicator.pull() to download.
    // pull() treats `path` as a db path and creates journal at path.parent()/journal/.
    let follower_base = dir.path().join("follower");
    std::fs::create_dir_all(&follower_base).unwrap();
    let follower_db_path = follower_base.join("db");

    let replicator = hakuzu::KuzuReplicator::new(object_store.clone(), prefix.clone());
    use hadb::Replicator;
    replicator.pull("graph", &follower_db_path).await.unwrap();

    // Verify journal segments were downloaded.
    let follower_journal = follower_base.join("journal");
    let files: Vec<_> = std::fs::read_dir(&follower_journal)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map_or(false, |ext| ext == "hadbj")
        })
        .collect();
    assert!(!files.is_empty(), "Expected downloaded journal segments");

    // Read entries and verify.
    let reader = graphstream::JournalReader::open(&follower_journal).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 5);
}
