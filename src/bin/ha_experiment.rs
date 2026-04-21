//! hakuzu HA experiment — standalone Kuzu/LadybugDB HA node.
//!
//! Proves the HA mechanism works end-to-end:
//!   1. Leader election via S3 leases (hadb)
//!   2. Journal replication via graphstream (.graphj files)
//!   3. Followers replay Cypher mutations from journal segments
//!   4. Automatic failover: kill leader → follower promotes → catches up
//!
//! Usage:
//!   # Terminal 1 — becomes leader
//!   hakuzu-ha-experiment --bucket my-bucket --prefix ha-test/ \
//!     --db /tmp/node1/graph --instance node1 --port 9001
//!
//!   # Terminal 2 — becomes follower
//!   hakuzu-ha-experiment --bucket my-bucket --prefix ha-test/ \
//!     --db /tmp/node2/graph --instance node2 --port 9002
//!
//!   # Writer — discovers leader and sends Cypher writes
//!   hakuzu-ha-writer --nodes http://localhost:9001,http://localhost:9002

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use anyhow::Result;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::Json;
use clap::Parser;
use graphstream::journal::{JournalCommand, PendingEntry};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

use hadb::{Coordinator, CoordinatorConfig, LeaseConfig, Role, RoleEvent};
use hadb_lease_s3::S3LeaseStore;
use hakuzu::KuzuFollowerBehavior;
use hakuzu::KuzuReplicator;

#[derive(Parser)]
#[command(name = "hakuzu-ha-experiment")]
#[command(about = "HA Kuzu/LadybugDB experiment using hakuzu + hadb")]
struct Args {
    /// S3 bucket for leader election and journal storage
    #[arg(long, env = "HA_BUCKET")]
    bucket: String,

    /// S3 prefix (e.g., "ha-test/")
    #[arg(long, env = "HA_PREFIX", default_value = "ha-test/")]
    prefix: String,

    /// S3 endpoint (for Tigris/MinIO)
    #[arg(long, env = "HA_S3_ENDPOINT")]
    endpoint: Option<String>,

    /// Local Kuzu database path (lbug creates this as a file)
    #[arg(long, env = "HA_DB_PATH", default_value = "/tmp/hakuzu-ha-node")]
    db: std::path::PathBuf,

    /// Instance ID (unique per node)
    #[arg(long, env = "HA_INSTANCE_ID")]
    instance: Option<String>,

    /// HTTP port for the app server
    #[arg(long, default_value = "9001")]
    port: u16,

    /// How many test nodes to create (leader only, first time)
    #[arg(long, default_value = "100")]
    test_rows: u32,

    /// Journal upload interval in milliseconds
    #[arg(long, default_value = "1000")]
    sync_interval_ms: u64,

    /// Lease TTL in seconds
    #[arg(long, default_value = "5")]
    lease_ttl: u64,

    /// Lease renew interval in milliseconds
    #[arg(long, default_value = "2000")]
    renew_interval_ms: u64,

    /// Follower poll interval in milliseconds (how often followers check for leader death)
    #[arg(long, default_value = "1000")]
    follower_poll_ms: u64,

    /// Follower pull interval in milliseconds (how often followers pull new journal segments)
    #[arg(long, default_value = "1000")]
    follower_pull_ms: u64,
}

/// Shared app state.
struct AppState {
    db: Arc<lbug::Database>,
    write_mutex: Mutex<()>,
    coordinator: Arc<Coordinator>,
    replicator: Arc<KuzuReplicator>,
    instance_id: String,
}

// ============================================================================
// Schema
// ============================================================================

const SCHEMA_QUERIES: &[&str] = &[
    "CREATE NODE TABLE IF NOT EXISTS TestData(id INT64, value STRING, PRIMARY KEY(id))",
];

fn apply_schema(conn: &lbug::Connection) {
    for query in SCHEMA_QUERIES {
        conn.query(query).expect("Schema creation failed");
    }
}

/// Helper: create a connection from the shared Database.
fn new_conn(db: &lbug::Database) -> std::result::Result<lbug::Connection<'_>, StatusCode> {
    lbug::Connection::new(db).map_err(|e| {
        error!("Failed to create connection: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })
}

/// Helper: count TestData rows.
fn count_test_data(conn: &lbug::Connection) -> i64 {
    let mut count: i64 = 0;
    if let Ok(mut result) = conn.query("MATCH (t:TestData) RETURN COUNT(*)") {
        for row in &mut result {
            if let lbug::Value::Int64(c) = &row[0] {
                count = *c;
            }
        }
    }
    count
}

// ============================================================================
// HTTP handlers
// ============================================================================

/// POST /cypher — execute a Cypher mutation. Body: {"query": "..."}
///
/// Only accepts writes on the Leader. Followers return 421 Misdirected.
async fn handle_cypher(
    State(state): State<Arc<AppState>>,
    Json(body): Json<serde_json::Value>,
) -> std::result::Result<Json<serde_json::Value>, StatusCode> {
    // Check role — only leader accepts writes.
    let role = state.coordinator.role("graph").await;
    if role != Some(Role::Leader) {
        return Err(StatusCode::MISDIRECTED_REQUEST); // 421
    }

    let query = body
        .get("query")
        .and_then(|v| v.as_str())
        .ok_or(StatusCode::BAD_REQUEST)?;

    // Serialize writes (Kuzu is single-writer).
    let _guard = state.write_mutex.lock().await;
    let conn = new_conn(&state.db)?;
    conn.query(query).map_err(|e| {
        error!("Cypher execution failed: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    drop(conn);
    drop(_guard);

    // Write to journal for replication.
    if let Some(tx) = state.replicator.journal_sender("graph").await {
        let _ = tx.send(JournalCommand::Write(PendingEntry {
            query: query.to_string(),
            params: vec![],
        }));
    }

    Ok(Json(serde_json::json!({"ok": true})))
}

/// GET /count — return current node count for TestData.
async fn handle_count(
    State(state): State<Arc<AppState>>,
) -> std::result::Result<Json<serde_json::Value>, StatusCode> {
    let conn = new_conn(&state.db)?;
    let count = count_test_data(&conn);
    Ok(Json(serde_json::json!({"count": count})))
}

/// GET /status — instance info + node count + role.
async fn handle_status(
    State(state): State<Arc<AppState>>,
) -> std::result::Result<Json<serde_json::Value>, StatusCode> {
    let conn = new_conn(&state.db)?;
    let count = count_test_data(&conn);

    let role = state
        .coordinator
        .role("graph")
        .await
        .unwrap_or(Role::Follower);

    let journal_seq = if let Some(js) = state.replicator.journal_state("graph").await {
        js.sequence
            .load(std::sync::atomic::Ordering::SeqCst)
    } else {
        0
    };

    Ok(Json(serde_json::json!({
        "instance_id": state.instance_id,
        "role": role.to_string(),
        "node_count": count,
        "journal_sequence": journal_seq,
    })))
}

/// GET /verify — data integrity check (count nodes, check for gaps in IDs).
async fn handle_verify(
    State(state): State<Arc<AppState>>,
) -> std::result::Result<Json<serde_json::Value>, StatusCode> {
    let conn = new_conn(&state.db)?;
    let count = count_test_data(&conn);

    // Get min and max IDs.
    let mut min_id: i64 = 0;
    let mut max_id: i64 = 0;
    if let Ok(mut result) = conn.query("MATCH (t:TestData) RETURN MIN(t.id), MAX(t.id)") {
        for row in &mut result {
            if let lbug::Value::Int64(lo) = &row[0] {
                min_id = *lo;
            }
            if let lbug::Value::Int64(hi) = &row[1] {
                max_id = *hi;
            }
        }
    }

    let expected = if count > 0 { max_id - min_id + 1 } else { 0 };
    let gaps = expected - count;
    let ok = gaps == 0 && count > 0;

    Ok(Json(serde_json::json!({
        "ok": ok,
        "count": count,
        "min_id": min_id,
        "max_id": max_id,
        "expected_count": expected,
        "gap_count": gaps,
    })))
}

/// GET /metrics — journal state + coordinator metrics.
async fn handle_metrics(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let role = state
        .coordinator
        .role("graph")
        .await
        .unwrap_or(Role::Follower);

    let journal_seq = if let Some(js) = state.replicator.journal_state("graph").await {
        js.sequence.load(std::sync::atomic::Ordering::SeqCst)
    } else {
        0
    };
    let alive = if let Some(js) = state.replicator.journal_state("graph").await {
        js.is_alive()
    } else {
        false
    };

    let metrics = state.coordinator.metrics().snapshot();

    Json(serde_json::json!({
        "role": role.to_string(),
        "journal_sequence": journal_seq,
        "journal_alive": alive,
        "instance_id": state.instance_id,
        "lease_claims_attempted": metrics.lease_claims_attempted,
        "lease_claims_succeeded": metrics.lease_claims_succeeded,
        "promotions_succeeded": metrics.promotions_succeeded,
        "follower_pulls_succeeded": metrics.follower_pulls_succeeded,
        "follower_pulls_failed": metrics.follower_pulls_failed,
        "follower_pulls_no_new_data": metrics.follower_pulls_no_new_data,
    }))
}

/// GET /health
async fn handle_health() -> StatusCode {
    StatusCode::OK
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    let instance_id = args.instance.clone().unwrap_or_else(|| {
        std::env::var("FLY_MACHINE_ID")
            .unwrap_or_else(|_| uuid::Uuid::new_v4().to_string())
    });
    let address = format!("http://localhost:{}", args.port);

    info!("=== hakuzu HA experiment ===");
    info!("Instance: {}", instance_id);
    info!("Bucket: {}", args.bucket);
    info!("Prefix: {}", args.prefix);
    info!("DB path: {}", args.db.display());
    info!("HTTP: {}", address);
    info!("Lease TTL: {}s, renew: {}ms", args.lease_ttl, args.renew_interval_ms);
    info!("Follower poll: {}ms, pull: {}ms", args.follower_poll_ms, args.follower_pull_ms);
    info!("Sync interval: {}ms", args.sync_interval_ms);

    // Ensure parent directory exists (lbug creates the DB path itself).
    if let Some(parent) = args.db.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Create Kuzu database + apply schema.
    let db = lbug::Database::new(&args.db, lbug::SystemConfig::default())
        .map_err(|e| anyhow::anyhow!("Failed to create Kuzu DB: {e}"))?;
    let shared_db = Arc::new(db);

    {
        let conn = lbug::Connection::new(&*shared_db)
            .map_err(|e| anyhow::anyhow!("Failed to create connection: {e}"))?;
        apply_schema(&conn);
        info!("Schema applied");
    }

    // Build S3 client + StorageBackend (experiment binary uses S3 directly).
    let s3_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let s3_client = aws_sdk_s3::Client::new(&s3_config);
    let storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(
        hadb_storage_s3::S3Storage::new(s3_client.clone(), args.bucket.clone()),
    );

    // Create HA components.
    let lease_store: Arc<dyn hadb::LeaseStore> =
        Arc::new(S3LeaseStore::new(s3_client.clone(), args.bucket.clone()));

    let replicator = Arc::new(
        KuzuReplicator::new(storage.clone(), args.prefix.clone())
            .with_upload_interval(Duration::from_millis(args.sync_interval_ms)),
    );

    let follower_behavior: Arc<dyn hadb::FollowerBehavior> = Arc::new(
        KuzuFollowerBehavior::new(storage)
            .with_shared_db(shared_db.clone()),
    );

    // Build coordinator config. Lease store now lives inside LeaseConfig
    // (hadb Phase Fjord) — Coordinator no longer takes it as a separate arg.
    let mut lease_config =
        LeaseConfig::new(lease_store, instance_id.clone(), address.clone());
    lease_config.ttl_secs = args.lease_ttl;
    lease_config.renew_interval = Duration::from_millis(args.renew_interval_ms);
    lease_config.follower_poll_interval = Duration::from_millis(args.follower_poll_ms);

    let coordinator_config = CoordinatorConfig {
        lease: Some(lease_config),
        follower_pull_interval: Duration::from_millis(args.follower_pull_ms),
        ..Default::default()
    };

    let coordinator = Coordinator::new(
        replicator.clone() as Arc<dyn hadb::Replicator>,
        None, // manifest_store
        None, // node_registry
        follower_behavior,
        &args.prefix,
        coordinator_config,
    );

    // Join the HA cluster.
    let join_result = coordinator.join("graph", &args.db).await?;
    let role = join_result.role;
    info!("*** THIS INSTANCE IS THE {} ***", role);

    // Spawn role event listener.
    {
        let mut rx = coordinator.role_events();
        tokio::spawn(async move {
            while let Ok(event) = rx.recv().await {
                match event {
                    RoleEvent::Joined { db_name, role } => {
                        info!("RoleEvent: '{}' joined as {}", db_name, role);
                    }
                    RoleEvent::Promoted { db_name } => {
                        info!("*** PROMOTED TO LEADER ({}) ***", db_name);
                    }
                    RoleEvent::Demoted { db_name } => {
                        warn!("*** DEMOTED TO FOLLOWER ({}) ***", db_name);
                    }
                    RoleEvent::Fenced { db_name } => {
                        error!("*** FENCED ({}) ***", db_name);
                    }
                    RoleEvent::Sleeping { db_name } => {
                        info!("*** SLEEPING ({}) ***", db_name);
                    }
                    RoleEvent::ManifestChanged { db_name, version } => {
                        info!("ManifestChanged: {} v{}", db_name, version);
                    }
                }
            }
        });
    }

    // Seed test data if leader and DB is empty.
    if role == Role::Leader && args.test_rows > 0 {
        let conn = lbug::Connection::new(&*shared_db)
            .map_err(|e| anyhow::anyhow!("Failed to create connection: {e}"))?;
        let count = count_test_data(&conn);

        if count == 0 {
            info!("Seeding {} test rows...", args.test_rows);
            for i in 1..=args.test_rows as i64 {
                let query = format!(
                    "CREATE (:TestData {{id: {}, value: 'row-{}'}})",
                    i, i
                );
                conn.query(&query).unwrap();

                // Write to journal.
                if let Some(tx) = replicator.journal_sender("graph").await {
                    let _ = tx.send(JournalCommand::Write(PendingEntry {
                        query,
                        params: vec![],
                    }));
                }
            }

            // Flush journal.
            if let Some(tx) = replicator.journal_sender("graph").await {
                let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
                let _ = tx.send(JournalCommand::Flush(ack_tx));
                let _ = ack_rx.recv();
            }

            info!("Seeded {} test rows", args.test_rows);
        } else {
            info!("Database already has {} rows", count);
        }
    }

    let state = Arc::new(AppState {
        db: shared_db,
        write_mutex: Mutex::new(()),
        coordinator: coordinator.clone(),
        replicator,
        instance_id: instance_id.clone(),
    });

    // HTTP server.
    let app = axum::Router::new()
        .route("/cypher", post(handle_cypher))
        .route("/count", get(handle_count))
        .route("/status", get(handle_status))
        .route("/verify", get(handle_verify))
        .route("/metrics", get(handle_metrics))
        .route("/health", get(handle_health))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", args.port)).await?;
    info!("HTTP server listening on port {}", args.port);

    let server = axum::serve(listener, app);

    // Wait for shutdown signal.
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        _ = sigterm.recv() => info!("Received SIGTERM"),
        _ = tokio::signal::ctrl_c() => info!("Received SIGINT"),
        result = server => {
            if let Err(e) = result {
                error!("HTTP server error: {}", e);
            }
        }
    }

    info!("Shutting down...");

    // Leave the HA cluster.
    if let Err(e) = coordinator.leave("graph").await {
        error!("Failed to leave cluster: {e}");
    }

    info!("Goodbye.");
    Ok(())
}
