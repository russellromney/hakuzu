//! `hakuzu serve` -- production HA Kuzu/LadybugDB server.
//!
//! Opens an HaKuzu instance with leader election + journal replication,
//! exposes a Cypher HTTP API + health/status/metrics endpoints.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::Json;
use tracing::{error, info};

use hadb::{CoordinatorConfig, LeaseConfig, Role};
use hadb_cli::SharedConfig;

use crate::cli_config::ServeConfig;
use crate::database::HaKuzu;
use crate::mode::{Durability, HaMode};

/// Shared state for HTTP handlers.
pub struct AppState {
    pub db: Arc<HaKuzu>,
    pub secret: Option<String>,
}

/// Build the axum Router with all routes. Public for testing.
pub fn build_router(state: Arc<AppState>) -> axum::Router {
    axum::Router::new()
        .route("/health", get(handle_health))
        .route("/status", get(handle_status))
        .route("/metrics", get(handle_metrics))
        .route("/cypher", post(handle_cypher))
        .with_state(state)
}

/// Run the hakuzu serve command.
pub async fn run(shared: &SharedConfig, serve: &ServeConfig) -> Result<()> {
    if shared.s3.bucket.is_empty() {
        anyhow::bail!("S3 bucket is required (set [s3] bucket in config or HADB_BUCKET env var)");
    }

    let db_path = &serve.db_path;
    let schema = serve.schema.as_deref().unwrap_or("");

    let instance_id = std::env::var("FLY_MACHINE_ID")
        .or_else(|_| std::env::var("HADB_INSTANCE_ID"))
        .unwrap_or_else(|_| uuid::Uuid::new_v4().to_string());

    let address = format!("http://0.0.0.0:{}", serve.forwarding_port);

    let mut lease_config = LeaseConfig::new(instance_id.clone(), address.clone());
    lease_config.ttl_secs = shared.lease.ttl_secs;
    lease_config.renew_interval = shared.lease.renew_interval();
    lease_config.follower_poll_interval = shared.lease.poll_interval();

    let coordinator_config = CoordinatorConfig {
        sync_interval: Duration::from_millis(serve.sync_interval_ms),
        follower_pull_interval: Duration::from_millis(serve.follower_pull_ms),
        lease: Some(lease_config),
        ..Default::default()
    };

    // Parse mode from env or config. Reject invalid values (no silent fallbacks).
    let mode_str = std::env::var("HAKUZU_MODE")
        .unwrap_or_else(|_| serve.mode.clone())
        .to_lowercase();
    let ha_mode = match mode_str.as_str() {
        "dedicated" => HaMode::Dedicated,
        "shared" => HaMode::Shared,
        other => anyhow::bail!(
            "invalid mode '{other}' (expected 'dedicated' or 'shared')"
        ),
    };

    // Parse durability from env or config. Reject invalid values.
    let durability_str = std::env::var("HAKUZU_DURABILITY")
        .unwrap_or_else(|_| serve.durability.clone())
        .to_lowercase();
    let durability = match durability_str.as_str() {
        "replicated" => Durability::Replicated,
        "synchronous" => Durability::Synchronous,
        other => anyhow::bail!(
            "invalid durability '{other}' (expected 'replicated' or 'synchronous')"
        ),
    };

    // Build HaKuzu.
    let mut builder = HaKuzu::builder(&shared.s3.bucket)
        .mode(ha_mode)
        .durability(durability)
        .prefix(&serve.prefix)
        .forwarding_port(serve.forwarding_port)
        .instance_id(&instance_id)
        .address(&address)
        .coordinator_config(coordinator_config);

    if let Some(ref endpoint) = shared.s3.endpoint {
        builder = builder.endpoint(endpoint);
    }
    if let Some(ref secret) = serve.secret {
        builder = builder.secret(secret);
    }

    // Wire ManifestStore for Synchronous durability (S3-backed).
    if matches!(durability, Durability::Synchronous) {
        let s3_config = match &shared.s3.endpoint {
            Some(endpoint) => {
                aws_config::defaults(aws_config::BehaviorVersion::latest())
                    .endpoint_url(endpoint)
                    .load()
                    .await
            }
            None => {
                aws_config::defaults(aws_config::BehaviorVersion::latest())
                    .load()
                    .await
            }
        };
        let s3_client = aws_sdk_s3::Client::new(&s3_config);
        let manifest_store = Arc::new(
            hadb_manifest_s3::S3ManifestStore::new(s3_client, shared.s3.bucket.clone()),
        );
        builder = builder.manifest_store(manifest_store);
        info!("using S3 manifest store for Synchronous durability");
    }

    // Snapshot config (Replicated mode only, Synchronous uses turbograph_sync).
    if serve.snapshot_interval_secs > 0 {
        builder = builder
            .snapshot_interval(Duration::from_secs(serve.snapshot_interval_secs))
            .snapshot_every_n_entries(serve.snapshot_every_n);
    }

    let db_path_str = db_path
        .to_str()
        .ok_or_else(|| anyhow!("db_path is not valid UTF-8"))?;

    let db = Arc::new(builder.open(db_path_str, schema).await?);

    let role = db.role().unwrap_or(Role::Follower);
    info!(
        instance = %instance_id,
        role = %role,
        mode = %ha_mode,
        durability = %durability,
        db = %db_path.display(),
        port = serve.port,
        forwarding_port = serve.forwarding_port,
        "hakuzu server started"
    );

    let state = Arc::new(AppState {
        db: db.clone(),
        secret: serve.secret.clone(),
    });

    let app = build_router(state);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", serve.port)).await?;
    info!(port = serve.port, "HTTP API listening");

    let server = axum::serve(listener, app);

    // Wait for shutdown.
    tokio::select! {
        _ = hadb_cli::shutdown_signal() => {}
        result = server => {
            if let Err(e) = result {
                error!("HTTP server error: {e}");
            }
        }
    }

    info!("shutting down");

    match Arc::try_unwrap(db) {
        Ok(db) => {
            if let Err(e) = db.close().await {
                error!("failed to close hakuzu: {e}");
            }
        }
        Err(_) => {
            error!("could not unwrap Arc<HaKuzu> for clean shutdown");
        }
    }

    info!("goodbye");
    Ok(())
}

// ============================================================================
// Auth
// ============================================================================

fn check_auth(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<(), (StatusCode, Json<serde_json::Value>)> {
    let secret = match &state.secret {
        Some(s) => s,
        None => return Ok(()),
    };

    let header = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| error_json(StatusCode::UNAUTHORIZED, "missing Authorization header"))?;

    let token = header
        .strip_prefix("Bearer ")
        .ok_or_else(|| {
            error_json(StatusCode::UNAUTHORIZED, "invalid Authorization format")
        })?;

    if token != secret {
        return Err(error_json(StatusCode::UNAUTHORIZED, "invalid token"));
    }

    Ok(())
}

fn error_json(
    status: StatusCode,
    message: &str,
) -> (StatusCode, Json<serde_json::Value>) {
    (status, Json(serde_json::json!({ "error": message })))
}

// ============================================================================
// HTTP handlers
// ============================================================================

/// GET /health -- liveness check (no auth).
async fn handle_health() -> StatusCode {
    StatusCode::OK
}

/// GET /status -- role and readiness info.
async fn handle_status(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    check_auth(&state, &headers)?;
    let role = state.db.role().unwrap_or(Role::Follower);
    let caught_up = state.db.is_caught_up();
    Ok(Json(serde_json::json!({
        "role": format!("{role}"),
        "caught_up": caught_up,
        "replay_position": state.db.replay_position(),
    })))
}

/// GET /metrics -- Prometheus exposition format.
async fn handle_metrics(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<String, (StatusCode, Json<serde_json::Value>)> {
    check_auth(&state, &headers)?;
    Ok(state.db.prometheus_metrics())
}

/// POST /cypher -- execute a Cypher statement.
///
/// Auto-routes reads vs writes:
/// - Mutations (CREATE, MERGE, DELETE, SET, etc.): routed through `execute()`,
///   which forwards to leader if this is a follower, journals the write, and
///   calls `turbograph_sync()` in Synchronous mode.
/// - Reads (MATCH, RETURN, etc.): executed locally via `query()`.
///
/// Request body:
/// ```json
/// { "query": "MATCH (p:Person) RETURN p.name", "params": {"id": 1} }
/// ```
///
/// Response body (reads):
/// ```json
/// { "columns": ["p.name"], "rows": [["Alice"]] }
/// ```
///
/// Response body (writes):
/// ```json
/// { "ok": true }
/// ```
async fn handle_cypher(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<CypherRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    check_auth(&state, &headers)?;

    if crate::mutation::is_mutation(&body.query) {
        // Write path: forwarded to leader, journaled, synced.
        state
            .db
            .execute(&body.query, body.params)
            .await
            .map_err(|e| error_json(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()))?;
        Ok(Json(serde_json::json!({ "ok": true })))
    } else {
        // Read path: execute locally.
        let result = state
            .db
            .query(&body.query, body.params)
            .await
            .map_err(|e| error_json(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()))?;
        Ok(Json(serde_json::json!({
            "columns": result.columns,
            "rows": result.rows,
        })))
    }
}

#[derive(serde::Deserialize)]
struct CypherRequest {
    query: String,
    #[serde(default)]
    params: Option<serde_json::Value>,
}
