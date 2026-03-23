//! Write forwarding for inter-node communication.
//!
//! Followers forward Cypher mutations to the leader's internal HTTP server.
//! The leader executes locally, journals via graphstream, and returns the result.

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::database::HaKuzuInner;
use hadb::Role;

/// Request body for forwarded execute calls.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ForwardedExecute {
    pub cypher: String,
    pub params: Option<serde_json::Value>,
}

/// Response body for forwarded execute calls.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ExecuteResult {
    pub ok: bool,
}

/// Shared state for the forwarding HTTP server.
pub(crate) struct ForwardingState {
    pub inner: Arc<HaKuzuInner>,
}

/// Check the `Authorization: Bearer <token>` header against the configured secret.
fn check_auth(state: &ForwardingState, headers: &HeaderMap) -> Result<(), StatusCode> {
    let secret = match &state.inner.secret {
        Some(s) => s,
        None => return Ok(()),
    };
    let header = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let token = header
        .strip_prefix("Bearer ")
        .ok_or(StatusCode::UNAUTHORIZED)?;
    if token != secret {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(())
}

/// Handler for `POST /hakuzu/execute` — receives forwarded writes from followers.
pub(crate) async fn handle_forwarded_execute(
    State(state): State<Arc<ForwardingState>>,
    headers: HeaderMap,
    Json(req): Json<ForwardedExecute>,
) -> Result<Json<ExecuteResult>, StatusCode> {
    check_auth(&state, &headers)?;

    // Only the leader should execute writes.
    let role = state.inner.current_role();
    if role != Some(Role::Leader) {
        return Err(StatusCode::MISDIRECTED_REQUEST);
    }

    // Execute locally via the inner's write path.
    state
        .inner
        .execute_write_local(&req.cypher, req.params.as_ref())
        .await
        .map_err(|e| {
            tracing::error!("Forwarded execute failed: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(ExecuteResult { ok: true }))
}
