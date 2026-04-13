//! HTTP handler tests for `hakuzu serve`.
//!
//! Uses tower::ServiceExt to test the axum Router without a TCP listener.
//! Proves that /health, /status, /metrics, /cypher work correctly,
//! including auth and mutation routing.

use std::sync::Arc;

use axum::body::Body;
use http_body_util::BodyExt;
use hyper::Request;
use serde_json::json;
use tempfile::TempDir;
use tower::ServiceExt;

use hakuzu::serve::{build_router, AppState};
use hakuzu::HaKuzu;

const SCHEMA: &str =
    "CREATE NODE TABLE IF NOT EXISTS Person(id INT64, name STRING, PRIMARY KEY(id))";

fn setup() -> (TempDir, Arc<HaKuzu>) {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db");
    let db = HaKuzu::local(db_path.to_str().unwrap(), SCHEMA).unwrap();
    (tmp, Arc::new(db))
}

fn app(db: Arc<HaKuzu>, secret: Option<String>) -> axum::Router {
    let state = Arc::new(AppState { db, secret });
    build_router(state)
}

async fn body_json(resp: axum::response::Response) -> serde_json::Value {
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

async fn body_string(resp: axum::response::Response) -> String {
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

// ============================================================================
// /health
// ============================================================================

#[tokio::test]
async fn health_returns_200() {
    let (_tmp, db) = setup();
    let resp = app(db, None)
        .oneshot(Request::get("/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn health_no_auth_required() {
    let (_tmp, db) = setup();
    let resp = app(db, Some("secret123".into()))
        .oneshot(Request::get("/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "health should not require auth");
}

// ============================================================================
// /status
// ============================================================================

#[tokio::test]
async fn status_returns_role_and_caught_up() {
    let (_tmp, db) = setup();
    let resp = app(db, None)
        .oneshot(Request::get("/status").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = body_json(resp).await;
    assert_eq!(body["role"], "Leader");
    assert_eq!(body["caught_up"], true);
}

#[tokio::test]
async fn status_requires_auth_when_secret_set() {
    let (_tmp, db) = setup();
    let resp = app(db, Some("secret123".into()))
        .oneshot(Request::get("/status").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn status_accepts_correct_auth() {
    let (_tmp, db) = setup();
    let resp = app(db, Some("secret123".into()))
        .oneshot(
            Request::get("/status")
                .header("authorization", "Bearer secret123")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

// ============================================================================
// /metrics
// ============================================================================

#[tokio::test]
async fn metrics_returns_prometheus_format() {
    let (_tmp, db) = setup();
    let resp = app(db, None)
        .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = body_string(resp).await;
    assert!(
        body.contains("hakuzu_follower_caught_up"),
        "should contain prometheus metrics"
    );
}

// ============================================================================
// /cypher (reads)
// ============================================================================

#[tokio::test]
async fn cypher_read_returns_results() {
    let (_tmp, db) = setup();
    let router = app(db, None);

    let resp = router
        .oneshot(
            Request::post("/cypher")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"query": "RETURN 1 AS n"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = body_json(resp).await;
    assert!(body["columns"].is_array());
    assert!(body["rows"].is_array());
}

// ============================================================================
// /cypher (writes)
// ============================================================================

#[tokio::test]
async fn cypher_write_creates_node() {
    let (_tmp, db) = setup();
    let router = app(db.clone(), None);

    // Write a node.
    let resp = router
        .oneshot(
            Request::post("/cypher")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "query": "CREATE (:Person {id: $id, name: $name})",
                        "params": {"id": 1, "name": "Alice"}
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = body_json(resp).await;
    assert_eq!(body["ok"], true, "write should return ok: true");

    // Verify data via read.
    let router2 = app(db, None);
    let resp = router2
        .oneshot(
            Request::post("/cypher")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"query": "MATCH (p:Person) RETURN p.name"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = body_json(resp).await;
    assert_eq!(body["rows"][0][0], "Alice");
}

// ============================================================================
// /cypher (auth)
// ============================================================================

#[tokio::test]
async fn cypher_requires_auth_when_secret_set() {
    let (_tmp, db) = setup();
    let resp = app(db, Some("s3cret".into()))
        .oneshot(
            Request::post("/cypher")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"query": "RETURN 1"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn cypher_rejects_wrong_token() {
    let (_tmp, db) = setup();
    let resp = app(db, Some("correct".into()))
        .oneshot(
            Request::post("/cypher")
                .header("content-type", "application/json")
                .header("authorization", "Bearer wrong")
                .body(Body::from(
                    json!({"query": "RETURN 1"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}
