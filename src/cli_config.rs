//! hakuzu CLI configuration.
//!
//! Product-specific TOML config for `hakuzu serve` and other CLI commands.
//! Loaded alongside hadb-cli's SharedConfig (S3, lease, retention).

use std::path::PathBuf;

use serde::Deserialize;

/// Top-level hakuzu configuration.
#[derive(Debug, Default, Deserialize)]
pub struct HakuzuConfig {
    /// Serve configuration section.
    pub serve: Option<ServeConfig>,
}

/// Configuration for `hakuzu serve`.
#[derive(Debug, Deserialize)]
pub struct ServeConfig {
    /// Path to the Kuzu database directory.
    pub db_path: PathBuf,
    /// Cypher schema to apply on startup (semicolon-separated statements).
    #[serde(default)]
    pub schema: Option<String>,
    /// HTTP API port. Default: 8080.
    #[serde(default = "default_port")]
    pub port: u16,
    /// Internal write-forwarding port. Default: 18080.
    #[serde(default = "default_forwarding_port")]
    pub forwarding_port: u16,
    /// S3 key prefix. Default: "hakuzu/".
    #[serde(default = "default_prefix")]
    pub prefix: String,
    /// Journal sync interval in ms. Default: 5000.
    #[serde(default = "default_sync_interval_ms")]
    pub sync_interval_ms: u64,
    /// Follower pull interval in ms. Default: 2000.
    #[serde(default = "default_follower_pull_ms")]
    pub follower_pull_ms: u64,
    /// Snapshot interval in seconds. 0 = disabled. Default: 300.
    #[serde(default = "default_snapshot_interval_secs")]
    pub snapshot_interval_secs: u64,
    /// Snapshot every N entries. Default: 10000.
    #[serde(default = "default_snapshot_every_n")]
    pub snapshot_every_n: u64,
    /// HA mode: "dedicated" or "shared". Default: "dedicated".
    #[serde(default = "default_mode")]
    pub mode: String,
    /// Durability: "replicated" or "synchronous". Default: "replicated".
    #[serde(default = "default_durability")]
    pub durability: String,
    /// Shared secret for API auth. None = no auth.
    pub secret: Option<String>,
}

fn default_port() -> u16 {
    8080
}
fn default_forwarding_port() -> u16 {
    18080
}
fn default_prefix() -> String {
    "hakuzu/".to_string()
}
fn default_sync_interval_ms() -> u64 {
    5000
}
fn default_follower_pull_ms() -> u64 {
    2000
}
fn default_snapshot_interval_secs() -> u64 {
    300
}
fn default_snapshot_every_n() -> u64 {
    10_000
}
fn default_mode() -> String {
    "dedicated".to_string()
}
fn default_durability() -> String {
    "replicated".to_string()
}
