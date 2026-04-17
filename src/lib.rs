//! hakuzu — HA Kuzu/LadybugDB with one line of code.
//!
//! Leader election, journal replication, write forwarding — just your app + an S3 bucket.
//!
//! hakuzu is to Kuzu/graphd what haqlite is to SQLite — the HA layer that wraps
//! graphstream's journal replication and hadb's coordination framework.
//!
//! ```ignore
//! use hakuzu::{HaKuzu, QueryResult};
//!
//! let db = HaKuzu::builder("my-bucket")
//!     .open("/data/graph", "CREATE NODE TABLE IF NOT EXISTS Person(id INT64, name STRING, PRIMARY KEY(id))")
//!     .await?;
//!
//! db.execute("CREATE (p:Person {id: $id, name: $name})", Some(json!({"id": 1, "name": "Alice"}))).await?;
//! let result = db.query("MATCH (p:Person) RETURN p.id, p.name", None).await?;
//! ```

pub mod builder;
pub mod cli_config;
pub mod database;
pub mod error;
pub mod follower_behavior;
pub mod forwarding;
pub mod metrics;
pub mod mode;
pub mod mutation;
pub mod replay;
pub mod replicator;
pub mod rewriter;
pub mod serve;
pub mod snapshot;
mod snapshot_loop;
pub mod turbograph_follower_behavior;
pub mod turbograph_manifest_json;
pub mod turbograph_replicator;
pub mod values;

// Primary API.
pub use builder::HaKuzuBuilder;
pub use database::{HaKuzu, QueryResult, SnapshotConfig, StagedJournalEntry};
pub use error::HakuzuError;
pub use metrics::HakuzuMetrics;
pub use hadb::{Durability, HaMode, validate_mode_durability};
pub use snapshot::SnapshotMeta;
pub use replicator::KuzuReplicator;
pub use turbograph_follower_behavior::TurbographFollowerBehavior;
pub use turbograph_replicator::TurbographReplicator;
pub use follower_behavior::KuzuFollowerBehavior;

// Re-export hadb types.
pub use hadb::{
    Coordinator, CoordinatorConfig, HaMetrics, InMemoryLeaseStore, JoinResult, LeaseConfig,
    LeaseData, LeaseStore, MetricsSnapshot, NodeRegistration, NodeRegistry, Role, RoleEvent,
};

// Re-export hadb-s3 implementations.
pub use hadb_lease_s3::{S3LeaseStore, S3NodeRegistry, S3StorageBackend};

// Re-export the byte-level storage trait so embedders can name `Arc<dyn StorageBackend>`
// without taking a direct `hadb-storage` dep.
pub use hadb_storage::{CasResult, StorageBackend};
