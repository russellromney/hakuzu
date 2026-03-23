//! hakuzu — HA Kuzu/LadybugDB with one line of code.
//!
//! Leader election, journal replication, write forwarding — just your app + an S3 bucket.
//!
//! hakuzu is to Kuzu/graphd what haqlite is to SQLite — the HA layer that wraps
//! graphstream's journal replication and hadb's coordination framework.

pub mod follower_behavior;
pub mod replay;
pub mod replicator;

pub use follower_behavior::KuzuFollowerBehavior;
pub use replicator::KuzuReplicator;
