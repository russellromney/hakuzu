//! Structured error types for hakuzu.
//!
//! Consumers (like cinch-engine) can match on `HakuzuError` variants
//! to handle different failure modes appropriately.

use std::fmt;

/// Structured error type for hakuzu operations.
#[derive(Debug)]
pub enum HakuzuError {
    /// Write forwarding failed — leader is unreachable or returned an error.
    LeaderUnavailable(String),

    /// Tried to execute a write but this node is not the leader
    /// and no leader address is available for forwarding.
    NotLeader,

    /// Kuzu database error (query, prepare, execute, connection).
    DatabaseError(String),

    /// Journal error (write, seal, recovery, replication).
    JournalError(String),

    /// Coordinator error (join, leave, handoff, lease).
    CoordinatorError(String),

    /// Engine is closed (semaphore closed, tasks aborted).
    EngineClosed,

    /// Manifest CAS conflict: another writer published a newer version.
    /// In Dedicated mode this signals split-brain (two active leaders).
    /// In Shared mode this is normal contention (caller should retry).
    ManifestCasConflict {
        db_name: String,
        expected_version: Option<u64>,
    },
}

impl fmt::Display for HakuzuError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LeaderUnavailable(msg) => write!(f, "Leader unavailable: {msg}"),
            Self::NotLeader => write!(f, "Not leader and no leader address available"),
            Self::DatabaseError(msg) => write!(f, "Database error: {msg}"),
            Self::JournalError(msg) => write!(f, "Journal error: {msg}"),
            Self::CoordinatorError(msg) => write!(f, "Coordinator error: {msg}"),
            Self::EngineClosed => write!(f, "Engine closed"),
            Self::ManifestCasConflict { db_name, expected_version } => {
                write!(
                    f,
                    "Manifest CAS conflict for '{}' (expected version {:?}, another writer is active)",
                    db_name, expected_version,
                )
            }
        }
    }
}

impl std::error::Error for HakuzuError {}

/// Convert from anyhow::Error — used internally for transitional compatibility.
impl From<anyhow::Error> for HakuzuError {
    fn from(err: anyhow::Error) -> Self {
        HakuzuError::DatabaseError(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, HakuzuError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display_leader_unavailable() {
        let err = HakuzuError::LeaderUnavailable("connection refused".into());
        assert_eq!(err.to_string(), "Leader unavailable: connection refused");
    }

    #[test]
    fn test_display_not_leader() {
        let err = HakuzuError::NotLeader;
        assert_eq!(
            err.to_string(),
            "Not leader and no leader address available"
        );
    }

    #[test]
    fn test_display_database_error() {
        let err = HakuzuError::DatabaseError("syntax error".into());
        assert_eq!(err.to_string(), "Database error: syntax error");
    }

    #[test]
    fn test_display_journal_error() {
        let err = HakuzuError::JournalError("seal failed".into());
        assert_eq!(err.to_string(), "Journal error: seal failed");
    }

    #[test]
    fn test_display_coordinator_error() {
        let err = HakuzuError::CoordinatorError("lease expired".into());
        assert_eq!(err.to_string(), "Coordinator error: lease expired");
    }

    #[test]
    fn test_display_engine_closed() {
        let err = HakuzuError::EngineClosed;
        assert_eq!(err.to_string(), "Engine closed");
    }

    #[test]
    fn test_error_trait() {
        let err: Box<dyn std::error::Error> =
            Box::new(HakuzuError::DatabaseError("test".into()));
        assert!(err.to_string().contains("test"));
    }

    #[test]
    fn test_from_anyhow() {
        let anyhow_err = anyhow::anyhow!("something broke");
        let hakuzu_err: HakuzuError = anyhow_err.into();
        assert!(matches!(hakuzu_err, HakuzuError::DatabaseError(_)));
        assert!(hakuzu_err.to_string().contains("something broke"));
    }

    #[test]
    fn test_result_type_alias() {
        fn returns_ok() -> Result<i32> {
            Ok(42)
        }
        fn returns_err() -> Result<i32> {
            Err(HakuzuError::NotLeader)
        }
        assert_eq!(returns_ok().unwrap(), 42);
        assert!(returns_err().is_err());
    }

    #[test]
    fn test_match_exhaustive() {
        // Ensures all variants can be matched.
        let errors = vec![
            HakuzuError::LeaderUnavailable("test".into()),
            HakuzuError::NotLeader,
            HakuzuError::DatabaseError("test".into()),
            HakuzuError::JournalError("test".into()),
            HakuzuError::CoordinatorError("test".into()),
            HakuzuError::EngineClosed,
            HakuzuError::ManifestCasConflict {
                db_name: "db".into(),
                expected_version: Some(5),
            },
        ];
        for err in errors {
            match err {
                HakuzuError::LeaderUnavailable(_) => {}
                HakuzuError::NotLeader => {}
                HakuzuError::DatabaseError(_) => {}
                HakuzuError::JournalError(_) => {}
                HakuzuError::CoordinatorError(_) => {}
                HakuzuError::EngineClosed => {}
                HakuzuError::ManifestCasConflict { .. } => {}
            }
        }
    }
}
