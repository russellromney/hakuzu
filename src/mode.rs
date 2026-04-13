//! Durability and HA topology modes for hakuzu.
//!
//! These enums control how data is made durable and how writes are coordinated
//! across nodes.

/// How data is made durable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// Graphstream journal shipping only. RPO = sync_interval.
    Replicated,
    /// Turbograph S3 page-level tiering. Every write durable in S3. RPO = 0.
    Synchronous,
}

/// Topology: who coordinates writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HaMode {
    /// Single persistent leader, followers replay.
    Dedicated,
    /// Multiple writers, lease-serialized per write.
    Shared,
}

impl Default for Durability {
    fn default() -> Self {
        Durability::Replicated
    }
}

impl Default for HaMode {
    fn default() -> Self {
        HaMode::Dedicated
    }
}

impl std::fmt::Display for Durability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Durability::Replicated => write!(f, "Replicated"),
            Durability::Synchronous => write!(f, "Synchronous"),
        }
    }
}

impl std::fmt::Display for HaMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HaMode::Dedicated => write!(f, "Dedicated"),
            HaMode::Shared => write!(f, "Shared"),
        }
    }
}

/// Validate a mode + durability combination.
///
/// Returns Ok(()) if the combination is valid, or an error describing why not.
pub fn validate_mode_durability(mode: HaMode, durability: Durability) -> Result<(), String> {
    match (mode, durability) {
        (HaMode::Dedicated, Durability::Replicated) => Ok(()),
        (HaMode::Dedicated, Durability::Synchronous) => Ok(()),
        (HaMode::Shared, Durability::Synchronous) => {
            Err("Shared mode not yet implemented".to_string())
        }
        (HaMode::Shared, Durability::Replicated) => {
            Err("Shared mode requires Synchronous durability".to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults() {
        assert_eq!(Durability::default(), Durability::Replicated);
        assert_eq!(HaMode::default(), HaMode::Dedicated);
    }

    #[test]
    fn display() {
        assert_eq!(format!("{}", Durability::Replicated), "Replicated");
        assert_eq!(format!("{}", Durability::Synchronous), "Synchronous");
        assert_eq!(format!("{}", HaMode::Dedicated), "Dedicated");
        assert_eq!(format!("{}", HaMode::Shared), "Shared");
    }

    #[test]
    fn valid_combinations() {
        assert!(validate_mode_durability(HaMode::Dedicated, Durability::Replicated).is_ok());
        assert!(validate_mode_durability(HaMode::Dedicated, Durability::Synchronous).is_ok());
    }

    #[test]
    fn shared_replicated_rejected() {
        let err = validate_mode_durability(HaMode::Shared, Durability::Replicated).unwrap_err();
        assert!(err.contains("Synchronous durability"), "got: {err}");
    }

    #[test]
    fn shared_synchronous_not_yet() {
        let err = validate_mode_durability(HaMode::Shared, Durability::Synchronous).unwrap_err();
        assert!(err.contains("not yet implemented"), "got: {err}");
    }
}
