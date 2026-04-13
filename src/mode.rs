//! Durability and HA topology modes for hakuzu.
//!
//! Re-exports hadb's canonical types. The mode matrix validation function
//! lives in hadb so both haqlite and hakuzu share the same rules.

pub use hadb::{Durability, HaMode, validate_mode_durability};

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
        assert!(validate_mode_durability(HaMode::Shared, Durability::Synchronous).is_ok());
    }

    #[test]
    fn shared_replicated_rejected() {
        let err = validate_mode_durability(HaMode::Shared, Durability::Replicated).unwrap_err();
        assert!(err.contains("Synchronous durability"), "got: {err}");
    }
}
