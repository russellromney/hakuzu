//! Mutation detection for auto-routing reads vs writes.
//!
//! Copied from graphd-engine's `journal_types.rs`. Word-boundary keyword check.
//! Over-journaling reads is harmless; missing mutations would be catastrophic.

const MUTATION_KEYWORDS: &[&str] = &[
    "CREATE", "MERGE", "DELETE", "DROP", "ALTER", "COPY", "SET", "REMOVE",
    "INSTALL", "UNINSTALL", "IMPORT", "ATTACH", "DETACH",
    // CALL is routed to the write path as a safe default. Some CALLs are
    // read-only (e.g., CALL current_setting('threads')), but others mutate
    // (e.g., CALL db.checkpoint()). Routing read-only CALLs through the write
    // path is slower but correct — missing a mutating CALL would be catastrophic.
    "CALL",
];

/// Returns true if the query likely contains a mutation keyword.
pub fn is_mutation(query: &str) -> bool {
    let upper = query.to_ascii_uppercase();
    let bytes = upper.as_bytes();
    for keyword in MUTATION_KEYWORDS {
        let kw_bytes = keyword.as_bytes();
        let kw_len = kw_bytes.len();
        let mut i = 0;
        while i + kw_len <= bytes.len() {
            if &bytes[i..i + kw_len] == kw_bytes {
                let before_ok = i == 0 || !bytes[i - 1].is_ascii_alphanumeric();
                let after_ok =
                    i + kw_len >= bytes.len() || !bytes[i + kw_len].is_ascii_alphanumeric();
                if before_ok && after_ok {
                    return true;
                }
            }
            i += 1;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_is_mutation() {
        assert!(is_mutation("CREATE (:Person {name: 'Alice'})"));
    }

    #[test]
    fn test_match_is_not_mutation() {
        assert!(!is_mutation("MATCH (p:Person) RETURN p"));
    }

    #[test]
    fn test_merge_is_mutation() {
        assert!(is_mutation("MERGE (p:Person {id: 1})"));
    }

    #[test]
    fn test_delete_is_mutation() {
        assert!(is_mutation("MATCH (p) DELETE p"));
    }

    #[test]
    fn test_set_is_mutation() {
        assert!(is_mutation("MATCH (p:Person) SET p.name = 'Bob'"));
    }

    #[test]
    fn test_drop_is_mutation() {
        assert!(is_mutation("DROP TABLE Person"));
    }

    #[test]
    fn test_return_is_not_mutation() {
        assert!(!is_mutation("RETURN 1 AS n"));
    }

    #[test]
    fn test_explain_is_not_mutation() {
        assert!(!is_mutation("EXPLAIN MATCH (p:Person) RETURN p"));
    }

    #[test]
    fn test_profile_is_not_mutation() {
        assert!(!is_mutation("PROFILE MATCH (p:Person) RETURN p"));
    }

    #[test]
    fn test_create_node_table_is_mutation() {
        assert!(is_mutation("CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id))"));
    }

    #[test]
    fn test_remove_is_mutation() {
        assert!(is_mutation("MATCH (n:Person) REMOVE n.age"));
    }

    #[test]
    fn test_word_boundary_no_false_positive() {
        // "CREATED" should not match "CREATE"
        assert!(!is_mutation("MATCH (p:CREATED) RETURN p"));
    }

    #[test]
    fn test_case_insensitive() {
        assert!(is_mutation("create (:Person {name: 'Alice'})"));
    }

    #[test]
    fn test_call_is_mutation() {
        assert!(is_mutation("CALL db.checkpoint()"));
    }

    #[test]
    fn test_call_read_only_is_mutation() {
        // Read-only CALLs are routed to write path as a safe default.
        assert!(is_mutation("CALL current_setting('threads')"));
    }

    #[test]
    fn test_call_case_insensitive() {
        assert!(is_mutation("call db.checkpoint()"));
    }

    #[test]
    fn test_callback_is_not_mutation() {
        // "CALLBACK" should not match "CALL" (word boundary).
        assert!(!is_mutation("MATCH (p:CALLBACK) RETURN p"));
    }
}
