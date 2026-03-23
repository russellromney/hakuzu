# hakuzu Roadmap

## Phase 6: Behavioral Parity with graphd/strana

Port deterministic rewriting and fix correctness gaps identified in behavioral audit against graphd.

### 6.1 Deterministic Query Rewriter
- Port `rewriter.rs` from strana → hakuzu
- Replace `gen_random_uuid()`, `current_timestamp()`, `current_date()` with parameter references + concrete values
- Rewrite `REMOVE n.prop` → `SET n.prop = NULL` (Kuzu doesn't support REMOVE)
- String-literal aware, case-insensitive, word-boundary checks

### 6.2 Wire Rewriter into Execute + Journal Path
- `execute_write_local()`: rewrite query before execution, execute with merged params
- Journal the **rewritten** query + **merged** params (not the original)
- Followers replay deterministic queries — identical state on all nodes

### 6.3 Fix Param Type Conversion Bugs
- `json_to_param_value()`: u64 > i64::MAX → fallback to f64 instead of Null
- `json_to_param_value()`: Object → JSON string instead of Null

### 6.4 Follower Replay Locking
- `replay_entries` in follower_behavior.rs must hold write_mutex + snapshot_lock
- Prevents concurrent reads from seeing partial replay state
