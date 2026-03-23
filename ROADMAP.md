# hakuzu Roadmap

## Phase 3: Operational Resilience (remaining)

### RSS profiling tools

Port walrust's `bench/measure_rss.py` pattern to hakuzu. Measure RSS at each stage:
- After Kuzu DB open + schema apply
- After coordinator join + role assignment
- After N write batches (varying batch sizes)
- After follower journal replay
- Idle after load

This catches regressions like walrust's 70MB→20MB fix. Keep the profiler in `bench/` permanently.

### Connection pool investigation

Currently creates a new `lbug::Connection` per operation inside `spawn_blocking` because Connection is not Send. Profile whether connection creation is a measurable cost under load. If so, investigate per-thread connection caching via `thread_local!`.

### Read semaphore tuning

Default read_semaphore is 16. This was inherited from graphd-engine without benchmarking in hakuzu's context. Profile read latency under concurrent load with different semaphore values (8, 16, 32, 64) to find the sweet spot for Kuzu's MVCC reader.

## Phase 4: Production Hardening (remaining)

### Cache cleanup timer

Any disk-based cache (snapshot staging, journal segment cache) needs a periodic cleanup timer. walrust v0.6.0 learned this the hard way — shadow mode had a cache but no cleanup, leading to unbounded growth. Pattern: 5-minute `tokio::time::interval` in the main select loop, applying retention duration + max size limits. Add this to any mode that caches journal segments or snapshots locally.

### Structured error types

Currently returns `anyhow::Result` everywhere. For a library API, callers need to match on error kinds:
- `LeaderUnavailable` — write forwarding failed, leader unreachable
- `NotLeader` — tried to execute write locally but not leader
- `DatabaseError` — Kuzu query/execution error
- `JournalError` — graphstream write failed
- `CoordinatorError` — hadb lease/join failure

### Metrics

Export Prometheus metrics beyond coordinator metrics:
- `hakuzu_writes_total` (counter, by role: leader/forwarded)
- `hakuzu_reads_total` (counter)
- `hakuzu_write_latency_seconds` (histogram)
- `hakuzu_read_latency_seconds` (histogram)
- `hakuzu_journal_sequence` (gauge)
- `hakuzu_follower_lag_entries` (gauge — leader sequence minus follower sequence)
- `hakuzu_forwarding_errors_total` (counter)
