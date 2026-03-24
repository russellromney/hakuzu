//! Prometheus-compatible metrics for hakuzu.
//!
//! Lightweight AtomicU64 counters and gauges — no external prometheus crate needed.
//! Thread-safe, zero-allocation on reads.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

pub struct HakuzuMetrics {
    // Writes
    pub writes_total: AtomicU64,
    pub writes_forwarded: AtomicU64,

    // Reads
    pub reads_total: AtomicU64,

    // Errors
    pub forwarding_errors: AtomicU64,

    // Timing (microseconds)
    pub last_write_duration_us: AtomicU64,
    pub last_read_duration_us: AtomicU64,
    pub last_forward_duration_us: AtomicU64,

    // Journal
    pub journal_sequence: AtomicU64,
}

impl HakuzuMetrics {
    pub fn new() -> Self {
        Self {
            writes_total: AtomicU64::new(0),
            writes_forwarded: AtomicU64::new(0),
            reads_total: AtomicU64::new(0),
            forwarding_errors: AtomicU64::new(0),
            last_write_duration_us: AtomicU64::new(0),
            last_read_duration_us: AtomicU64::new(0),
            last_forward_duration_us: AtomicU64::new(0),
            journal_sequence: AtomicU64::new(0),
        }
    }

    pub fn inc(&self, counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set(&self, gauge: &AtomicU64, value: u64) {
        gauge.store(value, Ordering::Relaxed);
    }

    pub fn record_duration(&self, target: &AtomicU64, start: Instant) {
        target.store(start.elapsed().as_micros() as u64, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> HakuzuMetricsSnapshot {
        HakuzuMetricsSnapshot {
            writes_total: self.writes_total.load(Ordering::Relaxed),
            writes_forwarded: self.writes_forwarded.load(Ordering::Relaxed),
            reads_total: self.reads_total.load(Ordering::Relaxed),
            forwarding_errors: self.forwarding_errors.load(Ordering::Relaxed),
            last_write_duration_us: self.last_write_duration_us.load(Ordering::Relaxed),
            last_read_duration_us: self.last_read_duration_us.load(Ordering::Relaxed),
            last_forward_duration_us: self.last_forward_duration_us.load(Ordering::Relaxed),
            journal_sequence: self.journal_sequence.load(Ordering::Relaxed),
        }
    }
}

impl Default for HakuzuMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct HakuzuMetricsSnapshot {
    pub writes_total: u64,
    pub writes_forwarded: u64,
    pub reads_total: u64,
    pub forwarding_errors: u64,
    pub last_write_duration_us: u64,
    pub last_read_duration_us: u64,
    pub last_forward_duration_us: u64,
    pub journal_sequence: u64,
}

impl HakuzuMetricsSnapshot {
    pub fn to_prometheus(&self) -> String {
        let mut out = String::with_capacity(1024);

        Self::counter(
            &mut out,
            "hakuzu_writes_total",
            "Total write operations",
            self.writes_total,
        );
        Self::counter(
            &mut out,
            "hakuzu_writes_forwarded_total",
            "Write operations forwarded to leader",
            self.writes_forwarded,
        );
        Self::counter(
            &mut out,
            "hakuzu_reads_total",
            "Total read operations",
            self.reads_total,
        );
        Self::counter(
            &mut out,
            "hakuzu_forwarding_errors_total",
            "Total write forwarding errors",
            self.forwarding_errors,
        );

        Self::gauge(
            &mut out,
            "hakuzu_last_write_duration_seconds",
            "Duration of last write operation",
            self.last_write_duration_us as f64 / 1_000_000.0,
        );
        Self::gauge(
            &mut out,
            "hakuzu_last_read_duration_seconds",
            "Duration of last read operation",
            self.last_read_duration_us as f64 / 1_000_000.0,
        );
        Self::gauge(
            &mut out,
            "hakuzu_last_forward_duration_seconds",
            "Duration of last forwarded write operation",
            self.last_forward_duration_us as f64 / 1_000_000.0,
        );
        Self::gauge(
            &mut out,
            "hakuzu_journal_sequence",
            "Current journal sequence number",
            self.journal_sequence as f64,
        );

        out
    }

    fn counter(out: &mut String, name: &str, help: &str, value: u64) {
        out.push_str(&format!(
            "# HELP {} {}\n# TYPE {} counter\n{} {}\n",
            name, help, name, name, value
        ));
    }

    fn gauge(out: &mut String, name: &str, help: &str, value: f64) {
        out.push_str(&format!(
            "# HELP {} {}\n# TYPE {} gauge\n{} {:.6}\n",
            name, help, name, name, value
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_write_read() {
        let m = HakuzuMetrics::new();
        m.inc(&m.writes_total);
        m.inc(&m.writes_total);
        m.inc(&m.writes_forwarded);
        m.inc(&m.reads_total);
        m.inc(&m.reads_total);
        m.inc(&m.reads_total);
        m.set(&m.journal_sequence, 42);

        let snap = m.snapshot();
        assert_eq!(snap.writes_total, 2);
        assert_eq!(snap.writes_forwarded, 1);
        assert_eq!(snap.reads_total, 3);
        assert_eq!(snap.journal_sequence, 42);
    }

    #[test]
    fn test_metrics_record_duration() {
        let m = HakuzuMetrics::new();
        let start = Instant::now();
        std::thread::sleep(std::time::Duration::from_millis(5));
        m.record_duration(&m.last_write_duration_us, start);

        let snap = m.snapshot();
        assert!(
            snap.last_write_duration_us >= 4000,
            "Should record >= 4ms, got {}us",
            snap.last_write_duration_us
        );
    }

    #[test]
    fn test_prometheus_format() {
        let m = HakuzuMetrics::new();
        m.inc(&m.writes_total);
        m.inc(&m.reads_total);
        m.set(&m.journal_sequence, 100);

        let text = m.snapshot().to_prometheus();
        assert!(text.contains("# TYPE hakuzu_writes_total counter"));
        assert!(text.contains("hakuzu_writes_total 1"));
        assert!(text.contains("hakuzu_reads_total 1"));
        assert!(text.contains("hakuzu_journal_sequence 100"));
    }

    #[test]
    fn test_prometheus_all_metrics_present() {
        let m = HakuzuMetrics::new();
        let text = m.snapshot().to_prometheus();

        let expected = [
            "hakuzu_writes_total",
            "hakuzu_writes_forwarded_total",
            "hakuzu_reads_total",
            "hakuzu_forwarding_errors_total",
            "hakuzu_last_write_duration_seconds",
            "hakuzu_last_read_duration_seconds",
            "hakuzu_last_forward_duration_seconds",
            "hakuzu_journal_sequence",
        ];
        for name in expected {
            assert!(text.contains(name), "Missing metric: {}", name);
        }
    }
}
