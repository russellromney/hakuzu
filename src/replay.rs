//! Replay journal entries against a Kuzu/LadybugDB database.
//!
//! This is the bridge between graphstream entries and Kuzu query execution.
//! Journal entries contain Cypher queries + params; replay executes them against
//! a local Kuzu database via lbug.

use std::path::Path;

use graphstream::journal::JournalReader;
use graphstream::types::{map_entries_to_param_values, ParamValue};
use tracing::debug;

/// Replay journal entries from `since_seq` forward against a Kuzu database.
///
/// Returns the last sequence number replayed, or `since_seq` if nothing was replayed.
pub fn replay_entries(
    conn: &lbug::Connection,
    journal_dir: &Path,
    since_seq: u64,
) -> Result<u64, String> {
    let reader = JournalReader::from_sequence(journal_dir, since_seq + 1, [0u8; 32])?;

    conn.query("BEGIN TRANSACTION")
        .map_err(|e| format!("BEGIN TRANSACTION failed: {e}"))?;

    let mut last_seq = since_seq;

    for item in reader {
        let entry = item?;
        if entry.sequence <= since_seq {
            continue;
        }

        let params = map_entries_to_param_values(&entry.entry.params);

        match execute_with_params(conn, &entry.entry.query, &params) {
            Ok(()) => {
                debug!("Replayed entry seq={}", entry.sequence);
                last_seq = entry.sequence;
            }
            Err(e) => {
                // Abort the transaction — a failed replay entry means the follower
                // would diverge from the leader. Fail fast so the caller can retry.
                let _ = conn.query("ROLLBACK");
                return Err(format!(
                    "Replay failed at seq={}: {e}",
                    entry.sequence
                ));
            }
        }
    }

    conn.query("COMMIT")
        .map_err(|e| format!("COMMIT failed: {e}"))?;

    Ok(last_seq)
}

/// Execute a Cypher query with typed parameters against a Kuzu connection.
fn execute_with_params(
    conn: &lbug::Connection,
    query: &str,
    params: &[(String, ParamValue)],
) -> Result<(), String> {
    if params.is_empty() {
        conn.query(query)
            .map_err(|e| format!("Query failed: {e}"))?;
        return Ok(());
    }

    // Prepare statement and execute with parameters.
    let mut stmt = conn
        .prepare(query)
        .map_err(|e| format!("Prepare failed: {e}"))?;

    let lbug_params: Vec<(&str, lbug::Value)> = params
        .iter()
        .map(|(name, value)| {
            param_value_to_lbug(value).map(|v| (name.as_str(), v))
        })
        .collect::<Result<Vec<_>, _>>()?;

    conn.execute(&mut stmt, lbug_params)
        .map_err(|e| format!("Execute failed: {e}"))?;

    Ok(())
}

/// Convert a ParamValue to a lbug::Value for query parameter binding.
///
/// Temporal variants carry ISO 8601 strings; they are parsed to native
/// `time::` types so Kuzu binds them as the correct temporal type rather than
/// as an opaque string (which would silently diverge from the leader).
pub fn param_value_to_lbug(v: &ParamValue) -> Result<lbug::Value, String> {
    match v {
        ParamValue::Null => Ok(lbug::Value::Null(lbug::LogicalType::Any)),
        ParamValue::Bool(b) => Ok(lbug::Value::Bool(*b)),
        ParamValue::Int(i) => Ok(lbug::Value::Int64(*i)),
        ParamValue::Float(f) => Ok(lbug::Value::Double(*f)),
        ParamValue::String(s) => Ok(lbug::Value::String(s.clone())),
        ParamValue::Bytes(b) => Ok(lbug::Value::Blob(b.clone())),
        ParamValue::List(items) => {
            let converted: Result<Vec<lbug::Value>, String> =
                items.iter().map(param_value_to_lbug).collect();
            let converted = converted?;
            let elem_type = converted
                .first()
                .map(lbug_value_logical_type)
                .unwrap_or(lbug::LogicalType::Any);
            Ok(lbug::Value::List(elem_type, converted))
        }
        ParamValue::Map(entries) => {
            let converted: Result<Vec<(lbug::Value, lbug::Value)>, String> = entries
                .iter()
                .map(|(k, v)| Ok((lbug::Value::String(k.clone()), param_value_to_lbug(v)?)))
                .collect();
            let converted = converted?;
            let val_type = converted
                .first()
                .map(|(_, v)| lbug_value_logical_type(v))
                .unwrap_or(lbug::LogicalType::Any);
            Ok(lbug::Value::Map(
                (lbug::LogicalType::String, val_type),
                converted,
            ))
        }
        ParamValue::Date(iso) => {
            let d = parse_iso_date(iso)?;
            Ok(lbug::Value::Date(d))
        }
        ParamValue::DateTime(iso) => {
            let ts = parse_iso_datetime(iso)?;
            Ok(lbug::Value::Timestamp(ts))
        }
        ParamValue::LocalDateTime(iso) => {
            // No offset: bind at UTC. Follower uses the same logic, so both sides
            // store the same point-in-time.
            let ts = parse_iso_local_datetime(iso)?;
            Ok(lbug::Value::Timestamp(ts))
        }
        ParamValue::Time(iso) | ParamValue::LocalTime(iso) => {
            // Kuzu has no pure Time type. Bind as string so a TIME column fails
            // fast with a type mismatch rather than silently coercing.
            Ok(lbug::Value::String(iso.clone()))
        }
        ParamValue::Duration(iso) => {
            let d = parse_iso_duration(iso)?;
            Ok(lbug::Value::Interval(d))
        }
        ParamValue::Point { srid, x, y, z } => {
            // Kuzu has no native point type. Bind as a STRUCT so replay fails
            // loudly on a column that expects one.
            let mut fields = vec![
                ("srid".to_string(), lbug::Value::Int64(*srid as i64)),
                ("x".to_string(), lbug::Value::Double(*x)),
                ("y".to_string(), lbug::Value::Double(*y)),
            ];
            if let Some(z) = z {
                fields.push(("z".to_string(), lbug::Value::Double(*z)));
            }
            Ok(lbug::Value::Struct(fields))
        }
    }
}

/// Infer the logical type from a lbug Value (for list element type).
fn lbug_value_logical_type(v: &lbug::Value) -> lbug::LogicalType {
    match v {
        lbug::Value::Bool(_) => lbug::LogicalType::Bool,
        lbug::Value::Int64(_) => lbug::LogicalType::Int64,
        lbug::Value::UInt64(_) => lbug::LogicalType::UInt64,
        lbug::Value::Double(_) => lbug::LogicalType::Double,
        lbug::Value::Float(_) => lbug::LogicalType::Float,
        lbug::Value::String(_) => lbug::LogicalType::String,
        _ => lbug::LogicalType::Any,
    }
}

fn parse_iso_date(iso: &str) -> Result<time::Date, String> {
    time::Date::parse(
        iso,
        &time::format_description::well_known::Iso8601::DATE,
    )
    .map_err(|e| format!("invalid ISO date '{iso}': {e}"))
}

fn parse_iso_datetime(iso: &str) -> Result<time::OffsetDateTime, String> {
    time::OffsetDateTime::parse(iso, &time::format_description::well_known::Rfc3339)
        .map_err(|e| format!("invalid ISO datetime '{iso}': {e}"))
}

fn parse_iso_local_datetime(iso: &str) -> Result<time::OffsetDateTime, String> {
    // Accept `YYYY-MM-DDTHH:MM:SS[.fff]` (no offset). Treat as UTC for binding;
    // leader and follower apply the same rule so the stored microsecond value
    // matches. If a user needs zone-accurate semantics, they should send a
    // DateTime with an explicit offset.
    let with_z = if iso.ends_with('Z') || iso.contains('+') {
        iso.to_string()
    } else {
        format!("{iso}Z")
    };
    time::OffsetDateTime::parse(&with_z, &time::format_description::well_known::Rfc3339)
        .map_err(|e| format!("invalid local datetime '{iso}': {e}"))
}

/// Parse a strict subset of ISO 8601 durations: `P[nY][nM][nD][T[nH][nM][nS]]`.
/// Supports integer and fractional seconds (e.g., `PT1.5S`).
fn parse_iso_duration(iso: &str) -> Result<time::Duration, String> {
    let rest = iso
        .strip_prefix('P')
        .ok_or_else(|| format!("invalid ISO duration '{iso}': missing 'P' prefix"))?;

    let (date_part, time_part) = match rest.split_once('T') {
        Some((d, t)) => (d, t),
        None => (rest, ""),
    };

    let mut total_secs: i64 = 0;
    let mut nanos: i64 = 0;

    for (value, unit) in tokenize_duration(date_part)? {
        let n = value
            .parse::<i64>()
            .map_err(|e| format!("invalid duration component '{value}{unit}': {e}"))?;
        match unit {
            'Y' => total_secs += n * 365 * 86_400,
            'M' => total_secs += n * 30 * 86_400,
            'W' => total_secs += n * 7 * 86_400,
            'D' => total_secs += n * 86_400,
            other => return Err(format!("unexpected date-part unit '{other}' in duration")),
        }
    }

    if !time_part.is_empty() {
        for (value, unit) in tokenize_duration(time_part)? {
            match unit {
                'H' => {
                    let n = value
                        .parse::<i64>()
                        .map_err(|e| format!("invalid hours '{value}': {e}"))?;
                    total_secs += n * 3600;
                }
                'M' => {
                    let n = value
                        .parse::<i64>()
                        .map_err(|e| format!("invalid minutes '{value}': {e}"))?;
                    total_secs += n * 60;
                }
                'S' => {
                    // Fractional seconds allowed here.
                    let f: f64 = value
                        .parse()
                        .map_err(|e| format!("invalid seconds '{value}': {e}"))?;
                    let whole = f.trunc() as i64;
                    let frac_ns = (f.fract() * 1_000_000_000.0).round() as i64;
                    total_secs += whole;
                    nanos += frac_ns;
                }
                other => return Err(format!("unexpected time-part unit '{other}' in duration")),
            }
        }
    }

    Ok(time::Duration::new(total_secs, nanos as i32))
}

fn tokenize_duration(s: &str) -> Result<Vec<(String, char)>, String> {
    let mut out = Vec::new();
    let mut buf = String::new();
    for c in s.chars() {
        if c.is_ascii_digit() || c == '.' || c == '-' {
            buf.push(c);
        } else if c.is_ascii_alphabetic() {
            if buf.is_empty() {
                return Err(format!("duration unit '{c}' with no value"));
            }
            out.push((std::mem::take(&mut buf), c));
        } else {
            return Err(format!("invalid character in duration: '{c}'"));
        }
    }
    if !buf.is_empty() {
        return Err(format!("duration has trailing value with no unit: '{buf}'"));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_param_value_to_lbug_scalars() {
        assert!(matches!(
            param_value_to_lbug(&ParamValue::Null).unwrap(),
            lbug::Value::Null(_)
        ));
        assert!(matches!(
            param_value_to_lbug(&ParamValue::Bool(true)).unwrap(),
            lbug::Value::Bool(true)
        ));
        match param_value_to_lbug(&ParamValue::Int(42)).unwrap() {
            lbug::Value::Int64(n) => assert_eq!(n, 42),
            other => panic!("Expected Int64, got {other:?}"),
        }
        match param_value_to_lbug(&ParamValue::Float(3.14)).unwrap() {
            lbug::Value::Double(f) => assert!((f - 3.14).abs() < 1e-10),
            other => panic!("Expected Double, got {other:?}"),
        }
    }

    #[test]
    fn test_param_value_to_lbug_bytes() {
        match param_value_to_lbug(&ParamValue::Bytes(vec![1, 2, 3])).unwrap() {
            lbug::Value::Blob(b) => assert_eq!(b, vec![1, 2, 3]),
            other => panic!("Expected Blob, got {other:?}"),
        }
    }

    #[test]
    fn test_param_value_to_lbug_map() {
        let pv = ParamValue::Map(vec![
            ("x".into(), ParamValue::Int(1)),
            ("y".into(), ParamValue::Int(2)),
        ]);
        match param_value_to_lbug(&pv).unwrap() {
            lbug::Value::Map(_, entries) => {
                assert_eq!(entries.len(), 2);
            }
            other => panic!("Expected Map, got {other:?}"),
        }
    }

    #[test]
    fn test_param_value_to_lbug_date() {
        match param_value_to_lbug(&ParamValue::Date("2024-01-15".into())).unwrap() {
            lbug::Value::Date(d) => {
                assert_eq!(d.year(), 2024);
                assert_eq!(u8::from(d.month()), 1);
                assert_eq!(d.day(), 15);
            }
            other => panic!("Expected Date, got {other:?}"),
        }
    }

    #[test]
    fn test_param_value_to_lbug_datetime() {
        let iso = "2024-01-15T10:30:00+00:00";
        match param_value_to_lbug(&ParamValue::DateTime(iso.into())).unwrap() {
            lbug::Value::Timestamp(ts) => {
                assert_eq!(ts.year(), 2024);
                assert_eq!(ts.hour(), 10);
                assert_eq!(ts.minute(), 30);
            }
            other => panic!("Expected Timestamp, got {other:?}"),
        }
    }

    #[test]
    fn test_param_value_to_lbug_duration_basic() {
        let d = match param_value_to_lbug(&ParamValue::Duration("PT1H30M".into())).unwrap() {
            lbug::Value::Interval(d) => d,
            other => panic!("Expected Interval, got {other:?}"),
        };
        assert_eq!(d.whole_seconds(), 3600 + 30 * 60);
    }

    #[test]
    fn test_param_value_to_lbug_duration_fractional_seconds() {
        let d = match param_value_to_lbug(&ParamValue::Duration("PT1.5S".into())).unwrap() {
            lbug::Value::Interval(d) => d,
            other => panic!("Expected Interval, got {other:?}"),
        };
        assert_eq!(d.whole_seconds(), 1);
        assert_eq!(d.subsec_nanoseconds(), 500_000_000);
    }

    #[test]
    fn test_param_value_to_lbug_duration_days() {
        let d = match param_value_to_lbug(&ParamValue::Duration("P2D".into())).unwrap() {
            lbug::Value::Interval(d) => d,
            other => panic!("Expected Interval, got {other:?}"),
        };
        assert_eq!(d.whole_seconds(), 2 * 86_400);
    }

    #[test]
    fn test_param_value_to_lbug_duration_invalid() {
        assert!(param_value_to_lbug(&ParamValue::Duration("garbage".into())).is_err());
        assert!(param_value_to_lbug(&ParamValue::Duration("P".into())).is_ok());
    }

    #[test]
    fn test_param_value_to_lbug_date_invalid() {
        assert!(param_value_to_lbug(&ParamValue::Date("not-a-date".into())).is_err());
    }
}
