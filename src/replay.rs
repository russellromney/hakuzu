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
fn param_value_to_lbug(v: &ParamValue) -> Result<lbug::Value, String> {
    match v {
        ParamValue::Null => Ok(lbug::Value::Null(lbug::LogicalType::Any)),
        ParamValue::Bool(b) => Ok(lbug::Value::Bool(*b)),
        ParamValue::Int(i) => Ok(lbug::Value::Int64(*i)),
        ParamValue::Float(f) => Ok(lbug::Value::Double(*f)),
        ParamValue::String(s) => Ok(lbug::Value::String(s.clone())),
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
