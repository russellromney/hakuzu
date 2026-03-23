//! Value conversions between lbug, JSON, and graphstream types.
//!
//! Three conversion directions:
//! - `lbug_to_json`: query results → JSON (for QueryResult)
//! - `json_to_lbug`: JSON params → lbug values (for query execution)
//! - `json_params_to_graphstream`: JSON params → graphstream ParamValue (for journaling)

use graphstream::types::ParamValue;
use serde_json::{json, Value};
use std::collections::HashMap;

/// Convert a lbug query result value to JSON.
pub fn lbug_to_json(v: &lbug::Value) -> Value {
    match v {
        lbug::Value::Null(_) => Value::Null,
        lbug::Value::Bool(b) => json!(*b),
        lbug::Value::Int8(n) => json!(*n as i64),
        lbug::Value::Int16(n) => json!(*n as i64),
        lbug::Value::Int32(n) => json!(*n as i64),
        lbug::Value::Int64(n) => json!(*n),
        lbug::Value::Int128(n) => json!(n.to_string()),
        lbug::Value::UInt8(n) => json!(*n as i64),
        lbug::Value::UInt16(n) => json!(*n as i64),
        lbug::Value::UInt32(n) => json!(*n as i64),
        lbug::Value::UInt64(n) => {
            if *n <= i64::MAX as u64 {
                json!(*n as i64)
            } else {
                json!(n.to_string())
            }
        }
        lbug::Value::Float(f) => json!(*f as f64),
        lbug::Value::Double(f) => json!(*f),
        lbug::Value::Decimal(d) => json!(d.to_string()),
        lbug::Value::String(s) => json!(s),
        lbug::Value::Blob(b) => {
            use base64::Engine;
            json!(base64::engine::general_purpose::STANDARD.encode(b))
        }
        lbug::Value::UUID(u) => json!(u.to_string()),
        lbug::Value::Date(d) => json!(d.to_string()),
        lbug::Value::Timestamp(t)
        | lbug::Value::TimestampTz(t)
        | lbug::Value::TimestampNs(t)
        | lbug::Value::TimestampMs(t)
        | lbug::Value::TimestampSec(t) => json!(t.to_string()),
        lbug::Value::Interval(d) => json!(d.to_string()),
        lbug::Value::List(_, items) | lbug::Value::Array(_, items) => {
            Value::Array(items.iter().map(lbug_to_json).collect())
        }
        lbug::Value::Map(_, entries) => {
            let mut map = serde_json::Map::new();
            for (k, v) in entries {
                let key = match k {
                    lbug::Value::String(s) => s.clone(),
                    other => format!("{other:?}"),
                };
                map.insert(key, lbug_to_json(v));
            }
            Value::Object(map)
        }
        lbug::Value::Struct(fields) => {
            let mut map = serde_json::Map::new();
            for (key, val) in fields {
                map.insert(key.clone(), lbug_to_json(val));
            }
            Value::Object(map)
        }
        lbug::Value::Node(node) => {
            let mut properties = HashMap::new();
            for (key, val) in node.get_properties() {
                properties.insert(key.clone(), lbug_to_json(val));
            }
            json!({
                "$type": "node",
                "id": {"table": node.get_node_id().table_id, "offset": node.get_node_id().offset},
                "label": node.get_label_name(),
                "properties": properties,
            })
        }
        lbug::Value::Rel(rel) => {
            let mut properties = HashMap::new();
            for (key, val) in rel.get_properties() {
                properties.insert(key.clone(), lbug_to_json(val));
            }
            json!({
                "$type": "rel",
                "label": rel.get_label_name(),
                "src": {"table": rel.get_src_node().table_id, "offset": rel.get_src_node().offset},
                "dst": {"table": rel.get_dst_node().table_id, "offset": rel.get_dst_node().offset},
                "properties": properties,
            })
        }
        lbug::Value::RecursiveRel { nodes, rels } => {
            json!({
                "$type": "path",
                "nodes": nodes.iter().map(|n| lbug_to_json(&lbug::Value::Node(n.clone()))).collect::<Vec<_>>(),
                "rels": rels.iter().map(|r| lbug_to_json(&lbug::Value::Rel(r.clone()))).collect::<Vec<_>>(),
            })
        }
        lbug::Value::InternalID(id) => {
            json!({"table": id.table_id, "offset": id.offset})
        }
        lbug::Value::Union { value, .. } => lbug_to_json(value),
    }
}

/// Convert a JSON value to a lbug value for query parameter binding.
pub fn json_to_lbug(v: &Value) -> Result<lbug::Value, String> {
    match v {
        Value::Null => Ok(lbug::Value::Null(lbug::LogicalType::Any)),
        Value::Bool(b) => Ok(lbug::Value::Bool(*b)),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(lbug::Value::Int64(i))
            } else if let Some(u) = n.as_u64() {
                Ok(lbug::Value::UInt64(u))
            } else if let Some(f) = n.as_f64() {
                Ok(lbug::Value::Double(f))
            } else {
                Err("Unsupported number type".into())
            }
        }
        Value::String(s) => Ok(lbug::Value::String(s.clone())),
        Value::Array(arr) => {
            let items: Result<Vec<lbug::Value>, String> = arr.iter().map(json_to_lbug).collect();
            let items = items?;
            let elem_type = items
                .first()
                .map(lbug_value_logical_type)
                .unwrap_or(lbug::LogicalType::Any);
            Ok(lbug::Value::List(elem_type, items))
        }
        Value::Object(_) => Err("Object params not supported — use primitive types".into()),
    }
}

/// Convert a JSON params object to Vec<(name, lbug::Value)> for query execution.
pub fn json_params_to_lbug(params: &Value) -> Result<Vec<(String, lbug::Value)>, String> {
    let obj = params
        .as_object()
        .ok_or("params must be a JSON object")?;
    obj.iter()
        .map(|(k, v)| Ok((k.clone(), json_to_lbug(v)?)))
        .collect()
}

/// Convert a JSON params object to graphstream's ParamValue format for journaling.
pub fn json_params_to_graphstream(params: &Value) -> Vec<(String, ParamValue)> {
    let obj = match params.as_object() {
        Some(o) => o,
        None => return vec![],
    };
    obj.iter()
        .map(|(k, v)| (k.clone(), json_to_param_value(v)))
        .collect()
}

fn json_to_param_value(v: &Value) -> ParamValue {
    match v {
        Value::Null => ParamValue::Null,
        Value::Bool(b) => ParamValue::Bool(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                ParamValue::Int(i)
            } else if let Some(f) = n.as_f64() {
                ParamValue::Float(f)
            } else {
                ParamValue::Null
            }
        }
        Value::String(s) => ParamValue::String(s.clone()),
        Value::Array(arr) => ParamValue::List(arr.iter().map(json_to_param_value).collect()),
        Value::Object(_) => ParamValue::Null, // graphstream doesn't support map params
    }
}

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
