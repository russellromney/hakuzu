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
        Value::Object(_) => Ok(lbug::Value::String(
            serde_json::to_string(v).map_err(|e| format!("JSON serialize failed: {e}"))?,
        )),
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
                // Covers u64 > i64::MAX — loses precision for very large integers.
                // This is acceptable: Kuzu uses INT64 (max 2^63-1), so values > i64::MAX
                // can't be stored anyway. All values in [0, i64::MAX] are handled by
                // as_i64() above with no precision loss.
                ParamValue::Float(f)
            } else {
                // Should be unreachable — serde_json numbers are always i64, u64, or f64
                ParamValue::String(n.to_string())
            }
        }
        Value::String(s) => ParamValue::String(s.clone()),
        Value::Array(arr) => ParamValue::List(arr.iter().map(json_to_param_value).collect()),
        Value::Object(_) => ParamValue::String(serde_json::to_string(v).expect("JSON object must serialize")),
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

#[cfg(test)]
mod tests {
    use super::*;

    // --- json_to_lbug ---

    #[test]
    fn test_json_to_lbug_null() {
        assert!(matches!(json_to_lbug(&Value::Null).unwrap(), lbug::Value::Null(_)));
    }

    #[test]
    fn test_json_to_lbug_bool() {
        assert!(matches!(json_to_lbug(&json!(true)).unwrap(), lbug::Value::Bool(true)));
        assert!(matches!(json_to_lbug(&json!(false)).unwrap(), lbug::Value::Bool(false)));
    }

    #[test]
    fn test_json_to_lbug_int() {
        match json_to_lbug(&json!(42)).unwrap() {
            lbug::Value::Int64(n) => assert_eq!(n, 42),
            other => panic!("Expected Int64, got {:?}", other),
        }
    }

    #[test]
    fn test_json_to_lbug_negative_int() {
        match json_to_lbug(&json!(-100)).unwrap() {
            lbug::Value::Int64(n) => assert_eq!(n, -100),
            other => panic!("Expected Int64, got {:?}", other),
        }
    }

    #[test]
    fn test_json_to_lbug_large_u64() {
        // u64::MAX cannot fit in i64, should become UInt64
        let val = json!(u64::MAX);
        match json_to_lbug(&val).unwrap() {
            lbug::Value::UInt64(n) => assert_eq!(n, u64::MAX),
            other => panic!("Expected UInt64, got {:?}", other),
        }
    }

    #[test]
    fn test_json_to_lbug_float() {
        match json_to_lbug(&json!(3.14)).unwrap() {
            lbug::Value::Double(f) => assert!((f - 3.14).abs() < 1e-10),
            other => panic!("Expected Double, got {:?}", other),
        }
    }

    #[test]
    fn test_json_to_lbug_string() {
        match json_to_lbug(&json!("hello")).unwrap() {
            lbug::Value::String(s) => assert_eq!(s, "hello"),
            other => panic!("Expected String, got {:?}", other),
        }
    }

    #[test]
    fn test_json_to_lbug_array() {
        let val = json!([1, 2, 3]);
        match json_to_lbug(&val).unwrap() {
            lbug::Value::List(_, items) => assert_eq!(items.len(), 3),
            other => panic!("Expected List, got {:?}", other),
        }
    }

    #[test]
    fn test_json_to_lbug_object_becomes_string() {
        let val = json!({"key": "value"});
        match json_to_lbug(&val).unwrap() {
            lbug::Value::String(s) => {
                let parsed: serde_json::Value = serde_json::from_str(&s).unwrap();
                assert_eq!(parsed["key"], "value");
            }
            other => panic!("Expected String (serialized JSON), got {:?}", other),
        }
    }

    // --- json_to_param_value ---

    #[test]
    fn test_param_value_null() {
        assert!(matches!(json_to_param_value(&Value::Null), ParamValue::Null));
    }

    #[test]
    fn test_param_value_bool() {
        assert!(matches!(json_to_param_value(&json!(true)), ParamValue::Bool(true)));
    }

    #[test]
    fn test_param_value_int() {
        match json_to_param_value(&json!(42)) {
            ParamValue::Int(n) => assert_eq!(n, 42),
            other => panic!("Expected Int, got {:?}", other),
        }
    }

    #[test]
    fn test_param_value_large_u64_becomes_float() {
        // u64::MAX can't fit in i64 → falls through to f64.
        // Acceptable: Kuzu uses INT64 (max i64::MAX), so these values
        // can't be stored in Kuzu anyway.
        let val = json!(u64::MAX);
        match json_to_param_value(&val) {
            ParamValue::Float(f) => assert!(f > 0.0),
            other => panic!("Expected Float for large u64, got {:?}", other),
        }
    }

    #[test]
    fn test_param_value_i64_max_is_exact_int() {
        // i64::MAX (2^63-1) is the largest value Kuzu INT64 can store.
        // Verify it's handled as Int (exact), not Float (lossy).
        let val = json!(i64::MAX);
        match json_to_param_value(&val) {
            ParamValue::Int(i) => assert_eq!(i, i64::MAX),
            other => panic!("Expected Int for i64::MAX, got {:?}", other),
        }
    }

    #[test]
    fn test_param_value_float() {
        match json_to_param_value(&json!(3.14)) {
            ParamValue::Float(f) => assert!((f - 3.14).abs() < 1e-10),
            other => panic!("Expected Float, got {:?}", other),
        }
    }

    #[test]
    fn test_param_value_string() {
        match json_to_param_value(&json!("hello")) {
            ParamValue::String(s) => assert_eq!(s, "hello"),
            other => panic!("Expected String, got {:?}", other),
        }
    }

    #[test]
    fn test_param_value_array() {
        let val = json!([1, 2]);
        match json_to_param_value(&val) {
            ParamValue::List(items) => assert_eq!(items.len(), 2),
            other => panic!("Expected List, got {:?}", other),
        }
    }

    #[test]
    fn test_param_value_object_becomes_json_string() {
        let val = json!({"nested": true});
        match json_to_param_value(&val) {
            ParamValue::String(s) => {
                let parsed: serde_json::Value = serde_json::from_str(&s).unwrap();
                assert_eq!(parsed["nested"], true);
            }
            other => panic!("Expected String (serialized JSON), got {:?}", other),
        }
    }

    // --- json_params_to_lbug ---

    #[test]
    fn test_json_params_to_lbug_basic() {
        let params = json!({"name": "Alice", "age": 30});
        let result = json_params_to_lbug(&params).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_json_params_to_lbug_not_object_fails() {
        let result = json_params_to_lbug(&json!("not an object"));
        assert!(result.is_err());
    }

    // --- json_params_to_graphstream ---

    #[test]
    fn test_json_params_to_graphstream_basic() {
        let params = json!({"name": "Alice"});
        let result = json_params_to_graphstream(&params);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, "name");
    }

    #[test]
    fn test_json_params_to_graphstream_not_object_returns_empty() {
        let result = json_params_to_graphstream(&json!(42));
        assert!(result.is_empty());
    }

    // --- lbug_to_json ---

    #[test]
    fn test_lbug_to_json_null() {
        assert_eq!(lbug_to_json(&lbug::Value::Null(lbug::LogicalType::Any)), Value::Null);
    }

    #[test]
    fn test_lbug_to_json_bool() {
        assert_eq!(lbug_to_json(&lbug::Value::Bool(true)), json!(true));
    }

    #[test]
    fn test_lbug_to_json_int64() {
        assert_eq!(lbug_to_json(&lbug::Value::Int64(42)), json!(42));
    }

    #[test]
    fn test_lbug_to_json_double() {
        assert_eq!(lbug_to_json(&lbug::Value::Double(3.14)), json!(3.14));
    }

    #[test]
    fn test_lbug_to_json_string() {
        assert_eq!(lbug_to_json(&lbug::Value::String("hello".into())), json!("hello"));
    }

    #[test]
    fn test_lbug_to_json_uint64_small() {
        assert_eq!(lbug_to_json(&lbug::Value::UInt64(100)), json!(100));
    }

    #[test]
    fn test_lbug_to_json_uint64_large() {
        let val = lbug_to_json(&lbug::Value::UInt64(u64::MAX));
        assert_eq!(val, json!(u64::MAX.to_string()));
    }

    #[test]
    fn test_lbug_to_json_list() {
        let list = lbug::Value::List(
            lbug::LogicalType::Int64,
            vec![lbug::Value::Int64(1), lbug::Value::Int64(2)],
        );
        assert_eq!(lbug_to_json(&list), json!([1, 2]));
    }
}
