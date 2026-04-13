//! Conversion between hadb's StorageManifest::Turbograph and turbograph's
//! internal JSON format.
//!
//! turbograph's C++ `Manifest::toJSON()` produces a specific JSON schema:
//! ```json
//! {
//!   "version": 5,
//!   "page_count": 50,
//!   "page_size": 4096,
//!   "pages_per_group": 4096,
//!   "page_group_keys": ["pg/0_v5"],
//!   "sub_pages_per_frame": 4,
//!   "frame_tables": [[[0, 8192, 2]]],
//!   "subframe_overrides": [{"0": {"key":"pg/0_f0_v5","offset":0,"len":4096,"pageCount":4}}],
//!   "encrypted": false,
//!   "journal_seq": 42
//! }
//! ```
//!
//! hadb stores the same data in structured Rust types (StorageManifest::Turbograph).
//! This module converts between the two representations so:
//! - Leader: parse turbograph JSON -> structured fields (for ManifestStore)
//! - Follower: structured fields -> turbograph JSON (for turbograph_set_manifest UDF)

use std::collections::BTreeMap;

use anyhow::{anyhow, Result};
use hadb::{HaManifest, StorageManifest};
use hadb::manifest::{FrameEntry, SubframeOverride};

/// Reconstruct turbograph's internal JSON from StorageManifest::Turbograph fields.
///
/// Produces the exact format that `Manifest::fromJSON()` on the C++ side expects.
pub fn to_turbograph_json(storage: &StorageManifest) -> Result<String> {
    let (
        turbograph_version,
        page_count,
        page_size,
        pages_per_group,
        sub_pages_per_frame,
        page_group_keys,
        frame_tables,
        subframe_overrides,
        encrypted,
        journal_seq,
    ) = match storage {
        StorageManifest::Turbograph {
            turbograph_version,
            page_count,
            page_size,
            pages_per_group,
            sub_pages_per_frame,
            page_group_keys,
            frame_tables,
            subframe_overrides,
            encrypted,
            journal_seq,
        } => (
            turbograph_version,
            page_count,
            page_size,
            pages_per_group,
            sub_pages_per_frame,
            page_group_keys,
            frame_tables,
            subframe_overrides,
            encrypted,
            journal_seq,
        ),
        _ => return Err(anyhow!("expected StorageManifest::Turbograph variant")),
    };

    // Build JSON matching turbograph's Manifest::toJSON() format exactly.
    let mut json = format!(
        r#"{{"version":{},"page_count":{},"page_size":{},"pages_per_group":{}"#,
        turbograph_version, page_count, page_size, pages_per_group,
    );

    // page_group_keys
    json.push_str(",\"page_group_keys\":[");
    for (i, key) in page_group_keys.iter().enumerate() {
        if i > 0 { json.push(','); }
        json.push('"');
        json.push_str(key);
        json.push('"');
    }
    json.push(']');

    // sub_pages_per_frame (omit when 0 for backward compat)
    if *sub_pages_per_frame > 0 {
        json.push_str(&format!(",\"sub_pages_per_frame\":{}", sub_pages_per_frame));
    }

    // frame_tables (omit when empty)
    if !frame_tables.is_empty() {
        json.push_str(",\"frame_tables\":[");
        for (g, group) in frame_tables.iter().enumerate() {
            if g > 0 { json.push(','); }
            json.push('[');
            for (f, entry) in group.iter().enumerate() {
                if f > 0 { json.push(','); }
                json.push_str(&format!("[{},{},{}]", entry.offset, entry.len, entry.page_count));
            }
            json.push(']');
        }
        json.push(']');
    }

    // subframe_overrides (omit when empty or all-empty)
    let has_overrides = subframe_overrides.iter().any(|m| !m.is_empty());
    if has_overrides {
        json.push_str(",\"subframe_overrides\":[");
        for (g, ov_map) in subframe_overrides.iter().enumerate() {
            if g > 0 { json.push(','); }
            if ov_map.is_empty() {
                json.push_str("{}");
            } else {
                json.push('{');
                let mut first = true;
                for (frame_idx, ov) in ov_map.iter() {
                    if !first { json.push(','); }
                    first = false;
                    json.push_str(&format!(
                        "\"{}\":{{\"key\":\"{}\",\"offset\":{},\"len\":{},\"pageCount\":{}}}",
                        frame_idx, ov.key, ov.entry.offset, ov.entry.len, ov.entry.page_count,
                    ));
                }
                json.push('}');
            }
        }
        json.push(']');
    }

    // encrypted (omit when false)
    if *encrypted {
        json.push_str(",\"encrypted\":true");
    }

    // journal_seq (omit when 0)
    if *journal_seq > 0 {
        json.push_str(&format!(",\"journal_seq\":{}", journal_seq));
    }

    json.push('}');
    Ok(json)
}

/// Parse turbograph's JSON into an HaManifest with fully populated structured fields.
///
/// Used by the leader after `turbograph_sync()` + `turbograph_get_manifest()`
/// to build the manifest that gets published to the ManifestStore.
pub fn parse_turbograph_json_to_ha_manifest(version: u64, json: &str) -> Result<HaManifest> {
    let v: serde_json::Value = serde_json::from_str(json)
        .map_err(|e| anyhow!("invalid turbograph manifest JSON: {e}"))?;

    let page_group_keys: Vec<String> = v["page_group_keys"]
        .as_array()
        .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        .unwrap_or_default();

    // Parse frame_tables: [[offset, len, pageCount], ...] per group
    let frame_tables: Vec<Vec<FrameEntry>> = v["frame_tables"]
        .as_array()
        .map(|groups| {
            groups.iter().map(|group| {
                group.as_array().map(|frames| {
                    frames.iter().filter_map(|frame| {
                        let arr = frame.as_array()?;
                        Some(FrameEntry {
                            offset: arr.first()?.as_u64()?,
                            len: arr.get(1)?.as_u64()? as u32,
                            page_count: arr.get(2)?.as_u64().unwrap_or(0) as u32,
                        })
                    }).collect()
                }).unwrap_or_default()
            }).collect()
        })
        .unwrap_or_default();

    // Parse subframe_overrides: array of objects, each maps frame index -> {key, offset, len, pageCount}
    let subframe_overrides: Vec<BTreeMap<usize, SubframeOverride>> = v["subframe_overrides"]
        .as_array()
        .map(|groups| {
            groups.iter().map(|group| {
                let obj = match group.as_object() {
                    Some(o) => o,
                    None => return BTreeMap::new(),
                };
                obj.iter().filter_map(|(idx_str, ov_val)| {
                    let idx: usize = idx_str.parse().ok()?;
                    Some((idx, SubframeOverride {
                        key: ov_val["key"].as_str()?.to_string(),
                        entry: FrameEntry {
                            offset: ov_val["offset"].as_u64()?,
                            len: ov_val["len"].as_u64()? as u32,
                            page_count: ov_val["pageCount"].as_u64().unwrap_or(0) as u32,
                        },
                    }))
                }).collect()
            }).collect()
        })
        .unwrap_or_default();

    Ok(HaManifest {
        version,
        writer_id: String::new(),
        lease_epoch: 0,
        timestamp_ms: 0,
        storage: StorageManifest::Turbograph {
            turbograph_version: version,
            page_count: v["page_count"].as_u64().unwrap_or(0),
            page_size: v["page_size"].as_u64().unwrap_or(0) as u32,
            pages_per_group: v["pages_per_group"].as_u64().unwrap_or(0) as u32,
            sub_pages_per_frame: v["sub_pages_per_frame"].as_u64().unwrap_or(0) as u32,
            page_group_keys,
            frame_tables,
            subframe_overrides,
            encrypted: v["encrypted"].as_bool().unwrap_or(false),
            journal_seq: v["journal_seq"].as_u64().unwrap_or(0),
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_manifest() -> StorageManifest {
        StorageManifest::Turbograph {
            turbograph_version: 5,
            page_count: 50,
            page_size: 4096,
            pages_per_group: 4096,
            sub_pages_per_frame: 4,
            page_group_keys: vec!["pg/0_v5".into(), "pg/1_v5".into()],
            frame_tables: vec![
                vec![
                    FrameEntry { offset: 0, len: 8192, page_count: 2 },
                    FrameEntry { offset: 8192, len: 4096, page_count: 1 },
                ],
            ],
            subframe_overrides: vec![BTreeMap::new()],
            encrypted: false,
            journal_seq: 42,
        }
    }

    #[test]
    fn round_trip_json() {
        let storage = sample_manifest();
        let json = to_turbograph_json(&storage).unwrap();

        // Parse back
        let ha = parse_turbograph_json_to_ha_manifest(5, &json).unwrap();
        assert_eq!(ha.version, 5);

        if let StorageManifest::Turbograph {
            page_count, page_size, page_group_keys, frame_tables, journal_seq, ..
        } = &ha.storage {
            assert_eq!(*page_count, 50);
            assert_eq!(*page_size, 4096);
            assert_eq!(page_group_keys.len(), 2);
            assert_eq!(frame_tables.len(), 1);
            assert_eq!(frame_tables[0].len(), 2);
            assert_eq!(frame_tables[0][0].page_count, 2);
            assert_eq!(frame_tables[0][1].page_count, 1);
            assert_eq!(*journal_seq, 42);
        } else {
            panic!("expected Turbograph variant");
        }
    }

    #[test]
    fn json_matches_turbograph_format() {
        let storage = sample_manifest();
        let json = to_turbograph_json(&storage).unwrap();

        // Verify key format details that turbograph's fromJSON expects.
        assert!(json.contains("\"version\":5"));
        assert!(json.contains("\"page_count\":50"));
        assert!(json.contains("\"page_group_keys\":[\"pg/0_v5\",\"pg/1_v5\"]"));
        // frame_tables: [[offset,len,pageCount], ...]
        assert!(json.contains("[0,8192,2]"));
        assert!(json.contains("[8192,4096,1]"));
        assert!(json.contains("\"journal_seq\":42"));
        // encrypted=false should be omitted
        assert!(!json.contains("encrypted"));
    }

    #[test]
    fn empty_manifest_round_trips() {
        let storage = StorageManifest::Turbograph {
            turbograph_version: 0,
            page_count: 0,
            page_size: 0,
            pages_per_group: 0,
            sub_pages_per_frame: 0,
            page_group_keys: vec![],
            frame_tables: vec![],
            subframe_overrides: vec![],
            encrypted: false,
            journal_seq: 0,
        };
        let json = to_turbograph_json(&storage).unwrap();
        let ha = parse_turbograph_json_to_ha_manifest(0, &json).unwrap();
        assert_eq!(ha.version, 0);
    }

    #[test]
    fn subframe_overrides_round_trip() {
        let mut ovr = BTreeMap::new();
        ovr.insert(2, SubframeOverride {
            key: "pg/0_f2_v5".into(),
            entry: FrameEntry { offset: 0, len: 4096, page_count: 4 },
        });
        let storage = StorageManifest::Turbograph {
            turbograph_version: 5,
            page_count: 50,
            page_size: 4096,
            pages_per_group: 4096,
            sub_pages_per_frame: 4,
            page_group_keys: vec!["pg/0_v5".into()],
            frame_tables: vec![],
            subframe_overrides: vec![ovr],
            encrypted: false,
            journal_seq: 0,
        };
        let json = to_turbograph_json(&storage).unwrap();
        assert!(json.contains("\"subframe_overrides\""));
        assert!(json.contains("\"2\":{\"key\":\"pg/0_f2_v5\""));

        let ha = parse_turbograph_json_to_ha_manifest(5, &json).unwrap();
        if let StorageManifest::Turbograph { subframe_overrides, .. } = &ha.storage {
            assert_eq!(subframe_overrides.len(), 1);
            assert!(subframe_overrides[0].contains_key(&2));
            let ov = &subframe_overrides[0][&2];
            assert_eq!(ov.key, "pg/0_f2_v5");
            assert_eq!(ov.entry.page_count, 4);
        } else {
            panic!("expected Turbograph variant");
        }
    }

    #[test]
    fn rejects_non_turbograph_variant() {
        // Use TurbographGraphstream as a non-Turbograph variant to test rejection.
        let storage = StorageManifest::TurbographGraphstream {
            turbograph_version: 0,
            page_count: 0,
            page_size: 0,
            pages_per_group: 0,
            sub_pages_per_frame: 0,
            page_group_keys: vec![],
            frame_tables: vec![],
            subframe_overrides: vec![],
            encrypted: false,
            journal_seq: 0,
            graphstream_segment_prefix: String::new(),
        };
        let result = to_turbograph_json(&storage);
        assert!(result.is_err());
    }
}
