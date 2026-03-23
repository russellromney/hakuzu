//! Snapshot creation, upload, download, and extraction for Kuzu databases.
//!
//! Snapshots allow followers to bootstrap from a recent database state instead of
//! replaying all journal entries from the beginning.
//!
//! S3 layout:
//! ```text
//! {prefix}{db_name}/snapshots/
//!   snap-0000000000010000.tar.zst
//!   snap-0000000000010000.meta.json
//! ```

use std::path::{Path, PathBuf};

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

/// Metadata for a database snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMeta {
    pub journal_seq: u64,
    pub chain_hash: String,
    pub timestamp_ms: i64,
    pub db_size_bytes: u64,
}

/// Create a tar.zst snapshot of a Kuzu database directory.
///
/// The caller must ensure no writes are happening to the database during this call
/// (e.g., hold snapshot_lock write, and checkpoint the database first).
///
/// Returns the size of the snapshot file in bytes.
pub fn create_snapshot(db_path: &Path, output_path: &Path) -> Result<u64> {
    if !db_path.exists() {
        return Err(anyhow!("Database path does not exist: {}", db_path.display()));
    }

    // Create parent directory for snapshot file.
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let file = std::fs::File::create(output_path)
        .map_err(|e| anyhow!("Create snapshot file: {e}"))?;

    // zstd level 3 — good balance of speed and compression.
    let encoder = zstd::Encoder::new(file, 3)
        .map_err(|e| anyhow!("Create zstd encoder: {e}"))?;

    let mut archive = tar::Builder::new(encoder);

    // Add all files from the database directory to the tar archive.
    // Use the directory name as the base path inside the archive.
    archive
        .append_dir_all("db", db_path)
        .map_err(|e| anyhow!("Add files to tar: {e}"))?;

    let encoder = archive
        .into_inner()
        .map_err(|e| anyhow!("Finish tar: {e}"))?;

    encoder
        .finish()
        .map_err(|e| anyhow!("Finish zstd: {e}"))?;

    let size = std::fs::metadata(output_path)
        .map_err(|e| anyhow!("Read snapshot metadata: {e}"))?
        .len();

    Ok(size)
}

/// Extract a tar.zst snapshot into the database directory.
///
/// Removes existing database files first to ensure a clean state.
pub fn extract_snapshot(snapshot_path: &Path, db_path: &Path) -> Result<()> {
    if !snapshot_path.exists() {
        return Err(anyhow!(
            "Snapshot file does not exist: {}",
            snapshot_path.display()
        ));
    }

    // Clean the target directory.
    if db_path.exists() {
        std::fs::remove_dir_all(db_path)
            .map_err(|e| anyhow!("Remove old database: {e}"))?;
    }
    std::fs::create_dir_all(db_path)
        .map_err(|e| anyhow!("Create database directory: {e}"))?;

    let file = std::fs::File::open(snapshot_path)
        .map_err(|e| anyhow!("Open snapshot: {e}"))?;

    let decoder = zstd::Decoder::new(file)
        .map_err(|e| anyhow!("Create zstd decoder: {e}"))?;

    let mut archive = tar::Archive::new(decoder);

    // Extract — the archive has files under "db/" prefix.
    // We need to strip the "db/" prefix and extract into db_path.
    for entry in archive.entries().map_err(|e| anyhow!("Read tar entries: {e}"))? {
        let mut entry = entry.map_err(|e| anyhow!("Read tar entry: {e}"))?;
        let path = entry
            .path()
            .map_err(|e| anyhow!("Read entry path: {e}"))?
            .into_owned();

        // Strip the "db/" prefix.
        let relative = match path.strip_prefix("db") {
            Ok(r) => r.to_path_buf(),
            Err(_) => path.clone(),
        };

        if relative.as_os_str().is_empty() {
            continue; // Skip the "db/" directory entry itself.
        }

        let target = db_path.join(&relative);

        if entry.header().entry_type().is_dir() {
            std::fs::create_dir_all(&target)
                .map_err(|e| anyhow!("Create dir {}: {e}", target.display()))?;
        } else {
            // Ensure parent exists.
            if let Some(parent) = target.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let mut out = std::fs::File::create(&target)
                .map_err(|e| anyhow!("Create file {}: {e}", target.display()))?;
            std::io::copy(&mut entry, &mut out)
                .map_err(|e| anyhow!("Write file {}: {e}", target.display()))?;
        }
    }

    Ok(())
}

/// Upload a snapshot + metadata to S3.
pub async fn upload_snapshot(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    db_name: &str,
    snapshot_path: &Path,
    meta: &SnapshotMeta,
) -> Result<()> {
    let snap_key = snapshot_s3_key(prefix, db_name, meta.journal_seq, "tar.zst");
    let meta_key = snapshot_s3_key(prefix, db_name, meta.journal_seq, "meta.json");

    // Upload snapshot tarball.
    let body = aws_sdk_s3::primitives::ByteStream::from_path(snapshot_path)
        .await
        .map_err(|e| anyhow!("Read snapshot for upload: {e}"))?;

    client
        .put_object()
        .bucket(bucket)
        .key(&snap_key)
        .body(body)
        .send()
        .await
        .map_err(|e| anyhow!("Upload snapshot: {e}"))?;

    // Upload metadata JSON.
    let meta_json = serde_json::to_string(meta)
        .map_err(|e| anyhow!("Serialize snapshot meta: {e}"))?;

    client
        .put_object()
        .bucket(bucket)
        .key(&meta_key)
        .body(meta_json.into_bytes().into())
        .send()
        .await
        .map_err(|e| anyhow!("Upload snapshot meta: {e}"))?;

    tracing::info!(
        "Uploaded snapshot: seq={}, size={}B, key={}",
        meta.journal_seq,
        meta.db_size_bytes,
        snap_key
    );

    Ok(())
}

/// Find and download the latest snapshot from S3.
///
/// Returns `None` if no snapshots exist. Downloads the tarball to `dest_dir/snapshot.tar.zst`.
pub async fn download_latest_snapshot(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    db_name: &str,
    dest_dir: &Path,
) -> Result<Option<(PathBuf, SnapshotMeta)>> {
    let snapshot_prefix = format!("{}{}/snapshots/", prefix, db_name);

    // List snapshot metadata files.
    let list_resp = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(&snapshot_prefix)
        .send()
        .await
        .map_err(|e| anyhow!("List snapshots: {e}"))?;

    // Find the latest .meta.json key.
    let mut latest_meta_key: Option<(String, u64)> = None;
    for obj in list_resp.contents() {
        if let Some(key) = obj.key() {
            if key.ends_with(".meta.json") {
                if let Some(seq) = parse_snapshot_seq(key) {
                    if latest_meta_key.as_ref().map_or(true, |(_, s)| seq > *s) {
                        latest_meta_key = Some((key.to_string(), seq));
                    }
                }
            }
        }
    }

    let (meta_key, seq) = match latest_meta_key {
        Some(k) => k,
        None => return Ok(None),
    };

    // Download metadata.
    let meta_resp = client
        .get_object()
        .bucket(bucket)
        .key(&meta_key)
        .send()
        .await
        .map_err(|e| anyhow!("Download snapshot meta: {e}"))?;

    let meta_bytes = meta_resp
        .body
        .collect()
        .await
        .map_err(|e| anyhow!("Read snapshot meta: {e}"))?
        .into_bytes();

    let meta: SnapshotMeta = serde_json::from_slice(&meta_bytes)
        .map_err(|e| anyhow!("Parse snapshot meta: {e}"))?;

    // Download the tarball.
    let snap_key = snapshot_s3_key(prefix, db_name, seq, "tar.zst");

    let snap_resp = client
        .get_object()
        .bucket(bucket)
        .key(&snap_key)
        .send()
        .await
        .map_err(|e| anyhow!("Download snapshot: {e}"))?;

    let snap_bytes = snap_resp
        .body
        .collect()
        .await
        .map_err(|e| anyhow!("Read snapshot body: {e}"))?
        .into_bytes();

    std::fs::create_dir_all(dest_dir)?;
    let local_path = dest_dir.join("snapshot.tar.zst");
    std::fs::write(&local_path, &snap_bytes)
        .map_err(|e| anyhow!("Write snapshot to disk: {e}"))?;

    tracing::info!(
        "Downloaded snapshot: seq={}, size={}B",
        meta.journal_seq,
        snap_bytes.len()
    );

    Ok(Some((local_path, meta)))
}

/// Generate the S3 key for a snapshot file.
fn snapshot_s3_key(prefix: &str, db_name: &str, seq: u64, extension: &str) -> String {
    format!(
        "{}{}/snapshots/snap-{:016}.{}",
        prefix, db_name, seq, extension
    )
}

/// Parse the journal sequence from a snapshot S3 key.
fn parse_snapshot_seq(key: &str) -> Option<u64> {
    let filename = key.rsplit('/').next()?;
    let stem = filename.strip_prefix("snap-")?;
    let num = stem.split('.').next()?;
    num.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_snapshot_seq() {
        assert_eq!(
            parse_snapshot_seq("prefix/db/snapshots/snap-0000000000010000.meta.json"),
            Some(10000)
        );
        assert_eq!(
            parse_snapshot_seq("prefix/db/snapshots/snap-0000000000010000.tar.zst"),
            Some(10000)
        );
        assert_eq!(parse_snapshot_seq("other/file.json"), None);
    }

    #[test]
    fn test_snapshot_s3_key() {
        assert_eq!(
            snapshot_s3_key("hakuzu/", "mydb", 10000, "tar.zst"),
            "hakuzu/mydb/snapshots/snap-0000000000010000.tar.zst"
        );
        assert_eq!(
            snapshot_s3_key("hakuzu/", "mydb", 10000, "meta.json"),
            "hakuzu/mydb/snapshots/snap-0000000000010000.meta.json"
        );
    }

    #[test]
    fn test_create_and_extract_snapshot() {
        let dir = tempfile::tempdir().unwrap();

        // Create a fake "database" directory with some files.
        let db_path = dir.path().join("source_db");
        std::fs::create_dir_all(&db_path).unwrap();
        std::fs::write(db_path.join("data.bin"), b"fake database data here").unwrap();
        std::fs::create_dir_all(db_path.join("subdir")).unwrap();
        std::fs::write(db_path.join("subdir/index.bin"), b"index data").unwrap();

        // Create snapshot.
        let snap_path = dir.path().join("snap.tar.zst");
        let size = create_snapshot(&db_path, &snap_path).unwrap();
        assert!(size > 0);
        assert!(snap_path.exists());

        // Extract to a different location.
        let dest_db = dir.path().join("restored_db");
        extract_snapshot(&snap_path, &dest_db).unwrap();

        // Verify files match.
        assert_eq!(
            std::fs::read_to_string(dest_db.join("data.bin")).unwrap(),
            "fake database data here"
        );
        assert_eq!(
            std::fs::read_to_string(dest_db.join("subdir/index.bin")).unwrap(),
            "index data"
        );
    }

    #[test]
    fn test_create_snapshot_empty_db() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("empty_db");
        std::fs::create_dir_all(&db_path).unwrap();

        let snap_path = dir.path().join("snap.tar.zst");
        let size = create_snapshot(&db_path, &snap_path).unwrap();
        assert!(size > 0);

        // Extract empty snapshot.
        let dest = dir.path().join("restored");
        extract_snapshot(&snap_path, &dest).unwrap();
        assert!(dest.exists());
    }

    #[test]
    fn test_create_snapshot_nonexistent_path() {
        let dir = tempfile::tempdir().unwrap();
        let result = create_snapshot(
            &dir.path().join("nonexistent"),
            &dir.path().join("snap.tar.zst"),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_snapshot_nonexistent() {
        let dir = tempfile::tempdir().unwrap();
        let result = extract_snapshot(
            &dir.path().join("nonexistent.tar.zst"),
            &dir.path().join("dest"),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_overwrites_existing() {
        let dir = tempfile::tempdir().unwrap();

        // Create DB with file A.
        let db_path = dir.path().join("db");
        std::fs::create_dir_all(&db_path).unwrap();
        std::fs::write(db_path.join("data.bin"), b"original data").unwrap();

        // Create snapshot of original.
        let snap_path = dir.path().join("snap.tar.zst");
        create_snapshot(&db_path, &snap_path).unwrap();

        // Modify DB — add extra file.
        std::fs::write(db_path.join("extra.bin"), b"should be removed").unwrap();
        std::fs::write(db_path.join("data.bin"), b"modified data").unwrap();

        // Extract snapshot should restore to original state.
        extract_snapshot(&snap_path, &db_path).unwrap();

        assert_eq!(
            std::fs::read_to_string(db_path.join("data.bin")).unwrap(),
            "original data"
        );
        // extra.bin should be gone (we removed the directory).
        assert!(!db_path.join("extra.bin").exists());
    }

    #[test]
    fn test_snapshot_meta_serialization() {
        let meta = SnapshotMeta {
            journal_seq: 42000,
            chain_hash: "abcdef".to_string(),
            timestamp_ms: 1711324800000,
            db_size_bytes: 52428800,
        };

        let json = serde_json::to_string(&meta).unwrap();
        let parsed: SnapshotMeta = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.journal_seq, 42000);
        assert_eq!(parsed.chain_hash, "abcdef");
        assert_eq!(parsed.timestamp_ms, 1711324800000);
        assert_eq!(parsed.db_size_bytes, 52428800);
    }

    #[test]
    fn test_parse_snapshot_seq_edge_cases() {
        // Zero seq.
        assert_eq!(
            parse_snapshot_seq("p/db/snapshots/snap-0000000000000000.meta.json"),
            Some(0)
        );
        // Max u64 won't fit in 16 digits — but parse still works if present.
        assert_eq!(
            parse_snapshot_seq("p/db/snapshots/snap-9999999999999999.meta.json"),
            Some(9999999999999999)
        );
        // No snap- prefix.
        assert_eq!(parse_snapshot_seq("p/db/snapshots/other-file.meta.json"), None);
        // Empty filename.
        assert_eq!(parse_snapshot_seq("p/db/snapshots/"), None);
        // Just snap- without number.
        assert_eq!(parse_snapshot_seq("p/db/snapshots/snap-.meta.json"), None);
        // Non-numeric after snap-.
        assert_eq!(
            parse_snapshot_seq("p/db/snapshots/snap-abcdef.meta.json"),
            None
        );
    }

    #[test]
    fn test_snapshot_s3_key_various_prefixes() {
        // No trailing slash on prefix.
        assert_eq!(
            snapshot_s3_key("hakuzu", "mydb", 1, "tar.zst"),
            "hakuzumydb/snapshots/snap-0000000000000001.tar.zst"
        );
        // Empty prefix.
        assert_eq!(
            snapshot_s3_key("", "mydb", 1, "tar.zst"),
            "mydb/snapshots/snap-0000000000000001.tar.zst"
        );
        // Seq = 0.
        assert_eq!(
            snapshot_s3_key("hakuzu/", "mydb", 0, "meta.json"),
            "hakuzu/mydb/snapshots/snap-0000000000000000.meta.json"
        );
    }

    #[test]
    fn test_create_snapshot_deeply_nested() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("db");
        std::fs::create_dir_all(db_path.join("a/b/c/d/e")).unwrap();
        std::fs::write(db_path.join("a/b/c/d/e/deep.bin"), b"deep file").unwrap();
        std::fs::write(db_path.join("root.bin"), b"root file").unwrap();

        let snap_path = dir.path().join("snap.tar.zst");
        create_snapshot(&db_path, &snap_path).unwrap();

        let dest = dir.path().join("restored");
        extract_snapshot(&snap_path, &dest).unwrap();

        assert_eq!(
            std::fs::read_to_string(dest.join("a/b/c/d/e/deep.bin")).unwrap(),
            "deep file"
        );
        assert_eq!(
            std::fs::read_to_string(dest.join("root.bin")).unwrap(),
            "root file"
        );
    }

    #[test]
    fn test_create_snapshot_large_file() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("db");
        std::fs::create_dir_all(&db_path).unwrap();

        // 1MB file — verify compression works.
        let data = vec![42u8; 1024 * 1024];
        std::fs::write(db_path.join("large.bin"), &data).unwrap();

        let snap_path = dir.path().join("snap.tar.zst");
        let size = create_snapshot(&db_path, &snap_path).unwrap();

        // Compressed size should be much smaller than 1MB (repetitive data).
        assert!(size < 1024 * 1024);

        let dest = dir.path().join("restored");
        extract_snapshot(&snap_path, &dest).unwrap();

        let restored = std::fs::read(dest.join("large.bin")).unwrap();
        assert_eq!(restored.len(), 1024 * 1024);
        assert!(restored.iter().all(|&b| b == 42));
    }

    #[test]
    fn test_create_snapshot_output_in_nested_dir() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("db");
        std::fs::create_dir_all(&db_path).unwrap();
        std::fs::write(db_path.join("file.bin"), b"data").unwrap();

        // Output path in a deeply nested directory that doesn't exist yet.
        let snap_path = dir.path().join("deep/nested/dir/snap.tar.zst");
        let size = create_snapshot(&db_path, &snap_path).unwrap();
        assert!(size > 0);
        assert!(snap_path.exists());
    }

    #[test]
    fn test_extract_creates_target_directory() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("db");
        std::fs::create_dir_all(&db_path).unwrap();
        std::fs::write(db_path.join("file.bin"), b"data").unwrap();

        let snap_path = dir.path().join("snap.tar.zst");
        create_snapshot(&db_path, &snap_path).unwrap();

        // Extract to a path that doesn't exist.
        let dest = dir.path().join("new/nested/dest");
        extract_snapshot(&snap_path, &dest).unwrap();

        assert_eq!(
            std::fs::read_to_string(dest.join("file.bin")).unwrap(),
            "data"
        );
    }

    #[test]
    fn test_snapshot_meta_json_roundtrip() {
        let meta = SnapshotMeta {
            journal_seq: 0,
            chain_hash: "0".repeat(64),
            timestamp_ms: 0,
            db_size_bytes: 0,
        };

        let json = serde_json::to_string_pretty(&meta).unwrap();
        let parsed: SnapshotMeta = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.journal_seq, 0);
        assert_eq!(parsed.chain_hash.len(), 64);
    }

    #[test]
    fn test_snapshot_multiple_files_integrity() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("db");
        std::fs::create_dir_all(&db_path).unwrap();

        // Create multiple files with known content.
        for i in 0..10 {
            std::fs::write(
                db_path.join(format!("file_{}.bin", i)),
                format!("content_{}", i).as_bytes(),
            )
            .unwrap();
        }

        let snap_path = dir.path().join("snap.tar.zst");
        create_snapshot(&db_path, &snap_path).unwrap();

        let dest = dir.path().join("restored");
        extract_snapshot(&snap_path, &dest).unwrap();

        // Verify ALL files are present with correct content.
        for i in 0..10 {
            assert_eq!(
                std::fs::read_to_string(dest.join(format!("file_{}.bin", i))).unwrap(),
                format!("content_{}", i)
            );
        }
    }
}
