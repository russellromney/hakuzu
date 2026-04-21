//! Shared test utilities for hakuzu tests.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use async_trait::async_trait;
use hadb_storage::{CasResult, StorageBackend};

/// In-memory `StorageBackend` for tests that don't need real S3.
///
/// Backed by a `HashMap`. Atomic CAS uses an etag string of `"v{N}"` where
/// `N` increments on every successful write — same scheme as
/// `hadb-storage-mem`.
pub struct MockObjectStore {
    inner: Mutex<HashMap<String, (Vec<u8>, u64)>>,
}

impl MockObjectStore {
    pub fn new() -> Arc<dyn StorageBackend> {
        Arc::new(Self {
            inner: Mutex::new(HashMap::new()),
        })
    }
}

#[async_trait]
impl StorageBackend for MockObjectStore {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.inner.lock().unwrap().get(key).map(|(v, _)| v.clone()))
    }

    async fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let next_version = inner
            .get(key)
            .map(|(_, v)| v.wrapping_add(1))
            .unwrap_or(1);
        inner.insert(key.to_string(), (data.to_vec(), next_version));
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.inner.lock().unwrap().remove(key);
        Ok(())
    }

    async fn list(&self, prefix: &str, after: Option<&str>) -> Result<Vec<String>> {
        let inner = self.inner.lock().unwrap();
        let mut keys: Vec<String> = inner
            .keys()
            .filter(|k| k.starts_with(prefix))
            .filter(|k| match after {
                Some(a) => k.as_str() > a,
                None => true,
            })
            .cloned()
            .collect();
        keys.sort();
        Ok(keys)
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        Ok(self.inner.lock().unwrap().contains_key(key))
    }

    async fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<CasResult> {
        let mut inner = self.inner.lock().unwrap();
        if inner.contains_key(key) {
            return Ok(CasResult { success: false, etag: None });
        }
        inner.insert(key.to_string(), (data.to_vec(), 1));
        Ok(CasResult { success: true, etag: Some("v1".to_string()) })
    }

    async fn put_if_match(&self, key: &str, data: &[u8], etag: &str) -> Result<CasResult> {
        let mut inner = self.inner.lock().unwrap();
        match inner.get(key) {
            Some((_, v)) if format!("v{v}") == etag => {
                let next_version = v.wrapping_add(1);
                inner.insert(key.to_string(), (data.to_vec(), next_version));
                Ok(CasResult { success: true, etag: Some(format!("v{next_version}")) })
            }
            _ => Ok(CasResult { success: false, etag: None }),
        }
    }

    fn backend_name(&self) -> &str {
        "mock"
    }
}
