//! Shared test utilities for hakuzu tests.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use async_trait::async_trait;

/// In-memory ObjectStore for tests that don't need real S3.
pub struct MockObjectStore {
    objects: Mutex<HashMap<String, Vec<u8>>>,
}

impl MockObjectStore {
    pub fn new() -> Arc<dyn hadb_io::ObjectStore> {
        Arc::new(Self {
            objects: Mutex::new(HashMap::new()),
        })
    }
}

#[async_trait]
impl hadb_io::ObjectStore for MockObjectStore {
    async fn upload_bytes(&self, key: &str, data: Vec<u8>) -> Result<()> {
        self.objects.lock().unwrap().insert(key.to_string(), data);
        Ok(())
    }
    async fn upload_bytes_with_checksum(&self, key: &str, data: Vec<u8>, _: &str) -> Result<()> {
        self.upload_bytes(key, data).await
    }
    async fn upload_file(&self, key: &str, path: &Path) -> Result<()> {
        let data = std::fs::read(path)?;
        self.upload_bytes(key, data).await
    }
    async fn upload_file_with_checksum(&self, key: &str, path: &Path, _: &str) -> Result<()> {
        self.upload_file(key, path).await
    }
    async fn download_bytes(&self, key: &str) -> Result<Vec<u8>> {
        self.objects
            .lock()
            .unwrap()
            .get(key)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Not found: {}", key))
    }
    async fn download_file(&self, key: &str, path: &Path) -> Result<()> {
        let data = self.download_bytes(key).await?;
        std::fs::write(path, data)?;
        Ok(())
    }
    async fn list_objects(&self, prefix: &str) -> Result<Vec<String>> {
        let objects = self.objects.lock().unwrap();
        let mut keys: Vec<String> = objects.keys().filter(|k| k.starts_with(prefix)).cloned().collect();
        keys.sort();
        Ok(keys)
    }
    async fn list_objects_after(&self, prefix: &str, start_after: &str) -> Result<Vec<String>> {
        let objects = self.objects.lock().unwrap();
        let mut keys: Vec<String> = objects.keys().filter(|k| k.starts_with(prefix) && k.as_str() > start_after).cloned().collect();
        keys.sort();
        Ok(keys)
    }
    async fn exists(&self, key: &str) -> Result<bool> {
        Ok(self.objects.lock().unwrap().contains_key(key))
    }
    async fn get_checksum(&self, _: &str) -> Result<Option<String>> {
        Ok(None)
    }
    async fn delete_object(&self, key: &str) -> Result<()> {
        self.objects.lock().unwrap().remove(key);
        Ok(())
    }
    async fn delete_objects(&self, keys: &[String]) -> Result<usize> {
        let mut objects = self.objects.lock().unwrap();
        let mut count = 0;
        for key in keys {
            if objects.remove(key).is_some() { count += 1; }
        }
        Ok(count)
    }
    fn bucket_name(&self) -> &str {
        "mock-bucket"
    }
}
