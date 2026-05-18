use parking_lot::RwLock;
#[cfg(feature = "rocksdb")]
use parking_lot::Mutex;
#[cfg(feature = "rocksdb")]
use rocksdb::{WriteBatch, Options, DB, Cache};
#[cfg(feature = "rocksdb")]
use once_cell::sync::Lazy;
use std::{any::Any, collections::HashMap};
use thiserror::Error;

use manifold_types::{RunMetadata, RunRecord, Value};

fn edge_storage_key(edge_type: &str, edge_id: &str) -> String {
    format!("edge:{}:{}", edge_type, edge_id)
}

fn vertex_index_key(target_id: &str) -> String {
    format!("vertex_idx:{}", target_id)
}

fn serialize_value(value: &Value) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(StoreError::from)
}

fn deserialize_value(bytes: &[u8]) -> Result<Value> {
    serde_json::from_slice(bytes).map_err(StoreError::from)
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("storage failed: {0}")]
    Storage(String),
}

pub type Result<T> = std::result::Result<T, StoreError>;

pub trait Store: Send + Sync {
    fn put_run(&self, record: RunRecord) -> Result<()>;
    fn get_run(&self, run_id: &str) -> Result<Option<RunRecord>>;
    fn list_runs(&self) -> Result<Vec<RunMetadata>>;
    fn put_edge(&self, edge_type: &str, edge_id: &str, targets: &[String]) -> Result<()>;
    fn get_edge_targets(&self, edge_type: &str, edge_id: &str) -> Result<Option<Vec<String>>>;
    fn get_vertex_edges(&self, target_id: &str) -> Result<Vec<String>>;
    fn put_entry(&self, key: &str, value: Value) -> Result<()>;
    fn get_entry(&self, key: &str) -> Result<Option<Value>>;
    fn delete_entry(&self, key: &str) -> Result<()>;
    fn scan_prefix(&self, prefix: &str, limit: usize, offset: usize) -> Result<Vec<(String, Value)>>;
    fn delete_edge(&self, edge_type: &str, edge_id: &str) -> Result<()>;
    fn as_any(&self) -> &dyn Any;
}

#[derive(Default)]
pub struct InMemoryStore {
    inner: RwLock<HashMap<String, Vec<u8>>>,
}

impl InMemoryStore {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }
}

impl Store for InMemoryStore {
    fn put_run(&self, record: RunRecord) -> Result<()> {
        let bytes = serde_json::to_vec(&record)?;
        let id = record.metadata.run_id.clone();
        self.inner.write().insert(id, bytes);
        Ok(())
    }

    fn get_run(&self, run_id: &str) -> Result<Option<RunRecord>> {
        let inner = self.inner.read();
        let bytes = match inner.get(run_id) {
            Some(bytes) => bytes.clone(),
            None => return Ok(None),
        };
        let record = serde_json::from_slice(&bytes)?;
        Ok(Some(record))
    }

    fn list_runs(&self) -> Result<Vec<RunMetadata>> {
        let inner = self.inner.read();
        let mut result = Vec::with_capacity(inner.len());
        for bytes in inner.values() {
            let record: RunRecord = serde_json::from_slice(bytes)?;
            result.push(record.metadata);
        }
        Ok(result)
    }

    fn put_entry(&self, key: &str, value: Value) -> Result<()> {
        let bytes = serialize_value(&value)?;
        self.inner.write().insert(key.to_string(), bytes);
        Ok(())
    }

    fn get_entry(&self, key: &str) -> Result<Option<Value>> {
        let inner = self.inner.read();
        match inner.get(key) {
            Some(bytes) => Ok(Some(deserialize_value(bytes)?)),
            None => Ok(None),
        }
    }

    fn delete_entry(&self, key: &str) -> Result<()> {
        self.inner.write().remove(key);
        Ok(())
    }

    fn put_edge(&self, edge_type: &str, edge_id: &str, targets: &[String]) -> Result<()> {
        let mut inner = self.inner.write();
        let edge_key = edge_storage_key(edge_type, edge_id);
        let old_targets = inner
            .get(&edge_key)
            .and_then(|bytes| deserialize_value(bytes).ok())
            .and_then(|value| {
                value.as_list().map(|list| list.iter().filter_map(Value::as_str).map(String::from).collect::<Vec<_>>())
            })
            .unwrap_or_default();

        let new_targets = targets.to_vec();
        let edge_ref = edge_key.clone();
        let serialized_targets = serialize_value(&Value::List(new_targets.iter().map(|t| Value::String(t.clone())).collect::<Vec<_>>()))?;
        inner.insert(edge_key.clone(), serialized_targets);

        for target in old_targets.iter().filter(|t| !new_targets.contains(t)) {
            let vertex_key = vertex_index_key(target);
            if let Some(existing_bytes) = inner.get(&vertex_key) {
                if let Ok(value) = deserialize_value(existing_bytes) {
                    let refs = value
                        .as_list()
                        .unwrap_or_default()
                        .iter()
                        .filter_map(Value::as_str)
                        .filter(|r| *r != edge_ref.as_str())
                        .map(String::from)
                        .collect::<Vec<_>>();
                    if refs.is_empty() {
                        inner.remove(&vertex_key);
                    } else {
                        inner.insert(vertex_key, serialize_value(&Value::List(refs.iter().map(|r| Value::String(r.clone())).collect()))?);
                    }
                }
            }
        }

        for target in new_targets.iter() {
            let vertex_key = vertex_index_key(target);
            let refs = inner
                .get(&vertex_key)
                .and_then(|bytes| deserialize_value(bytes).ok())
                .and_then(|value| value.as_list().map(|list| list.iter().filter_map(Value::as_str).map(String::from).collect::<Vec<_>>()));
            let mut refs = refs.unwrap_or_default();
            if !refs.contains(&edge_ref) {
                refs.push(edge_ref.clone());
            }
            inner.insert(vertex_key, serialize_value(&Value::List(refs.iter().map(|r| Value::String(r.clone())).collect()))?);
        }

        Ok(())
    }

    fn get_edge_targets(&self, edge_type: &str, edge_id: &str) -> Result<Option<Vec<String>>> {
        let inner = self.inner.read();
        let key = edge_storage_key(edge_type, edge_id);
        let bytes = match inner.get(&key) {
            Some(bytes) => bytes,
            None => return Ok(None),
        };
        let value = deserialize_value(bytes)?;
        let targets = value
            .as_list()
            .unwrap_or_default()
            .iter()
            .filter_map(Value::as_str)
            .map(String::from)
            .collect();
        Ok(Some(targets))
    }

    fn get_vertex_edges(&self, target_id: &str) -> Result<Vec<String>> {
        let inner = self.inner.read();
        let key = vertex_index_key(target_id);
        let bytes = match inner.get(&key) {
            Some(bytes) => bytes,
            None => return Ok(Vec::new()),
        };
        let value = deserialize_value(bytes)?;
        let refs = value
            .as_list()
            .unwrap_or_default()
            .iter()
            .filter_map(Value::as_str)
            .map(String::from)
            .collect();
        Ok(refs)
    }

    fn scan_prefix(&self, prefix: &str, limit: usize, offset: usize) -> Result<Vec<(String, Value)>> {
        let inner = self.inner.read();
        let items = inner
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .skip(offset)
            .take(limit)
            .map(|(k, v)| Ok((k.clone(), deserialize_value(v)?)))
            .collect::<Result<Vec<_>>>()?;
        Ok(items)
    }

    fn delete_edge(&self, edge_type: &str, edge_id: &str) -> Result<()> {
        let mut inner = self.inner.write();
        let edge_key = edge_storage_key(edge_type, edge_id);
        let edge_ref = edge_key.clone();
        if let Some(bytes) = inner.remove(&edge_key) {
            if let Ok(value) = deserialize_value(&bytes) {
                let targets = value
                    .as_list()
                    .unwrap_or_default()
                    .iter()
                    .filter_map(Value::as_str)
                    .map(String::from)
                    .collect::<Vec<_>>();
                for target in targets {
                    let vertex_key = vertex_index_key(&target);
                    if let Some(existing_bytes) = inner.get(&vertex_key) {
                        if let Ok(value) = deserialize_value(existing_bytes) {
                            let refs = value
                                .as_list()
                                .unwrap_or_default()
                                .iter()
                                .filter_map(Value::as_str)
                                .filter(|r| *r != edge_ref.as_str())
                                .map(String::from)
                                .collect::<Vec<_>>();
                            if refs.is_empty() {
                                inner.remove(&vertex_key);
                            } else {
                                inner.insert(vertex_key, serialize_value(&Value::List(refs.iter().map(|r| Value::String(r.clone())).collect()))?);
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(feature = "rocksdb")]
pub use rocksdb_store::RocksDbStore;

#[cfg(feature = "rocksdb")]
mod rocksdb_store {
    use super::*;
    use rocksdb::DB;

    static GLOBAL_ROCKS_CACHE: Lazy<Cache> = Lazy::new(|| {
        // 512MB shared LRU cache across all RocksDB instances
        Cache::new_lru_cache(512 * 1024 * 1024)
    });

    pub struct RocksDbStore {
        db: DB,
        write_lock: Mutex<()>,
    }

    impl RocksDbStore {
        pub fn open(path: &str) -> Result<Self> {
            let mut opts = Options::default();
            opts.create_if_missing(true);
            opts.set_max_open_files(64);
            opts.set_write_buffer_size(8 * 1024 * 1024);
            opts.set_max_write_buffer_number(3);
            opts.set_min_write_buffer_number_to_merge(1);
            opts.set_target_file_size_base(16 * 1024 * 1024);
            opts.set_target_file_size_multiplier(1);
            opts.set_max_bytes_for_level_base(64 * 1024 * 1024);
            opts.set_block_size(4 * 1024);
            opts.set_disable_auto_compactions(false);
            // attach global shared block cache
            opts.set_block_cache(&GLOBAL_ROCKS_CACHE);

            let db = DB::open(&opts, path).map_err(|e| StoreError::Storage(e.to_string()))?;
            Ok(Self {
                db,
                write_lock: Mutex::new(()),
            })
        }
    }

    impl RocksDbStore {
        pub fn flush(&self) -> Result<()> {
            self.db.flush().map_err(|e| StoreError::Storage(e.to_string()))?;
            Ok(())
        }
    }

    impl RocksDbStore {
        pub fn put_run_with_key(&self, key: &str, record: RunRecord) -> Result<()> {
            let _guard = self.write_lock.lock();
            let bytes = serde_json::to_vec(&record)?;
            let mut batch = WriteBatch::default();
            batch.put(key.as_bytes(), &bytes);
            batch.put(record.metadata.run_id.as_bytes(), &bytes);
            self.db.write(batch).map_err(|e| StoreError::Storage(e.to_string()))?;
            Ok(())
        }
    }

    impl Store for RocksDbStore {
        fn put_run(&self, record: RunRecord) -> Result<()> {
            let _guard = self.write_lock.lock();
            let bytes = serde_json::to_vec(&record)?;
            let mut batch = WriteBatch::default();
            batch.put(record.metadata.run_id.as_bytes(), &bytes);
            batch.put(format!("run:{}", record.metadata.run_id).as_bytes(), &bytes);
            self.db.write(batch).map_err(|e| StoreError::Storage(e.to_string()))?;
            Ok(())
        }

        fn get_run(&self, run_id: &str) -> Result<Option<RunRecord>> {
            let bytes = self.db.get(run_id.as_bytes()).map_err(|e| StoreError::Storage(e.to_string()))?;
            match bytes {
                Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
                None => Ok(None),
            }
        }

        fn list_runs(&self) -> Result<Vec<RunMetadata>> {
            let iter = self.db.iterator(rocksdb::IteratorMode::Start);
            let mut result = Vec::new();
            for item in iter {
                let (key, value) = item.map_err(|e| StoreError::Storage(e.to_string()))?;
                if key.starts_with(b"run:") {
                    continue;
                }
                let record: RunRecord = serde_json::from_slice(&value)?;
                result.push(record.metadata);
            }
            Ok(result)
        }

        fn put_entry(&self, key: &str, value: Value) -> Result<()> {
            let _guard = self.write_lock.lock();
            let bytes = serialize_value(&value)?;
            self.db.put(key.as_bytes(), bytes).map_err(|e| StoreError::Storage(e.to_string()))?;
            Ok(())
        }

        fn get_entry(&self, key: &str) -> Result<Option<Value>> {
            let bytes = self.db.get(key.as_bytes()).map_err(|e| StoreError::Storage(e.to_string()))?;
            match bytes {
                Some(bytes) => Ok(Some(deserialize_value(&bytes)?)),
                None => Ok(None),
            }
        }

        fn delete_entry(&self, key: &str) -> Result<()> {
            let _guard = self.write_lock.lock();
            self.db.delete(key.as_bytes()).map_err(|e| StoreError::Storage(e.to_string()))?;
            Ok(())
        }

        fn put_edge(&self, edge_type: &str, edge_id: &str, targets: &[String]) -> Result<()> {
            let _guard = self.write_lock.lock();
            let edge_key = edge_storage_key(edge_type, edge_id);
            let mut batch = WriteBatch::default();

            let old_targets = self
                .db
                .get(edge_key.as_bytes())
                .map_err(|e| StoreError::Storage(e.to_string()))?
                .and_then(|bytes| deserialize_value(&bytes).ok())
                .and_then(|value| {
                    value.as_list().map(|list| list.iter().filter_map(Value::as_str).map(String::from).collect::<Vec<_>>())
                })
                .unwrap_or_default();

            let new_targets = targets.iter().cloned().collect::<Vec<_>>();
            let edge_ref = edge_key.clone();
            let serialized_targets = serialize_value(&Value::List(new_targets.iter().map(|t| Value::String(t.clone())).collect::<Vec<_>>()))?;
            batch.put(edge_key.as_bytes(), &serialized_targets);

            for target in old_targets.iter().filter(|t| !new_targets.contains(t)) {
                let vertex_key = vertex_index_key(target);
                if let Some(bytes) = self.db.get(vertex_key.as_bytes()).map_err(|e| StoreError::Storage(e.to_string()))? {
                    if let Ok(value) = deserialize_value(&bytes) {
                        let refs = value
                            .as_list()
                            .unwrap_or_default()
                            .iter()
                            .filter_map(Value::as_str)
                            .filter(|r| *r != &edge_ref)
                            .map(String::from)
                            .collect::<Vec<_>>();
                        if refs.is_empty() {
                            batch.delete(vertex_key.as_bytes());
                        } else {
                            batch.put(vertex_key.as_bytes(), serialize_value(&Value::List(refs.iter().map(|r| Value::String(r.clone())).collect()))?);
                        }
                    }
                }
            }

            for target in new_targets.iter() {
                let vertex_key = vertex_index_key(target);
                let mut refs = if let Some(bytes) = self.db.get(vertex_key.as_bytes()).map_err(|e| StoreError::Storage(e.to_string()))? {
                    deserialize_value(&bytes)
                        .ok()
                        .and_then(|value| {
                            value.as_list().map(|list| list.iter().filter_map(Value::as_str).map(String::from).collect::<Vec<_>>())
                        })
                        .unwrap_or_default()
                } else {
                    Vec::new()
                };
                if !refs.contains(&edge_ref) {
                    refs.push(edge_ref.clone());
                }
                batch.put(vertex_key.as_bytes(), serialize_value(&Value::List(refs.iter().map(|r| Value::String(r.clone())).collect()))?);
            }

            self.db.write(batch).map_err(|e| StoreError::Storage(e.to_string()))?;
            Ok(())
        }

        fn get_edge_targets(&self, edge_type: &str, edge_id: &str) -> Result<Option<Vec<String>>> {
            let key = edge_storage_key(edge_type, edge_id);
            let bytes = self.db.get(key.as_bytes()).map_err(|e| StoreError::Storage(e.to_string()))?;
            match bytes {
                Some(bytes) => {
                    let value = deserialize_value(&bytes)?;
                    let targets = value
                        .as_list()
                        .unwrap_or_default()
                        .iter()
                        .filter_map(Value::as_str)
                        .map(String::from)
                        .collect();
                    Ok(Some(targets))
                }
                None => Ok(None),
            }
        }

        fn get_vertex_edges(&self, target_id: &str) -> Result<Vec<String>> {
            let key = vertex_index_key(target_id);
            let bytes = self.db.get(key.as_bytes()).map_err(|e| StoreError::Storage(e.to_string()))?;
            if let Some(bytes) = bytes {
                let value = deserialize_value(&bytes)?;
                let refs = value
                    .as_list()
                    .unwrap_or_default()
                    .iter()
                    .filter_map(Value::as_str)
                    .map(String::from)
                    .collect();
                Ok(refs)
            } else {
                Ok(Vec::new())
            }
        }

        fn scan_prefix(&self, prefix: &str, limit: usize, offset: usize) -> Result<Vec<(String, Value)>> {
            let mut iter = self.db.iterator(rocksdb::IteratorMode::From(prefix.as_bytes(), rocksdb::Direction::Forward));
            let mut items = Vec::new();
            for item in iter {
                let (key, value) = item.map_err(|e| StoreError::Storage(e.to_string()))?;
                let key_str = String::from_utf8(key.to_vec()).map_err(|e| StoreError::Storage(e.to_string()))?;
                if !key_str.starts_with(prefix) {
                    break;
                }
                if items.len() >= offset + limit {
                    break;
                }
                if items.len() >= offset {
                    let deserialized = deserialize_value(&value)?;
                    items.push((key_str.clone(), deserialized));
                }
            }
            Ok(items)
        }

        fn delete_edge(&self, edge_type: &str, edge_id: &str) -> Result<()> {
            let _guard = self.write_lock.lock();
            let edge_key = edge_storage_key(edge_type, edge_id);
            if let Some(bytes) = self.db.get(edge_key.as_bytes()).map_err(|e| StoreError::Storage(e.to_string()))? {
                if let Ok(value) = deserialize_value(&bytes) {
                    let targets = value
                        .as_list()
                        .unwrap_or_default()
                        .iter()
                        .filter_map(Value::as_str)
                        .map(String::from)
                        .collect::<Vec<_>>();
                    let mut batch = WriteBatch::default();
                    batch.delete(edge_key.as_bytes());
                    for target in targets {
                        let vertex_key = vertex_index_key(&target);
                        if let Some(existing_bytes) = self.db.get(vertex_key.as_bytes()).map_err(|e| StoreError::Storage(e.to_string()))? {
                            if let Ok(value) = deserialize_value(&existing_bytes) {
                                let refs = value
                                    .as_list()
                                    .unwrap_or_default()
                                    .iter()
                                    .filter_map(Value::as_str)
                                    .filter(|r| *r != &edge_key)
                                    .map(String::from)
                                    .collect::<Vec<_>>();
                                if refs.is_empty() {
                                    batch.delete(vertex_key.as_bytes());
                                } else {
                                    batch.put(vertex_key.as_bytes(), serialize_value(&Value::List(refs.iter().map(|r| Value::String(r.clone())).collect()))?);
                                }
                            }
                        }
                    }
                    self.db.write(batch).map_err(|e| StoreError::Storage(e.to_string()))?;
                }
            }
            Ok(())
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    pub fn open(path: &str) -> Result<RocksDbStore> {
        RocksDbStore::open(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InMemoryStore;
    use manifold_types::{RunMetadata, RunRecord, RunStatus, Value};
    use chrono::Utc;

    #[test]
    fn in_memory_store_puts_and_gets_run_records() {
        let store = InMemoryStore::new();
        let metadata = RunMetadata {
            run_id: "test-run".to_string(),
            pipeline_name: "test".to_string(),
            status: RunStatus::Completed,
            start_time: Utc::now(),
            end_time: Utc::now(),
            duration_ms: 5,
            nodes_executed: 1,
            ancestor_run_id: None,
            error_message: None,
        };

        let record = RunRecord {
            metadata: metadata.clone(),
            nodes: vec![],
            config_snapshot: "{}".to_string(),
            input_snapshot: Value::Null,
            output_snapshot: Value::String("ok".to_string()),
        };

        store.put_run(record.clone()).unwrap();
        let loaded = store.get_run("test-run").unwrap().expect("record must exist");
        assert_eq!(loaded.metadata.run_id, metadata.run_id);
        assert_eq!(loaded.output_snapshot, Value::String("ok".to_string()));

        let listed = store.list_runs().unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].run_id, "test-run");
    }

    #[test]
    fn in_memory_store_edge_indexing_and_scan_prefix() {
        let store = InMemoryStore::new();
        store.put_edge("friendship", "edge-1", &["alice".to_string(), "bob".to_string()]).unwrap();
        store.put_edge("friendship", "edge-2", &["bob".to_string(), "carol".to_string()]).unwrap();

        let alice_edges = store.get_vertex_edges("alice").unwrap();
        assert_eq!(alice_edges, vec!["edge:friendship:edge-1".to_string()]);

        let bob_edges = store.get_vertex_edges("bob").unwrap();
        assert_eq!(bob_edges.len(), 2);
        assert!(bob_edges.contains(&"edge:friendship:edge-1".to_string()));
        assert!(bob_edges.contains(&"edge:friendship:edge-2".to_string()));

        let scan_results = store.scan_prefix("edge:friendship:", 10, 0).unwrap();
        assert_eq!(scan_results.len(), 2);
        assert!(scan_results.iter().any(|(k, _)| k == "edge:friendship:edge-1"));
        assert!(scan_results.iter().any(|(k, _)| k == "edge:friendship:edge-2"));
    }

    #[cfg(feature = "rocksdb")]
    #[test]
    fn rocksdb_put_run_is_atomic() {
        let temp_dir = std::env::temp_dir().join(format!("manifold_store_atomic_{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let store = RocksDbStore::open(temp_dir.to_str().unwrap()).unwrap();

        let metadata = RunMetadata {
            run_id: "atomic-test-run".to_string(),
            pipeline_name: "test".to_string(),
            status: RunStatus::Completed,
            start_time: Utc::now(),
            end_time: Utc::now(),
            duration_ms: 1,
            nodes_executed: 0,
            ancestor_run_id: None,
            error_message: None,
        };

        let record = RunRecord {
            metadata: metadata.clone(),
            nodes: vec![],
            config_snapshot: "{}".to_string(),
            input_snapshot: Value::Null,
            output_snapshot: Value::String("ok".to_string()),
        };

        store.put_run(record.clone()).unwrap();
        let loaded = store.get_run("atomic-test-run").unwrap().unwrap();
        assert_eq!(loaded.metadata.run_id, "atomic-test-run");

        let secondary_key = format!("run:{}", loaded.metadata.run_id);
        let secondary_bytes = store.db.get(secondary_key.as_bytes()).unwrap().unwrap();
        let secondary_record: RunRecord = serde_json::from_slice(&secondary_bytes).unwrap();
        assert_eq!(secondary_record.metadata.run_id, "atomic-test-run");
    }
}
