use crate::core::common::error::DbError;
use crate::core::types::DataType;
use crate::core::execution::{ExecutionOperator, Tuple};
use crate::core::storage::engine::traits::KeyValueStore;
use crate::core::indexing::manager::IndexManager;
use crate::core::common::serialization::deserialize_data_type;
use crate::core::query::commands::Key;
use std::sync::{Arc, RwLock}; // Added RwLock
use std::collections::HashSet;

pub struct IndexScanOperator<S: KeyValueStore<Key, Vec<u8>>> {
    store: Arc<RwLock<S>>, // Changed to Arc<RwLock<S>>
    index_manager: Arc<IndexManager>,
    index_name: String,
    scan_value: Vec<u8>,
    snapshot_id: u64,
    committed_ids: Arc<HashSet<u64>>,
    executed: bool,
}

impl<S: KeyValueStore<Key, Vec<u8>>> IndexScanOperator<S> {
    pub fn new(
        store: Arc<RwLock<S>>, // Changed to Arc<RwLock<S>>
        index_manager: Arc<IndexManager>,
        index_name: String,
        scan_value: Vec<u8>,
        snapshot_id: u64,
        committed_ids: Arc<HashSet<u64>>
    ) -> Self {
        IndexScanOperator {
            store,
            index_manager,
            index_name,
            scan_value,
            snapshot_id,
            committed_ids,
            executed: false,
        }
    }
}

impl<S: KeyValueStore<Key, Vec<u8>> + 'static> ExecutionOperator for IndexScanOperator<S> {
    fn execute(&mut self) -> Result<Box<dyn Iterator<Item = Result<Tuple, DbError>> + Send + Sync>, DbError> {
        if self.executed {
            return Err(DbError::Internal("IndexScanOperator cannot be executed more than once".to_string()));
        }
        self.executed = true;

        let primary_keys = match self.index_manager.find_by_index(&self.index_name, &self.scan_value)? {
            Some(pks) => pks,
            None => Vec::new(),
        };

        if primary_keys.is_empty() {
            return Ok(Box::new(std::iter::empty()));
        }

        // self.store is Arc<RwLock<S>>, so clone it for the move into the iterator map.
        // The lock will be acquired per .get() call.
        let store_arc_clone = Arc::clone(&self.store);
        let snapshot_id = self.snapshot_id; // Copy, as u64 is Copy
        let committed_ids_clone = Arc::clone(&self.committed_ids);

        let iterator = primary_keys.into_iter().filter_map(move |pk| {
            // Acquire read lock for each get operation
            let store_guard = store_arc_clone.read().unwrap();
            match store_guard.get(&pk, snapshot_id, &committed_ids_clone) {
                Ok(Some(value_bytes)) => {
                    match deserialize_data_type(&value_bytes) {
                        Ok(data_type) => {
                            let tuple = match data_type {
                                DataType::Map(map_data) => {
                                    map_data.values().cloned().collect::<Vec<DataType>>()
                                }
                                DataType::JsonBlob(json_value) => {
                                    if json_value.is_object() {
                                        json_value.as_object().unwrap().values().map(|v| {
                                            DataType::String(v.to_string())
                                        }).collect::<Vec<DataType>>()
                                    } else {
                                        vec![DataType::JsonBlob(json_value)]
                                    }
                                }
                                single_val => vec![single_val],
                            };
                            Some(Ok(tuple))
                        }
                        Err(e) => Some(Err(DbError::SerializationError(format!("Failed to deserialize data during index scan for PK {:?}: {}", pk, e)))),
                    }
                }
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            }
        });

        Ok(Box::new(iterator))
    }
}
