use crate::core::common::serialization::deserialize_data_type;
use crate::core::common::OxidbError;
use crate::core::execution::{ExecutionOperator, Tuple};
use crate::core::indexing::manager::IndexManager;
use crate::core::query::commands::Key;
use crate::core::storage::engine::traits::KeyValueStore;
use crate::core::types::DataType; // Import JsonSafeMap
use std::collections::HashSet;
use std::sync::{Arc, RwLock}; // Added RwLock

pub struct IndexScanOperator<S: KeyValueStore<Key, Vec<u8>>> {
    /// The underlying key-value store.
    store: Arc<RwLock<S>>, // Changed to Arc<RwLock<S>>
    /// The index manager to access indexes.
    index_manager: Arc<RwLock<IndexManager>>, // Changed to Arc<RwLock<IndexManager>>
    /// The name of the index to scan.
    index_name: String,
    /// The serialized value to scan for in the index.
    scan_value: Vec<u8>,
    /// The snapshot ID for MVCC visibility.
    snapshot_id: u64,
    /// The set of committed transaction IDs for MVCC visibility.
    committed_ids: Arc<HashSet<u64>>,
    /// Flag to ensure the operator is executed only once.
    executed: bool,
}

impl<S: KeyValueStore<Key, Vec<u8>>> IndexScanOperator<S> {
    pub fn new(
        store: Arc<RwLock<S>>,                    // Changed to Arc<RwLock<S>>
        index_manager: Arc<RwLock<IndexManager>>, // Changed to Arc<RwLock<IndexManager>>
        index_name: String,
        scan_value: Vec<u8>,
        snapshot_id: u64,
        committed_ids: Arc<HashSet<u64>>,
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
    fn execute(
        &mut self,
    ) -> Result<Box<dyn Iterator<Item = Result<Tuple, OxidbError>> + Send + Sync>, OxidbError> {
        // Changed DbError to OxidbError
        if self.executed {
            return Err(OxidbError::Internal(
                // Changed
                "IndexScanOperator cannot be executed more than once".to_string(),
            ));
        }
        self.executed = true;

        let primary_keys: std::vec::Vec<std::vec::Vec<u8>> = (self
            .index_manager
            .read()
            .map_err(|e| {
                OxidbError::LockTimeout(format!("Failed to acquire read lock on index manager: {}", e))
            })?
            .find_by_index(&self.index_name, &self.scan_value)?) // Acquire read lock
        .unwrap_or_default();

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
            let store_guard = match store_arc_clone.read() {
                Ok(guard) => guard,
                Err(e) => {
                    return Some(Err(OxidbError::LockTimeout(format!(
                        "Failed to acquire read lock on store for PK {:?}: {}",
                        pk, e
                    ))))
                }
            };
            match store_guard.get(&pk, snapshot_id, &committed_ids_clone) {
                Ok(Some(value_bytes)) => match deserialize_data_type(&value_bytes) {
                    Ok(row_data_type) => {
                        // row_data_type is likely a DataType::Map
                        // Prepend the actual KV store key (pk) to the tuple
                        let key_data_type = DataType::RawBytes(pk.clone()); // Use RawBytes for keys

                        // The tuple for projection should be [key, col1, col2, ...] if original row was a map
                        // Or [key, single_value_if_not_map]
                        // For UPDATEs/DELETEs, the projection expects just the key.
                        // However, a general IndexScan might be used by SELECT.
                        // The current ProjectOperator for UPDATE specifically asks for index "0".
                        // So, the first element *must* be the key.
                        // If row_data_type is a Map, we might want its fields too for SELECT *.
                        // Let's form a tuple: [key_as_RawBytes, actual_row_map_or_value]
                        // This matches TableScanOperator's output structure.
                        let tuple = vec![key_data_type, row_data_type];
                        Some(Ok(tuple))
                    }
                    Err(e) => Some(Err(OxidbError::Deserialization(format!(
                        "Failed to deserialize row data for key {:?}: {}",
                        String::from_utf8_lossy(&pk),
                        e
                    )))),
                },
                Ok(None) => None, // Row pointed to by index key not found or not visible
                Err(e) => Some(Err(e)),
            }
        });

        Ok(Box::new(iterator))
    }
}
