use crate::core::common::serialization::deserialize_data_type;
use crate::core::common::OxidbError;
use crate::core::execution::{ExecutionOperator, Tuple};
use crate::core::query::commands::Key;
use crate::core::storage::engine::traits::KeyValueStore;
use crate::core::types::DataType; // Import JsonSafeMap
use std::collections::HashSet;
use std::sync::{Arc, RwLock}; // Added RwLock

pub struct TableScanOperator<S: KeyValueStore<Key, Vec<u8>>> {
    store: Arc<RwLock<S>>, // Changed to Arc<RwLock<S>>
    #[allow(dead_code)]
    table_name: String,
    #[allow(dead_code)]
    snapshot_id: u64,
    #[allow(dead_code)]
    committed_ids: Arc<HashSet<u64>>,
    executed: bool,
}

impl<S: KeyValueStore<Key, Vec<u8>>> TableScanOperator<S> {
    pub fn new(
        store: Arc<RwLock<S>>, // Changed to Arc<RwLock<S>>
        table_name: String,
        snapshot_id: u64,
        committed_ids: Arc<HashSet<u64>>,
    ) -> Self {
        TableScanOperator { store, table_name, snapshot_id, committed_ids, executed: false }
    }
}

impl<S: KeyValueStore<Key, Vec<u8>> + 'static> ExecutionOperator for TableScanOperator<S> {
    fn execute(
        &mut self,
    ) -> Result<Box<dyn Iterator<Item = Result<Tuple, OxidbError>> + Send + Sync>, OxidbError> {
        // Changed
        if self.executed {
            return Err(OxidbError::Internal(
                // Changed
                "TableScanOperator cannot be executed more than once".to_string(),
            ));
        }
        self.executed = true;

        // Now self.store is Arc<RwLock<S>>, so we need to lock it for reading.
        let store_guard = self.store.read().unwrap();
        let all_kvs = store_guard.scan()?; // This can return OxidbError
                                           // Drop the guard explicitly after scan is done if possible, though iterator might hold it implicitly.
                                           // For filter_map, the guard might be held longer. This needs careful thought in real async scenarios.
                                           // For now, this synchronous version should be okay.

        let iterator =
            all_kvs.into_iter().filter_map(move |(key_bytes, value_bytes)| {
                // Filter out schema keys (and potentially other internal metadata)
                if key_bytes.starts_with(b"_schema_") {
                    return None; // Skip schema entries
                }

                match deserialize_data_type(&value_bytes) {
                    Ok(row_data_type) => {
                        // Convert the raw key_bytes to a DataType.
                        // This assumes the actual row key (which might be a PK value or a generated UUID)
                        // is stored as a string or can be meaningfully represented as one here.
                        // For the purpose of UPDATE, the first element of this tuple is crucial
                        // as it's used to fetch the row again.
                        let key_data_type = DataType::String(String::from_utf8_lossy(&key_bytes).into_owned());

                        // The tuple now contains the KV store's key as the first element,
                        // and the deserialized row data (expected to be a DataType::Map) as the second.
                        let tuple = vec![key_data_type, row_data_type];
                        Some(Ok(tuple))
                    }
                    Err(e) => Some(Err(OxidbError::Deserialization(format!(
                        "Failed to deserialize row data for key {:?}: {}",
                        String::from_utf8_lossy(&key_bytes), e
                    )))),
                }
            });

        Ok(Box::new(iterator))
    }
}
