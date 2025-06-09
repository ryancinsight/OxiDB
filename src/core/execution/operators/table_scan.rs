use crate::core::common::OxidbError;
use crate::core::common::serialization::deserialize_data_type;
use crate::core::execution::{ExecutionOperator, Tuple};
use crate::core::query::commands::Key;
use crate::core::storage::engine::traits::KeyValueStore;
use crate::core::types::DataType;
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
    ) -> Result<Box<dyn Iterator<Item = Result<Tuple, OxidbError>> + Send + Sync>, OxidbError> { // Changed
        if self.executed {
            return Err(OxidbError::Internal( // Changed
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
            all_kvs.into_iter().filter_map(move |(_key, value_bytes)| match deserialize_data_type( // deserialize_data_type now returns OxidbError
                &value_bytes,
            ) {
                Ok(data_type) => {
                    let tuple = match data_type {
                        DataType::Map(map_data) => {
                            map_data.values().cloned().collect::<Vec<DataType>>()
                        }
                        DataType::JsonBlob(json_value) => {
                            if json_value.is_object() {
                                json_value
                                    .as_object()
                                    .unwrap()
                                    .values()
                                    .map(|v| DataType::String(v.to_string()))
                                    .collect::<Vec<DataType>>()
                            } else {
                                vec![DataType::JsonBlob(json_value)]
                            }
                        }
                        single_val => vec![single_val],
                    };
                    Some(Ok(tuple))
                }
                Err(e) => Some(Err(e)), // Changed to pass through OxidbError
            });

        Ok(Box::new(iterator))
    }
}
