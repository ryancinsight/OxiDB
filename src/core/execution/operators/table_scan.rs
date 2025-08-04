use crate::core::common::serialization::deserialize_data_type;
use crate::core::common::OxidbError;
use crate::core::execution::{ExecutionOperator, Tuple};
use crate::core::query::commands::Key;
use crate::core::storage::engine::traits::KeyValueStore;
use crate::core::types::{DataType, schema::Schema}; // Import JsonSafeMap and Schema
use std::collections::HashSet;
use std::sync::{Arc, RwLock}; // Added RwLock

pub struct TableScanOperator<S: KeyValueStore<Key, Vec<u8>>> {
    /// The underlying key-value store instance, wrapped for thread-safe access.
    store: Arc<RwLock<S>>,
    /// The name of the table to scan. Currently unused in execute but kept for context.
    #[allow(dead_code)]
    table_name: String,
    /// The table schema to extract columns in the correct order
    schema: Schema,
    /// Transaction snapshot ID
    #[allow(dead_code)]
    snapshot_id: u64,
    /// Set of committed transaction IDs for visibility check
    #[allow(dead_code)]
    committed_ids: Arc<HashSet<u64>>,
}

impl<S: KeyValueStore<Key, Vec<u8>>> TableScanOperator<S> {
    pub const fn new(
        store: Arc<RwLock<S>>, // Changed to Arc<RwLock<S>>
        table_name: String,
        schema: Schema,
        snapshot_id: u64,
        committed_ids: Arc<HashSet<u64>>,
    ) -> Self {
        Self { store, table_name, schema, snapshot_id, committed_ids }
    }
}

impl<S: KeyValueStore<Key, Vec<u8>> + 'static> ExecutionOperator for TableScanOperator<S> {
    fn execute(
        &mut self,
    ) -> Result<Box<dyn Iterator<Item = Result<Tuple, OxidbError>> + Send + Sync>, OxidbError> {
        // Now self.store is Arc<RwLock<S>>, so we need to lock it for reading.
        let store_guard = self.store.read().map_err(|e| {
            OxidbError::LockTimeout(format!("Failed to acquire read lock on store: {e}"))
        })?;

        let all_kvs = store_guard.scan()?; // This can return OxidbError
                                           // Drop the guard explicitly after scan is done if possible, though iterator might hold it implicitly.
                                           // For filter_map, the guard might be held longer. This needs careful thought in real async scenarios.
                                           // For now, this synchronous version should be okay.

        let table_name = self.table_name.clone();
        let schema = self.schema.clone();
        let iterator = all_kvs
    .into_iter()
    .filter(|(key_bytes, _)| {
        // Filter out schema keys and non-table entries
        !key_bytes.starts_with(b"_schema_") &&
        String::from_utf8_lossy(key_bytes).starts_with(&table_name)
    })
    .map(move |(key_bytes, value_bytes)| {
        match deserialize_data_type(&value_bytes) {
            Ok(DataType::Map(map_data)) => {
                // Use map/collect instead of imperative for-loop
                let tuple: Vec<DataType> = schema.columns.iter()
                    .map(|col_def| {
                        let col_name_bytes = col_def.name.as_bytes();
                        map_data.0.get(col_name_bytes)
                            .cloned()
                            .unwrap_or(DataType::Null)
                    })
                    .collect();
                Ok(tuple)
            },
            Ok(other) => Err(OxidbError::Internal(format!(
                "Expected DataType::Map for row data, got {:?}",
                other
            ))),
            Err(e) => Err(OxidbError::Deserialization(format!(
                "Failed to deserialize row data for key {:?}: {}",
                String::from_utf8_lossy(&key_bytes),
                e
            ))),
        }
    });

Ok(Box::new(iterator))
    }
}
