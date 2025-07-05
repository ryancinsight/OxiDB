use crate::core::common::serialization::deserialize_data_type; // TODO: This likely needs to change to deserialize a full Tuple (Vec<Value>)
use crate::core::common::types::{Schema, Value}; // Added Value
use crate::core::common::OxidbError;
use crate::core::execution::{ExecutionOperator, Tuple};
use crate::core::query::commands::Key;
use crate::core::storage::engine::traits::KeyValueStore;
// use crate::core::types::DataType; // DataType might still be needed if values are stored that way. Value is the target for Tuple.
use std::collections::HashSet;
use std::sync::{Arc, RwLock};

pub struct TableScanOperator<S: KeyValueStore<Key, Vec<u8>>> {
    store: Arc<RwLock<S>>,
    table_name: String, // Keep for context, schema loading
    schema: Arc<Schema>, // Store the schema of the table
    snapshot_id: u64,
    committed_ids: Arc<HashSet<u64>>,
    executed: bool,
}

impl<S: KeyValueStore<Key, Vec<u8>>> TableScanOperator<S> {
    pub fn new(
        store: Arc<RwLock<S>>,
        table_name: String,
        schema: Arc<Schema>, // Accept schema during construction
        snapshot_id: u64,
        committed_ids: Arc<HashSet<u64>>,
    ) -> Self {
        TableScanOperator {
            store,
            table_name,
            schema,
            snapshot_id,
            committed_ids,
            executed: false,
        }
    }
}

impl<S: KeyValueStore<Key, Vec<u8>> + Send + Sync + 'static> ExecutionOperator for TableScanOperator<S> {
    fn execute(
        &mut self,
    ) -> Result<Box<dyn Iterator<Item = Result<Tuple, OxidbError>> + Send + Sync>, OxidbError> {
        if self.executed {
            return Err(OxidbError::Internal(
                "TableScanOperator cannot be executed more than once".to_string(),
            ));
        }
        self.executed = true;

        let store_guard = self.store.read().map_err(|e| {
            OxidbError::Lock(format!("Failed to acquire read lock on store: {}", e))
        })?;
        let all_kvs = store_guard.scan()?;

        // Clone schema for use in the closure.
        // This is somewhat problematic if rows are not stored in a way that directly maps to schema.
        // The current deserialization path (deserialize_data_type) returns a single DataType,
        // which was then assumed to be a Map. This needs to align with actual row storage.
        // For now, we'll assume the deserialized Value is a Map that needs to be converted
        // to a Tuple according to self.schema. This is a significant assumption.
        // A better approach would be:
        // 1. deserialize_row(&value_bytes, &self.schema) -> Result<Tuple, OxidbError>
        // This is a TODO for storage/serialization alignment.

        let schema_clone = self.schema.clone(); // Clone Arc for the iterator

        let iterator = all_kvs.into_iter().filter_map(move |(key_bytes, value_bytes)| {
            if key_bytes.starts_with(b"_schema_") { // Filter out schema entries from the store
                return None;
            }

            // TODO: Replace this with robust row deserialization matching the schema.
            // Current: deserialize_data_type -> Value (hopefully a Map) -> convert to Tuple
            match deserialize_data_type(&value_bytes) { // This returns a single Value
                Ok(deserialized_value) => {
                    match deserialized_value {
                        Value::Map(map) => {
                            // Attempt to construct the tuple based on schema column order
                            let mut tuple_values = Vec::with_capacity(schema_clone.columns.len());
                            let mut conversion_ok = true;
                            for col_def in &schema_clone.columns {
                                if let Some(val) = map.get(&col_def.name) {
                                    // TODO: Type check val against col_def.data_type if strict
                                    tuple_values.push(val.clone());
                                } else {
                                    // Column not found in map, push Null or error
                                    // For now, let's assume schema compliance means it should be there,
                                    // or it's a nullable column. Pushing Null if missing.
                                    // This behavior should be more robust.
                                    // If a column is NOT NULL and missing, it's an error.
                                    if col_def.is_nullable.unwrap_or(true) { // Assuming a new is_nullable field or default
                                         tuple_values.push(Value::Null);
                                    } else {
                                        conversion_ok = false;
                                        return Some(Err(OxidbError::Serialization(format!(
                                            "Non-nullable column '{}' missing in stored row for key {:?}",
                                            col_def.name, String::from_utf8_lossy(&key_bytes)
                                        ))));
                                    }
                                }
                            }
                            if conversion_ok {
                                Some(Ok(tuple_values))
                            } else {
                                None // Error already returned
                            }
                        }
                        _ => {
                             // Expected row data to be a Map (or similar structured type)
                            Some(Err(OxidbError::Serialization(format!(
                                "Expected stored row data to be a Map for key {:?}, found {:?}",
                                String::from_utf8_lossy(&key_bytes), deserialized_value.get_type()
                            ))))
                        }
                    }
                }
                Err(e) => Some(Err(OxidbError::Deserialization(format!(
                    "Failed to deserialize row data for key {:?}: {}",
                    String::from_utf8_lossy(&key_bytes),
                    e
                )))),
            }
        });

        Ok(Box::new(iterator))
    }

    fn get_output_schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}
