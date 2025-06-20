// src/core/execution/operators/delete.rs

use crate::core::common::types::TransactionId; // Changed path for TransactionId
use crate::core::common::OxidbError;
use crate::core::execution::{ExecutionOperator, Tuple}; // Changed to ExecutionOperator
use crate::core::query::commands::Key; // For primary key type
use crate::core::storage::engine::traits::KeyValueStore;
use crate::core::wal::log_manager::LogManager;
use std::collections::HashSet; // Added HashSet
use std::sync::{Arc, RwLock};

// Removed #[derive(Debug)] because Box<dyn ExecutionOperator> is not Debug
pub struct DeleteOperator<S: KeyValueStore<Vec<u8>, Vec<u8>> + Send + Sync + 'static> {
    // Input is now Box<dyn ExecutionOperator ...>
    pub input: Box<dyn ExecutionOperator + Send + Sync>,
    pub table_name: String,
    pub store: Arc<RwLock<S>>,
    pub log_manager: Arc<LogManager>,
    pub transaction_id: TransactionId,
    pub primary_key_column_index: usize,
    pub committed_ids: Arc<HashSet<u64>>, // Added committed_ids
    // deleted_count will be stored in the iterator after execute
    // processed_input tracks if perform_deletes has run
    processed_input: bool,
    schema: Arc<Schema>, // Added schema field
}

impl<S: KeyValueStore<Vec<u8>, Vec<u8>> + Send + Sync + 'static> DeleteOperator<S> {
    pub fn new(
        input: Box<dyn ExecutionOperator + Send + Sync>, // Changed to ExecutionOperator
        table_name: String,
        store: Arc<RwLock<S>>,
        log_manager: Arc<LogManager>,
        transaction_id: TransactionId,
        primary_key_column_index: usize,
        committed_ids: Arc<HashSet<u64>>, // Added committed_ids
        schema: Arc<Schema>,              // Added schema parameter
    ) -> Self {
        Self {
            input,
            table_name,
            store,
            log_manager,
            transaction_id,
            primary_key_column_index,
            committed_ids, // Store committed_ids
            processed_input: false,
            schema, // Store schema
        }
    }

    // Helper method to perform the actual delete logic, called by execute
    // Now returns Vec<(Key, Vec<u8>)>: list of (primary_key, serialized_row_data)
    fn perform_deletes(&mut self) -> Result<Vec<(Key, Vec<u8>)>, OxidbError> {
        let mut deleted_rows_info = Vec::new();
        let mut input_iterator = self.input.execute()?;

        while let Some(tuple_result) = input_iterator.next() {
            let tuple = tuple_result?;

            let pk_data_type = tuple.get(self.primary_key_column_index).ok_or_else(|| {
                OxidbError::Execution(
                    "Primary key column missing in input tuple for DELETE.".to_string(),
                )
            })?;

            let primary_key: Key = match pk_data_type {
                DataType::String(s) => s.as_bytes().to_vec(),
                DataType::Integer(i) => i.to_be_bytes().to_vec(),
                DataType::RawBytes(b) => b.clone(), // Handle RawBytes
                _ => {
                    return Err(OxidbError::Execution(format!(
                        "Unsupported primary key type {:?} for DELETE.",
                        pk_data_type
                    )))
                }
            };

            // Construct row map from tuple and schema for serialization
            let mut row_map_data = std::collections::HashMap::new();
            if tuple.len() != self.schema.columns.len() {
                return Err(OxidbError::Execution(
                    "Tuple length does not match schema column count in DeleteOperator."
                        .to_string(),
                ));
            }
            for (idx, col_def) in self.schema.columns.iter().enumerate() {
                row_map_data.insert(col_def.name.as_bytes().to_vec(), tuple[idx].clone());
            }
            let serialized_row_data =
                serialize_data_type(&DataType::Map(JsonSafeMap(row_map_data)))?;

            let lsn = self.log_manager.next_lsn();
            let tx_for_store = crate::core::transaction::Transaction::new(self.transaction_id);

            let was_deleted = self.store.write().unwrap().delete(
                &primary_key,
                &tx_for_store,
                lsn,
                &self.committed_ids,
            )?;
            if was_deleted {
                // count += 1; // No longer returning count directly
                deleted_rows_info.push((primary_key, serialized_row_data));
            }
        }
        // Ok(count)
        Ok(deleted_rows_info)
    }
}

use crate::core::common::serialization::serialize_data_type; // For serializing row map
use crate::core::types::schema::Schema;
use crate::core::types::{DataType, JsonSafeMap}; // Added for constructing row map // Required to interpret the tuple correctly

// Implement the ExecutionOperator trait
impl<S: KeyValueStore<Vec<u8>, Vec<u8>> + Send + Sync + 'static> ExecutionOperator
    for DeleteOperator<S>
{
    fn execute(
        &mut self,
    ) -> Result<Box<dyn Iterator<Item = Result<Tuple, OxidbError>> + Send + Sync>, OxidbError> {
        if self.processed_input {
            return Err(OxidbError::Execution(
                "DeleteOperator cannot be executed more than once.".to_string(),
            ));
        }

        let deleted_rows_info = self.perform_deletes()?;
        self.processed_input = true;

        // Return an iterator that yields tuples representing deleted rows (pk, serialized_data)
        // For now, to fit the existing structure that expects a count, we'll return the count.
        // The actual deleted_rows_info will need to be passed back to QueryExecutor::handle_sql_delete
        // This requires a more significant change in how results are passed or DeleteOperator is used.
        // For this step, let's make perform_deletes accessible to QueryExecutor or change execute's return.
        //
        // Option A: Change execute to return Vec<(Key, Vec<u8>)> or similar. This breaks ExecutionOperator trait.
        // Option B: Store deleted_rows_info in DeleteOperator and add a method to retrieve it.
        // Option C: QueryExecutor::handle_sql_delete will re-fetch rows before physical delete, which is inefficient.
        //
        // Let's assume for now that QueryExecutor will handle the index logic by re-fetching data
        // before calling the physical delete, or that DeleteOperator's role is simplified to just deleting
        // from the store and the index logic is fully in QueryExecutor::handle_sql_delete before plan execution.
        // This is a mismatch with the idea of DeleteOperator handling the data.
        //
        // Revisit: The most direct way is for DeleteOperator to do its job, including preparing info for indexes.
        // The `execute` method should probably return an iterator of `(Key, RowData)` that were deleted.
        // For now, to make progress, `handle_sql_delete` will be more complex and re-fetch data if needed.
        // The current `DeleteResultIterator` returns a count. This is what `handle_sql_delete` expects.
        // To pass the deleted rows info, the design needs to change.
        //
        // Let's stick to the original plan: `DeleteOperator`'s iterator returns `(Key, Vec<u8>)`.
        // This means `DeleteResultIterator` needs to change.
        // And `handle_sql_delete` will process this iterator.

        Ok(Box::new(DeleteResultIterator {
            deleted_rows: deleted_rows_info, // Pass owned data
            current_index: 0,
        }))
    }
}

// Iterator to return the result of the DeleteOperation: (Key, SerializedRowData)
struct DeleteResultIterator {
    deleted_rows: Vec<(Key, Vec<u8>)>, // Stores (primary_key, serialized_row_data)
    current_index: usize,
}

impl Iterator for DeleteResultIterator {
    type Item = Result<Tuple, OxidbError>; // Tuple will contain [DataType::Bytes(pk), DataType::Bytes(serialized_row)]

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_index >= self.deleted_rows.len() {
            None
        } else {
            let (pk_bytes, row_bytes) = self.deleted_rows[self.current_index].clone(); // Clone to avoid lifetime issues with self
            self.current_index += 1;
            // Represent PK and row_bytes as DataType::RawBytes within a Tuple
            let tuple = vec![DataType::RawBytes(pk_bytes), DataType::RawBytes(row_bytes)];
            Some(Ok(tuple))
        }
    }
}

// A helper method to get the count, if still needed by some parts of the system.
impl DeleteResultIterator {
    #[allow(dead_code)]
    pub fn count(&self) -> usize {
        self.deleted_rows.len()
    }
}
