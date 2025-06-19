// src/core/execution/operators/delete.rs

use crate::core::common::OxidbError;
use crate::core::execution::{ExecutionOperator, Tuple}; // Changed to ExecutionOperator
use crate::core::storage::engine::traits::KeyValueStore;
use crate::core::wal::log_manager::LogManager;
use crate::core::query::commands::Key; // For primary key type
use crate::core::common::types::TransactionId; // Changed path for TransactionId
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
    // deleted_count will be stored in the iterator after execute
    // processed_input tracks if perform_deletes has run
    processed_input: bool,
}

impl<S: KeyValueStore<Vec<u8>, Vec<u8>> + Send + Sync + 'static> DeleteOperator<S> {
    pub fn new(
        input: Box<dyn ExecutionOperator + Send + Sync>, // Changed to ExecutionOperator
        table_name: String,
        store: Arc<RwLock<S>>,
        log_manager: Arc<LogManager>,
        transaction_id: TransactionId,
        primary_key_column_index: usize,
    ) -> Self {
        Self {
            input,
            table_name,
            store,
            log_manager,
            transaction_id,
            primary_key_column_index,
            processed_input: false,
        }
    }

    // Helper method to perform the actual delete logic, called by execute
    fn perform_deletes(&mut self) -> Result<usize, OxidbError> {
        let mut count = 0;
        // Get the iterator from the input operator
        let mut input_iterator = self.input.execute()?;

        while let Some(tuple_result) = input_iterator.next() {
            let tuple = tuple_result?; // Propagate error if tuple itself is an error

            let pk_data_type = tuple.get(self.primary_key_column_index).ok_or_else(|| {
                OxidbError::Execution("Primary key column missing in input tuple for DELETE.".to_string())
            })?;

            let primary_key: Key = match pk_data_type {
                 crate::core::types::DataType::String(s) => s.as_bytes().to_vec(),
                 crate::core::types::DataType::Integer(i) => i.to_be_bytes().to_vec(),
                 _ => return Err(OxidbError::Execution("Unsupported primary key type for DELETE.".to_string())),
            };

            let lsn = self.log_manager.next_lsn();
            let tx_for_store = crate::core::transaction::Transaction::new(self.transaction_id);

            let was_deleted = self.store.write().unwrap().delete(&primary_key, &tx_for_store, lsn)?;
            if was_deleted {
                count += 1;
            }
        }
        Ok(count)
    }
}

// Implement the ExecutionOperator trait
impl<S: KeyValueStore<Vec<u8>, Vec<u8>> + Send + Sync + 'static> ExecutionOperator for DeleteOperator<S> {
    fn execute(
        &mut self,
    ) -> Result<Box<dyn Iterator<Item = Result<Tuple, OxidbError>> + Send + Sync>, OxidbError> { // Removed '_
        if self.processed_input {
            return Err(OxidbError::Execution(
                "DeleteOperator cannot be executed more than once.".to_string(),
            ));
        }

        let deleted_count = self.perform_deletes()?; // perform_deletes now returns the count
        self.processed_input = true;

        // Return an iterator that yields a single tuple with the count
        Ok(Box::new(DeleteResultIterator {
            deleted_count, // Pass owned count
            output_sent: false, // Iterator manages its own sent state
        }))
    }
}

// Iterator to return the result of the DeleteOperation
struct DeleteResultIterator { // No lifetime 'a needed
    deleted_count: usize,
    output_sent: bool, // Manages its own state
}

impl Iterator for DeleteResultIterator { // No lifetime 'a needed
    type Item = Result<Tuple, OxidbError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.output_sent {
            None
        } else {
            self.output_sent = true;
            // self.deleted_count is usize, cast directly to i64
            Some(Ok(vec![crate::core::types::DataType::Integer(self.deleted_count as i64)]))
        }
    }
}
