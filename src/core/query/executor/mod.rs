// src/core/query/executor/mod.rs

// Module declarations
pub mod command_handlers;
pub mod ddl_handlers;
pub mod planner; // Added planner module
pub mod select_execution;
#[cfg(test)]
pub mod tests;
pub mod transaction_handlers;
pub mod update_execution;
pub mod utils;

// Re-export planner contents

// Necessary imports for struct definitions and the `new` method
use crate::core::common::OxidbError;
use crate::core::indexing::manager::IndexManager;
use crate::core::wal::log_manager::LogManager; // Added LogManager
use crate::core::wal::writer::WalWriter;
use crate::core::optimizer::Optimizer;
use crate::core::storage::engine::traits::KeyValueStore;
use crate::core::storage::engine::SimpleFileKvStore;
use crate::core::transaction::lock_manager::LockManager;
use crate::core::transaction::manager::TransactionManager;
use crate::core::transaction::Transaction;
use crate::core::types::DataType;
use std::collections::HashSet; // Added HashSet import
use crate::core::common::types::TransactionId; // Ensure TransactionId is imported
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

#[derive(Debug, PartialEq)]
pub enum ExecutionResult {
    Value(Option<DataType>),
    Success,
    Deleted(bool),
    Values(Vec<DataType>),
}

#[derive(Debug)]
pub struct QueryExecutor<S: KeyValueStore<Vec<u8>, Vec<u8>>> {
    pub(crate) store: Arc<RwLock<S>>,
    pub(crate) transaction_manager: TransactionManager,
    pub(crate) lock_manager: LockManager,
    pub(crate) index_manager: Arc<IndexManager>,
    pub(crate) optimizer: Optimizer, // Added optimizer field
    pub(crate) log_manager: Arc<LogManager>, // Added log_manager field
}

// The `new` method remains here as it's tied to the struct definition visibility
impl<S: KeyValueStore<Vec<u8>, Vec<u8>>> QueryExecutor<S> {
    pub fn new(
        store: S,
        index_base_path: PathBuf,
        wal_writer: WalWriter,
        log_manager: Arc<LogManager>, // Parameter already here
    ) -> Result<Self, OxidbError> {
        let mut index_manager = IndexManager::new(index_base_path)?;

        if index_manager.get_index("default_value_index").is_none() {
            index_manager.create_index("default_value_index".to_string(), "hash").map_err(|e| {
                OxidbError::Index(format!("Failed to create default_value_index: {}", e))
            })?;
        }

        // Pass a clone of log_manager to TransactionManager, store original in self
        let mut transaction_manager = TransactionManager::new(wal_writer, log_manager.clone());
        transaction_manager.add_committed_tx_id(TransactionId(0)); // Use TransactionId struct
        let index_manager_arc = Arc::new(index_manager);

        Ok(QueryExecutor {
            store: Arc::new(RwLock::new(store)),
            transaction_manager,
            lock_manager: LockManager::new(),
            optimizer: Optimizer::new(index_manager_arc.clone()), // Initialize optimizer
            index_manager: index_manager_arc,
            log_manager, // Store log_manager
        })
    }
}

// Methods specific to QueryExecutor when the store is SimpleFileKvStore
impl QueryExecutor<SimpleFileKvStore> {
    pub fn persist(&mut self) -> Result<(), OxidbError> {
        self.store.read().unwrap().persist()?;
        self.index_manager.save_all_indexes()
    }

    pub fn index_base_path(&self) -> PathBuf {
        self.index_manager.base_path()
    }
}

// Moved DML handlers to the generic QueryExecutor impl block for visibility by command_handlers.rs
impl<S: KeyValueStore<Vec<u8>, Vec<u8>> + Send + Sync + 'static> QueryExecutor<S> {
    // Note: persist() and index_base_path() are specific to SimpleFileKvStore, so they remain in that impl block.
    // New home for handle_insert, handle_get, handle_delete:

    pub(crate) fn handle_insert(&mut self, key: Vec<u8>, value: DataType) -> Result<ExecutionResult, OxidbError> {
        let current_op_tx_id = self
            .transaction_manager
            .current_active_transaction_id()
            .unwrap_or(TransactionId(0)); // Use TransactionId struct

        // Create a temporary transaction representation for the store operation
        let tx_for_store = Transaction::new(current_op_tx_id); // Pass TransactionId struct

        // Generate LSN for this operation
        let new_lsn = self.log_manager.next_lsn();

        // If there's a real, active transaction managed by TransactionManager, update its prev_lsn
        if current_op_tx_id != TransactionId(0) { // Compare with TransactionId(0)
            if let Some(active_tx_mut) = self.transaction_manager.get_active_transaction_mut() {
                active_tx_mut.prev_lsn = new_lsn;
            }
        }

        // Convert DataType to Vec<u8> for storage
        let value_bytes = bincode::serialize(&value)
            .map_err(|e| OxidbError::Serialization(e.to_string()))?;

        self.store
            .write()
            .unwrap()
            .put(key.clone(), value_bytes, &tx_for_store, new_lsn)?; // Pass new_lsn

        // Indexing: Use on_insert_data instead of get_index_mut
        let mut indexed_values_map = std::collections::HashMap::new();
        // Assuming 'value' (DataType) is serialized for the "default_value_index"
        // The actual indexed value depends on how "default_value_index" is configured/used.
        // For this example, we'll serialize the entire DataType for indexing.
        let serialized_value_for_index = bincode::serialize(&value)
            .map_err(|e| OxidbError::Serialization(format!("Failed to serialize value for indexing: {}", e)))?;
        indexed_values_map.insert("default_value_index".to_string(), serialized_value_for_index);

        self.index_manager.on_insert_data(&indexed_values_map, &key)?;

        Ok(ExecutionResult::Success)
    }

    pub(crate) fn handle_get(&mut self, key: Vec<u8>) -> Result<ExecutionResult, OxidbError> {
        let snapshot_id = self.transaction_manager.current_active_transaction_id().unwrap_or(TransactionId(0)); // Use TransactionId
        // Collect TransactionId.0 (u64) for HashSet<u64>
        let committed_ids_set: HashSet<u64> = self.transaction_manager.get_committed_tx_ids_snapshot().into_iter().map(|tx_id| tx_id.0).collect();

        // The KeyValueStore::get method expects snapshot_id as u64.
        let result_bytes_opt = self.store.read().unwrap().get(&key, snapshot_id.0, &committed_ids_set)?;

        match result_bytes_opt {
            Some(bytes) => {
                let value_dt: DataType = bincode::deserialize(&bytes)
                    .map_err(|e| OxidbError::Deserialization(e.to_string()))?;
                Ok(ExecutionResult::Value(Some(value_dt)))
            }
            None => Ok(ExecutionResult::Value(None)),
        }
    }

    pub(crate) fn handle_delete(&mut self, key: Vec<u8>) -> Result<ExecutionResult, OxidbError> {
        let current_op_tx_id = self
            .transaction_manager
            .current_active_transaction_id()
            .unwrap_or(TransactionId(0)); // Use TransactionId struct

        let tx_for_store = Transaction::new(current_op_tx_id); // Pass TransactionId struct

        // Generate LSN for this operation
        let new_lsn = self.log_manager.next_lsn();

        // If there's a real, active transaction, update its prev_lsn
        if current_op_tx_id != TransactionId(0) { // Compare with TransactionId(0)
             if let Some(active_tx_mut) = self.transaction_manager.get_active_transaction_mut() {
                active_tx_mut.prev_lsn = new_lsn;
            }
        }

        // For indexing: retrieve the value before deleting it from the store
        // Collect TransactionId.0 (u64) for HashSet<u64>
        let committed_ids_set: HashSet<u64> = self.transaction_manager.get_committed_tx_ids_snapshot().into_iter().map(|tx_id| tx_id.0).collect();
        // KeyValueStore::get expects snapshot_id as u64
        let value_to_delete_opt = self.store.read().unwrap().get(&key, current_op_tx_id.0, &committed_ids_set)?;

        let deleted = self
            .store
            .write()
            .unwrap()
            .delete(&key, &tx_for_store, new_lsn)?; // Pass new_lsn

        if deleted {
            if let Some(value_bytes) = value_to_delete_opt {
                // Indexing: Use on_delete_data
                let mut indexed_values_map = std::collections::HashMap::new();
                // Assuming the "default_value_index" indexed the serialized version of the DataType
                indexed_values_map.insert("default_value_index".to_string(), value_bytes);
                self.index_manager.on_delete_data(&indexed_values_map, &key)?;
            }
        }
        Ok(ExecutionResult::Deleted(deleted))
    }

    // handle_find_by_index, handle_vacuum - these are in ddl_handlers.rs and transaction_handlers.rs respectively.
    // handle_select, handle_update - these are in select_execution.rs and update_execution.rs respectively.
}
