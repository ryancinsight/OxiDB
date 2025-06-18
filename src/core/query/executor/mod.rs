// src/core/query/executor/mod.rs

// Module declarations
pub mod command_handlers;
pub mod ddl_handlers;
pub mod planner; // Added planner module
pub mod processors;
pub mod select_execution;
#[cfg(test)]
pub mod tests;
pub mod transaction_handlers;
pub mod update_execution;
pub mod utils;

// Re-export planner contents

// Necessary imports for struct definitions and the `new` method
use crate::core::common::types::TransactionId; // Ensure TransactionId is imported
use crate::core::common::OxidbError;
use crate::core::indexing::manager::IndexManager;
use crate::core::optimizer::Optimizer;
use crate::core::storage::engine::traits::KeyValueStore;
use crate::core::storage::engine::SimpleFileKvStore;
use crate::core::transaction::lock_manager::LockManager;
use crate::core::transaction::manager::TransactionManager;
use crate::core::transaction::LockType; // Added LockType import
use crate::core::transaction::Transaction;
use crate::core::types::DataType;
use crate::core::wal::log_manager::LogManager; // Added LogManager
use crate::core::wal::writer::WalWriter;
use std::collections::HashSet; // Added HashSet import
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
    pub(crate) optimizer: Optimizer,         // Added optimizer field
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

    pub(crate) fn handle_insert(
        &mut self,
        key: Vec<u8>,
        value: DataType,
    ) -> Result<ExecutionResult, OxidbError> {
        let current_op_tx_id =
            self.transaction_manager.current_active_transaction_id().unwrap_or(TransactionId(0)); // Use TransactionId struct

        // Acquire Exclusive lock if in an active transaction
        if current_op_tx_id != TransactionId(0) {
            self.lock_manager.acquire_lock(current_op_tx_id.0, &key, LockType::Exclusive)?;
        }

        // Create a temporary transaction representation for the store operation
        let tx_for_store = Transaction::new(current_op_tx_id); // Pass TransactionId struct

        // Generate LSN for this operation
        let new_lsn = self.log_manager.next_lsn();

        // If there's a real, active transaction managed by TransactionManager, update its prev_lsn
        if current_op_tx_id != TransactionId(0) {
            // Get committed IDs *before* mutably borrowing transaction_manager for active_tx_mut
            let committed_ids_for_read: HashSet<u64> = self
                .transaction_manager
                .get_committed_tx_ids_snapshot()
                .into_iter()
                .map(|tx_id| tx_id.0)
                .collect();

            // Compare with TransactionId(0)
            if let Some(active_tx_mut) = self.transaction_manager.get_active_transaction_mut() {
                active_tx_mut.prev_lsn = new_lsn;

                // --- Enhanced Undo Logging for Inserts (acting as Updates) ---
                // Check if the key already exists to log the correct store undo operation.
                // We need to read what the state would be for the current transaction before this operation.
                // This uses the current_op_tx_id as snapshot_id for the get.
                let old_value_bytes_opt = self.store.read().unwrap().get(
                    &key,
                    current_op_tx_id.0,
                    &committed_ids_for_read,
                )?;

                if let Some(old_value_bytes) = old_value_bytes_opt {
                    // Key exists, this is an update. Log RevertUpdate.
                    active_tx_mut.add_undo_operation(
                        crate::core::transaction::transaction::UndoOperation::RevertUpdate {
                            key: key.clone(),
                            old_value: old_value_bytes, // Pass Vec<u8> directly
                        },
                    );
                    // TODO: Index undo for updates should be IndexRevertUpdate, which requires old and new indexed values.
                    // For now, keeping the existing IndexRevertInsert, which is not fully correct for updates.
                    // This simplification is to limit scope for this turn.
                    let new_value_for_index_bytes =
                        crate::core::common::serialization::serialize_data_type(&value)?;
                    active_tx_mut.add_undo_operation(
                        crate::core::transaction::transaction::UndoOperation::IndexRevertInsert {
                            // Should be IndexRevertUpdate
                            index_name: "default_value_index".to_string(),
                            key: key.clone(),
                            value_for_index: new_value_for_index_bytes.clone(),
                        },
                    );
                } else {
                    // Key does not exist, this is a true insert.
                    active_tx_mut.add_undo_operation(
                        crate::core::transaction::transaction::UndoOperation::RevertInsert {
                            key: key.clone(),
                        },
                    );
                    let new_value_for_index_bytes =
                        crate::core::common::serialization::serialize_data_type(&value)?;
                    active_tx_mut.add_undo_operation(
                        crate::core::transaction::transaction::UndoOperation::IndexRevertInsert {
                            index_name: "default_value_index".to_string(),
                            key: key.clone(),
                            value_for_index: new_value_for_index_bytes.clone(),
                        },
                    );
                }
                // --- End of Enhanced Undo Logging ---
            }
        }

        // Convert DataType to Vec<u8> for storage using the project's standard serialization
        let value_bytes = crate::core::common::serialization::serialize_data_type(&value)?;

        self.store.write().unwrap().put(
            key.clone(),
            value_bytes.clone(),
            &tx_for_store,
            new_lsn,
        )?;

        // Indexing: Use on_insert_data
        let mut indexed_values_map = std::collections::HashMap::new();
        // For "default_value_index", use the already serialized `value_bytes` (from serialize_data_type).
        let serialized_value_for_index = value_bytes;
        indexed_values_map
            .insert("default_value_index".to_string(), serialized_value_for_index.clone());

        self.index_manager.on_insert_data(&indexed_values_map, &key)?;

        // Original index undo log (now part of the conditional block above for active transactions)
        // if current_op_tx_id != TransactionId(0) {
        //     if let Some(active_tx_mut) = self.transaction_manager.get_active_transaction_mut() {
        //         // This specific IndexRevertInsert is now handled above, conditionally
        //     }
        // }
        Ok(ExecutionResult::Success)
    }

    pub(crate) fn handle_get(&mut self, key: Vec<u8>) -> Result<ExecutionResult, OxidbError> {
        let current_tx_id_opt = self.transaction_manager.current_active_transaction_id();

        // Acquire Shared lock if in an active transaction
        if let Some(current_tx_id) = current_tx_id_opt {
            if current_tx_id != TransactionId(0) {
                // Don't acquire for "auto-commit" tx
                self.lock_manager.acquire_lock(current_tx_id.0, &key, LockType::Shared)?;
            }
        }

        let snapshot_id = current_tx_id_opt.unwrap_or(TransactionId(0));
        let committed_ids_set: HashSet<u64> = self
            .transaction_manager
            .get_committed_tx_ids_snapshot()
            .into_iter()
            .map(|tx_id| tx_id.0)
            .collect();

        let result_bytes_opt =
            self.store.read().unwrap().get(&key, snapshot_id.0, &committed_ids_set)?;

        match result_bytes_opt {
            Some(bytes) => {
                // Deserialize using the project's standard deserialization
                let value_dt = crate::core::common::serialization::deserialize_data_type(&bytes)?;
                Ok(ExecutionResult::Value(Some(value_dt)))
            }
            None => Ok(ExecutionResult::Value(None)),
        }
    }

    pub(crate) fn handle_delete(&mut self, key: Vec<u8>) -> Result<ExecutionResult, OxidbError> {
        let current_op_tx_id =
            self.transaction_manager.current_active_transaction_id().unwrap_or(TransactionId(0)); // Use TransactionId struct

        // Acquire Exclusive lock if in an active transaction
        if current_op_tx_id != TransactionId(0) {
            self.lock_manager.acquire_lock(current_op_tx_id.0, &key, LockType::Exclusive)?;
        }

        let tx_for_store = Transaction::new(current_op_tx_id); // Pass TransactionId struct

        // Generate LSN for this operation
        let new_lsn = self.log_manager.next_lsn();

        // If there's a real, active transaction, update its prev_lsn
        if current_op_tx_id != TransactionId(0) {
            // Compare with TransactionId(0)
            if let Some(active_tx_mut) = self.transaction_manager.get_active_transaction_mut() {
                active_tx_mut.prev_lsn = new_lsn;
            }
        }

        // For indexing: retrieve the value before deleting it from the store
        // Collect TransactionId.0 (u64) for HashSet<u64>
        let committed_ids_set: HashSet<u64> = self
            .transaction_manager
            .get_committed_tx_ids_snapshot()
            .into_iter()
            .map(|tx_id| tx_id.0)
            .collect();
        // KeyValueStore::get expects snapshot_id as u64
        let value_to_delete_opt =
            self.store.read().unwrap().get(&key, current_op_tx_id.0, &committed_ids_set)?;

        let deleted = self.store.write().unwrap().delete(&key, &tx_for_store, new_lsn)?; // Pass new_lsn

        if deleted {
            if let Some(value_bytes) = value_to_delete_opt {
                // Indexing: Use on_delete_data
                let mut indexed_values_map = std::collections::HashMap::new();
                // Assuming the "default_value_index" indexed the serialized version of the DataType
                indexed_values_map.insert("default_value_index".to_string(), value_bytes.clone()); // Clone for undo log
                self.index_manager.on_delete_data(&indexed_values_map, &key)?;

                // Add to undo log for index if in an active transaction
                if current_op_tx_id != TransactionId(0) {
                    if let Some(active_tx_mut) =
                        self.transaction_manager.get_active_transaction_mut()
                    {
                        // Add UndoOperation for the data itself
                        active_tx_mut.add_undo_operation(crate::core::transaction::transaction::UndoOperation::RevertDelete {
                            key: key.clone(),
                            old_value: value_bytes.clone(), // value_bytes is Vec<u8>
                        });
                        // Add UndoOperation for the index
                        active_tx_mut.add_undo_operation(crate::core::transaction::transaction::UndoOperation::IndexRevertDelete {
                            index_name: "default_value_index".to_string(),
                            key: key.clone(),
                            old_value_for_index: value_bytes, // Pass the original serialized value
                        });
                    }
                }
            }
        }
        Ok(ExecutionResult::Deleted(deleted))
    }

    // handle_find_by_index, handle_vacuum - these are in ddl_handlers.rs and transaction_handlers.rs respectively.
    // handle_select, handle_update - these are in select_execution.rs and update_execution.rs respectively.
}
