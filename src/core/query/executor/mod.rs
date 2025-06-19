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
    Updated { count: usize }, // Added for update operations
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
                            old_value: old_value_bytes.clone(), // Clone for RevertUpdate
                        },
                    );
                    // Log IndexRevertUpdate for the index.
                    // `old_value_bytes` is the serialized form of the old DataType.
                    // `value_bytes` (which will be computed shortly for the main store operation)
                    // is the serialized form of the new DataType.
                    let new_value_for_index_bytes =
                        crate::core::common::serialization::serialize_data_type(&value)?; // This is the new value
                    active_tx_mut.add_undo_operation(
                        crate::core::transaction::transaction::UndoOperation::IndexRevertUpdate {
                            index_name: "default_value_index".to_string(),
                            key: key.clone(),
                            old_value_for_index: old_value_bytes, // Pass the original old_value_bytes
                            new_value_for_index: new_value_for_index_bytes, // This is the new value's serialized form
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
                println!("[QE::handle_get] Bytes before deserializing for key '{:?}': {:?}", String::from_utf8_lossy(&key), bytes);
                println!("[QE::handle_get] Bytes as string for key '{:?}': '{}'", String::from_utf8_lossy(&key), String::from_utf8_lossy(&bytes));
                // Deserialize using the project's standard deserialization
                let value_dt = crate::core::common::serialization::deserialize_data_type(&bytes)?;
                println!("[QE::handle_get] Deserialized DataType for key '{:?}': {:?}", String::from_utf8_lossy(&key), value_dt);
                Ok(ExecutionResult::Value(Some(value_dt)))
            }
            None => {
                println!("[QE::handle_get] Key '{:?}' not found in store.", String::from_utf8_lossy(&key)); // Debug print
                Ok(ExecutionResult::Value(None))
            }
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
        eprintln!("[QE::handle_delete] Key: '{}', OpTxID: {}. value_to_delete_opt.is_some(): {}", String::from_utf8_lossy(&key), current_op_tx_id.0, value_to_delete_opt.is_some());

        let deleted = self.store.write().unwrap().delete(&key, &tx_for_store, new_lsn)?; // Pass new_lsn
        eprintln!("[QE::handle_delete] Key: '{}', OpTxID: {}. Boolean from store.delete(): {}", String::from_utf8_lossy(&key), current_op_tx_id.0, deleted);

        if deleted {
            eprintln!("[QE::handle_delete] Key: '{}', OpTxID: {}. 'if deleted' block entered (store reported true).", String::from_utf8_lossy(&key), current_op_tx_id.0);
            if let Some(value_bytes) = value_to_delete_opt {
                 eprintln!("[QE::handle_delete] Key: '{}', OpTxID: {}. 'if let Some(value_bytes)' block entered for index/undo.", String::from_utf8_lossy(&key), current_op_tx_id.0);
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
        } else {
            eprintln!("[QE::handle_delete] Key: '{}', OpTxID: {}. 'if deleted' block NOT entered (store reported false).", String::from_utf8_lossy(&key), current_op_tx_id.0);
        }
        // key variable might have been moved, so not logging it here.
        eprintln!("[QE::handle_delete] OpTxID: {}. About to return ExecutionResult::Deleted({})", current_op_tx_id.0, deleted);
        Ok(ExecutionResult::Deleted(deleted))
    }

    #[allow(unused_variables)] // Remove when implemented
    pub(crate) fn handle_sql_delete(
        &mut self,
        table_name: String,
        condition: Option<crate::core::query::commands::SqlCondition>,
    ) -> Result<ExecutionResult, OxidbError> {
        let current_op_tx_id = self
            .transaction_manager
            .current_active_transaction_id()
            .unwrap_or(TransactionId(0)); // Default to 0 for auto-commit

        let is_auto_commit = current_op_tx_id == TransactionId(0);

        if is_auto_commit {
            // For auto-commit, we'd ideally start a transaction here if the operation was complex
            // and needed to be atomic beyond a single key-value store operation.
            // However, individual operations in DeleteOperator will use tx_id 0.
            // The main concern for auto-commit is logging a final commit record to WAL.
        }

        // 1. Construct AST (already done by parser, effectively passed in as table_name & condition)
        // We need to create an ast::DeleteStatement to build the initial plan.
        // This is a bit of a workaround as the executor typically gets Commands, not raw parts to rebuild an AST.
        // This suggests the planning/optimization pipeline might need to start earlier, from the Command itself.
        // For now, reconstruct a minimal AST for the optimizer.

        let ast_condition = if let Some(cond) = condition {
            Some(crate::core::query::sql::ast::Condition {
                column: cond.column,
                operator: cond.operator,
                value: crate::core::query::sql::translator::translate_datatype_to_ast_literal(&cond.value)?,
            })
        } else {
            None
        };

        let ast_delete_stmt = crate::core::query::sql::ast::Statement::Delete(
            crate::core::query::sql::ast::DeleteStatement {
                table_name: table_name.clone(), // Optimizer expects String
                condition: ast_condition,
            },
        );

        // 2. Build Logical Plan
        let logical_plan = self.optimizer.build_initial_plan(&ast_delete_stmt)?;

        // 3. Build Physical Plan (Execution Tree)
        // `snapshot_id` for build_execution_tree is the current transaction_id for visibility rules.
        let committed_ids_snapshot = Arc::new(
            self.transaction_manager
                .get_committed_tx_ids_snapshot()
                .into_iter()
                .map(|tx_id| tx_id.0)
                .collect(),
        );
        let mut physical_plan_root = self.build_execution_tree(
            logical_plan,
            current_op_tx_id.0,
            committed_ids_snapshot,
        )?;

        // 4. Execute the plan
        let mut deleted_count = 0;
        // The DeleteOperator is designed to do all work in its first `next()` call
        // and then return a summary.
        // physical_plan_root is Box<dyn ExecutionOperator>, call execute() to get iterator
        let mut result_iterator = physical_plan_root.execute()?;
        if let Some(result_tuple_res) = result_iterator.next() {
            let result_tuple = result_tuple_res?; // Handle potential error from iterator item
            if let Some(DataType::Integer(count)) = result_tuple.get(0) { // count is &i64
                deleted_count = *count as usize; // Dereference count
            } else {
                return Err(OxidbError::Execution(
                    "DeleteOperator did not return a count.".to_string(),
                ));
            }
        }

        // 5. Handle Auto-Commit for physical WAL
        if is_auto_commit {
            // If this was an auto-commit operation (tx_id 0), we need to ensure
            // that the underlying SimpleFileKvStore, if it buffers WAL entries internally
            // before a commit marker, gets a commit signal.
            // SimpleFileKvStore's delete() logs WalEntry::Delete with tx_id 0.
            // It does not automatically log a TransactionCommit for tx_id 0.
            // The TransactionManager handles logical Begin/Commit/Rollback for its own WAL.
            // For physical WAL consistency with tx_id 0, a commit marker might be needed
            // if the store batches. However, SimpleFileKvStore's WalWriter in default config
            // flushes on each entry if no transaction is active or buffer limits are hit.
            // Let's assume for now that individual WAL entries from DeleteOperator are flushed.
            // A dedicated store.log_wal_entry(WalEntry::TransactionCommit{lsn, tx_id:0}) would be cleaner.
            // This part is tricky without a direct store.commit(tx_id) or store.log_control_wal_entry().
            // The test `test_physical_wal_lsn_integration` will verify if LSNs are okay.
            // For now, we rely on the DeleteOperator's individual WAL writes.
            // The logical TransactionManager is not involved for auto-commit tx_id 0 ops.

            // Log a physical TransactionCommit for auto-commit scenario
            let commit_lsn = self.log_manager.next_lsn();
            self.store.write().unwrap().log_wal_entry(&crate::core::storage::engine::wal::WalEntry::TransactionCommit {
                lsn: commit_lsn,
                transaction_id: current_op_tx_id.0, // Should be 0 if is_auto_commit
            })?;
            // Note: TransactionManager is not involved for auto-commit's logical state,
            // but we've logged a physical commit marker.
        }

        Ok(ExecutionResult::Updated { count: deleted_count })
    }

    // handle_find_by_index, handle_vacuum - these are in ddl_handlers.rs and transaction_handlers.rs respectively.
    // handle_select, handle_update - these are in select_execution.rs and update_execution.rs respectively.
}
