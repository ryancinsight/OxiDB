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
    RankedResults(Vec<(f32, Vec<DataType>)>), // For similarity search results (distance, row_data)
}

#[derive(Debug)]
pub struct QueryExecutor<S: KeyValueStore<Vec<u8>, Vec<u8>>> {
    /// The underlying key-value store, wrapped for thread-safe access.
    pub(crate) store: Arc<RwLock<S>>,
    /// Manages transactions, including their state and undo/redo logs.
    pub(crate) transaction_manager: TransactionManager,
    /// Manages locks on data to ensure transaction isolation.
    pub(crate) lock_manager: LockManager,
    /// Manages indexes for efficient data retrieval.
    pub(crate) index_manager: Arc<RwLock<IndexManager>>,
    /// Optimizes query plans for more efficient execution.
    pub(crate) optimizer: Optimizer,
    /// Manages the write-ahead log for durability.
    pub(crate) log_manager: Arc<LogManager>,
}

// UniquenessCheckContext struct definition is removed as part of the revert.

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
        let index_manager_arc = Arc::new(RwLock::new(index_manager)); // Wrap in RwLock

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
        self.store
            .read()
            .map_err(|e| {
                OxidbError::Lock(format!("Failed to acquire read lock on store for persist: {}", e))
            })?
            .persist()?;
        self.index_manager
            .read()
            .map_err(|e| {
                OxidbError::Lock(format!(
                    "Failed to acquire read lock on index manager for persist: {}",
                    e
                ))
            })?
            .save_all_indexes()
    }

    pub fn index_base_path(&self) -> PathBuf {
        // Using expect here as base_path is not expected to fail often and is not directly part of core query execution flow.
        // A more robust solution might propagate the error.
        self.index_manager
            .read()
            .map_err(|e| {
                OxidbError::Lock(format!(
                    "Failed to acquire read lock on index manager for base_path: {}",
                    e
                ))
            })
            .expect("Failed to get lock for index_base_path; this should not happen in normal operation as it's a read lock.")
            .base_path()
    }
}

// Moved DML handlers to the generic QueryExecutor impl block for visibility by command_handlers.rs
impl<S: KeyValueStore<Vec<u8>, Vec<u8>> + Send + Sync + 'static> QueryExecutor<S> {
    // Note: persist() and index_base_path() are specific to SimpleFileKvStore, so they remain in that impl block.

    /// Helper function to construct the key used for storing a table's schema.
    fn schema_key(table_name: &str) -> Vec<u8> {
        format!("_schema_{}", table_name).into_bytes()
    }

    /// Retrieves the schema for a given table name.
    /// This involves constructing the schema key and using the store's get_schema method.
    /// It uses snapshot_id = 0 (read committed state) as schemas are DDL and should be stable.
    pub(crate) fn get_table_schema(
        &self,
        table_name: &str,
    ) -> Result<Option<Arc<crate::core::types::schema::Schema>>, OxidbError> {
        let schema_key = Self::schema_key(table_name);
        let committed_ids: HashSet<u64> = self
            .transaction_manager
            .get_committed_tx_ids_snapshot()
            .into_iter()
            .map(|id| id.0)
            .collect();

        // Use snapshot_id 0 for reading schema, as DDL operations are typically immediately committed
        // and visible, or we want the latest committed version of the schema.
        match self
            .store
            .read()
            .map_err(|e| {
                OxidbError::Lock(format!(
                    "Failed to acquire read lock on store for get_table_schema: {}",
                    e
                ))
            })?
            .get_schema(&schema_key, 0, &committed_ids)?
        {
            Some(schema) => Ok(Some(Arc::new(schema))),
            None => Ok(None),
        }
    }

    /// Checks if a value is unique for a given column, optionally excluding a specific primary key (for UPDATEs).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn check_uniqueness(
        &self,
        table_name: &str,
        column_to_check: &crate::core::types::schema::ColumnDef,
        value_to_check: &DataType,
        current_row_pk_bytes: Option<&[u8]>,
    ) -> Result<(), OxidbError> {
        // 1. Construct the index name
        let index_name = format!("idx_{}_{}", table_name, column_to_check.name);

        // 2. Serialize value_to_check
        let serialized_value =
            crate::core::common::serialization::serialize_data_type(value_to_check)?;

        // 3. Call self.index_manager.find_by_index
        match self
            .index_manager
            .read()
            .map_err(|e| {
                OxidbError::Lock(format!(
                    "Failed to acquire read lock on index manager for check_uniqueness: {}",
                    e
                ))
            })?
            .find_by_index(&index_name, &serialized_value)
        {
            Ok(Some(pks)) => {
                if pks.is_empty() {
                    eprintln!("[Executor::check_uniqueness] Warning: Value {:?} found in index '{}' but with no associated primary keys.", value_to_check, index_name);
                    Ok(())
                } else {
                    match current_row_pk_bytes {
                        None => {
                            // INSERT
                            if !pks.is_empty() {
                                Err(OxidbError::ConstraintViolation {
                                    message: format!(
                                        "UNIQUE constraint failed for column '{}' in table '{}'. Value {:?} already exists.",
                                        column_to_check.name, table_name, value_to_check
                                    ),
                                })
                            } else {
                                Ok(())
                            }
                        }
                        Some(current_pk) => {
                            // UPDATE
                            let current_pk_vec = current_pk.to_vec();
                            if pks.iter().any(|pk_from_index| *pk_from_index != current_pk_vec) {
                                Err(OxidbError::ConstraintViolation {
                                    message: format!(
                                        "UNIQUE constraint failed for column '{}' in table '{}'. Value {:?} already exists in another row.",
                                        column_to_check.name, table_name, value_to_check
                                    ),
                                })
                            } else {
                                Ok(())
                            }
                        }
                    }
                }
            }
            Ok(None) => Ok(()), // Value not found in index, so it's unique.
            Err(OxidbError::Index(msg)) if msg.contains("not found") => {
                eprintln!(
                    "[Executor::check_uniqueness] Error: Index '{}' not found for unique check on table '{}', column '{}'. This might indicate an internal issue or a missing index for a unique column.",
                    index_name, table_name, column_to_check.name
                );
                Err(OxidbError::Internal(format!(
                    "Index '{}' not found during uniqueness check for column '{}' in table '{}'. Unique columns must have an index.",
                    index_name, column_to_check.name, table_name
                )))
            }
            Err(e) => Err(e),
        }
    }

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
                        crate::core::transaction::UndoOperation::RevertUpdate {
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
                        crate::core::transaction::UndoOperation::IndexRevertUpdate {
                            index_name: "default_value_index".to_string(),
                            key: key.clone(),
                            old_value_for_index: old_value_bytes, // Pass the original old_value_bytes
                            new_value_for_index: new_value_for_index_bytes, // This is the new value's serialized form
                        },
                    );
                } else {
                    // Key does not exist, this is a true insert.
                    active_tx_mut.add_undo_operation(
                        crate::core::transaction::UndoOperation::RevertInsert {
                            key: key.clone(),
                        },
                    );
                    let new_value_for_index_bytes =
                        crate::core::common::serialization::serialize_data_type(&value)?;
                    active_tx_mut.add_undo_operation(
                        crate::core::transaction::UndoOperation::IndexRevertInsert {
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

        self.index_manager.write().unwrap().on_insert_data(&indexed_values_map, &key)?; // Acquire write lock

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
                println!(
                    "[QE::handle_get] Bytes before deserializing for key '{:?}': {:?}",
                    String::from_utf8_lossy(&key),
                    bytes
                );
                println!(
                    "[QE::handle_get] Bytes as string for key '{:?}': '{}'",
                    String::from_utf8_lossy(&key),
                    String::from_utf8_lossy(&bytes)
                );
                // Deserialize using the project's standard deserialization
                let value_dt = crate::core::common::serialization::deserialize_data_type(&bytes)?;
                println!(
                    "[QE::handle_get] Deserialized DataType for key '{:?}': {:?}",
                    String::from_utf8_lossy(&key),
                    value_dt
                );
                Ok(ExecutionResult::Value(Some(value_dt)))
            }
            None => {
                println!(
                    "[QE::handle_get] Key '{:?}' not found in store.",
                    String::from_utf8_lossy(&key)
                ); // Debug print
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
        eprintln!(
            "[QE::handle_delete] Key: '{}', OpTxID: {}. value_to_delete_opt.is_some(): {}",
            String::from_utf8_lossy(&key),
            current_op_tx_id.0,
            value_to_delete_opt.is_some()
        );

        // Pass committed_ids_set to the delete operation
        let deleted =
            self.store.write().unwrap().delete(&key, &tx_for_store, new_lsn, &committed_ids_set)?;
        eprintln!(
            "[QE::handle_delete] Key: '{}', OpTxID: {}. Boolean from store.delete(): {}",
            String::from_utf8_lossy(&key),
            current_op_tx_id.0,
            deleted
        );

        if deleted {
            eprintln!("[QE::handle_delete] Key: '{}', OpTxID: {}. 'if deleted' block entered (store reported true).", String::from_utf8_lossy(&key), current_op_tx_id.0);
            if let Some(value_bytes) = value_to_delete_opt {
                eprintln!("[QE::handle_delete] Key: '{}', OpTxID: {}. 'if let Some(value_bytes)' block entered for index/undo.", String::from_utf8_lossy(&key), current_op_tx_id.0);
                // Indexing: Use on_delete_data
                let mut indexed_values_map = std::collections::HashMap::new();
                // Assuming the "default_value_index" indexed the serialized version of the DataType
                indexed_values_map.insert("default_value_index".to_string(), value_bytes.clone()); // Clone for undo log
                self.index_manager.write().unwrap().on_delete_data(&indexed_values_map, &key)?; // Acquire write lock

                // Add to undo log for index if in an active transaction
                if current_op_tx_id != TransactionId(0) {
                    if let Some(active_tx_mut) =
                        self.transaction_manager.get_active_transaction_mut()
                    {
                        // Add UndoOperation for the data itself
                        active_tx_mut.add_undo_operation(
                            crate::core::transaction::UndoOperation::RevertDelete {
                                key: key.clone(),
                                old_value: value_bytes.clone(), // value_bytes is Vec<u8>
                            },
                        );
                        // Add UndoOperation for the index
                        active_tx_mut.add_undo_operation(crate::core::transaction::UndoOperation::IndexRevertDelete {
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
        eprintln!(
            "[QE::handle_delete] OpTxID: {}. About to return ExecutionResult::Deleted({})",
            current_op_tx_id.0, deleted
        );
        Ok(ExecutionResult::Deleted(deleted))
    }

    #[allow(unused_variables)] // Remove when implemented
    pub(crate) fn handle_sql_delete(
        &mut self,
        table_name: String,
        condition: Option<crate::core::query::commands::SqlConditionTree>, // Changed
    ) -> Result<ExecutionResult, OxidbError> {
        let current_op_tx_id =
            self.transaction_manager.current_active_transaction_id().unwrap_or(TransactionId(0)); // Default to 0 for auto-commit

        let is_auto_commit = current_op_tx_id == TransactionId(0);

        // The execute_command wrapper handles starting Tx0 if is_auto_commit is true
        // (based on current_active_transaction_id being None initially).
        // So, current_op_tx_id will be 0 here if execute_command started it.
        // No need to call begin_transaction_with_id(0) again here.

        // 1. Construct AST (already done by parser, effectively passed in as table_name & condition)
        // We need to create an ast::DeleteStatement to build the initial plan.
        // This is a bit of a workaround as the executor typically gets Commands, not raw parts to rebuild an AST.
        // This suggests the planning/optimization pipeline might need to start earlier, from the Command itself.
        // For now, reconstruct a minimal AST for the optimizer.

        let ast_condition_tree = match condition {
            Some(sql_cond_tree) => Some(
                super::select_execution::command_condition_tree_to_ast_condition_tree(
                    &sql_cond_tree,
                    self,
                )?,
            ),
            None => None,
        };

        let ast_delete_stmt = crate::core::query::sql::ast::Statement::Delete(
            crate::core::query::sql::ast::DeleteStatement {
                table_name: table_name.clone(), // Optimizer expects String
                condition: ast_condition_tree,  // Changed
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
        let mut physical_plan_root =
            self.build_execution_tree(logical_plan, current_op_tx_id.0, committed_ids_snapshot)?;

        // 4. Execute the plan
        // The DeleteOperator's iterator now yields (Key, SerializedRowData) tuples.
        let mut deleted_items_info: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut result_iterator = physical_plan_root.execute()?;
        while let Some(result_tuple_res) = result_iterator.next() {
            let tuple = result_tuple_res?; // This tuple is Vec<DataType>
            if tuple.len() == 2 {
                let key_bytes_opt = match &tuple[0] {
                    DataType::RawBytes(b) => Some(b.clone()),
                    _ => None,
                };
                let row_bytes_opt = match &tuple[1] {
                    DataType::RawBytes(b) => Some(b.clone()),
                    _ => None,
                };
                if let (Some(key), Some(row_data)) = (key_bytes_opt, row_bytes_opt) {
                    deleted_items_info.push((key, row_data));
                } else {
                    return Err(OxidbError::Execution(
                        "DeleteOperator returned unexpected tuple format (expected RawBytes, RawBytes).".to_string(),
                    ));
                }
            } else if tuple.len() == 1 && matches!(tuple[0], DataType::Integer(_)) {
                // This case handles the old DeleteOperator that returned a single count.
                // This path should ideally not be taken if DeleteOperator is correctly updated.
                // For now, we'll assume the new format. If this is hit, it means DeleteOperator wasn't updated as expected.
                return Err(OxidbError::Execution(
                    "DeleteOperator returned a count, but expected (Key, SerializedRowData). Operator not updated?".to_string(),
                ));
            } else if !tuple.is_empty() {
                // If it's not empty but not the format we want
                return Err(OxidbError::Execution(format!(
                    "DeleteOperator returned unexpected tuple format with length {}.",
                    tuple.len()
                )));
            }
            // If tuple is empty, iterator is exhausted.
        }

        let deleted_count = deleted_items_info.len();
        let schema_arc = self.get_table_schema(&table_name)?.ok_or_else(|| {
            OxidbError::Execution(format!("Table '{}' not found for DELETE.", table_name))
        })?;
        let schema = schema_arc.as_ref();

        for (key_to_delete, serialized_row_to_delete_vec) in deleted_items_info {
            // Deserialize the row data
            let deleted_row_datatype = crate::core::common::serialization::deserialize_data_type(
                &serialized_row_to_delete_vec,
            )?;
            let deleted_row_map_data = match deleted_row_datatype {
                DataType::Map(map_data) => map_data.0, // JsonSafeMap's inner HashMap
                // If it's not a map, it might be the placeholder serialization from DeleteOperator
                // For now, we'll try to proceed if it's Bytes, assuming it's a single value.
                // This part needs to be robust based on actual placeholder logic if used.
                DataType::RawBytes(bytes) => {
                    // This is a hack. If we got here, it means the placeholder serialization was used.
                    // We can't reconstruct the full map for per-column indexing without schema.
                    // We can only work with `bytes` if it represents the primary key or a known value.
                    // For now, let's log a warning and skip complex indexing for this row.
                    // The `default_value_index` might still be usable.
                    eprintln!("[handle_sql_delete] Warning: Deleted row data was not a map, possibly due to placeholder serialization. Full per-column de-indexing might be skipped.");
                    // Create a dummy map, or handle based on what `bytes` represents.
                    // If `bytes` is the PK, we might not need the map for some operations.
                    // For now, let's use an empty map to avoid crashing, but this is not correct.
                    std::collections::HashMap::new()
                }
                _ => {
                    return Err(OxidbError::Execution(format!(
                        "Deleted row data is not a map or expected placeholder. Type: {:?}",
                        deleted_row_datatype
                    )))
                }
            };

            // Per-column index deletions
            for col_def in &schema.columns {
                if col_def.is_primary_key || col_def.is_unique {
                    let value_for_column = deleted_row_map_data
                        .get(col_def.name.as_bytes())
                        .cloned()
                        .unwrap_or(DataType::Null);

                    if value_for_column == DataType::Null && !col_def.is_primary_key {
                        continue; // Skip de-indexing NULLs for non-PK unique columns
                    }

                    let index_name = format!("idx_{}_{}", table_name, col_def.name);
                    let serialized_column_value =
                        crate::core::common::serialization::serialize_data_type(&value_for_column)?;

                    self.index_manager.write().unwrap().delete_from_index(
                        &index_name,
                        &serialized_column_value,
                        Some(&key_to_delete),
                    )?; // Acquire write lock

                    // Add undo log for this index deletion
                    if !is_auto_commit {
                        if let Some(active_tx_mut) =
                            self.transaction_manager.get_active_transaction_mut()
                        {
                            active_tx_mut.add_undo_operation(
                                    crate::core::transaction::UndoOperation::IndexRevertInsert { // To revert delete, we insert
                                    index_name,
                                    key: key_to_delete.clone(),
                                    value_for_index: serialized_column_value,
                                },
                            );
                        }
                    }
                }
            }

            // Add undo log for the main row data deletion (RevertDelete)
            // This should be done for each actual deleted row.
            // The low-level `self.store.delete` inside DeleteOperator already logged a WAL entry for the physical delete.
            // This undo log is for the logical transaction.
            if !is_auto_commit {
                if let Some(active_tx_mut) = self.transaction_manager.get_active_transaction_mut() {
                    active_tx_mut.add_undo_operation(
                            crate::core::transaction::UndoOperation::RevertDelete {
                            key: key_to_delete.clone(),
                            old_value: serialized_row_to_delete_vec.clone(), // The full serialized row
                        },
                    );
                }
            }

            // The "default_value_index" is more complex.
            // The original `handle_delete` (low-level) handles `default_value_index` and its undo ops.
            // If `DeleteOperator` calls that `handle_delete`, that part is covered.
            // However, `DeleteOperator` currently calls `store.delete` directly.
            // For consistency, `default_value_index` for the entire row should also be handled here.
            if !is_auto_commit {
                // Only if in an active transaction
                if let Some(active_tx_mut) = self.transaction_manager.get_active_transaction_mut() {
                    active_tx_mut.add_undo_operation(
                            crate::core::transaction::UndoOperation::IndexRevertInsert {
                            index_name: "default_value_index".to_string(),
                            key: key_to_delete.clone(),
                            value_for_index: serialized_row_to_delete_vec.clone(),
                        },
                    );
                }
            }
            // Also update the default_value_index itself
            let mut default_index_map: std::collections::HashMap<String, Vec<u8>> =
                std::collections::HashMap::new();
            default_index_map
                .insert("default_value_index".to_string(), serialized_row_to_delete_vec);
            self.index_manager
                .write()
                .unwrap()
                .on_delete_data(&default_index_map, &key_to_delete)?; // Acquire write lock
        }

        // 5. Handle Auto-Commit for physical WAL
        // The execute_command wrapper handles committing Tx0 if is_auto_commit was true for it.
        // No need to call commit_transaction() again here.

        Ok(ExecutionResult::Updated { count: deleted_count })
    }

    // handle_find_by_index, handle_vacuum - these are in ddl_handlers.rs and transaction_handlers.rs respectively.
    // handle_select, handle_update - these are in select_execution.rs and update_execution.rs respectively.
}
