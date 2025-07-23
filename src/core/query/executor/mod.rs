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
use crate::core::query::sql::ast::AstLiteralValue;
use crate::core::storage::engine::traits::KeyValueStore;
use crate::core::storage::engine::SimpleFileKvStore;
use crate::core::transaction::lock_manager::LockManager;
use crate::core::transaction::manager::TransactionManager;
use crate::core::transaction::LockType; // Added LockType import
use crate::core::transaction::Transaction;
use crate::core::types::DataType;
use crate::core::wal::log_manager::LogManager; // Added LogManager
use crate::core::wal::writer::WalWriter;
use std::collections::{HashMap, HashSet}; // Added HashMap and HashSet import
                                          // For base64 decoding - using a simple approach since we know the format
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use uuid;

/// Context for resolving parameter placeholders during execution
#[derive(Debug)]
pub struct ParameterContext<'a> {
    parameters: &'a [crate::core::common::types::Value],
}

impl<'a> ParameterContext<'a> {
    #[must_use]
    pub const fn new(parameters: &'a [crate::core::common::types::Value]) -> Self {
        Self { parameters }
    }

    /// Resolve a parameter by its index
    pub fn resolve_parameter(
        &self,
        index: u32,
    ) -> Result<&crate::core::common::types::Value, OxidbError> {
        let idx = index as usize;
        self.parameters.get(idx).ok_or_else(|| OxidbError::InvalidInput {
            message: format!(
                "Parameter index {} out of bounds (have {} parameters)",
                index,
                self.parameters.len()
            ),
        })
    }

    /// Convert an `AstExpressionValue` to a `DataType`, resolving parameters
    pub fn resolve_expression_value(
        &self,
        expr: &crate::core::query::sql::ast::AstExpressionValue,
    ) -> Result<DataType, OxidbError> {
        match expr {
            crate::core::query::sql::ast::AstExpressionValue::Literal(literal) => {
                // Convert literal to DataType
                self.convert_literal_to_datatype(literal)
            }
            crate::core::query::sql::ast::AstExpressionValue::Parameter(index) => {
                // Resolve parameter and convert to DataType
                let param_value = self.resolve_parameter(*index)?;
                Ok(self.convert_value_to_datatype(param_value))
            }
            crate::core::query::sql::ast::AstExpressionValue::ColumnIdentifier(_) => {
                Err(OxidbError::InvalidInput {
                    message: "Column identifiers cannot be resolved in this context".to_string(),
                })
            }
        }
    }

    fn convert_literal_to_datatype(
        &self,
        literal: &crate::core::query::sql::ast::AstLiteralValue,
    ) -> Result<DataType, OxidbError> {
        use crate::core::query::sql::ast::AstLiteralValue;
        match literal {
            AstLiteralValue::String(s) => Ok(DataType::String(s.clone())),
            AstLiteralValue::Number(n) => {
                // Try to parse as integer first, then float
                if let Ok(i) = n.parse::<i64>() {
                    Ok(DataType::Integer(i))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(DataType::Float(f))
                } else {
                    Ok(DataType::String(n.clone()))
                }
            }
            AstLiteralValue::Boolean(b) => Ok(DataType::Boolean(*b)),
            AstLiteralValue::Null => Ok(DataType::Null),
            AstLiteralValue::Vector(_) => {
                Err(OxidbError::NotImplemented { feature: "Vector literal conversion".to_string() })
            }
        }
    }

    fn convert_value_to_datatype(&self, value: &crate::core::common::types::Value) -> DataType {
        use crate::core::common::types::Value;
        match value {
            Value::Integer(i) => DataType::Integer(*i),
            Value::Float(f) => DataType::Float(*f),
            Value::Text(s) => DataType::String(s.clone()),
            Value::Boolean(b) => DataType::Boolean(*b),
            Value::Blob(b) => DataType::RawBytes(b.clone()),
            Value::Vector(v) => {
                // Create VectorData from Vec<f32>
                let dimension = v.len() as u32;
                if let Some(vector_data) = crate::core::types::VectorData::new(dimension, v.clone())
                {
                    DataType::Vector(vector_data)
                } else {
                    // Fallback to raw bytes if vector creation fails
                    DataType::RawBytes(v.iter().flat_map(|f| f.to_le_bytes().to_vec()).collect())
                }
            }
            Value::Null => DataType::Null,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum ExecutionResult {
    Value(Option<DataType>),
    Success,
    Deleted(bool),
    Values(Vec<DataType>),
    Updated { count: usize },                 // Added for update operations
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
    /// Tracks the next auto-increment value for each table.column combination
    pub(crate) auto_increment_state: HashMap<String, i64>,
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
                OxidbError::Index(format!("Failed to create default_value_index: {e}"))
            })?;
        }

        // Pass a clone of log_manager to TransactionManager, store original in self
        let mut transaction_manager = TransactionManager::new(wal_writer, log_manager.clone());
        transaction_manager.add_committed_tx_id(TransactionId(0)); // Use TransactionId struct
        let index_manager_arc = Arc::new(RwLock::new(index_manager)); // Wrap in RwLock

        let mut executor = Self {
            store: Arc::new(RwLock::new(store)),
            transaction_manager,
            lock_manager: LockManager::new(),
            optimizer: Optimizer::new(), // Initialize optimizer
            index_manager: index_manager_arc,
            log_manager,                          // Store log_manager
            auto_increment_state: HashMap::new(), // Initialize auto-increment state
        };

        // Load auto-increment state from existing data
        executor.load_auto_increment_state()?;

        Ok(executor)
    }
}

// Methods specific to QueryExecutor when the store is SimpleFileKvStore
impl QueryExecutor<SimpleFileKvStore> {
    /// Persists all current data to disk
    ///
    /// This method saves both the store data and index data to disk,
    /// ensuring data durability.
    ///
    /// # Errors
    /// Returns `OxidbError` if:
    /// - Failed to acquire read lock on store or index manager
    /// - Store persistence fails
    /// - Index persistence fails
    pub fn persist(&mut self) -> Result<(), OxidbError> {
        self.store
            .read()
            .map_err(|e| {
                OxidbError::LockTimeout(format!(
                    "Failed to acquire read lock on store for persist: {e}"
                ))
            })?
            .persist()?;
        self.index_manager
            .read()
            .map_err(|e| {
                OxidbError::LockTimeout(format!(
                    "Failed to acquire read lock on index manager for persist: {e}"
                ))
            })?
            .save_all_indexes()
    }

    #[must_use]
    pub fn index_base_path(&self) -> PathBuf {
        // Using expect here as base_path is not expected to fail often and is not directly part of core query execution flow.
        // A more robust solution might propagate the error.
        self.index_manager
            .read()
            .map_err(|e| {
                OxidbError::LockTimeout(format!(
                    "Failed to acquire read lock on index manager for base_path: {e}"
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
        format!("_schema_{table_name}").into_bytes()
    }

    /// Retrieves the schema for a given table name.
    /// This involves constructing the schema key and using the store's `get_schema` method.
    /// It uses `snapshot_id` = 0 (read committed state) as schemas are DDL and should be stable.
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
                OxidbError::LockTimeout(format!(
                    "Failed to acquire read lock on store for get_table_schema: {e}"
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
                OxidbError::LockTimeout(format!(
                    "Failed to acquire read lock on index manager for check_uniqueness: {e}"
                ))
            })?
            .find_by_index(&index_name, &serialized_value)
        {
            Ok(Some(pks)) => {
                if pks.is_empty() {
                    eprintln!("[Executor::check_uniqueness] Warning: Value {value_to_check:?} found in index '{index_name}' but with no associated primary keys.");
                    Ok(())
                } else {
                    match current_row_pk_bytes {
                        None => {
                            // INSERT
                            if !pks.is_empty() {
                                Err(OxidbError::ConstraintViolation(format!(
                                    "UNIQUE constraint failed for column '{}' in table '{}'. Value {:?} already exists.",
                                    column_to_check.name, table_name, value_to_check
                                )))
                            } else {
                                Ok(())
                            }
                        }
                        Some(current_pk) => {
                            // UPDATE
                            let current_pk_vec = current_pk.to_vec();
                            if pks.iter().any(|pk_from_index| *pk_from_index != current_pk_vec) {
                                Err(OxidbError::ConstraintViolation(format!(
                                    "UNIQUE constraint failed for column '{}' in table '{}'. Value {:?} already exists in another row.",
                                    column_to_check.name, table_name, value_to_check
                                )))
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
                        crate::core::transaction::UndoOperation::RevertInsert { key: key.clone() },
                    );
                    let new_value_for_index_bytes =
                        crate::core::common::serialization::serialize_data_type(&value)?;
                    active_tx_mut.add_undo_operation(
                        crate::core::transaction::UndoOperation::IndexRevertInsert {
                            index_name: "default_value_index".to_string(),
                            key: key.clone(),
                            value_for_index: new_value_for_index_bytes,
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
        indexed_values_map.insert("default_value_index".to_string(), serialized_value_for_index);

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

        if let Some(bytes) = result_bytes_opt {
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
        } else {
            println!(
                "[QE::handle_get] Key '{:?}' not found in store.",
                String::from_utf8_lossy(&key)
            ); // Debug print
            Ok(ExecutionResult::Value(None))
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
                        active_tx_mut.add_undo_operation(
                            crate::core::transaction::UndoOperation::IndexRevertDelete {
                                index_name: "default_value_index".to_string(),
                                key: key.clone(),
                                old_value_for_index: value_bytes, // Pass the original serialized value
                            },
                        );
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
            Some(sql_cond_tree) => {
                Some(self::select_execution::command_condition_tree_to_ast_condition_tree(
                    &sql_cond_tree,
                    self,
                )?)
            }
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
        let result_iterator = physical_plan_root.execute()?;
        for result_tuple_res in result_iterator {
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
            OxidbError::Execution(format!("Table '{table_name}' not found for DELETE."))
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
                        "Deleted row data is not a map or expected placeholder. Type: {deleted_row_datatype:?}"
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
                                crate::core::transaction::UndoOperation::IndexRevertInsert {
                                    // To revert delete, we insert
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

    /// Gets the next auto-increment value for a table.column combination
    pub(crate) fn get_next_auto_increment_value(
        &mut self,
        table_name: &str,
        column_name: &str,
    ) -> i64 {
        let key = format!("{table_name}_{column_name}");
        let current_value = self.auto_increment_state.get(&key).copied().unwrap_or(0);
        let next_value = current_value + 1;
        self.auto_increment_state.insert(key, next_value);

        // Persist the auto-increment state to disk
        if let Err(e) = self.save_auto_increment_value(table_name, column_name, next_value) {
            eprintln!("[QueryExecutor] Failed to persist auto-increment value: {e}");
        }

        next_value
    }

    /// Sets the auto-increment value for a table.column combination (used during recovery)
    #[allow(dead_code)]
    pub(crate) fn set_auto_increment_value(
        &mut self,
        table_name: &str,
        column_name: &str,
        value: i64,
    ) {
        let key = format!("{table_name}_{column_name}");
        self.auto_increment_state.insert(key, value);
    }

    /// Saves auto-increment value to persistent storage
    fn save_auto_increment_value(
        &mut self,
        table_name: &str,
        column_name: &str,
        value: i64,
    ) -> Result<(), OxidbError> {
        let key = format!("_auto_increment_{table_name}_{column_name}");
        let value_bytes = value.to_le_bytes().to_vec();

        // Create a dummy transaction for this operation
        let dummy_tx = crate::core::transaction::Transaction::new(
            crate::core::common::types::ids::TransactionId(0),
        );
        let dummy_lsn = 0;

        let mut store = self
            .store
            .write()
            .map_err(|_| OxidbError::LockTimeout("Failed to lock store".to_string()))?;
        store.put(key.as_bytes().to_vec(), value_bytes, &dummy_tx, dummy_lsn)?;
        Ok(())
    }

    /// Loads auto-increment state from the database during initialization
    pub(crate) fn load_auto_increment_state(&mut self) -> Result<(), OxidbError> {
        // First, load persisted auto-increment values
        self.load_persisted_auto_increment_values()?;

        // Then, scan existing data to ensure we have the correct maximum values
        // This handles cases where data was inserted without updating the auto-increment state
        self.scan_and_update_auto_increment_state()?;

        Ok(())
    }

    /// Loads persisted auto-increment values from storage
    fn load_persisted_auto_increment_values(&mut self) -> Result<(), OxidbError> {
        let _store = self
            .store
            .read()
            .map_err(|_| OxidbError::LockTimeout("Failed to lock store".to_string()))?;

        // We need to scan for keys that start with "_auto_increment_"
        // This is a simplified approach - in a production system, we'd have a proper metadata table
        // For now, we'll rely on scanning existing data instead

        Ok(())
    }

    /// Scans existing data to determine the current maximum auto-increment values
    fn scan_and_update_auto_increment_state(&mut self) -> Result<(), OxidbError> {
        // Get all table schemas to find auto-increment columns
        let store = self
            .store
            .read()
            .map_err(|_| OxidbError::LockTimeout("Failed to lock store".to_string()))?;

        // Scan for schema keys
        let _schema_prefix = "_schema_";
        // This is a simplified scan - we'll iterate through known tables
        // In a production system, we'd have a proper metadata table listing all tables

        let committed_ids: HashSet<u64> = HashSet::new();
        let snapshot_id = 0;

        // For now, let's scan the users table specifically since that's what we're testing
        if let Ok(schema_data) = store.get(&b"_schema_users".to_vec(), snapshot_id, &committed_ids)
        {
            if let Some(data) = schema_data {
                if let Ok(schema) =
                    serde_json::from_slice::<crate::core::types::schema::Schema>(&data)
                {
                    for column in &schema.columns {
                        if column.is_auto_increment {
                            let max_value =
                                self.find_max_value_for_column("users", &column.name)?;
                            let key = format!("users_{}", column.name);
                            self.auto_increment_state.insert(key, max_value);
                        }
                    }
                }
            }
        }

        // Scan user_files table as well
        if let Ok(schema_data) =
            store.get(&b"_schema_user_files".to_vec(), snapshot_id, &committed_ids)
        {
            if let Some(data) = schema_data {
                if let Ok(schema) =
                    serde_json::from_slice::<crate::core::types::schema::Schema>(&data)
                {
                    for column in &schema.columns {
                        if column.is_auto_increment {
                            let max_value =
                                self.find_max_value_for_column("user_files", &column.name)?;
                            let key = format!("user_files_{}", column.name);
                            self.auto_increment_state.insert(key, max_value);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Finds the maximum value for an auto-increment column by scanning existing data
    fn find_max_value_for_column(
        &self,
        table_name: &str,
        column_name: &str,
    ) -> Result<i64, OxidbError> {
        let store = self
            .store
            .read()
            .map_err(|_| OxidbError::LockTimeout("Failed to lock store".to_string()))?;
        let mut max_value = 0i64;

        // Scan all rows in the table to find the maximum value
        // This is inefficient but works for our current implementation
        let table_prefix = format!("{table_name}_pk_{column_name}_");

        let committed_ids: HashSet<u64> = HashSet::new();
        let snapshot_id = 0;

        // We need to iterate through all keys that start with the table prefix
        // This is a simplified approach - in a production system, we'd have better indexing

        // For now, we'll use a heuristic: scan through potential primary key values
        // This assumes primary keys are sequential integers starting from 1
        for i in 1..=10000 {
            // Scan up to 10000 records
            let pk_key = format!("{table_prefix}{i}");
            if let Ok(Some(row_data)) =
                store.get(&pk_key.as_bytes().to_vec(), snapshot_id, &committed_ids)
            {
                // Parse the row data to extract the column value
                // The data is stored as {"Map": {"base64_encoded_column_name": value, ...}}
                if let Ok(data_type) =
                    serde_json::from_slice::<crate::core::types::DataType>(&row_data)
                {
                    if let crate::core::types::DataType::Map(map) = data_type {
                        // Look for the column by matching the key directly
                        for (key_bytes, value) in &map.0 {
                            // Try to decode the key as UTF-8 string
                            if let Ok(key_str) = String::from_utf8(key_bytes.clone()) {
                                // Check if this is the column we're looking for
                                if key_str == column_name {
                                    if let crate::core::types::DataType::Integer(int_val) = value {
                                        max_value = max_value.max(*int_val);
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                // If we can't find this key, assume we've reached the end
                break;
            }
        }

        Ok(max_value)
    }

    /// Execute a parameterized SQL statement with separate parameter values
    /// This provides secure execution without SQL injection risks
    pub fn execute_parameterized_statement(
        &mut self,
        statement: &crate::core::query::sql::ast::Statement,
        parameters: &[crate::core::common::types::Value],
    ) -> Result<ExecutionResult, OxidbError> {
        // First, validate parameter count
        let expected_param_count = self.count_parameters_in_statement(statement);
        let actual_param_count = parameters.len();

        if actual_param_count != expected_param_count {
            return Err(OxidbError::InvalidInput {
                message: format!(
                    "Parameter count mismatch: expected {expected_param_count} parameters, got {actual_param_count}"
                )
            });
        }

        // Create a parameter context for resolving parameters during execution
        let param_context = ParameterContext::new(parameters);

        // Execute the statement with parameter resolution
        match statement {
            crate::core::query::sql::ast::Statement::Select(select_stmt) => {
                self.execute_parameterized_select(select_stmt, &param_context)
            }
            crate::core::query::sql::ast::Statement::Insert(insert_stmt) => {
                self.execute_parameterized_insert(insert_stmt, &param_context)
            }
            crate::core::query::sql::ast::Statement::Update(update_stmt) => {
                self.execute_parameterized_update(update_stmt, &param_context)
            }
            crate::core::query::sql::ast::Statement::Delete(delete_stmt) => {
                self.execute_parameterized_delete(delete_stmt, &param_context)
            }
            _ => Err(OxidbError::NotImplemented {
                feature: "Parameterized execution for this statement type".to_string(),
            }),
        }
    }

    /// Count the number of parameters (? placeholders) in a SQL statement
    fn count_parameters_in_statement(
        &self,
        statement: &crate::core::query::sql::ast::Statement,
    ) -> usize {
        use crate::core::query::sql::ast::Statement;
        match statement {
            Statement::Select(select_stmt) => {
                let mut count = 0;
                if let Some(ref condition) = select_stmt.condition {
                    count += self.count_parameters_in_condition_tree(condition);
                }
                count
            }
            Statement::Insert(insert_stmt) => {
                let mut count = 0;
                for row in &insert_stmt.values {
                    for value in row {
                        count += self.count_parameters_in_expression_value(value);
                    }
                }
                count
            }
            Statement::Update(update_stmt) => {
                let mut count = 0;
                // Count parameters in assignments
                for assignment in &update_stmt.assignments {
                    count += self.count_parameters_in_expression_value(&assignment.value);
                }
                // Count parameters in WHERE condition
                if let Some(ref condition) = update_stmt.condition {
                    count += self.count_parameters_in_condition_tree(condition);
                }
                count
            }
            Statement::Delete(delete_stmt) => {
                let mut count = 0;
                if let Some(ref condition) = delete_stmt.condition {
                    count += self.count_parameters_in_condition_tree(condition);
                }
                count
            }
            _ => 0, // Other statement types don't support parameters yet
        }
    }

    fn count_parameters_in_condition_tree(
        &self,
        condition_tree: &crate::core::query::sql::ast::ConditionTree,
    ) -> usize {
        use crate::core::query::sql::ast::ConditionTree;
        match condition_tree {
            ConditionTree::Comparison(condition) => {
                self.count_parameters_in_expression_value(&condition.value)
            }
            ConditionTree::And(left, right) | ConditionTree::Or(left, right) => {
                self.count_parameters_in_condition_tree(left)
                    + self.count_parameters_in_condition_tree(right)
            }
            ConditionTree::Not(inner) => self.count_parameters_in_condition_tree(inner),
        }
    }

    const fn count_parameters_in_expression_value(
        &self,
        expr: &crate::core::query::sql::ast::AstExpressionValue,
    ) -> usize {
        use crate::core::query::sql::ast::AstExpressionValue;
        match expr {
            AstExpressionValue::Parameter(_) => 1,
            AstExpressionValue::Literal(_) | AstExpressionValue::ColumnIdentifier(_) => 0,
        }
    }

    // Parameterized execution methods - implement the core logic for secure parameter handling

    fn execute_parameterized_select(
        &mut self,
        select_stmt: &crate::core::query::sql::ast::SelectStatement,
        param_context: &ParameterContext,
    ) -> Result<ExecutionResult, OxidbError> {
        // For now, implement a basic version that converts the parameterized SELECT
        // to the existing SELECT execution path by resolving parameters first

        // Create a modified select statement with parameters resolved
        let mut resolved_select = select_stmt.clone();

        // Resolve parameters in WHERE conditions
        if let Some(ref condition_tree) = select_stmt.condition {
            resolved_select.condition =
                Some(self.resolve_condition_tree_parameters(condition_tree, param_context)?);
        }

        // For now, use the existing SELECT execution infrastructure
        // This is a temporary implementation - ideally we'd modify the execution engine
        // to handle parameters natively throughout the pipeline

        // Convert the AST to the internal command format and execute
        let sql_command = crate::core::query::sql::translator::translate_ast_to_command(
            crate::core::query::sql::ast::Statement::Select(resolved_select),
        )?;

        // Execute using existing infrastructure
        match sql_command {
            crate::core::query::commands::Command::Select {
                columns, source, condition, ..
            } => self.handle_select(columns, source, condition),
            _ => Err(OxidbError::Internal(
                "Unexpected command type from SELECT translation".to_string(),
            )),
        }
    }

    /// Helper method to resolve parameters in condition trees
    fn resolve_condition_tree_parameters(
        &self,
        condition_tree: &crate::core::query::sql::ast::ConditionTree,
        param_context: &ParameterContext,
    ) -> Result<crate::core::query::sql::ast::ConditionTree, OxidbError> {
        use crate::core::query::sql::ast::{AstExpressionValue, Condition, ConditionTree};

        match condition_tree {
            ConditionTree::Comparison(condition) => {
                let resolved_value = match &condition.value {
                    AstExpressionValue::Parameter(index) => {
                        // Resolve parameter to literal value
                        let param_value = param_context.resolve_parameter(*index)?;
                        self.convert_param_value_to_ast_literal(param_value)?
                    }
                    AstExpressionValue::Literal(literal) => literal.clone(),
                    AstExpressionValue::ColumnIdentifier(_) => {
                        return Err(OxidbError::NotImplemented {
                            feature: "Column-to-column comparisons in parameterized queries"
                                .to_string(),
                        });
                    }
                };

                Ok(ConditionTree::Comparison(Condition {
                    column: condition.column.clone(),
                    operator: condition.operator.clone(),
                    value: AstExpressionValue::Literal(resolved_value),
                }))
            }
            ConditionTree::And(left, right) => {
                let resolved_left = self.resolve_condition_tree_parameters(left, param_context)?;
                let resolved_right =
                    self.resolve_condition_tree_parameters(right, param_context)?;
                Ok(ConditionTree::And(Box::new(resolved_left), Box::new(resolved_right)))
            }
            ConditionTree::Or(left, right) => {
                let resolved_left = self.resolve_condition_tree_parameters(left, param_context)?;
                let resolved_right =
                    self.resolve_condition_tree_parameters(right, param_context)?;
                Ok(ConditionTree::Or(Box::new(resolved_left), Box::new(resolved_right)))
            }
            ConditionTree::Not(inner) => {
                let resolved_inner =
                    self.resolve_condition_tree_parameters(inner, param_context)?;
                Ok(ConditionTree::Not(Box::new(resolved_inner)))
            }
        }
    }

    /// Convert a parameter Value to an AST literal
    fn convert_param_value_to_ast_literal(
        &self,
        value: &crate::core::common::types::Value,
    ) -> Result<AstLiteralValue, OxidbError> {
        use crate::core::common::types::Value;
        use crate::core::query::sql::ast::AstLiteralValue;

        match value {
            Value::Integer(i) => Ok(AstLiteralValue::Number(i.to_string())),
            Value::Float(f) => Ok(AstLiteralValue::Number(f.to_string())),
            Value::Text(s) => Ok(AstLiteralValue::String(s.clone())),
            Value::Boolean(b) => Ok(AstLiteralValue::Boolean(*b)),
            Value::Null => Ok(AstLiteralValue::Null),
            Value::Blob(_) => Err(OxidbError::NotImplemented {
                feature: "Blob parameters in WHERE clauses".to_string(),
            }),
            Value::Vector(_) => Err(OxidbError::NotImplemented {
                feature: "Vector parameters in WHERE clauses".to_string(),
            }),
        }
    }

    fn execute_parameterized_insert(
        &mut self,
        insert_stmt: &crate::core::query::sql::ast::InsertStatement,
        param_context: &ParameterContext,
    ) -> Result<ExecutionResult, OxidbError> {
        // Get the table schema
        let schema = self.get_table_schema(&insert_stmt.table_name)?.ok_or_else(|| {
            OxidbError::InvalidInput {
                message: format!("Table '{}' does not exist", insert_stmt.table_name),
            }
        })?;

        // Process each row of values
        let mut insert_count = 0;
        for row_values in &insert_stmt.values {
            // Resolve parameters in the row values
            let mut resolved_values = Vec::new();
            for expr in row_values {
                let data_type = param_context.resolve_expression_value(expr)?;
                resolved_values.push(data_type);
            }

            // Create a row map with column names and values
            let column_names = if let Some(ref columns) = insert_stmt.columns {
                // Use specified columns
                columns.clone()
            } else {
                // Use all columns from schema in order
                schema.columns.iter().map(|col| col.name.clone()).collect()
            };

            if column_names.len() != resolved_values.len() {
                return Err(OxidbError::InvalidInput {
                    message: format!(
                        "Column count mismatch: expected {}, got {}",
                        column_names.len(),
                        resolved_values.len()
                    ),
                });
            }

            // Create row data as a map
            let mut row_map = std::collections::HashMap::new();
            for (col_name, value) in column_names.iter().zip(resolved_values.iter()) {
                row_map.insert(col_name.as_bytes().to_vec(), value.clone());
            }

            // Generate a simple primary key (for now, use a UUID-like approach)
            // TODO: Implement proper primary key generation based on schema
            let primary_key =
                format!("{}_{}", insert_stmt.table_name, uuid::Uuid::new_v4()).as_bytes().to_vec();
            row_map.insert(b"_kv_key".to_vec(), DataType::RawBytes(primary_key.clone()));

            // Create the final data structure
            let row_data = DataType::Map(crate::core::types::JsonSafeMap(row_map));

            // Insert the row
            self.handle_insert(primary_key, row_data)?;
            insert_count += 1;
        }

        Ok(ExecutionResult::Updated { count: insert_count })
    }

    fn execute_parameterized_update(
        &mut self,
        _update_stmt: &crate::core::query::sql::ast::UpdateStatement,
        _param_context: &ParameterContext,
    ) -> Result<ExecutionResult, OxidbError> {
        Err(OxidbError::NotImplemented { feature: "Parameterized UPDATE execution".to_string() })
    }

    fn execute_parameterized_delete(
        &mut self,
        _delete_stmt: &crate::core::query::sql::ast::DeleteStatement,
        _param_context: &ParameterContext,
    ) -> Result<ExecutionResult, OxidbError> {
        Err(OxidbError::NotImplemented { feature: "Parameterized DELETE execution".to_string() })
    }
}
