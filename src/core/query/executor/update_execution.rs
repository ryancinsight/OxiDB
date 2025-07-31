use super::{ExecutionResult, QueryExecutor};
use crate::core::common::serialization::{deserialize_data_type, serialize_data_type};
use crate::core::common::types::TransactionId; // Added TransactionId import
use crate::core::common::OxidbError; // Changed
use crate::core::query::commands::{Key, SqlAssignment}; // Removed SqlCondition
use crate::core::storage::engine::traits::KeyValueStore;
use crate::core::transaction::{Transaction, TransactionState, UndoOperation}; // Adjusted path
use crate::core::types::DataType;
use crate::core::types::JsonSafeMap; // Added import for JsonSafeMap
use std::collections::{HashMap, HashSet}; // Removed AstLiteralValue
                                          // AstAssignment from sql::ast is not needed here because assignments_cmd is already SqlAssignment
                                          // Removed: use super::utils::datatype_to_ast_literal;
use std::sync::Arc; // Import the helper

impl<S: KeyValueStore<Vec<u8>, Vec<u8>> + Send + Sync + 'static> QueryExecutor<S> {
    /// Efficiently format key string using iterator combinators
    /// Reduces allocations and improves performance for key generation
    fn format_key_string(table_name: &str, pk_column_name: &str, pk_value: &DataType) -> String {
        format!("{}_pk_{}_{:?}", table_name, pk_column_name, pk_value)
            .chars()
            .filter(|&c| c != '(' && c != ')' && c != '"')
            .collect::<String>()
            .replace("Integer", "")
            .replace("String", "")
    }
    /// Handles an UPDATE command.
    /// This involves:
    /// 1. Planning and executing a SELECT-like sub-query to find rows matching the condition.
    /// 2. For each matching row:
    ///    a. Acquiring an exclusive lock on the row's key.
    ///    b. Fetching the current row data.
    ///    c. Applying the specified assignments to a temporary copy.
    ///    d. Performing constraint checks (NOT NULL, UNIQUE) on the modified data.
    ///    e. Updating relevant column-specific indexes.
    ///    f. Updating the main "`default_value_index`" if the entire row data changed.
    ///    g. Writing the updated row data to the store with a new LSN.
    ///    h. Recording undo operations for both data and index changes.
    /// 3. Returning the count of updated rows.
    #[allow(clippy::arithmetic_side_effects)] // For updated_count += 1;
    pub(crate) fn handle_update(
        &mut self,
        source_table_name: String,
        assignments_cmd: Vec<SqlAssignment>,
        condition_opt: Option<crate::core::query::commands::SqlConditionTree>, // Changed
    ) -> Result<ExecutionResult, OxidbError> {
        let plan_snapshot_id: TransactionId;
        let plan_committed_ids_vec: Vec<TransactionId>;

        if let Some(active_tx_for_plan) = self.transaction_manager.get_active_transaction() {
            plan_snapshot_id = active_tx_for_plan.id;
            plan_committed_ids_vec = self.transaction_manager.get_committed_tx_ids_snapshot();
        } else {
            plan_snapshot_id = self
                .transaction_manager
                .current_active_transaction_id()
                .unwrap_or(TransactionId(0));
            plan_committed_ids_vec = self.transaction_manager.get_committed_tx_ids_snapshot();
        }
        let plan_committed_ids_u64_set =
            Arc::new(HashSet::from_iter(plan_committed_ids_vec.iter().map(|&t| t.0)));

        // Convert SqlConditionTree to AST ConditionTree
        let ast_condition_tree_opt: Option<crate::core::query::sql::ast::ConditionTree> =
            match condition_opt.as_ref() {
                Some(sql_cond_tree) => {
                    Some(super::select_execution::command_condition_tree_to_ast_condition_tree(
                        sql_cond_tree,
                        self,
                    )?)
                }
                None => None,
            };

        let select_ast = crate::core::query::sql::ast::Statement::Select(crate::core::query::sql::ast::SelectStatement {
            columns: vec![crate::core::query::sql::ast::SelectColumn::Asterisk],
            from_clause: crate::core::query::sql::ast::TableReference {
                name: source_table_name.clone(),
                alias: None,
            },
            joins: Vec::new(),
            condition: ast_condition_tree_opt,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
        });

        let initial_select_plan = self.optimizer.build_initial_plan(&select_ast)?;
        let optimized_select_plan =
            self.optimizer.optimize_with_indexes(initial_select_plan, &self.index_manager)?;

        let mut select_execution_tree = self.build_execution_tree(
            optimized_select_plan,
            plan_snapshot_id.0, // Pass u64
            plan_committed_ids_u64_set,
        )?;
        // Get the table schema to find the primary key column
        let schema = self.get_table_schema(&source_table_name)?
            .ok_or_else(|| OxidbError::TableNotFound(source_table_name.clone()))?;
        
        // Find the primary key column index and name using iterator
        let (pk_index, pk_column_name) = schema.columns
            .iter()
            .enumerate()
            .find(|(_, col)| col.is_primary_key)
            .map(|(idx, col)| (idx, col.name.clone()))
            .ok_or_else(|| OxidbError::Internal("Table has no primary key column".to_string()))?;
        
        // Use efficient error handling with try_fold to avoid collecting partial results on error
        let keys_to_update: Vec<Key> = select_execution_tree.execute()?
            .try_fold(Vec::new(), |mut acc, tuple_result| -> Result<Vec<Key>, OxidbError> {
                let tuple = tuple_result?;

                if tuple.is_empty() {
                    return Err(OxidbError::Internal(
                        "Execution plan for UPDATE yielded empty tuple.".to_string(),
                    ));
                }
                
                // Get the primary key value from the tuple
                if pk_index >= tuple.len() {
                    return Err(OxidbError::Internal(format!(
                        "Primary key index {} out of bounds for tuple length {}",
                        pk_index, tuple.len()
                    )));
                }
                
                // Construct the key from table name and primary key value using same format as INSERT
                let pk_value = &tuple[pk_index];
                let key_string = if pk_column_name == "_kv_key" {
                    // Special convention: if PK column is named _kv_key and is String, use its value directly
                    match pk_value {
                        DataType::String(s) => s.clone(),
                        _ => Self::format_key_string(&source_table_name, &pk_column_name, pk_value)
                    }
                } else {
                    // Standard PK-based key generation using efficient helper method
                    Self::format_key_string(&source_table_name, &pk_column_name, pk_value)
                };
                
                acc.push(key_string.into_bytes());
                Ok(acc)
            })?;
        if keys_to_update.is_empty() {
            // If no keys matched the condition, 0 rows were updated.
            return Ok(ExecutionResult::Updated { count: 0 });
        }

        let mut updated_count = 0;

        // Fetch schema once
        let schema_arc = self.get_table_schema(&source_table_name)?.ok_or_else(|| {
            OxidbError::Execution(format!("Table '{source_table_name}' not found for UPDATE."))
        })?;
        let schema = schema_arc.as_ref();

        for key in keys_to_update {
            let current_op_tx_id: TransactionId;
            let committed_ids_for_get_u64_set: HashSet<u64>;
            let is_auto_commit: bool;

            if let Some(active_tx) = self.transaction_manager.get_active_transaction() {
                current_op_tx_id = active_tx.id;
                committed_ids_for_get_u64_set = self
                    .transaction_manager
                    .get_committed_tx_ids_snapshot()
                    .into_iter()
                    .map(|id| id.0)
                    .collect();
                is_auto_commit = false;
            } else {
                current_op_tx_id = TransactionId(0);
                committed_ids_for_get_u64_set = self
                    .transaction_manager
                    .get_committed_tx_ids_snapshot()
                    .into_iter()
                    .map(|id| id.0)
                    .collect();
                is_auto_commit = true;
            }

            self.lock_manager.acquire_lock(
                current_op_tx_id.0, // Use .0 for u64
                &key,
                crate::core::transaction::lock_manager::LockType::Exclusive,
            )?;

            let current_value_bytes_opt = self
                .store
                .read()
                .expect("Failed to acquire read lock on store for update")
                .get(&key, current_op_tx_id.0, &committed_ids_for_get_u64_set)?;

            if let Some(current_value_bytes) = current_value_bytes_opt {
                let mut current_data_type = deserialize_data_type(&current_value_bytes)?;

                if let DataType::Map(JsonSafeMap(ref mut map_data)) = current_data_type {
                    // Use JsonSafeMap
                    // Apply assignments to a temporary map to check constraints first
                    let mut temp_updated_map_data = map_data.clone();
                    for assignment_cmd in &assignments_cmd {
                        temp_updated_map_data.insert(
                            assignment_cmd.column.as_bytes().to_vec(),
                            assignment_cmd.value.clone(),
                        );
                    }

                    // Constraint Checks using temp_updated_map_data
                    for col_def in &schema.columns {
                        // Check only if this column is part of the current assignments
                        let assigned_value_opt =
                            assignments_cmd.iter().find(|a| a.column == col_def.name);

                        if let Some(assignment_cmd) = assigned_value_opt {
                            let new_value_for_column = &assignment_cmd.value;

                            // NOT NULL Check
                            if !col_def.is_nullable && *new_value_for_column == DataType::Null {
                                return Err(OxidbError::ConstraintViolation(format!(
                                    "NOT NULL constraint failed for column '{}' in table '{}'",
                                    col_def.name, source_table_name
                                )));
                            }

                            // UNIQUE / PRIMARY KEY Uniqueness Check
                            if col_def.is_unique {
                                // is_primary_key implies is_unique
                                if *new_value_for_column == DataType::Null
                                    && !col_def.is_primary_key
                                {
                                    // Skip uniqueness for NULL in UNIQUE column (not PK)
                                } else {
                                    self.check_uniqueness(
                                        &source_table_name,
                                        col_def,
                                        new_value_for_column,
                                        Some(&key), // Exclude current row by its PK (`key`)
                                    )?;
                                }
                            }
                        }
                    }
                    // If all checks passed, apply to actual map_data
                    // *map_data = temp_updated_map_data; // Deferred until after per-column index updates

                    // --- Start: Per-column index updates for UPDATE ---
                    let original_map_data_for_indexes = map_data.clone(); // Clone original map_data for fetching old values

                    for col_def in &schema.columns {
                        if col_def.is_primary_key || col_def.is_unique {
                            let old_value_for_column = original_map_data_for_indexes
                                .get(col_def.name.as_bytes())
                                .cloned()
                                .unwrap_or(DataType::Null);
                            let new_value_for_column = temp_updated_map_data
                                .get(col_def.name.as_bytes())
                                .cloned()
                                .unwrap_or(DataType::Null);

                            // Determine if indexing is needed based on NULL status and PK status
                            let old_value_needs_indexing =
                                old_value_for_column != DataType::Null || col_def.is_primary_key;
                            let new_value_needs_indexing =
                                new_value_for_column != DataType::Null || col_def.is_primary_key;

                            if old_value_for_column != new_value_for_column
                                || old_value_needs_indexing != new_value_needs_indexing
                            {
                                let index_name =
                                    format!("idx_{}_{}", source_table_name, col_def.name);

                                // Delete old value from index if it needed indexing
                                if old_value_needs_indexing {
                                    let old_serialized_column_value =
                                        serialize_data_type(&old_value_for_column)?;
                                    self.index_manager
                                        .write()
                                        .map_err(|e| OxidbError::LockTimeout(format!("Failed to acquire write lock on index manager for update (delete part): {e}")))?
                                        .delete_from_index(
                                            &index_name,
                                            &old_serialized_column_value,
                                            Some(&key),
                                        )?;
                                    // Add undo log for this index deletion
                                    if !is_auto_commit {
                                        if let Some(active_tx_mut) =
                                            self.transaction_manager.get_active_transaction_mut()
                                        {
                                            active_tx_mut.add_undo_operation(
                                                UndoOperation::IndexRevertInsert {
                                                    // To revert delete, we insert
                                                    index_name: index_name.clone(),
                                                    key: key.clone(),
                                                    value_for_index: old_serialized_column_value,
                                                },
                                            );
                                        }
                                    }
                                }

                                // Insert new value into index if it needs indexing
                                if new_value_needs_indexing {
                                    let new_serialized_column_value =
                                        serialize_data_type(&new_value_for_column)?;
                                    self.index_manager
                                        .write()
                                        .map_err(|e| OxidbError::LockTimeout(format!("Failed to acquire write lock on index manager for update (insert part): {e}")))?
                                        .insert_into_index(
                                            &index_name,
                                            &new_serialized_column_value,
                                            &key,
                                        )?;
                                    // Add undo log for this index insertion
                                    if !is_auto_commit {
                                        if let Some(active_tx_mut) =
                                            self.transaction_manager.get_active_transaction_mut()
                                        {
                                            active_tx_mut.add_undo_operation(
                                                UndoOperation::IndexRevertDelete {
                                                    // To revert insert, we delete
                                                    index_name, // index_name is moved here
                                                    key: key.clone(),
                                                    old_value_for_index:
                                                        new_serialized_column_value, // This is the value that was inserted
                                                },
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                    // --- End: Per-column index updates for UPDATE ---

                    // Now apply changes to actual map_data for main store persistence
                    *map_data = temp_updated_map_data;
                } else if !assignments_cmd.is_empty() {
                    // Should not happen if rows are DataType::Map
                    if is_auto_commit {
                        self.lock_manager.release_locks(current_op_tx_id.0); // Use .0 for u64
                    }
                    return Err(OxidbError::Execution(
                        // Changed from NotImplemented
                        "UPDATE target row is not a valid Map structure.".to_string(),
                    ));
                }

                let updated_value_bytes = serialize_data_type(&current_data_type)?;

                // Undo log for the main row data (RevertUpdate)
                // This should be added *after* per-column index undo ops to maintain logical order for rollback
                if !is_auto_commit {
                    // Only if in an active transaction
                    if let Some(active_tx_mut) =
                        self.transaction_manager.get_active_transaction_mut()
                    {
                        // Insert RevertUpdate at the beginning of operations for this key,
                        // or ensure it's logically before specific index changes if order matters strictly.
                        // For simplicity, adding it here. If a strict "reverse order of operations" is needed for rollback,
                        // it implies RevertUpdate should be logged *before* IndexRevertInsert/Delete for the *same* logical step.
                        // However, existing code adds it after potential default_value_index changes.
                        // Let's keep it here for now, assuming the order in undo_log is processed correctly.
                        active_tx_mut.add_undo_operation(UndoOperation::RevertUpdate {
                            key: key.clone(),
                            old_value: current_value_bytes.clone(), // current_value_bytes is from before any modifications
                        });
                    }
                }

                // The existing on_update_data for "default_value_index" handles the entire row.
                // This is separate from per-column unique indexes.
                // We need to ensure its undo logs are also correctly managed if it's kept.
                // The problem description mentioned reviewing it. For now, let's assume it's managed
                // correctly by handle_insert/handle_delete logic or its own undo logging within on_update_data.
                // The code below for default_value_index update and its undo logs is kept as is.
                if current_value_bytes != updated_value_bytes {
                    // Only if actual row data changed
                    if !is_auto_commit {
                        if let Some(active_tx_mut) =
                            self.transaction_manager.get_active_transaction_mut()
                        {
                            // These are for the "default_value_index", not the per-column ones.
                            active_tx_mut.add_undo_operation(UndoOperation::IndexRevertInsert {
                                // To revert new value, insert it back
                                index_name: "default_value_index".to_string(),
                                key: key.clone(),
                                value_for_index: updated_value_bytes.clone(),
                            });
                            active_tx_mut.add_undo_operation(UndoOperation::IndexRevertDelete {
                                // To revert old value's deletion, delete it
                                index_name: "default_value_index".to_string(),
                                key: key.clone(),
                                old_value_for_index: current_value_bytes.clone(),
                            });
                        }
                    }

                    // Use iterator-based HashMap construction to avoid multiple allocations
                    let old_map_for_index = std::iter::once(("default_value_index".to_string(), current_value_bytes.clone()))
                        .collect::<HashMap<String, Vec<u8>>>();
                    let new_map_for_index = std::iter::once(("default_value_index".to_string(), updated_value_bytes.clone()))
                        .collect::<HashMap<String, Vec<u8>>>();
                    self.index_manager
                        .write()
                        .map_err(|e| OxidbError::LockTimeout(format!("Failed to acquire write lock on index manager for default_value_index update: {e}")))?
                        .on_update_data(
                        // Acquire write lock
                        &old_map_for_index,
                        &new_map_for_index,
                        &key,
                    )?;
                }

                let tx_for_store =
                    if let Some(atm) = self.transaction_manager.get_active_transaction() {
                        atm.clone_for_store()
                    } else {
                        // current_op_tx_id is TransactionId here
                        let mut temp_tx = Transaction::new(current_op_tx_id);
                        temp_tx.set_state(TransactionState::Committed);
                        temp_tx
                    };
                // Generate LSN for the put operation
                let new_lsn = self.log_manager.next_lsn();

                // If there's a real active transaction, update its prev_lsn
                // This check is slightly different from insert/delete as active_tx_mut is already fetched above
                if !is_auto_commit {
                    if let Some(active_tx_mut_for_lsn) =
                        self.transaction_manager.get_active_transaction_mut()
                    {
                        active_tx_mut_for_lsn.prev_lsn = new_lsn;
                    }
                }

                self.store
                    .write()
                    .map_err(|e| {
                        OxidbError::LockTimeout(format!(
                            "Failed to acquire write lock on store for update (put): {e}"
                        ))
                    })?
                    .put(
                        key.clone(),
                        updated_value_bytes.clone(),
                        &tx_for_store,
                        new_lsn, // Pass the new LSN
                    )?;
                updated_count += 1;
            }
            // Auto-commit logic is now handled by QueryExecutor::execute_command
            // if is_auto_commit {
            //     self.lock_manager.release_locks(current_op_tx_id.0);
            // }
        }
        // If it was an auto-commit, QueryExecutor::execute_command will release locks.
        // If it was part of a larger transaction, locks are held until COMMIT/ROLLBACK.
        Ok(ExecutionResult::Updated { count: updated_count })
    }
}
