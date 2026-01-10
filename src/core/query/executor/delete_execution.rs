// src/core/query/executor/delete_execution.rs

use super::{ExecutionResult, QueryExecutor};
use crate::core::common::types::TransactionId;
use crate::core::common::OxidbError;
use crate::core::execution::operators::delete::DeleteOperator;
use crate::core::execution::ExecutionOperator;
// use crate::core::query::commands::Key; // Unused import
use crate::core::storage::engine::traits::KeyValueStore;
use crate::core::transaction::UndoOperation;
use crate::core::types::DataType;
use std::collections::HashSet;
use std::sync::Arc;

impl<S: KeyValueStore<Vec<u8>, Vec<u8>> + Send + Sync + 'static> QueryExecutor<S> {
    /// Handles a DELETE command.
    /// This involves:
    /// 1. Planning and executing a SELECT-like sub-query to find rows matching the condition.
    /// 2. For each matching row, executing a DeleteOperator.
    /// 3. Managing locks, indexes, and WAL entries.
    pub(crate) fn handle_delete(
        &mut self,
        table_name: String,
        condition_opt: Option<crate::core::query::commands::SqlConditionTree>,
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
                Some(sql_cond_tree) => Some(
                    super::select_execution::command_condition_tree_to_ast_condition_tree(
                        sql_cond_tree,
                        self,
                    )?,
                ),
                None => None,
            };

        let select_ast = crate::core::query::sql::ast::Statement::Select(
            crate::core::query::sql::ast::SelectStatement {
                columns: vec![crate::core::query::sql::ast::SelectColumn::Asterisk],
                from_clause: crate::core::query::sql::ast::TableReference {
                    name: table_name.clone(),
                    alias: None,
                },
                joins: Vec::new(),
                condition: ast_condition_tree_opt,
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
            },
        );

        let initial_select_plan = self.optimizer.build_initial_plan(&select_ast)?;
        let optimized_select_plan = self
            .optimizer
            .optimize_with_indexes(initial_select_plan, &self.index_manager)?;

        let select_execution_tree = self.build_execution_tree(
            optimized_select_plan,
            plan_snapshot_id.0, // Pass u64
            plan_committed_ids_u64_set.clone(),
        )?;

        // Get the table schema to find the primary key column
        let schema_arc = self
            .get_table_schema(&table_name)?
            .ok_or_else(|| OxidbError::TableNotFound(table_name.clone()))?;
        let schema = schema_arc.clone();

        // Find the primary key column index
        let pk_index = schema
            .columns
            .iter()
            .position(|col| col.is_primary_key)
            .ok_or_else(|| OxidbError::Internal("Table has no primary key column".to_string()))?;

        // Determine transaction ID for the delete operation
        let current_op_tx_id = self
            .transaction_manager
            .current_active_transaction_id()
            .unwrap_or(TransactionId(0));

        let is_auto_commit =
            self.transaction_manager.get_active_transaction().is_none() && current_op_tx_id.0 == 0;

        // Create the DeleteOperator
        let mut delete_operator = DeleteOperator::new(
            select_execution_tree,
            table_name.clone(),
            self.store.clone(),
            self.log_manager.clone(),
            current_op_tx_id,
            pk_index,
            plan_committed_ids_u64_set,
            schema.clone(),
        );

        // Execute the delete operation
        let mut deleted_count = 0;
        let delete_iterator = delete_operator.execute()?;

        for result_tuple in delete_iterator {
            let tuple = result_tuple?;
            // Tuple format from DeleteOperator: [DataType::RawBytes(pk), DataType::RawBytes(row_data)]
            if tuple.len() != 2 {
                return Err(OxidbError::Execution(format!(
                    "Unexpected tuple length from DeleteOperator: {}",
                    tuple.len()
                )));
            }

            let pk_bytes = match &tuple[0] {
                DataType::RawBytes(b) => b.clone(),
                _ => {
                    return Err(OxidbError::Execution(
                        "Expected RawBytes for PK in DeleteOperator result".to_string(),
                    ))
                }
            };

            let row_data_bytes = match &tuple[1] {
                DataType::RawBytes(b) => b.clone(),
                _ => {
                    return Err(OxidbError::Execution(
                        "Expected RawBytes for row data in DeleteOperator result".to_string(),
                    ))
                }
            };

            // Deserialize row data to access columns for index updates
            let row_data = crate::core::common::serialization::deserialize_data_type(&row_data_bytes)?;
            let row_map = match row_data {
                DataType::Map(crate::core::types::JsonSafeMap(map)) => map,
                _ => {
                    return Err(OxidbError::Execution(
                        "Expected Map for row data in DeleteOperator result".to_string(),
                    ))
                }
            };

            // --- Start: Update indexes and add Undo Logs ---
            // 1. RevertInsert for the main row deletion (so we can re-insert it on rollback)
            if !is_auto_commit {
                if let Some(active_tx_mut) = self.transaction_manager.get_active_transaction_mut() {
                    active_tx_mut.add_undo_operation(UndoOperation::RevertDelete {
                        key: pk_bytes.clone(),
                        old_value: row_data_bytes.clone(),
                    });
                }
            }

            // 2. Remove from indexes
            for col_def in &schema.columns {
                if col_def.is_primary_key || col_def.is_unique {
                    let value_for_column =
                        row_map.get(col_def.name.as_bytes()).cloned().unwrap_or(DataType::Null);

                    if value_for_column == DataType::Null && !col_def.is_primary_key {
                        continue;
                    }

                    let index_name = format!("idx_{}_{}", table_name, col_def.name);
                    let serialized_column_value =
                        crate::core::common::serialization::serialize_data_type(&value_for_column)?;

                    // Delete from index
                    self.index_manager
                        .write()
                        .map_err(|e| {
                            OxidbError::LockTimeout(format!(
                                "Failed to acquire write lock on index manager for delete: {e}"
                            ))
                        })?
                        .delete_from_index(
                            &index_name,
                            &serialized_column_value,
                            Some(&pk_bytes),
                        )?;

                    // Add Undo Log: IndexRevertDelete -> We need to INSERT back into index on rollback
                    if !is_auto_commit {
                        if let Some(active_tx_mut) =
                            self.transaction_manager.get_active_transaction_mut()
                        {
                            active_tx_mut.add_undo_operation(UndoOperation::IndexRevertDelete {
                                index_name,
                                key: pk_bytes.clone(),
                                old_value_for_index: serialized_column_value,
                            });
                        }
                    }
                }
            }
            // --- End: Update indexes ---

            deleted_count += 1;
        }

        Ok(ExecutionResult::Updated { count: deleted_count })
    }
}
