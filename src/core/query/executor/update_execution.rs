use super::{ExecutionResult, QueryExecutor};
use crate::core::common::OxidbError; // Changed
use crate::core::common::serialization::{deserialize_data_type, serialize_data_type};
use crate::core::query::commands::{Key, SqlAssignment, SqlCondition};
use crate::core::common::types::TransactionId; // Added TransactionId import
use crate::core::query::sql::ast::{
    Condition as AstCondition, SelectColumn, Statement as AstStatement,
};
use crate::core::storage::engine::traits::KeyValueStore;
use crate::core::transaction::transaction::{Transaction, TransactionState, UndoOperation};
use crate::core::types::DataType;
use std::collections::{HashMap, HashSet}; // Removed AstLiteralValue
                                          // AstAssignment from sql::ast is not needed here because assignments_cmd is already SqlAssignment
use super::utils::datatype_to_ast_literal;
use std::sync::Arc; // Import the helper

impl<S: KeyValueStore<Vec<u8>, Vec<u8>> + Send + Sync + 'static> QueryExecutor<S> {
    pub(crate) fn handle_update(
        &mut self,
        source_table_name: String,
        assignments_cmd: Vec<SqlAssignment>,
        condition_opt: Option<SqlCondition>,
    ) -> Result<ExecutionResult, OxidbError> {
        let plan_snapshot_id: TransactionId;
        let plan_committed_ids_vec: Vec<TransactionId>;

        if let Some(active_tx_for_plan) = self.transaction_manager.get_active_transaction() {
            plan_snapshot_id = active_tx_for_plan.id;
            plan_committed_ids_vec = self.transaction_manager.get_committed_tx_ids_snapshot();
        } else {
            plan_snapshot_id = self.transaction_manager.current_active_transaction_id().unwrap_or(TransactionId(0));
            plan_committed_ids_vec = self.transaction_manager.get_committed_tx_ids_snapshot();
        }
        let plan_committed_ids_u64_set =
            Arc::new(plan_committed_ids_vec.into_iter().map(|id| id.0).collect::<HashSet<u64>>());

        let ast_select_items = vec![SelectColumn::Asterisk];

        // Convert Option<SqlCondition> to Option<ast::Condition>
        let ast_sql_condition_for_select: Option<AstCondition> = match condition_opt.as_ref() {
            Some(sql_cond) => Some(AstCondition {
                column: sql_cond.column.clone(),
                operator: sql_cond.operator.clone(),
                value: datatype_to_ast_literal(&sql_cond.value)?, // Use the helper
            }),
            None => None,
        };

        let ast_statement_for_select =
            AstStatement::Select(crate::core::query::sql::ast::SelectStatement {
                columns: ast_select_items,
                source: source_table_name.clone(),
                condition: ast_sql_condition_for_select,
                // alias field is not present in sql::ast::SelectStatement
            });

        let initial_select_plan = self.optimizer.build_initial_plan(&ast_statement_for_select)?;
        let optimized_select_plan = self.optimizer.optimize(initial_select_plan)?;

        let mut select_execution_tree = self.build_execution_tree(
            optimized_select_plan,
            plan_snapshot_id.0, // Pass u64
            plan_committed_ids_u64_set.clone(),
        )?;
        let mut keys_to_update: Vec<Key> = Vec::new();
        let rows_iter = select_execution_tree.execute()?;
        for tuple_result in rows_iter {
            let tuple = tuple_result?;
            if tuple.is_empty() {
                return Err(OxidbError::Internal( // Changed
                    "Execution plan for UPDATE yielded empty tuple.".to_string(),
                ));
            }
            match tuple[0].clone() {
                DataType::String(s) => keys_to_update.push(s.into_bytes()),
                DataType::Integer(i) => keys_to_update.push(i.to_le_bytes().to_vec()),
                val => {
                    return Err(OxidbError::Type(format!( // Changed
                        "Unsupported key type {:?} from UPDATE selection plan.",
                        val
                    )))
                }
            }
        }

        if keys_to_update.is_empty() {
            return Ok(ExecutionResult::Success);
        }

        // TODO: Consider returning the updated_count in ExecutionResult
        let mut _updated_count = 0;

        for key in keys_to_update {
            let current_op_tx_id: TransactionId;
            let committed_ids_for_get_u64_set: HashSet<u64>;
            let is_auto_commit: bool;

            if let Some(active_tx) = self.transaction_manager.get_active_transaction() {
                current_op_tx_id = active_tx.id;
                committed_ids_for_get_u64_set = self.transaction_manager.get_committed_tx_ids_snapshot().into_iter().map(|id| id.0).collect();
                is_auto_commit = false;
            } else {
                current_op_tx_id = TransactionId(0);
                committed_ids_for_get_u64_set = self.transaction_manager.get_committed_tx_ids_snapshot().into_iter().map(|id| id.0).collect();
                is_auto_commit = true;
            }

            self.lock_manager.acquire_lock(
                current_op_tx_id.0, // Use .0 for u64
                &key,
                crate::core::transaction::lock_manager::LockType::Exclusive,
            )?;

            let current_value_bytes_opt =
                self.store.read().unwrap().get(&key, current_op_tx_id.0, &committed_ids_for_get_u64_set)?;

            if let Some(current_value_bytes) = current_value_bytes_opt {
                let mut current_data_type = deserialize_data_type(&current_value_bytes)?;

                if let DataType::Map(ref mut map_data) = current_data_type {
                    for assignment_cmd in &assignments_cmd {
                        map_data.insert(
                            assignment_cmd.column.as_bytes().to_vec(),
                            assignment_cmd.value.clone(),
                        );
                    }
                } else if !assignments_cmd.is_empty() {
                    if is_auto_commit {
                        self.lock_manager.release_locks(current_op_tx_id.0); // Use .0 for u64
                    }
                    return Err(OxidbError::NotImplemented{feature:
                        "Cannot apply field assignments to non-Map DataType".to_string(),
                    });
                }

                let updated_value_bytes = serialize_data_type(&current_data_type)?;

                if let Some(active_tx_mut) = self.transaction_manager.get_active_transaction_mut() {
                    active_tx_mut.undo_log.push(UndoOperation::RevertUpdate {
                        key: key.clone(),
                        old_value: current_value_bytes.clone(),
                    });

                    if current_value_bytes != updated_value_bytes {
                        active_tx_mut.undo_log.push(UndoOperation::IndexRevertInsert {
                            index_name: "default_value_index".to_string(),
                            key: key.clone(),
                            value_for_index: updated_value_bytes.clone(),
                        });
                        active_tx_mut.undo_log.push(UndoOperation::IndexRevertDelete {
                            index_name: "default_value_index".to_string(),
                            key: key.clone(),
                            old_value_for_index: current_value_bytes.clone(),
                        });
                    }
                }

                if current_value_bytes != updated_value_bytes {
                    let mut old_map_for_index = HashMap::new();
                    old_map_for_index
                        .insert("default_value_index".to_string(), current_value_bytes.clone());
                    let mut new_map_for_index = HashMap::new();
                    new_map_for_index
                        .insert("default_value_index".to_string(), updated_value_bytes.clone());
                    self.index_manager.on_update_data(
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
                    if let Some(active_tx_mut_for_lsn) = self.transaction_manager.get_active_transaction_mut() {
                        active_tx_mut_for_lsn.prev_lsn = new_lsn;
                    }
                }

                self.store.write().unwrap().put(
                    key.clone(),
                    updated_value_bytes.clone(),
                    &tx_for_store,
                    new_lsn, // Pass the new LSN
                )?;

                if is_auto_commit {
                    // Generate LSN for the auto-commit WalEntry
                    let commit_lsn = self.log_manager.next_lsn();
                    let commit_entry =
                        crate::core::storage::engine::wal::WalEntry::TransactionCommit {
                            lsn: commit_lsn,
                            transaction_id: current_op_tx_id.0, // Use .0 for u64
                        };
                    self.store.write().unwrap().log_wal_entry(&commit_entry)?;
                    self.transaction_manager.add_committed_tx_id(current_op_tx_id); // Pass TransactionId
                }
                _updated_count += 1;
            }

            if is_auto_commit {
                self.lock_manager.release_locks(current_op_tx_id.0); // Use .0 for u64
            }
        }

        Ok(ExecutionResult::Success)
    }
}
