use super::utils::datatype_to_ast_literal;
use super::{ExecutionResult, QueryExecutor};
use crate::core::common::OxidbError; // Changed
use crate::core::query::commands::SelectColumnSpec; // Removed SqlCondition, Key
use crate::core::query::sql::ast::{SelectColumn, Statement as AstStatement}; // Removed AstCondition, AstLiteralValue
use crate::core::storage::engine::traits::KeyValueStore;
use crate::core::types::DataType;
use std::collections::HashSet;
use std::sync::Arc; // Import the helper

// Make sure KeyValueStore is Send + Sync + 'static for build_execution_tree
impl<S: KeyValueStore<Vec<u8>, Vec<u8>> + Send + Sync + 'static> QueryExecutor<S> {
    /// Handles a SELECT query.
    /// This involves:
    /// 1. Determining the transaction context (snapshot ID, committed IDs).
    /// 2. Converting the SELECT command into an AST statement.
    /// 3. Building an initial query plan using the optimizer.
    /// 4. Optimizing the query plan.
    /// 5. Building an execution tree from the optimized plan.
    /// 6. Executing the tree and collecting the results.
    pub(crate) fn handle_select(
        &mut self,
        select_columns_spec: SelectColumnSpec,
        source_table_name: String,
        condition_opt: Option<crate::core::query::commands::SqlConditionTree>, // Changed
    ) -> Result<ExecutionResult, OxidbError> {
        let snapshot_id: crate::core::common::types::TransactionId; // Explicitly TransactionId
        let committed_ids_vec: Vec<crate::core::common::types::TransactionId>;

        if let Some(active_tx) = self.transaction_manager.get_active_transaction() {
            snapshot_id = active_tx.id; // This is TransactionId
            committed_ids_vec = self.transaction_manager.get_committed_tx_ids_snapshot();
        } else {
            // For a SELECT without an active transaction, snapshot should see all committed data.
            // Using TransactionId(0) as a convention.
            snapshot_id = self
                .transaction_manager
                .current_active_transaction_id()
                .unwrap_or(crate::core::common::types::TransactionId(0));
            committed_ids_vec = self.transaction_manager.get_committed_tx_ids_snapshot();
        }

        // Map Vec<TransactionId> to HashSet<u64> for use with store.get() if needed by planner/execution tree
        let committed_ids_u64_set =
            Arc::new(committed_ids_vec.into_iter().map(|id| id.0).collect::<HashSet<u64>>());

        let ast_select_items = match select_columns_spec {
            SelectColumnSpec::All => vec![SelectColumn::Asterisk],
            SelectColumnSpec::Specific(cols) => {
                cols.into_iter().map(SelectColumn::ColumnName).collect()
            }
        };

        // Convert Option<commands::SqlConditionTree> to Option<ast::ConditionTree>
        let ast_condition_tree_opt: Option<crate::core::query::sql::ast::ConditionTree> =
            match condition_opt {
                Some(sql_cond_tree) => {
                    Some(command_condition_tree_to_ast_condition_tree(&sql_cond_tree, self)?)
                } // Pass self if needed by helper
                None => None,
            };

        let ast_statement = AstStatement::Select(crate::core::query::sql::ast::SelectStatement {
            columns: ast_select_items,
            from_clause: crate::core::query::sql::ast::TableReference {
                name: source_table_name,
                alias: None,
            },
            joins: Vec::new(), // Added new empty joins vector
            condition: ast_condition_tree_opt, // Changed
            order_by: None,
            limit: None,
        });

        let initial_plan = self.optimizer.build_initial_plan(&ast_statement)?;
        let optimized_plan = self.optimizer.optimize(initial_plan)?;

        let mut execution_tree_root = self.build_execution_tree(
            optimized_plan,
            snapshot_id.0,
            committed_ids_u64_set.clone(),
        )?; // Pass snapshot_id.0 (u64)

        let results_iter = execution_tree_root.execute()?;
        let mut all_datatypes_from_tuples: Vec<DataType> = Vec::new();

        for tuple_result in results_iter {
            let tuple = tuple_result?;
            for data_type in tuple {
                all_datatypes_from_tuples.push(data_type);
            }
        }

        Ok(ExecutionResult::Values(all_datatypes_from_tuples))
    }
}

// Helper function to convert commands::SqlConditionTree to ast::ConditionTree
pub(super) fn command_condition_tree_to_ast_condition_tree<
    S: KeyValueStore<Vec<u8>, Vec<u8>> + Send + Sync + 'static,
>(
    sql_tree: &crate::core::query::commands::SqlConditionTree,
    _executor: &QueryExecutor<S>, // May be needed if type conversion requires context, e.g. schema
) -> Result<crate::core::query::sql::ast::ConditionTree, OxidbError> {
    match sql_tree {
        crate::core::query::commands::SqlConditionTree::Comparison(sql_simple_cond) => {
            Ok(crate::core::query::sql::ast::ConditionTree::Comparison(
                crate::core::query::sql::ast::Condition {
                    column: sql_simple_cond.column.clone(),
                    operator: sql_simple_cond.operator.clone(),
                    value: datatype_to_ast_literal(&sql_simple_cond.value)?,
                },
            ))
        }
        crate::core::query::commands::SqlConditionTree::And(left_sql, right_sql) => {
            let left_ast = command_condition_tree_to_ast_condition_tree(left_sql, _executor)?;
            let right_ast = command_condition_tree_to_ast_condition_tree(right_sql, _executor)?;
            Ok(crate::core::query::sql::ast::ConditionTree::And(
                Box::new(left_ast),
                Box::new(right_ast),
            ))
        }
        crate::core::query::commands::SqlConditionTree::Or(left_sql, right_sql) => {
            let left_ast = command_condition_tree_to_ast_condition_tree(left_sql, _executor)?;
            let right_ast = command_condition_tree_to_ast_condition_tree(right_sql, _executor)?;
            Ok(crate::core::query::sql::ast::ConditionTree::Or(
                Box::new(left_ast),
                Box::new(right_ast),
            ))
        }
        crate::core::query::commands::SqlConditionTree::Not(sql_cond) => {
            let ast_cond = command_condition_tree_to_ast_condition_tree(sql_cond, _executor)?;
            Ok(crate::core::query::sql::ast::ConditionTree::Not(Box::new(ast_cond)))
        }
    }
}
