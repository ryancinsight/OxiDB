// src/core/query/executor/planner.rs
use crate::core::common::serialization::serialize_data_type;
use crate::core::common::OxidbError;
use crate::core::execution::operators::{
    FilterOperator, IndexScanOperator, NestedLoopJoinOperator, ProjectOperator, TableScanOperator,
};
use crate::core::execution::ExecutionOperator;
use crate::core::optimizer::QueryPlanNode;
use crate::core::query::executor::QueryExecutor;
use crate::core::storage::engine::traits::KeyValueStore;
use crate::core::types::schema::Schema; // Ensure Schema is imported for Arc<Schema>

use std::collections::HashSet;
use std::sync::Arc;

impl<S: KeyValueStore<Vec<u8>, Vec<u8>> + Send + Sync + 'static> QueryExecutor<S> {
    pub(crate) fn build_execution_tree(
        &self,
        plan: QueryPlanNode,
        snapshot_id: u64,
        committed_ids: Arc<HashSet<u64>>,
    ) -> Result<Box<dyn ExecutionOperator + Send + Sync>, OxidbError> {
        match plan {
            QueryPlanNode::TableScan { table_name, alias: _ } => {
                // Fetch the schema for the table
                let table_schema_arc = self.get_table_schema(&table_name)?
                    .ok_or_else(|| OxidbError::Execution(format!("Table '{}' not found during planning TableScan.", table_name)))?;

                let operator = TableScanOperator::new(
                    self.store.clone(),
                    table_name, // table_name is already String
                    table_schema_arc, // Pass the fetched schema
                    snapshot_id,
                    committed_ids.clone(),
                );
                Ok(Box::new(operator))
            }
            QueryPlanNode::IndexScan { index_name, table_name: _, alias: _, scan_condition } => {
                let simple_predicate = scan_condition.ok_or_else(|| {
                    OxidbError::SqlParsing("IndexScan requires a scan condition".to_string())
                })?;
                let scan_value_dt = simple_predicate.value;
                let serialized_scan_value = serialize_data_type(&scan_value_dt)?;
                let operator = IndexScanOperator::new(
                    self.store.clone(),
                    self.index_manager.clone(),
                    index_name,
                    serialized_scan_value,
                    snapshot_id,
                    committed_ids.clone(),
                );
                Ok(Box::new(operator))
            }
            QueryPlanNode::Filter { input, predicate } => {
                let input_operator =
                    self.build_execution_tree(*input, snapshot_id, committed_ids.clone())?;
                let operator = FilterOperator::new(input_operator, predicate);
                Ok(Box::new(operator))
            }
            QueryPlanNode::Project { input, expressions } => { // `expressions` is now Vec<BoundExpression>
                let input_operator =
                    self.build_execution_tree(*input, snapshot_id, committed_ids.clone())?;

                // Get the schema from the input operator
                let input_schema = input_operator.get_output_schema();

                // The expressions are already bound, so we can pass them directly.
                // The check for empty expressions (previously for "*") should ideally be handled
                // by the optimizer ensuring `expressions` is populated correctly (e.g., expanding "*").
                // If `expressions` is empty here, ProjectOperator will produce empty tuples.
                // This might be valid for `SELECT;` if allowed, or should be an error earlier.
                if expressions.is_empty() {
                    // This case should ideally not be hit if SELECT * is expanded by the optimizer,
                    // or if zero-column projections are disallowed earlier.
                    // For now, ProjectOperator handles empty expressions by producing empty tuples.
                    // Depending on desired SQL behavior, an error might be more appropriate here.
                     eprintln!("[Planner] Warning: QueryPlanNode::Project has empty expressions. This will result in empty tuples.");
                }

                let operator = ProjectOperator::new(input_operator, (*input_schema).clone(), expressions);
                Ok(Box::new(operator))
            }
            QueryPlanNode::NestedLoopJoin { left, right, join_predicate } => {
                let left_operator =
                    self.build_execution_tree(*left, snapshot_id, committed_ids.clone())?;
                let right_operator =
                    self.build_execution_tree(*right, snapshot_id, committed_ids.clone())?;
                let operator =
                    NestedLoopJoinOperator::new(left_operator, right_operator, join_predicate);
                Ok(Box::new(operator))
            }
            QueryPlanNode::DeleteNode { input, table_name } => {
                let input_operator =
                    self.build_execution_tree(*input, snapshot_id, committed_ids.clone())?;

                // Placeholder for primary_key_column_index.
                // A robust solution would fetch this from schema.
                let primary_key_column_index = 0;

                let schema_arc: Arc<Schema> =
                    self.get_table_schema(&table_name)?.ok_or_else(|| {
                        OxidbError::Execution(format!(
                            "Table '{}' not found when building DeleteNode.",
                            table_name
                        ))
                    })?;

                // Call DeleteOperator::new with 8 individual arguments, matching the signature
                // reportedly now in src/core/execution/operators/delete.rs
                let delete_operator = crate::core::execution::operators::DeleteOperator::new(
                    input_operator,           // 1. input: Box<dyn ExecutionOperator + Send + Sync>
                    table_name,               // 2. table_name: String
                    self.store.clone(),       // 3. store: Arc<RwLock<S>>
                    self.log_manager.clone(), // 4. log_manager: Arc<LogManager>
                    crate::core::common::types::TransactionId(snapshot_id), // 5. transaction_id: TransactionId
                    primary_key_column_index, // 6. primary_key_column_index: usize
                    committed_ids.clone(),    // 7. committed_ids: Arc<HashSet<u64>>
                    schema_arc,               // 8. schema: Arc<Schema>
                );
                Ok(Box::new(delete_operator))
            }
        }
    }
}
