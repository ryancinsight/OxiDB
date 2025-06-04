use crate::core::common::error::DbError;
use crate::core::common::serialization::serialize_data_type;
use crate::core::execution::operators::{
    TableScanOperator, IndexScanOperator, FilterOperator, ProjectOperator, NestedLoopJoinOperator,
};
use crate::core::execution::{ExecutionOperator, Tuple};
use crate::core::optimizer::QueryPlanNode;
use crate::core::query::executor::QueryExecutor; // To access self.store, self.index_manager
use crate::core::storage::engine::traits::KeyValueStore;
use crate::core::types::DataType;
use std::collections::HashSet;
use std::sync::Arc;

impl<S: KeyValueStore<Vec<u8>, Vec<u8>> + Send + Sync + 'static> QueryExecutor<S> {
    pub(crate) fn build_execution_tree(
        &self,
        plan: QueryPlanNode,
        snapshot_id: u64,
        committed_ids: Arc<HashSet<u64>>,
    ) -> Result<Box<dyn ExecutionOperator + Send + Sync>, DbError> {
        match plan {
            QueryPlanNode::TableScan { table_name, alias: _ } => {
                // Alias is ignored for now by TableScanOperator
                let operator = TableScanOperator::new(
                    self.store.clone(),
                    table_name,
                    snapshot_id,
                    committed_ids.clone(),
                );
                Ok(Box::new(operator))
            }
            QueryPlanNode::IndexScan {
                index_name,
                table_name: _, // table_name currently unused by IndexScanOperator directly
                alias: _,      // alias currently unused
                scan_condition,
            } => {
                // scan_condition is Option<SimplePredicate>
                // IndexScanOperator requires a specific value to scan for.
                let simple_predicate = scan_condition.ok_or_else(|| DbError::InvalidQuery("IndexScan requires a scan condition".to_string()))?;
                let scan_value_dt = simple_predicate.value; // This is already a DataType

                // IndexScanOperator expects Vec<u8>, so serialize the DataType
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
            QueryPlanNode::Project { input, columns } => {
                let input_operator =
                    self.build_execution_tree(*input, snapshot_id, committed_ids.clone())?;

                // Temporary Simplification for Project:
                // Assume columns are 0-based numeric indices as strings.
                let mut column_indices = Vec::new();
                if columns.len() == 1 && columns[0] == "*" {
                    // This is a tricky case. ProjectOperator needs explicit indices.
                    // If it's '*', it means all columns from the input.
                    // However, without knowing the schema or output of the input operator,
                    // we can't determine these indices here.
                    // This needs a more robust solution, possibly by having build_execution_tree
                    // also return the schema/column count of the produced operator, or by
                    // making ProjectOperator itself smarter (e.g. by passing a special "all" marker).
                    // For now, let's return an error or a very simplified behavior.
                    // Simplest approach: If input is TableScan, it produces all columns.
                    // But what if input is another Project or Join?
                    // This simplification will likely fail for complex queries with '*'.
                    // A better simplification might be to pass an empty Vec<usize> to ProjectOperator
                    // and have IT interpret empty as "all columns".
                    // For now, let's assume ProjectOperator handles empty indices as "all".
                    // If columns contains only "*", pass an empty Vec to ProjectOperator,
                    // which will interpret it as "project all".
                    column_indices = Vec::new();
                } else {
                    for col_str in columns {
                        match col_str.parse::<usize>() {
                            Ok(idx) => column_indices.push(idx),
                            Err(_) => {
                                // If a column is not "*" and not parseable to usize, it's an error.
                                return Err(DbError::InvalidQuery(format!(
                                    "Project column '{}' is not a valid numeric index and not '*'.",
                                    col_str
                                )));
                            }
                        }
                    }
                }
                let operator = ProjectOperator::new(input_operator, column_indices);
                Ok(Box::new(operator))
            }
            QueryPlanNode::NestedLoopJoin {
                left,
                right,
                join_predicate,
            } => {
                let left_operator =
                    self.build_execution_tree(*left, snapshot_id, committed_ids.clone())?;
                let right_operator =
                    self.build_execution_tree(*right, snapshot_id, committed_ids.clone())?;
                let operator = NestedLoopJoinOperator::new(left_operator, right_operator, join_predicate);
                Ok(Box::new(operator))
            }
            // Add other QueryPlanNode variants as they are defined and supported
             _ => Err(DbError::NotImplemented(format!(
                "QueryPlanNode variant not yet supported in build_execution_tree: {:?}",
                plan
            ))),
        }
    }
}
