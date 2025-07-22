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

                let operator = TableScanOperator::new(
                    self.store.clone(),
                    table_name,
                    snapshot_id,
                    committed_ids,
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
                    committed_ids,
                );
                Ok(Box::new(operator))
            }
            QueryPlanNode::Filter { input, predicate } => {
                let input_operator =
                    self.build_execution_tree(*input, snapshot_id, committed_ids)?;
                let operator = FilterOperator::new(input_operator, predicate);
                Ok(Box::new(operator))
            }
            QueryPlanNode::Project { input, columns } => {
                let input_operator =
                    self.build_execution_tree(*input, snapshot_id, committed_ids)?;
                let mut column_indices = Vec::new();
                if columns.len() == 1 && columns[0] == "*" {
                    column_indices = Vec::new(); // ProjectOperator interprets empty as all columns
                } else {
                    for col_str in columns {
                        match col_str.parse::<usize>() {
                            Ok(idx) => column_indices.push(idx),
                            Err(_) => {
                                return Err(OxidbError::SqlParsing(format!(
                                    "Project column '{col_str}' is not a valid numeric index and not '*'."
                                )));
                            }
                        }
                    }
                }
                let operator = ProjectOperator::new(input_operator, column_indices);
                Ok(Box::new(operator))
            }
            QueryPlanNode::NestedLoopJoin { left, right, join_predicate } => {
                let left_operator =
                    self.build_execution_tree(*left, snapshot_id, committed_ids.clone())?;
                let right_operator =
                    self.build_execution_tree(*right, snapshot_id, committed_ids)?;
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
                            "Table '{table_name}' not found when building DeleteNode."
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
                    committed_ids,    // 7. committed_ids: Arc<HashSet<u64>>
                    schema_arc,               // 8. schema: Arc<Schema>
                );
                Ok(Box::new(delete_operator))
            }
        }
    }
}
