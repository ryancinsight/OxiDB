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
    /// Extract the table name from a query plan node
    fn extract_table_name(&self, plan: &QueryPlanNode) -> Result<String, OxidbError> {
        match plan {
            QueryPlanNode::TableScan { table_name, .. } => Ok(table_name.clone()),
            QueryPlanNode::Filter { input, .. } => self.extract_table_name(input),
            QueryPlanNode::Project { input, .. } => self.extract_table_name(input),
            QueryPlanNode::IndexScan { table_name, .. } => Ok(table_name.clone()),
            QueryPlanNode::NestedLoopJoin { .. } => {
                Err(OxidbError::SqlParsing("Cannot resolve column names for JOIN queries yet".to_string()))
            }
            QueryPlanNode::DeleteNode { .. } => {
                Err(OxidbError::SqlParsing("Cannot resolve column names for DELETE queries".to_string()))
            }
        }
    }
    
    pub(crate) fn build_execution_tree(
        &self,
        plan: QueryPlanNode,
        snapshot_id: u64,
        committed_ids: Arc<HashSet<u64>>,
    ) -> Result<Box<dyn ExecutionOperator + Send + Sync>, OxidbError> {
        match plan {
            QueryPlanNode::TableScan { table_name, alias: _ } => {
                // Get the table schema
                let schema = self.get_table_schema(&table_name)?
                    .ok_or_else(|| OxidbError::TableNotFound(table_name.clone()))?;
                
                let operator = TableScanOperator::new(
                    self.store.clone(),
                    table_name,
                    (*schema).clone(),
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
                // First build the input operator
                let input_operator =
                    self.build_execution_tree(*input.clone(), snapshot_id, committed_ids.clone())?;
                
                // Try to extract table name and get schema
                // If the input is a projection, we need to handle column mapping differently
                let schema = match &*input {
                    QueryPlanNode::Project { input: table_input, columns } => {
                        // For projections, we need to create a mapped schema
                        let table_name = self.extract_table_name(table_input)?;
                        let original_schema = self.get_table_schema(&table_name)?
                            .ok_or_else(|| OxidbError::TableNotFound(table_name.clone()))?;
                        
                        // Create a new schema with only the projected columns
                        let mut new_columns = Vec::new();
                        for col_name in columns {
                            if let Some(col_def) = original_schema.columns.iter().find(|c| &c.name == col_name) {
                                new_columns.push(col_def.clone());
                            }
                        }
                        Arc::new(crate::core::types::schema::Schema {
                            columns: new_columns,
                        })
                    }
                    _ => {
                        // For non-projection inputs, use the table schema directly
                        let table_name = self.extract_table_name(&input)?;
                        self.get_table_schema(&table_name)?
                            .ok_or_else(|| OxidbError::TableNotFound(table_name.clone()))?
                    }
                };
                
                let operator = FilterOperator::with_schema(input_operator, predicate, schema);
                Ok(Box::new(operator))
            }
            QueryPlanNode::Project { input, columns } => {
                let input_operator =
                    self.build_execution_tree(*input.clone(), snapshot_id, committed_ids.clone())?;
                
                let mut column_indices = Vec::new();
                if columns.len() == 1 && columns[0] == "*" {
                    column_indices = Vec::new(); // ProjectOperator interprets empty as all columns
                } else {
                    // Try to get the table name from the input plan to resolve column names
                    let table_name = self.extract_table_name(&input)?;
                    let schema = self.get_table_schema(&table_name)?
                        .ok_or_else(|| OxidbError::TableNotFound(table_name.clone()))?;
                    
                    // Resolve column names to indices
                    for col in columns {
                        if let Some(idx) = schema.get_column_index(&col) {
                            column_indices.push(idx);
                        } else {
                            return Err(OxidbError::SqlParsing(format!(
                                "Column '{}' not found in table '{}'",
                                col, table_name
                            )));
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
                    committed_ids,            // 7. committed_ids: Arc<HashSet<u64>>
                    schema_arc,               // 8. schema: Arc<Schema>
                );
                Ok(Box::new(delete_operator))
            }
        }
    }
}
