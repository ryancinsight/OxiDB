#![allow(dead_code)] // Allow dead code for now
#![allow(unused_variables)] // Allow unused variables for now

use crate::core::optimizer::QueryPlanNode;
use crate::core::optimizer::{Expression, SimplePredicate}; // Removed JoinPredicate
use crate::core::query::sql::ast::{Statement as AstStatement, Condition as AstCondition, SelectColumn as AstSelectColumn, AstLiteralValue}; // Removed unused AstSelectStatement, AstUpdateStatement
// Command related imports are not used yet in build_initial_plan if it only takes AstStatement
// use crate::core::query::commands::{Command, SqlCondition, SelectColumnSpec};
use crate::core::types::DataType;
use crate::core::common::error::DbError;
use crate::core::indexing::manager::IndexManager; // For index selection later
use crate::core::common::serialization::serialize_data_type;
use std::sync::Arc; // For IndexManager if shared

// pub struct SchemaManager { /* ... */ }

pub struct Optimizer {
    index_manager: Arc<IndexManager>,
    // schema_manager: Arc<SchemaManager>,
}

impl Optimizer {
    pub fn new(index_manager: Arc<IndexManager>) -> Self {
        Optimizer { index_manager }
    }

    pub fn build_initial_plan(&self, statement: &AstStatement) -> Result<QueryPlanNode, DbError> {
        match statement {
            AstStatement::Select(select_ast) => {
                let mut plan_node = QueryPlanNode::TableScan {
                    table_name: select_ast.source.clone(),
                    alias: None,
                };

                if let Some(ref condition_ast) = select_ast.condition {
                    let expression = self.ast_condition_to_expression(condition_ast)?;
                    plan_node = QueryPlanNode::Filter {
                        input: Box::new(plan_node),
                        predicate: expression,
                    };
                }

                let projection_columns: Vec<String> = match select_ast.columns.get(0) {
                    Some(AstSelectColumn::Asterisk) => vec!["*".to_string()],
                    _ => select_ast.columns.iter().filter_map(|col| match col {
                        AstSelectColumn::ColumnName(name) => Some(name.clone()),
                        _ => None,
                    }).collect(),
                };

                if projection_columns.is_empty() && !select_ast.columns.iter().any(|c| matches!(c, AstSelectColumn::Asterisk)) {
                     plan_node = QueryPlanNode::Project {
                        input: Box::new(plan_node),
                        columns: vec!["*".to_string()],
                    };
                } else {
                    plan_node = QueryPlanNode::Project {
                        input: Box::new(plan_node),
                        columns: projection_columns,
                    };
                }

                Ok(plan_node)
            }
            AstStatement::Update(update_ast) => {
                let mut plan_node = QueryPlanNode::TableScan {
                    table_name: update_ast.source.clone(),
                    alias: None,
                };

                if let Some(ref condition_ast) = update_ast.condition {
                    let expression = self.ast_condition_to_expression(condition_ast)?;
                    plan_node = QueryPlanNode::Filter {
                        input: Box::new(plan_node),
                        predicate: expression,
                    };
                }
                 plan_node = QueryPlanNode::Project {
                    input: Box::new(plan_node),
                    columns: vec!["*".to_string()],
                };
                Ok(plan_node)
            }
            // TODO: Handle other AstStatement variants like Insert, Delete, CreateTable, etc.
            // For now, we only handle Select and Update for plan generation.
            _ => Err(DbError::NotImplemented("Plan generation for this statement type is not implemented.".to_string())),
        }
    }

    fn ast_condition_to_expression(&self, ast_cond: &AstCondition) -> Result<Expression, DbError> {
        let value = match &ast_cond.value {
            AstLiteralValue::String(s) => DataType::String(s.clone()),
            AstLiteralValue::Number(n_str) => {
                if let Ok(i_val) = n_str.parse::<i64>() { DataType::Integer(i_val) }
                else if let Ok(f_val) = n_str.parse::<f64>() { DataType::Float(f_val) }
                else { return Err(DbError::InvalidQuery(format!("Cannot parse numeric literal '{}'", n_str))); }
            }
            AstLiteralValue::Boolean(b) => DataType::Boolean(*b),
            AstLiteralValue::Null => DataType::Null,
        };

        Ok(Expression::Predicate(SimplePredicate {
            column: ast_cond.column.clone(),
            operator: ast_cond.operator.clone(),
            value,
        }))
    }

    pub fn optimize(&self, plan: QueryPlanNode) -> Result<QueryPlanNode, DbError> {
        let plan = self.apply_predicate_pushdown(plan);
        let plan = self.apply_index_selection(plan)?; // Add this line
        Ok(plan)
    }

    fn apply_predicate_pushdown(&self, plan_node: QueryPlanNode) -> QueryPlanNode {
        match plan_node {
            QueryPlanNode::Filter { input, predicate } => {
                let optimized_input = self.apply_predicate_pushdown(*input);
                match optimized_input {
                    QueryPlanNode::Project { input: project_input, columns } => {
                        let pushed_filter = QueryPlanNode::Filter {
                            input: project_input,
                            predicate: predicate.clone(),
                        };
                        let optimized_pushed_filter = self.apply_predicate_pushdown(pushed_filter);
                        QueryPlanNode::Project {
                            input: Box::new(optimized_pushed_filter),
                            columns,
                        }
                    }
                    _ => {
                        QueryPlanNode::Filter {
                            input: Box::new(optimized_input),
                            predicate,
                        }
                    }
                }
            }
            QueryPlanNode::Project { input, columns } => {
                QueryPlanNode::Project {
                    input: Box::new(self.apply_predicate_pushdown(*input)),
                    columns,
                }
            }
            QueryPlanNode::NestedLoopJoin { left, right, join_predicate } => {
                QueryPlanNode::NestedLoopJoin {
                    left: Box::new(self.apply_predicate_pushdown(*left)),
                    right: Box::new(self.apply_predicate_pushdown(*right)),
                    join_predicate,
                }
            }
            QueryPlanNode::TableScan { .. } | QueryPlanNode::IndexScan { .. } => {
                plan_node
            }
        }
    }

    fn apply_index_selection(&self, plan_node: QueryPlanNode) -> Result<QueryPlanNode, DbError> {
        match plan_node {
            QueryPlanNode::Filter { input, predicate } => {
                let optimized_input = self.apply_index_selection(*input)?;

                // Check if optimized_input can be converted to IndexScan
                // We need to match and then decide whether to return a new node or the reconstructed Filter.
                let mut transformed_to_index_scan = false;
                let mut new_plan_node = optimized_input.clone(); // Clone to modify, or reconstruct later

                if let QueryPlanNode::TableScan { ref table_name, ref alias } = optimized_input {
                    // Changed from if let to let as Expression only has Predicate variant for now
                    let simple_predicate = match &predicate {
                        Expression::Predicate(sp) => sp,
                        // _ => return Ok(QueryPlanNode::Filter { input: Box::new(optimized_input), predicate }), // Should not happen with current Expression def
                    };
                    let index_name_candidate = format!("idx_{}_{}", table_name, simple_predicate.column);

                    if simple_predicate.operator == "=" && self.index_manager.get_index(&index_name_candidate).is_some() {
                            let scan_value_bytes = serialize_data_type(&simple_predicate.value)?;

                            new_plan_node = QueryPlanNode::IndexScan {
                                index_name: index_name_candidate,
                                table_name: table_name.clone(),
                                alias: alias.clone(), // Clone the ref alias
                                scan_condition: Some(simple_predicate.clone()),
                            };
                        transformed_to_index_scan = true;
                    }
                    // } // This was the end of the original if let for Expression::Predicate
                }

                if transformed_to_index_scan {
                    Ok(new_plan_node)
                } else {
                    // If no transformation, reconstruct the Filter node with the (potentially) optimized_input.
                    // The optimized_input itself might have changed deeper in recursion, so use it.
                    Ok(QueryPlanNode::Filter {
                        input: Box::new(optimized_input), // Use the result of recursive call
                        predicate,
                    })
                }
            }
            QueryPlanNode::Project { input, columns } => {
                Ok(QueryPlanNode::Project {
                    input: Box::new(self.apply_index_selection(*input)?),
                    columns,
                })
            }
            QueryPlanNode::NestedLoopJoin { left, right, join_predicate } => {
                Ok(QueryPlanNode::NestedLoopJoin {
                    left: Box::new(self.apply_index_selection(*left)?),
                    right: Box::new(self.apply_index_selection(*right)?),
                    join_predicate,
                })
            }
            // Base cases: Scans or already optimized nodes.
            // For TableScan, if it reaches here directly (not as input to Filter), it means no filter above it could be pushed.
            // For IndexScan, it's already considered optimized in terms of index usage.
            node @ QueryPlanNode::TableScan { .. } | node @ QueryPlanNode::IndexScan { .. } => Ok(node),
        }
    }
}
