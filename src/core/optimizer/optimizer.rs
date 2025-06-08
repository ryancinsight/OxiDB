#![allow(dead_code)]
#![allow(unused_variables)]

use crate::core::optimizer::QueryPlanNode;
use crate::core::optimizer::{Expression, SimplePredicate};
// Use fully qualified paths for SQL AST items to avoid ambiguity
use crate::core::query::sql::ast::{Statement as AstStatement, Condition as AstSqlCondition, SelectColumn as AstSqlSelectColumn, AstLiteralValue as AstSqlLiteralValue};
use crate::core::types::DataType;
use crate::core::common::error::DbError;
use crate::core::indexing::manager::IndexManager;
use crate::core::common::serialization::serialize_data_type;
use std::sync::Arc;

#[derive(Debug)]
pub struct Optimizer {
    index_manager: Arc<IndexManager>,
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
                    // condition_ast here is &sql::ast::Condition
                    let expression = self.ast_sql_condition_to_optimizer_expression(condition_ast)?;
                    plan_node = QueryPlanNode::Filter {
                        input: Box::new(plan_node),
                        predicate: expression,
                    };
                }

                let projection_columns: Vec<String> = select_ast.columns.iter().map(|col| match col {
                    AstSqlSelectColumn::ColumnName(name) => name.clone(),
                    AstSqlSelectColumn::Asterisk => "*".to_string(),
                }).collect();

                if projection_columns.is_empty() && !select_ast.columns.iter().any(|c| matches!(c, AstSqlSelectColumn::Asterisk)) {
                     return Err(DbError::InvalidQuery("SELECT statement with no columns specified.".to_string()));
                }

                plan_node = QueryPlanNode::Project {
                    input: Box::new(plan_node),
                    columns: projection_columns,
                };

                Ok(plan_node)
            }
            AstStatement::Update(update_ast) => {
                let mut plan_node = QueryPlanNode::TableScan {
                    table_name: update_ast.source.clone(),
                    alias: None,
                };

                if let Some(ref condition_ast) = update_ast.condition {
                    let expression = self.ast_sql_condition_to_optimizer_expression(condition_ast)?;
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
            // If other AstStatement variants (like Insert, Delete) are added,
            // this match will become non-exhaustive, requiring updates.
        }
    }

    // Converts sql::ast::Condition to optimizer::Expression
    fn ast_sql_condition_to_optimizer_expression(&self, ast_cond: &AstSqlCondition) -> Result<Expression, DbError> {
        let value = match &ast_cond.value {
            AstSqlLiteralValue::String(s) => DataType::String(s.clone()),
            AstSqlLiteralValue::Number(n_str) => {
                if let Ok(i_val) = n_str.parse::<i64>() { DataType::Integer(i_val) }
                else if let Ok(f_val) = n_str.parse::<f64>() { DataType::Float(f_val) }
                else { return Err(DbError::InvalidQuery(format!("Cannot parse numeric literal '{}'", n_str))); }
            }
            AstSqlLiteralValue::Boolean(b) => DataType::Boolean(*b),
            AstSqlLiteralValue::Null => DataType::Null,
        };

        Ok(Expression::Predicate(SimplePredicate {
            column: ast_cond.column.clone(),
            operator: ast_cond.operator.clone(),
            value,
        }))
    }

    // Removed the old ast_condition_to_expression_new as it was based on incorrect assumptions about ast::Expression

    pub fn optimize(&self, plan: QueryPlanNode) -> Result<QueryPlanNode, DbError> {
        let plan = self.apply_predicate_pushdown(plan);
        let plan = self.apply_index_selection(plan)?;
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
                let mut transformed_to_index_scan = false;
                let mut new_plan_node = optimized_input.clone();

                if let QueryPlanNode::TableScan { ref table_name, ref alias } = optimized_input {
                    let simple_predicate = match &predicate {
                        Expression::Predicate(sp) => sp,
                        // Expression in optimizer only has Predicate variant for now
                    };
                    let index_name_candidate = format!("idx_{}_{}", table_name, simple_predicate.column);

                    if simple_predicate.operator == "=" && self.index_manager.get_index(&index_name_candidate).is_some() {
                            let _scan_value_bytes = serialize_data_type(&simple_predicate.value)?;

                            new_plan_node = QueryPlanNode::IndexScan {
                                index_name: index_name_candidate,
                                table_name: table_name.clone(),
                                alias: alias.clone(),
                                scan_condition: Some(simple_predicate.clone()),
                            };
                        transformed_to_index_scan = true;
                    }
                }

                if transformed_to_index_scan {
                    Ok(new_plan_node)
                } else {
                    Ok(QueryPlanNode::Filter {
                        input: Box::new(optimized_input),
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
            node @ QueryPlanNode::TableScan { .. } | node @ QueryPlanNode::IndexScan { .. } => Ok(node),
        }
    }
}
