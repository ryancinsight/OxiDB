#![allow(dead_code)]
#![allow(unused_variables)]

use crate::core::optimizer::QueryPlanNode;
use crate::core::optimizer::{Expression, SimplePredicate};
// Use fully qualified paths for SQL AST items to avoid ambiguity
use crate::core::common::serialization::serialize_data_type;
use crate::core::common::OxidbError; // Changed
use crate::core::indexing::manager::IndexManager;
use crate::core::optimizer::rules::apply_constant_folding_rule;
use crate::core::optimizer::rules::apply_noop_filter_removal_rule;
use crate::core::query::sql::ast::{
    AstLiteralValue as AstSqlLiteralValue, Condition as AstSqlCondition,
    SelectColumn as AstSqlSelectColumn, Statement as AstStatement,
};
use crate::core::types::DataType;
use std::sync::Arc;

#[derive(Debug)]
pub struct Optimizer {
    index_manager: Arc<IndexManager>,
}

impl Optimizer {
    pub fn new(index_manager: Arc<IndexManager>) -> Self {
        Optimizer { index_manager }
    }

    pub fn build_initial_plan(
        &self,
        statement: &AstStatement,
    ) -> Result<QueryPlanNode, OxidbError> {
        // Changed
        match statement {
            AstStatement::Select(select_ast) => {
                let mut plan_node =
                    QueryPlanNode::TableScan { table_name: select_ast.source.clone(), alias: None };

                if let Some(ref condition_ast) = select_ast.condition {
                    // condition_ast here is &sql::ast::Condition
                    let expression =
                        self.ast_sql_condition_to_optimizer_expression(condition_ast)?;
                    plan_node =
                        QueryPlanNode::Filter { input: Box::new(plan_node), predicate: expression };
                }

                let projection_columns: Vec<String> = select_ast
                    .columns
                    .iter()
                    .map(|col| match col {
                        AstSqlSelectColumn::ColumnName(name) => name.clone(),
                        AstSqlSelectColumn::Asterisk => "*".to_string(),
                    })
                    .collect();

                if projection_columns.is_empty()
                    && !select_ast.columns.iter().any(|c| matches!(c, AstSqlSelectColumn::Asterisk))
                {
                    return Err(OxidbError::SqlParsing(
                        // Changed
                        "SELECT statement with no columns specified.".to_string(),
                    ));
                }

                plan_node = QueryPlanNode::Project {
                    input: Box::new(plan_node),
                    columns: projection_columns,
                };

                Ok(plan_node)
            }
            AstStatement::Update(update_ast) => {
                let mut plan_node =
                    QueryPlanNode::TableScan { table_name: update_ast.source.clone(), alias: None };

                if let Some(ref condition_ast) = update_ast.condition {
                    let expression =
                        self.ast_sql_condition_to_optimizer_expression(condition_ast)?;
                    plan_node =
                        QueryPlanNode::Filter { input: Box::new(plan_node), predicate: expression };
                }
                plan_node = QueryPlanNode::Project {
                    input: Box::new(plan_node),
                    columns: vec!["*".to_string()],
                };
                Ok(plan_node)
            }
            AstStatement::CreateTable(_) => {
                // CREATE TABLE is a DDL operation and does not produce a data-retrieval plan
                // in the same way SELECT or UPDATE (which starts with a selection) does.
                // It could be handled by returning a specific DDL plan node if the executor
                // expects it, or an error if the optimizer is only for DML.
                // For now, returning NotImplemented seems appropriate for build_initial_plan.
                Err(OxidbError::NotImplemented {
                    feature: "Query planning for CREATE TABLE statements".to_string(),
                })
            }
            AstStatement::Insert(_) => {
                // INSERT statements, like CREATE TABLE, are DDL/DML that don't produce a plan for data retrieval
                // in the same way SELECT does. They are typically handled more directly by the executor.
                // The optimizer might have a role in validating or rewriting them in complex systems,
                // but for now, indicating it's not a plannable query here is sufficient.
                Err(OxidbError::NotImplemented {
                    feature: "Query planning for INSERT statements".to_string(),
                })
            }
        }
    }

    // Converts sql::ast::Condition to optimizer::Expression
    fn ast_sql_condition_to_optimizer_expression(
        &self,
        ast_cond: &AstSqlCondition,
    ) -> Result<Expression, OxidbError> {
        // Changed
        let value = match &ast_cond.value {
            AstSqlLiteralValue::String(s) => DataType::String(s.clone()),
            AstSqlLiteralValue::Number(n_str) => {
                if let Ok(i_val) = n_str.parse::<i64>() {
                    DataType::Integer(i_val)
                } else if let Ok(f_val) = n_str.parse::<f64>() {
                    DataType::Float(f_val)
                } else {
                    return Err(OxidbError::SqlParsing(format!(
                        // Changed
                        "Cannot parse numeric literal '{}'",
                        n_str
                    )));
                }
            }
            AstSqlLiteralValue::Boolean(b) => DataType::Boolean(*b),
            AstSqlLiteralValue::Null => DataType::Null,
        };

        Ok(Expression::CompareOp {
            left: Box::new(Expression::Column(ast_cond.column.clone())),
            op: ast_cond.operator.clone(),
            right: Box::new(Expression::Literal(value)),
        })
    }

    // Removed the old ast_condition_to_expression_new as it was based on incorrect assumptions about ast::Expression

    pub fn optimize(&self, plan: QueryPlanNode) -> Result<QueryPlanNode, OxidbError> {
        // Changed
        let plan = self.apply_predicate_pushdown(plan);
        let plan = self.apply_index_selection(plan)?;
        let plan = apply_constant_folding_rule(plan);
        let plan = apply_noop_filter_removal_rule(plan);
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
                        QueryPlanNode::Project { input: Box::new(optimized_pushed_filter), columns }
                    }
                    _ => QueryPlanNode::Filter { input: Box::new(optimized_input), predicate },
                }
            }
            QueryPlanNode::Project { input, columns } => QueryPlanNode::Project {
                input: Box::new(self.apply_predicate_pushdown(*input)),
                columns,
            },
            QueryPlanNode::NestedLoopJoin { left, right, join_predicate } => {
                QueryPlanNode::NestedLoopJoin {
                    left: Box::new(self.apply_predicate_pushdown(*left)),
                    right: Box::new(self.apply_predicate_pushdown(*right)),
                    join_predicate,
                }
            }
            QueryPlanNode::TableScan { .. } | QueryPlanNode::IndexScan { .. } => plan_node,
        }
    }

    fn apply_index_selection(&self, plan_node: QueryPlanNode) -> Result<QueryPlanNode, OxidbError> {
        // Changed
        match plan_node {
            QueryPlanNode::Filter { input, predicate } => {
                let optimized_input = self.apply_index_selection(*input)?;
                let mut transformed_to_index_scan = false;
                let mut new_plan_node = optimized_input.clone();

                if let QueryPlanNode::TableScan { ref table_name, ref alias } = optimized_input {
                    // Check if the predicate is a CompareOp with Column on left and Literal on right
                    if let Expression::CompareOp { left, op, right } = &predicate {
                        if let (Expression::Column(column_name), Expression::Literal(literal_value)) =
                            (&**left, &**right)
                        {
                            let index_name_candidate =
                                format!("idx_{}_{}", table_name, column_name);

                            // Check for specific conditions suitable for index scan (e.g., equality on indexed column)
                            if *op == "="
                                && self.index_manager.get_index(&index_name_candidate).is_some()
                            {
                                // Ensure serialize_data_type is handled or removed if not strictly needed for this logic block
                                // let _scan_value_bytes = serialize_data_type(literal_value)?;

                                let scan_predicate = SimplePredicate {
                                    column: column_name.clone(),
                                    operator: op.clone(),
                                    value: literal_value.clone(),
                                };

                                new_plan_node = QueryPlanNode::IndexScan {
                                    index_name: index_name_candidate,
                                    table_name: table_name.clone(),
                                    alias: alias.clone(),
                                    scan_condition: Some(scan_predicate),
                                };
                                transformed_to_index_scan = true;
                            }
                        }
                    }
                }

                if transformed_to_index_scan {
                    Ok(new_plan_node)
                } else {
                    Ok(QueryPlanNode::Filter { input: Box::new(optimized_input), predicate })
                }
            }
            QueryPlanNode::Project { input, columns } => Ok(QueryPlanNode::Project {
                input: Box::new(self.apply_index_selection(*input)?),
                columns,
            }),
            QueryPlanNode::NestedLoopJoin { left, right, join_predicate } => {
                Ok(QueryPlanNode::NestedLoopJoin {
                    left: Box::new(self.apply_index_selection(*left)?),
                    right: Box::new(self.apply_index_selection(*right)?),
                    join_predicate,
                })
            }
            node @ QueryPlanNode::TableScan { .. } | node @ QueryPlanNode::IndexScan { .. } => {
                Ok(node)
            }
        }
    }
}
