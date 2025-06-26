#![allow(dead_code)]
#![allow(unused_variables)]

// This is the optimizer module
// Initially, it will define the QueryPlanNode enum and related structs.
// More optimization logic will be added here in the future.

// Optimizer struct and impl moved here from optimizer.rs

// use crate::core::optimizer::QueryPlanNode; // Not needed: QueryPlanNode defined below in same mod
// use crate::core::optimizer::{Expression, SimplePredicate}; // Not needed: Expression & SimplePredicate defined below
use crate::core::common::OxidbError;
use crate::core::indexing::manager::IndexManager;
use crate::core::optimizer::rules::apply_constant_folding_rule;
use crate::core::optimizer::rules::apply_noop_filter_removal_rule;
use crate::core::query::sql::ast::{
    AstLiteralValue as AstSqlLiteralValue, // Removed AstSqlCondition
    SelectColumn as AstSqlSelectColumn, Statement as AstStatement,
};
use crate::core::types::DataType; // Unified import
use std::sync::{Arc, RwLock};

/// The `Optimizer` is responsible for transforming an initial query plan
/// (derived directly from the AST) into a more efficient execution plan.
/// It applies a series of rules, such as predicate pushdown, index selection,
/// and constant folding, to achieve this.
#[derive(Debug)]
pub struct Optimizer {
    /// A shared reference to the `IndexManager` to access available indexes.
    index_manager: Arc<RwLock<IndexManager>>,
}

impl Optimizer {
    pub fn new(index_manager: Arc<RwLock<IndexManager>>) -> Self {
        Optimizer { index_manager }
    }

    pub fn build_initial_plan(
        &self,
        statement: &AstStatement,
    ) -> Result<QueryPlanNode, OxidbError> {
        match statement {
            AstStatement::Select(select_ast) => {
                let mut plan_node =
                    QueryPlanNode::TableScan { table_name: select_ast.source.clone(), alias: None };

                if let Some(ref condition_ast) = select_ast.condition {
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
                    columns: vec!["0".to_string()],
                };
                Ok(plan_node)
            }
            AstStatement::CreateTable(_) => {
                Err(OxidbError::NotImplemented {
                    feature: "Query planning for CREATE TABLE statements".to_string(),
                })
            }
            AstStatement::Insert(_) => {
                Err(OxidbError::NotImplemented {
                    feature: "Query planning for INSERT statements".to_string(),
                })
            }
            AstStatement::Delete(delete_ast) => {
                let mut plan_node = QueryPlanNode::TableScan {
                    table_name: delete_ast.table_name.clone(),
                    alias: None,
                };

                if let Some(ref condition_ast) = delete_ast.condition {
                    let expression =
                        self.ast_sql_condition_to_optimizer_expression(condition_ast)?;
                    plan_node =
                        QueryPlanNode::Filter { input: Box::new(plan_node), predicate: expression };
                }
                Ok(QueryPlanNode::DeleteNode {
                    input: Box::new(plan_node),
                    table_name: delete_ast.table_name.clone(),
                })
            }
            AstStatement::DropTable(_) => {
                Err(OxidbError::NotImplemented {
                    feature: "Query planning for DROP TABLE statements".to_string(),
                })
            }
        }
    }

    fn ast_sql_condition_to_optimizer_expression(
        &self,
        ast_cond_tree: &crate::core::query::sql::ast::ConditionTree,
    ) -> Result<Expression, OxidbError> {
        match ast_cond_tree {
            crate::core::query::sql::ast::ConditionTree::Comparison(ast_simple_cond) => {
                let value = match &ast_simple_cond.value {
                    AstSqlLiteralValue::String(s) => DataType::String(s.clone()),
                    AstSqlLiteralValue::Number(n_str) => {
                        if let Ok(i_val) = n_str.parse::<i64>() {
                            DataType::Integer(i_val)
                        } else if let Ok(f_val) = n_str.parse::<f64>() {
                            DataType::Float(f_val)
                        } else {
                            return Err(OxidbError::SqlParsing(format!(
                                "Cannot parse numeric literal '{}'",
                                n_str
                            )));
                        }
                    }
                    AstSqlLiteralValue::Boolean(b) => DataType::Boolean(*b),
                    AstSqlLiteralValue::Null => DataType::Null,
                    AstSqlLiteralValue::Vector(_) => {
                        // Optimizer might not handle vector comparisons directly yet.
                        // This could be an error or a specific non-optimizable expression.
                        return Err(OxidbError::NotImplemented {
                            feature: "Vector comparison in optimizer expressions".to_string(),
                        });
                    }
                };

                Ok(Expression::CompareOp {
                    left: Box::new(Expression::Column(ast_simple_cond.column.clone())),
                    op: ast_simple_cond.operator.clone(),
                    right: Box::new(Expression::Literal(value)),
                })
            }
            crate::core::query::sql::ast::ConditionTree::And(left_ast, right_ast) => {
                let left_expr = self.ast_sql_condition_to_optimizer_expression(left_ast)?;
                let right_expr = self.ast_sql_condition_to_optimizer_expression(right_ast)?;
                Ok(Expression::BinaryOp {
                    left: Box::new(left_expr),
                    op: "AND".to_string(), // Optimizer uses "AND" for BinaryOp
                    right: Box::new(right_expr),
                })
            }
            crate::core::query::sql::ast::ConditionTree::Or(left_ast, right_ast) => {
                let left_expr = self.ast_sql_condition_to_optimizer_expression(left_ast)?;
                let right_expr = self.ast_sql_condition_to_optimizer_expression(right_ast)?;
                Ok(Expression::BinaryOp {
                    left: Box::new(left_expr),
                    op: "OR".to_string(), // Optimizer uses "OR" for BinaryOp
                    right: Box::new(right_expr),
                })
            }
            crate::core::query::sql::ast::ConditionTree::Not(ast_cond) => {
                let expr = self.ast_sql_condition_to_optimizer_expression(ast_cond)?;
                Ok(Expression::UnaryOp {
                    op: "NOT".to_string(),
                    expr: Box::new(expr),
                })
            }
        }
    }

    pub fn optimize(&self, plan: QueryPlanNode) -> Result<QueryPlanNode, OxidbError> {
        let plan = self.apply_predicate_pushdown(plan);
        let plan = self.apply_index_selection(plan)?;
        let plan = apply_constant_folding_rule(plan);
        let plan = apply_noop_filter_removal_rule(plan);
        Ok(plan)
    }

    #[allow(clippy::only_used_in_recursion)]
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
            QueryPlanNode::DeleteNode { input, table_name } => QueryPlanNode::DeleteNode {
                input: Box::new(self.apply_predicate_pushdown(*input)),
                table_name,
            },
        }
    }

    fn apply_index_selection(&self, plan_node: QueryPlanNode) -> Result<QueryPlanNode, OxidbError> {
        match plan_node {
            QueryPlanNode::Filter { input, predicate } => {
                let optimized_input = self.apply_index_selection(*input)?;
                let mut transformed_to_index_scan = false;
                let mut new_plan_node = optimized_input.clone();

                if let QueryPlanNode::TableScan { ref table_name, ref alias } = optimized_input {
                    if let Expression::CompareOp { left, op, right } = &predicate {
                        if let (
                            Expression::Column(column_name),
                            Expression::Literal(literal_value),
                        ) = (&**left, &**right)
                        {
                            let index_name_candidate =
                                format!("idx_{}_{}", table_name, column_name);

                            if *op == "=" {
                                let index_manager_guard =
                                    self.index_manager.read().map_err(|e| {
                                        OxidbError::Lock(format!(
                                            "Failed to acquire read lock on index manager: {}",
                                            e
                                        ))
                                    })?;
                                if index_manager_guard.get_index(&index_name_candidate).is_some() {
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
            QueryPlanNode::DeleteNode { input, table_name } => Ok(QueryPlanNode::DeleteNode {
                input: Box::new(self.apply_index_selection(*input)?),
                table_name,
            }),
        }
    }
}

pub mod rules;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum QueryPlanNode {
    TableScan {
        table_name: String,
        alias: Option<String>,
    },
    IndexScan {
        index_name: String,
        table_name: String,
        alias: Option<String>,
        scan_condition: Option<SimplePredicate>,
    },
    Filter {
        input: Box<QueryPlanNode>,
        predicate: Expression,
    },
    Project {
        input: Box<QueryPlanNode>,
        columns: Vec<String>,
    },
    NestedLoopJoin {
        left: Box<QueryPlanNode>,
        right: Box<QueryPlanNode>,
        join_predicate: Option<JoinPredicate>,
    },
    DeleteNode {
        input: Box<QueryPlanNode>,
        table_name: String,
    },
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct SimplePredicate {
    pub column: String,
    pub operator: String,
    pub value: DataType,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    Literal(DataType),
    Column(String),
    BinaryOp {
        left: Box<Expression>,
        op: String,
        right: Box<Expression>,
    },
    CompareOp {
        left: Box<Expression>,
        op: String,
        right: Box<Expression>,
    },
    UnaryOp { // Added for NOT
        op: String, // e.g., "NOT"
        expr: Box<Expression>,
    },
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct JoinPredicate {
    pub left_column: String,
    pub right_column: String,
}
