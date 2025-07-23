//! Query Optimization Module
//!
//! This module provides query optimization capabilities including cost-based
//! optimization, rule-based optimization, and query planning.

use crate::core::common::OxidbError;
use crate::core::types::DataType;

pub mod planner; // Cost-based query planner
pub mod rule; // Optimization rule trait
pub mod rules;

// Re-exports for easier access
pub use planner::CostBasedPlanner;
pub use rule::{OptimizationRule, RuleManager};

/// Main optimizer interface
/// Follows SOLID's Single Responsibility Principle - coordinates optimization
#[derive(Debug)]
pub struct Optimizer {
    planner: CostBasedPlanner,
    rule_manager: RuleManager,
}

impl Optimizer {
    /// Create a new optimizer
    #[must_use]
    pub fn new() -> Self {
        Self { planner: CostBasedPlanner::new(), rule_manager: RuleManager::new() }
    }

    /// Get the cost-based planner
    #[must_use]
    pub const fn planner(&self) -> &CostBasedPlanner {
        &self.planner
    }

    /// Get the rule manager
    #[must_use]
    pub const fn rule_manager(&self) -> &RuleManager {
        &self.rule_manager
    }

    /// Build an initial logical plan from an AST statement
    pub fn build_initial_plan(
        &self,
        statement: &crate::core::query::sql::ast::Statement,
    ) -> Result<QueryPlanNode, crate::core::common::error::OxidbError> {
        use crate::core::query::sql::ast::{SelectColumn, Statement};

        match statement {
            Statement::Select(select_stmt) => {
                // Create the base table scan
                let table_scan = QueryPlanNode::TableScan {
                    table_name: select_stmt.from_clause.name.clone(),
                    alias: select_stmt.from_clause.alias.clone(),
                };

                // Handle column selection - create a projection if needed
                let plan_with_projection = match &select_stmt.columns[..] {
                    [SelectColumn::Asterisk] => {
                        // For SELECT *, we need to determine all columns from the table schema
                        // For now, return the table scan directly and let the execution handle it
                        table_scan
                    }
                    columns => {
                        // For specific columns, create a projection
                        let column_names: Vec<String> = columns
                            .iter()
                            .filter_map(|col| match col {
                                SelectColumn::ColumnName(name) => Some(name.clone()),
                                SelectColumn::Asterisk => None, // Mixed * and columns not supported yet
                            })
                            .collect();

                        if column_names.is_empty() {
                            // All asterisks - treat as SELECT *
                            table_scan
                        } else {
                            QueryPlanNode::Project {
                                input: Box::new(table_scan),
                                columns: column_names,
                            }
                        }
                    }
                };

                // Add filter if there's a WHERE clause
                let plan_with_filter = if let Some(condition) = &select_stmt.condition {
                    // Basic WHERE clause implementation
                    // For now, we'll create a Filter node with a simple expression converter
                    QueryPlanNode::Filter {
                        input: Box::new(plan_with_projection),
                        predicate: self.convert_simple_condition_to_expression(condition)?,
                    }
                } else {
                    plan_with_projection
                };

                Ok(plan_with_filter)
            }
            Statement::Delete(delete_stmt) => {
                // Create a basic DELETE plan
                // DELETE needs to scan the table and filter rows to delete
                let table_scan = QueryPlanNode::TableScan {
                    table_name: delete_stmt.table_name.clone(),
                    alias: None,
                };

                // Add filter if there's a WHERE clause
                let filtered_scan = if let Some(condition) = &delete_stmt.condition {
                    QueryPlanNode::Filter {
                        input: Box::new(table_scan),
                        predicate: self.convert_simple_condition_to_expression(condition)?,
                    }
                } else {
                    // DELETE without WHERE - delete all rows
                    table_scan
                };

                // Wrap in a DELETE node
                Ok(QueryPlanNode::DeleteNode {
                    input: Box::new(filtered_scan),
                    table_name: delete_stmt.table_name.clone(),
                })
            }
            _ => Err(crate::core::common::error::OxidbError::NotImplemented {
                feature: "Non-SELECT/DELETE statements in optimizer".to_string(),
            }),
        }
    }

    /// Convert a simple AST condition to an optimizer expression
    /// This is a basic implementation that handles simple equality conditions
    fn convert_simple_condition_to_expression(
        &self,
        condition: &crate::core::query::sql::ast::ConditionTree,
    ) -> Result<Expression, crate::core::common::error::OxidbError> {
        use crate::core::query::sql::ast::{AstExpressionValue, AstLiteralValue, ConditionTree};

        match condition {
            ConditionTree::Comparison(cond) => {
                // Handle simple column = value conditions
                match &cond.value {
                    AstExpressionValue::Literal(literal_value) => {
                        // Convert the literal to a DataType
                        let data_type = match literal_value {
                            AstLiteralValue::Number(n) => {
                                // Try to parse as integer first, then float
                                if let Ok(i) = n.parse::<i64>() {
                                    crate::core::types::DataType::Integer(i)
                                } else if let Ok(f) = n.parse::<f64>() {
                                    crate::core::types::DataType::Float(f)
                                } else {
                                    // Fallback to string if parsing fails
                                    crate::core::types::DataType::String(n.clone())
                                }
                            }
                            AstLiteralValue::String(s) => {
                                crate::core::types::DataType::String(s.clone())
                            }
                            AstLiteralValue::Boolean(b) => {
                                crate::core::types::DataType::Boolean(*b)
                            }
                            AstLiteralValue::Null => crate::core::types::DataType::Null,
                            AstLiteralValue::Vector(_) => {
                                // Vector literals in WHERE clauses are not yet supported
                                // Returning a placeholder that evaluates to true (1=1) would be dangerous
                                // as it could cause DELETE/UPDATE to affect all rows unintentionally
                                return Err(OxidbError::NotImplemented {
                                    feature: "Vector literals in WHERE clauses".to_string(),
                                });
                            }
                        };

                        Ok(Expression::CompareOp {
                            left: Box::new(Expression::Column(cond.column.clone())),
                            op: cond.operator.clone(),
                            right: Box::new(Expression::Literal(data_type)),
                        })
                    }
                    AstExpressionValue::ColumnIdentifier(_) => {
                        // Column-to-column comparisons are not yet supported
                        // Returning a placeholder that evaluates to true (1=1) would be dangerous
                        // as it could cause DELETE/UPDATE to affect all rows unintentionally
                        Err(OxidbError::NotImplemented {
                            feature: "Column-to-column comparisons in WHERE clauses".to_string(),
                        })
                    }
                    AstExpressionValue::Parameter(_param_index) => {
                        // Parameters should be resolved at execution time
                        // For optimizer purposes, create a placeholder expression
                        // The actual parameter substitution will happen during execution
                        Ok(Expression::CompareOp {
                            left: Box::new(Expression::Column(cond.column.clone())),
                            op: cond.operator.clone(),
                            right: Box::new(Expression::Literal(
                                crate::core::types::DataType::Null,
                            )), // Placeholder
                        })
                    }
                }
            }
            _ => {
                // Complex conditions (AND, OR, NOT) are not yet fully supported in the optimizer
                // Returning a placeholder that evaluates to true (1=1) would be dangerous
                // as it could cause DELETE/UPDATE to affect all rows unintentionally
                Err(OxidbError::NotImplemented {
                    feature: "Complex condition trees (AND, OR, NOT) in optimizer".to_string(),
                })
            }
        }
    }

    /// Optimize a logical plan using optimization rules
    pub const fn optimize(
        &self,
        plan: QueryPlanNode,
    ) -> Result<QueryPlanNode, crate::core::common::error::OxidbError> {
        // For now, just return the plan as-is
        // In the future, this would apply optimization rules
        Ok(plan)
    }

    /// Optimize a plan with access to index manager for intelligent index selection
    pub fn optimize_with_indexes(
        &self,
        plan: QueryPlanNode,
        index_manager: &std::sync::Arc<
            std::sync::RwLock<crate::core::indexing::manager::IndexManager>,
        >,
    ) -> Result<QueryPlanNode, crate::core::common::error::OxidbError> {
        // First apply rule-based optimizations
        let rule_optimized = self.optimize(plan)?;

        // Then apply index selection
        let index_optimized = self.apply_index_selection(rule_optimized, index_manager)?;

        Ok(index_optimized)
    }

    /// Apply intelligent index selection to the plan
    fn apply_index_selection(
        &self,
        plan: QueryPlanNode,
        index_manager: &std::sync::Arc<
            std::sync::RwLock<crate::core::indexing::manager::IndexManager>,
        >,
    ) -> Result<QueryPlanNode, crate::core::common::error::OxidbError> {
        match plan {
            // Look for Filter over TableScan - prime candidate for index usage
            QueryPlanNode::Filter { input, predicate } => {
                if let QueryPlanNode::TableScan { table_name, alias } = *input {
                    // Try to convert to IndexScan if beneficial
                    if let Some(index_scan) = self.try_convert_to_index_scan(
                        &table_name,
                        &predicate,
                        alias.clone(),
                        index_manager,
                    )? {
                        Ok(index_scan)
                    } else {
                        // Keep original plan but continue optimizing the predicate
                        Ok(QueryPlanNode::Filter {
                            input: Box::new(QueryPlanNode::TableScan { table_name, alias }),
                            predicate,
                        })
                    }
                } else {
                    // Recursively optimize the input
                    let optimized_input = self.apply_index_selection(*input, index_manager)?;
                    Ok(QueryPlanNode::Filter { input: Box::new(optimized_input), predicate })
                }
            }

            // Recursively optimize other node types
            QueryPlanNode::Project { input, columns } => {
                let optimized_input = self.apply_index_selection(*input, index_manager)?;
                Ok(QueryPlanNode::Project { input: Box::new(optimized_input), columns })
            }

            QueryPlanNode::NestedLoopJoin { left, right, join_predicate } => {
                let optimized_left = self.apply_index_selection(*left, index_manager)?;
                let optimized_right = self.apply_index_selection(*right, index_manager)?;
                Ok(QueryPlanNode::NestedLoopJoin {
                    left: Box::new(optimized_left),
                    right: Box::new(optimized_right),
                    join_predicate,
                })
            }

            QueryPlanNode::DeleteNode { input, table_name } => {
                let optimized_input = self.apply_index_selection(*input, index_manager)?;
                Ok(QueryPlanNode::DeleteNode { input: Box::new(optimized_input), table_name })
            }

            // Base cases - no further optimization needed
            QueryPlanNode::TableScan { .. } | QueryPlanNode::IndexScan { .. } => Ok(plan),
        }
    }

    /// Try to convert a Filter over `TableScan` to an `IndexScan`
    fn try_convert_to_index_scan(
        &self,
        table_name: &str,
        predicate: &Expression,
        alias: Option<String>,
        index_manager: &std::sync::Arc<
            std::sync::RwLock<crate::core::indexing::manager::IndexManager>,
        >,
    ) -> Result<Option<QueryPlanNode>, crate::core::common::error::OxidbError> {
        // Extract simple equality predicates that can use indexes
        if let Some(simple_pred) = self.extract_indexable_predicate(predicate) {
            // Check if we have an index that can satisfy this predicate
            if let Some(index_name) = self.find_suitable_index(&simple_pred, index_manager)? {
                return Ok(Some(QueryPlanNode::IndexScan {
                    index_name,
                    table_name: table_name.to_string(),
                    alias,
                    scan_condition: Some(simple_pred),
                }));
            }
        }

        Ok(None)
    }

    /// Extract a simple indexable predicate from a complex expression
    fn extract_indexable_predicate(&self, expr: &Expression) -> Option<SimplePredicate> {
        match expr {
            Expression::CompareOp { left, op, right } if op == "=" => {
                // Handle "column = value" pattern
                if let (Expression::Column(column), Expression::Literal(value)) =
                    (left.as_ref(), right.as_ref())
                {
                    return Some(SimplePredicate {
                        column: column.clone(),
                        operator: op.clone(),
                        value: value.clone(),
                    });
                }
                // Handle "value = column" pattern
                if let (Expression::Literal(value), Expression::Column(column)) =
                    (left.as_ref(), right.as_ref())
                {
                    return Some(SimplePredicate {
                        column: column.clone(),
                        operator: op.clone(),
                        value: value.clone(),
                    });
                }
            }
            _ => {
                // For now, only handle simple equality predicates
            }
        }
        None
    }

    /// Find a suitable index for the given predicate
    fn find_suitable_index(
        &self,
        predicate: &SimplePredicate,
        index_manager: &std::sync::Arc<
            std::sync::RwLock<crate::core::indexing::manager::IndexManager>,
        >,
    ) -> Result<Option<String>, crate::core::common::error::OxidbError> {
        let _index_manager_guard = index_manager.read().map_err(|e| {
            crate::core::common::error::OxidbError::LockTimeout(format!(
                "Failed to acquire read lock on index manager: {e}"
            ))
        })?;

        if predicate.operator == "=" {
            // Look for a column-specific index first
            // Index names follow the pattern: idx_{table}_{column}
            // We need to extract the table name from context, but for now we'll skip this optimization
            // and return None to force table scan with filtering

            // TODO: Implement proper column-specific index lookup
            // For now, don't use default_value_index for column-specific queries
            // as it's designed for full-row indexing, not individual column values
        }

        Ok(None)
    }

    /// Get mutable access to the planner
    pub fn planner_mut(&mut self) -> &mut CostBasedPlanner {
        &mut self.planner
    }

    /// Get mutable access to the rule manager
    pub fn rule_manager_mut(&mut self) -> &mut RuleManager {
        &mut self.rule_manager
    }
}

impl Default for Optimizer {
    fn default() -> Self {
        Self::new()
    }
}

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
    UnaryOp {
        // Added for NOT
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
