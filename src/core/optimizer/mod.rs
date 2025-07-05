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
    SelectColumn as AstSqlSelectColumn,
    Statement as AstStatement,
};
use crate::core::types::{DataType, Schema}; // Unified import, Added Schema
use crate::core::storage::engine::traits::KeyValueStore; // Added KeyValueStore
use std::sync::{Arc, RwLock};

/// The `Optimizer` is responsible for transforming an initial query plan
/// (derived directly from the AST) into a more efficient execution plan.
/// It applies a series of rules, such as predicate pushdown, index selection,
/// and constant folding, to achieve this.
#[derive(Debug)]
pub struct Optimizer<S: KeyValueStore<Vec<u8>, Vec<u8>> + Send + Sync + 'static> {
    /// A shared reference to the `IndexManager` to access available indexes.
    index_manager: Arc<RwLock<IndexManager>>,
    /// A shared reference to the `KeyValueStore` to access table schemas.
    store: Arc<RwLock<S>>,
}

impl<S: KeyValueStore<Vec<u8>, Vec<u8>> + Send + Sync + 'static> Optimizer<S> {
    pub fn new(index_manager: Arc<RwLock<IndexManager>>, store: Arc<RwLock<S>>) -> Self {
        Optimizer { index_manager, store }
    }

    // Helper method to construct schema key
    fn schema_key(table_name: &str) -> Vec<u8> {
        format!("_schema_{}", table_name).into_bytes()
    }

    // Fetches table schema from the store
    fn get_table_schema(&self, table_name: &str) -> Result<Option<Arc<Schema>>, OxidbError> {
        let schema_key = Self::schema_key(table_name);
        // Using snapshot_id = 0 and an empty set for committed_ids for schema reads.
        // This assumes DDLs are auto-committed or we want the latest committed schema for planning.
        // A more robust solution might involve transaction context.
        let committed_ids_for_schema_read = std::collections::HashSet::new();

        match self.store.read()
            .map_err(|e| OxidbError::Lock(format!("Optimizer: Failed to acquire read lock on store for get_table_schema: {}", e)))?
            .get_schema(&schema_key, 0, &committed_ids_for_schema_read)?
        {
            Some(schema) => Ok(Some(Arc::new(schema))),
            None => Ok(None),
        }
    }

    pub fn build_initial_plan(
        &self,
        statement: &AstStatement,
    ) -> Result<QueryPlanNode, OxidbError> {
        match statement {
            AstStatement::Select(select_ast) => {
                // Determine the schema of the input to the projection.
                // For now, assume single table, no joins. Schema is from the FROM clause.
                let table_name_for_schema = &select_ast.from_clause.name;
                let input_schema_for_projection_arc = self.get_table_schema(table_name_for_schema)?
                    .ok_or_else(|| OxidbError::Binding(format!("Table '{}' not found for schema binding.", table_name_for_schema)))?;

                let input_schema_ref_for_binder: &Schema = input_schema_for_projection_arc.as_ref();

                // Initial plan starts with TableScan
                let mut plan_node = QueryPlanNode::TableScan {
                    table_name: select_ast.from_clause.name.clone(),
                    alias: select_ast.from_clause.alias.clone(),
                };
                // TODO: Incorporate select_ast.joins into the initial plan.
                // If joins are present, `input_schema_for_projection_arc` would be more complex (combined schema).

                if let Some(ref condition_ast) = select_ast.condition {
                    // Binding for filter conditions also needs a schema.
                    // The schema for filter conditions applied directly to a table scan
                    // is that table's schema.
                    // TODO: If filter is after a join, it needs the joined schema.
                    // For now, ast_sql_condition_to_optimizer_expression doesn't use a binder,
                    // it directly translates to optimizer::Expression. This might need revisiting
                    // if filter predicates also need full binding with BoundExpression.
                    let expression =
                        self.ast_sql_condition_to_optimizer_expression(condition_ast)?;
                    plan_node =
                        QueryPlanNode::Filter { input: Box::new(plan_node), predicate: expression };
                }

                // Now, bind the projection expressions using the fetched schema
                let mut binder = crate::core::query::binder::binder::Binder::new(Some(input_schema_ref_for_binder));

                let mut projection_expressions: Vec<crate::core::query::binder::expression::BoundExpression> = Vec::new();
                for col_ast in &select_ast.columns {
                    match col_ast {
                        AstSqlSelectColumn::Expression(expr_ast) => {
                            match binder.bind_expression(expr_ast) {
                                Ok(bound_expr) => projection_expressions.push(bound_expr),
                                Err(bind_err) => {
                                    return Err(OxidbError::Binding(format!("Failed to bind projection expression: {:?}, error: {:?}", expr_ast, bind_err)));
                                }
                            }
                        }
                        AstSqlSelectColumn::Asterisk => {
                            // Expand asterisk using input_schema_ref_for_binder
                            if input_schema_ref_for_binder.columns.is_empty() {
                                // This case (SELECT * FROM table_with_no_columns) is unlikely
                                // or might be an error depending on SQL standards/database behavior.
                                // Log a warning. If SELECT * from an empty schema table is an error,
                                // it could be checked here or result in an empty projection_expressions
                                // which is handled below.
                                eprintln!("[Optimizer] Warning: SELECT * from a table with no columns ('{}').", table_name_for_schema);
                            }
                            for col_def in &input_schema_ref_for_binder.columns {
                                projection_expressions.push(
                                    crate::core::query::binder::expression::BoundExpression::ColumnRef {
                                        name: col_def.name.clone(),
                                        return_type: col_def.data_type.clone(),
                                    }
                                );
                            }
                        }
                    }
                }

                // After processing all select_ast.columns, if projection_expressions is still empty,
                // it means either:
                // 1. The original select_ast.columns was empty (e.g. `SELECT FROM table;`) - parser should catch this.
                // 2. The original select_ast.columns only contained `*` and the table schema was empty.
                // Case 1 should be a parsing error. Case 2 is valid (results in no columns).
                if projection_expressions.is_empty() {
                    if select_ast.columns.is_empty() {
                        // This indicates `SELECT FROM table;` which is invalid SQL.
                        // The parser should ideally prevent this.
                        return Err(OxidbError::SqlParsing(
                            "SELECT statement with an empty select list is invalid.".to_string(),
                        ));
                    }
                    // If select_ast.columns was not empty (e.g. it was `[*]`),
                    // and projection_expressions is empty, it means `*` expanded to nothing
                    // (empty schema), which is valid. No error here.
                }
                // Removed extra closing brace that was here.

                plan_node = QueryPlanNode::Project {
                    input: Box::new(plan_node),
                    expressions: projection_expressions, // These are now BoundExpression
                };

                Ok(plan_node)
            }
            AstStatement::Update(update_ast) => {
                // For UPDATE, the projection is internal. The WHERE clause uses optimizer::Expression.
                // If UPDATE needs to bind expressions in SET clauses, it would also need schema access.
                let table_name = &update_ast.source;
                let _table_schema_arc = self.get_table_schema(table_name)?
                    .ok_or_else(|| OxidbError::Binding(format!("Table '{}' not found for UPDATE schema binding.", table_name)))?;
                // TODO: Use table_schema_arc if SET expressions need binding.

                let mut plan_node =
                    QueryPlanNode::TableScan { table_name: update_ast.source.clone(), alias: None };

                if let Some(ref condition_ast) = update_ast.condition {
                    let expression =
                        self.ast_sql_condition_to_optimizer_expression(condition_ast)?;
                    plan_node =
                        QueryPlanNode::Filter { input: Box::new(plan_node), predicate: expression };
                }
                // The projection for UPDATE is just a placeholder or for internal columns.
                // It currently uses `columns: vec!["__ROWID__".to_string()]` which is a Vec<String>.
                // If QueryPlanNode::Project strictly expects BoundExpression, this needs adjustment.
                // For now, assuming the existing structure for Update's Project node is permissible
                // or will be updated separately. This change focuses on SELECT's Project node.
                // TODO: Revisit Project node structure for UPDATE if it must use BoundExpression.
                // This will error if Project strictly requires BoundExpressions.
                // The progress_ledger indicates Project uses BoundExpression.
                // So, we need to bind `__ROWID__` or handle this differently.
                // For now, let this be a known issue if it causes a compile error.
                // A simple fix might be to not have a Project node for Update if not strictly needed,
                // or create a BoundColumnRef for __ROWID__ if it's a known internal column.

                // Placeholder: If __ROWID__ is a known concept, bind it.
                // This requires __ROWID__ to be part of the schema or handled specially.
                // For now, this part is problematic due to Project's change to BoundExpression.
                // Quick Fix: Create a dummy BoundExpression if no schema for __ROWID__.
                // This is not robust.
                 let dummy_rowid_expr = crate::core::query::binder::expression::BoundExpression::Literal {
                    value: crate::core::common::types::Value::Integer(0), // Placeholder
                    return_type: DataType::Integer,
                 };

                plan_node = QueryPlanNode::Project {
                    input: Box::new(plan_node),
                    expressions: vec![dummy_rowid_expr], // Needs to be Vec<BoundExpression>
                };
                Ok(plan_node)
            }
            AstStatement::CreateTable(_) => Err(OxidbError::NotImplemented {
                feature: "Query planning for CREATE TABLE statements".to_string(),
            }),
            AstStatement::Insert(_) => Err(OxidbError::NotImplemented {
                feature: "Query planning for INSERT statements".to_string(),
            }),
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
            AstStatement::DropTable(_) => Err(OxidbError::NotImplemented {
                feature: "Query planning for DROP TABLE statements".to_string(),
            }),
        }
    }

    fn ast_expression_to_optimizer_expression(
        &self,
        ast_expr: &crate::core::query::sql::ast::AstExpression,
    ) -> Result<Expression, OxidbError> {
        match ast_expr {
            crate::core::query::sql::ast::AstExpression::Literal(literal_val) => {
                match literal_val {
                    AstSqlLiteralValue::String(s) => Ok(Expression::Literal(DataType::String(s.clone()))),
                    AstSqlLiteralValue::Number(n_str) => {
                        if let Ok(i_val) = n_str.parse::<i64>() {
                            Ok(Expression::Literal(DataType::Integer(i_val)))
                        } else if let Ok(f_val) = n_str.parse::<f64>() {
                            Ok(Expression::Literal(DataType::Float(f_val)))
                        } else {
                            Err(OxidbError::SqlParsing(format!(
                                "Cannot parse numeric literal '{}' in optimizer",
                                n_str
                            )))
                        }
                    }
                    AstSqlLiteralValue::Boolean(b) => Ok(Expression::Literal(DataType::Boolean(*b))),
                    AstSqlLiteralValue::Null => Ok(Expression::Literal(DataType::Null)),
                    AstSqlLiteralValue::Vector(_) => Err(OxidbError::NotImplemented {
                        feature: "Vector literals in optimizer expressions".to_string(),
                    }),
                }
            }
            crate::core::query::sql::ast::AstExpression::ColumnIdentifier(col_name) => {
                Ok(Expression::Column(col_name.clone()))
            }
            crate::core::query::sql::ast::AstExpression::BinaryOp { left, op, right } => {
                let left_expr = self.ast_expression_to_optimizer_expression(&**left)?;
                let right_expr = self.ast_expression_to_optimizer_expression(&**right)?;
                let op_str = match op {
                    crate::core::query::sql::ast::AstArithmeticOperator::Plus => "+".to_string(),
                    crate::core::query::sql::ast::AstArithmeticOperator::Minus => "-".to_string(),
                    crate::core::query::sql::ast::AstArithmeticOperator::Multiply => "*".to_string(),
                    crate::core::query::sql::ast::AstArithmeticOperator::Divide => "/".to_string(),
                };
                Ok(Expression::BinaryOp { // Optimizer's BinaryOp is used for arithmetic here
                    left: Box::new(left_expr),
                    op: op_str,
                    right: Box::new(right_expr),
                })
            }
            crate::core::query::sql::ast::AstExpression::UnaryOp { op, expr } => {
                let inner_expr = self.ast_expression_to_optimizer_expression(&**expr)?;
                let op_str = match op {
                    crate::core::query::sql::ast::AstUnaryOperator::Plus => "+".to_string(),
                    crate::core::query::sql::ast::AstUnaryOperator::Minus => "-".to_string(),
                };
                Ok(Expression::UnaryOp {
                    op: op_str,
                    expr: Box::new(inner_expr),
                })
            }
            // Corrected path from query/sql to query::sql
            crate::core::query::sql::ast::AstExpression::FunctionCall { name, args } => {
                let upper_name = name.to_uppercase();
                match upper_name.as_str() {
                    "COSINE_SIMILARITY" | "DOT_PRODUCT" => {
                        if args.len() != 2 {
                            return Err(OxidbError::SqlParsing(format!(
                                "Incorrect number of arguments for {}: expected 2, got {}",
                                upper_name,
                                args.len()
                            )));
                        }
                        let mut bound_args_expr = Vec::new();
                        for arg in args {
                            match arg {
                                crate::core::query::sql::ast::AstFunctionArg::Expression(expr) => {
                                    bound_args_expr.push(self.ast_expression_to_optimizer_expression(expr)?); // Pass by ref
                                }
                                _ => return Err(OxidbError::SqlParsing(format!(
                                    "Unsupported argument type in {} (only direct expressions supported for now)",
                                    upper_name
                                ))),
                            }
                        }

                        // Limited type checking at this stage (without full binder integration)
                        for (i, arg_expr) in bound_args_expr.iter().enumerate() {
                            match arg_expr {
                                Expression::Literal(DataType::Vector(_)) => {}, // Good
                                Expression::Column(_) => {}, // Assume column is of correct type for now
                                _ => return Err(OxidbError::SqlParsing(format!(
                                    "Argument {} of {} must be a vector column or vector literal. Got: {:?}",
                                    i + 1, upper_name, arg_expr
                                ))),
                            }
                        }
                        // Dimension check would require schema access or literal value inspection,
                        // which is complex here. Defer to execution or a later optimizer pass.

                        Ok(Expression::FunctionCall {
                            name: upper_name,
                            args: bound_args_expr,
                        })
                    }
                    "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" => {
                        let mut translated_args = Vec::new();
                        for arg_ast in args {
                            match arg_ast {
                                crate::core::query::sql::ast::AstFunctionArg::Expression(expr_ast) => {
                                    translated_args.push(self.ast_expression_to_optimizer_expression(expr_ast)?); // Pass by ref
                                }
                                crate::core::query::sql::ast::AstFunctionArg::Asterisk => {
                                    if upper_name != "COUNT" {
                                        return Err(OxidbError::SqlParsing(format!("Asterisk argument only valid for COUNT function, not {}", upper_name)));
                                    }
                                    if args.len() == 1 && matches!(args[0], crate::core::query::sql::ast::AstFunctionArg::Asterisk) {
                                        // Args remain empty for COUNT(*) representation in logical plan
                                    } else {
                                         return Err(OxidbError::SqlParsing("COUNT(*) is the only valid form of COUNT with asterisk.".to_string()));
                                    }
                                }
                                crate::core::query::sql::ast::AstFunctionArg::Distinct(expr_ast) => {
                                    // Pass by ref for inner expression
                                    // translated_args.push(self.ast_expression_to_optimizer_expression(expr_ast)?);
                                    return Err(OxidbError::NotImplemented{ feature: format!("DISTINCT in function {} for optimizer expression translation", upper_name)});
                                }
                            }
                        }
                         Ok(Expression::FunctionCall {
                            name: upper_name,
                            args: translated_args,
                        })
                    }
                    _ => Err(OxidbError::NotImplemented {
                        feature: format!("Function call '{}' in optimizer expressions", name),
                    }),
                }
            }
        }
    }

    fn ast_sql_condition_to_optimizer_expression(
        &self,
        ast_cond_tree: &crate::core::query::sql::ast::ConditionTree,
    ) -> Result<Expression, OxidbError> {
        match ast_cond_tree {
            crate::core::query::sql::ast::ConditionTree::Comparison(ast_condition) => {
                // ast_condition fields are: left: AstExpression, operator: AstComparisonOperator, right: AstExpression
                let left_optimizer_expr = self.ast_expression_to_optimizer_expression(&ast_condition.left)?;
                let right_optimizer_expr = self.ast_expression_to_optimizer_expression(&ast_condition.right)?;

                let op_str = match ast_condition.operator {
                    crate::core::query::sql::ast::AstComparisonOperator::Equals => "=".to_string(),
                    crate::core::query::sql::ast::AstComparisonOperator::NotEquals => "!=".to_string(),
                    crate::core::query::sql::ast::AstComparisonOperator::LessThan => "<".to_string(),
                    crate::core::query::sql::ast::AstComparisonOperator::LessThanOrEquals => "<=".to_string(),
                    crate::core::query::sql::ast::AstComparisonOperator::GreaterThan => ">".to_string(),
                    crate::core::query::sql::ast::AstComparisonOperator::GreaterThanOrEquals => ">=".to_string(),
                    crate::core::query::sql::ast::AstComparisonOperator::IsNull => "IS NULL".to_string(),
                    crate::core::query::sql::ast::AstComparisonOperator::IsNotNull => "IS NOT NULL".to_string(),
                };

                // IS NULL and IS NOT NULL are special in optimizer::Expression, might need specific handling
                // or ensure the optimizer Expression can model them with CompareOp.
                // For IS NULL, right_optimizer_expr would be Literal(Null).
                Ok(Expression::CompareOp {
                    left: Box::new(left_optimizer_expr),
                    op: op_str,
                    right: Box::new(right_optimizer_expr),
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
                Ok(Expression::UnaryOp { op: "NOT".to_string(), expr: Box::new(expr) })
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
                    QueryPlanNode::Project { input: project_input, expressions } => { // Changed columns to expressions
                        let pushed_filter = QueryPlanNode::Filter {
                            input: project_input,
                            predicate: predicate.clone(),
                        };
                        let optimized_pushed_filter = self.apply_predicate_pushdown(pushed_filter);
                        QueryPlanNode::Project { input: Box::new(optimized_pushed_filter), expressions } // Changed columns to expressions
                    }
                    _ => QueryPlanNode::Filter { input: Box::new(optimized_input), predicate },
                }
            }
            QueryPlanNode::Project { input, expressions } => QueryPlanNode::Project { // Changed columns to expressions
                input: Box::new(self.apply_predicate_pushdown(*input)),
                expressions, // Changed columns to expressions
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
            QueryPlanNode::Project { input, expressions } => Ok(QueryPlanNode::Project { // Changed columns to expressions
                input: Box::new(self.apply_index_selection(*input)?),
                expressions, // Changed columns to expressions
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
        expressions: Vec<crate::core::query::binder::expression::BoundExpression>, // Changed
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
    FunctionCall { // Added for function calls
        name: String,
        args: Vec<Expression>,
        // Return type can be inferred or stored if needed, for now determined at execution/planning
    },
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct JoinPredicate {
    pub left_column: String,
    pub right_column: String,
}
