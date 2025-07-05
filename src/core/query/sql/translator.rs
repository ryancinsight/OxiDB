use super::ast;
use crate::core::common::OxidbError; // Changed
use crate::core::query::commands::{self, Command};
use crate::core::types::{DataType, VectorData}; // Added VectorData

pub fn translate_ast_to_command(ast_statement: ast::Statement) -> Result<Command, OxidbError> {
    // Changed
    match ast_statement {
        ast::Statement::Select(select_ast) => {
            let columns_spec = translate_select_columns(select_ast.columns);
            let condition_cmd = match select_ast.condition {
                Some(cond_tree_ast) => {
                    Some(translate_condition_tree_to_sql_condition_tree(&cond_tree_ast)?)
                }
                None => None,
            };
            let order_by_cmd = match select_ast.order_by {
                Some(order_by_ast_list) => {
                    order_by_ast_list
                        .iter()
                        .map(translate_order_by_expr)
                        .collect::<Result<Vec<commands::SqlOrderByExpr>, OxidbError>>()
                        .map(Some)?
                }
                None => None,
            };

            let limit_cmd = match select_ast.limit {
                Some(ast::AstLiteralValue::Number(n_str)) => {
                    n_str.parse::<u64>().map(Some).map_err(|_| {
                        OxidbError::SqlParsing(format!(
                            "Invalid numeric literal '{}' for LIMIT clause",
                            n_str
                        ))
                    })?
                }
                Some(other_literal) => {
                    return Err(OxidbError::SqlParsing(format!(
                        "LIMIT clause expects a numeric literal, found {:?}",
                        other_literal
                    )));
                }
                None => None,
            };

            Ok(Command::Select {
                columns: columns_spec,
                source: select_ast.from_clause.name.clone(), // Changed from select_ast.source
                // Note: select_ast.joins is ignored here as Command::Select doesn't support it.
                condition: condition_cmd,
                order_by: order_by_cmd,
                limit: limit_cmd,
            })
        }
        ast::Statement::Update(update_ast) => {
            let assignments_cmd = update_ast
                .assignments
                .iter()
                .map(translate_assignment_to_sql_assignment)
                .collect::<Result<Vec<_>, _>>()?;
            let condition_cmd = match update_ast.condition {
                Some(cond_tree_ast) => {
                    Some(translate_condition_tree_to_sql_condition_tree(&cond_tree_ast)?)
                }
                None => None,
            };
            Ok(Command::Update {
                source: update_ast.source,
                assignments: assignments_cmd,
                condition: condition_cmd,
            })
        }
        ast::Statement::CreateTable(create_ast) => {
            let mut command_columns = Vec::new();
            for ast_col_def in create_ast.columns {
                let data_type = match ast_col_def.data_type {
                    ast::AstDataType::Integer => DataType::Integer(0), // Default value for schema
                    ast::AstDataType::Text => DataType::String("".to_string()),
                    ast::AstDataType::Boolean => DataType::Boolean(false),
                    ast::AstDataType::Float => DataType::Float(0.0),
                    ast::AstDataType::Blob => DataType::RawBytes(Vec::new()), // Assuming RawBytes is the engine type for Blob
                    ast::AstDataType::Vector { dimension } => {
                        // For schema definition, data is empty. Dimension is key.
                        crate::core::types::VectorData::new(dimension, vec![])
                            .map(DataType::Vector)
                            .ok_or_else(|| OxidbError::SqlParsing(format!(
                                "Invalid dimension {} for VECTOR type in CREATE TABLE (should not happen if parser validated > 0)",
                                dimension
                            )))?
                    } // Potentially other AstDataTypes if added
                                                                               // _ => return Err(OxidbError::SqlParsing(format!(
                                                                               //    "Unsupported AST column type during CREATE TABLE translation: {:?}",
                                                                               //    ast_col_def.data_type
                                                                               // ))),
                };

                let mut is_primary_key = false;
                let mut is_unique = false;
                let mut is_nullable = true; // Default to nullable

                for constraint in ast_col_def.constraints {
                    match constraint {
                        ast::AstColumnConstraint::PrimaryKey => {
                            is_primary_key = true;
                            is_unique = true; // Primary key implies unique
                            is_nullable = false; // Primary key implies not nullable
                        }
                        ast::AstColumnConstraint::Unique => {
                            is_unique = true;
                        }
                        ast::AstColumnConstraint::NotNull => {
                            is_nullable = false;
                        }
                    }
                }

                // If PrimaryKey was set, it already set is_nullable to false and is_unique to true.
                // If NotNull was set explicitly, is_nullable is false.
                // If Unique was set explicitly, is_unique is true.
                // This order of processing within the loop and then using the flags should be fine.

                command_columns.push(crate::core::types::schema::ColumnDef {
                    name: ast_col_def.name,
                    data_type,
                    is_primary_key,
                    is_unique,
                    is_nullable,
                });
            }
            Ok(Command::CreateTable { table_name: create_ast.table_name, columns: command_columns })
        }
        ast::Statement::Insert(insert_ast) => {
            let mut translated_values_list = Vec::new();
            for row_values_ast in insert_ast.values {
                let mut translated_row = Vec::new();
                for val_ast in row_values_ast {
                    translated_row.push(translate_literal(&val_ast)?);
                }
                translated_values_list.push(translated_row);
            }
            Ok(Command::SqlInsert {
                table_name: insert_ast.table_name,
                columns: insert_ast.columns,
                values: translated_values_list,
            })
        }
        ast::Statement::Delete(delete_stmt) => {
            let condition_cmd = match delete_stmt.condition {
                Some(cond_tree_ast) => {
                    Some(translate_condition_tree_to_sql_condition_tree(&cond_tree_ast)?)
                }
                None => None,
            };
            Ok(Command::SqlDelete { table_name: delete_stmt.table_name, condition: condition_cmd })
        }
        ast::Statement::DropTable(drop_stmt) => Ok(Command::DropTable {
            table_name: drop_stmt.table_name,
            if_exists: drop_stmt.if_exists,
        }),
    }
}

fn translate_order_by_expr(
    ast_expr: &ast::OrderByExpr,
) -> Result<commands::SqlOrderByExpr, OxidbError> {
    let direction = match ast_expr.direction {
        Some(ast::OrderDirection::Asc) => Some(commands::SqlOrderDirection::Asc),
        Some(ast::OrderDirection::Desc) => Some(commands::SqlOrderDirection::Desc),
        None => None,
    };

    // Attempt to simplify or stringify the AstExpression for the command layer
    let expr_str = match &ast_expr.expression {
        ast::AstExpression::ColumnIdentifier(name) => name.clone(),
        // TODO: More robust stringification or structured representation if command layer evolves
        _ => format!("{:?}", ast_expr.expression), // Fallback to debug string
    };

    Ok(commands::SqlOrderByExpr {
        expression: expr_str,
        direction,
    })
}

// Helper function to convert DataType back to AstLiteralValue (subset)
// This is needed for reconstructing AST parts in the executor for now.
pub fn translate_datatype_to_ast_literal(
    data_type: &DataType,
) -> Result<ast::AstLiteralValue, OxidbError> {
    match data_type {
        DataType::String(s) => Ok(ast::AstLiteralValue::String(s.clone())),
        DataType::Integer(i) => Ok(ast::AstLiteralValue::Number(i.to_string())),
        DataType::Float(f) => Ok(ast::AstLiteralValue::Number(f.to_string())),
        DataType::Boolean(b) => Ok(ast::AstLiteralValue::Boolean(*b)),
        DataType::Null => Ok(ast::AstLiteralValue::Null),
        DataType::RawBytes(bytes) => Ok(ast::AstLiteralValue::String(hex::encode(bytes))),
        DataType::Map(_) | DataType::JsonBlob(_) => Err(OxidbError::SqlParsing(
            "Cannot translate complex DataType (Map/JsonBlob) to simple AST literal for conditions.".to_string(),
        )),
        DataType::Vector(_) => todo!("Handle DataType::Vector in translate_datatype_to_ast_literal"),
    }
}

fn translate_literal(literal: &ast::AstLiteralValue) -> Result<DataType, OxidbError> {
    // Changed
    match literal {
        ast::AstLiteralValue::String(s) => Ok(DataType::String(s.clone())),
        ast::AstLiteralValue::Number(n_str) => {
            if let Ok(i_val) = n_str.parse::<i64>() {
                Ok(DataType::Integer(i_val))
            } else if let Ok(f_val) = n_str.parse::<f64>() {
                Ok(DataType::Float(f_val))
            } else {
                Err(OxidbError::SqlParsing(format!("Cannot parse numeric literal '{}'", n_str)))
                // Changed
            }
        }
        ast::AstLiteralValue::Boolean(b) => Ok(DataType::Boolean(*b)),
        ast::AstLiteralValue::Null => Ok(DataType::Null),
        ast::AstLiteralValue::Vector(elements_ast) => {
            let mut float_elements = Vec::with_capacity(elements_ast.len());
            for el_ast in elements_ast {
                match translate_literal(el_ast)? {
                    DataType::Integer(i) => float_elements.push(i as f32),
                    DataType::Float(f) => float_elements.push(f as f32),
                    // DataType::Number(s) => { // If translate_literal returned Number variant
                    //    match s.parse::<f32>() {
                    //        Ok(f) => float_elements.push(f),
                    //        Err(_) => return Err(OxidbError::SqlParsing(format!(
                    //            "Invalid numeric string '{}' in vector literal", s
                    //        ))),
                    //    }
                    // }
                    other_type => {
                        return Err(OxidbError::SqlParsing(format!(
                        "Vector literal elements must be numbers, found type {:?} (value: {:?})",
                        other_type.type_name(), other_type
                    )))
                    }
                }
            }
            let dimension = float_elements.len() as u32;
            // VectorData::new performs validation if dimension matches data length,
            // which it will by construction here.
            VectorData::new(dimension, float_elements)
                .map(DataType::Vector)
                .ok_or_else(|| OxidbError::SqlParsing(
                    "Failed to create VectorData from parsed elements (dimension mismatch, should not happen here)".to_string()
                ))
        }
    }
}

// Helper to attempt to simplify an AstExpression to a simple column name string
fn try_ast_expression_to_column_name(ast_expr: &ast::AstExpression) -> Option<String> {
    if let ast::AstExpression::ColumnIdentifier(name) = ast_expr {
        Some(name.clone())
    } else {
        None // Cannot simplify to a simple column name
    }
}

// Helper to attempt to simplify an AstExpression to a DataType literal
fn try_ast_expression_to_datatype_literal(ast_expr: &ast::AstExpression) -> Result<Option<DataType>, OxidbError> {
    if let ast::AstExpression::Literal(literal_val) = ast_expr {
        translate_literal(literal_val).map(Some)
    } else {
        Ok(None) // Not a simple literal
    }
}


// Updated to translate ConditionTree with new AstExpression fields
fn translate_condition_tree_to_sql_condition_tree(
    ast_tree: &ast::ConditionTree,
) -> Result<commands::SqlConditionTree, OxidbError> {
    match ast_tree {
        ast::ConditionTree::Comparison(ast_condition) => { // ast_condition is now ast::Condition
            let op_str = match ast_condition.operator {
                ast::AstComparisonOperator::Equals => "=".to_string(),
                ast::AstComparisonOperator::NotEquals => "!=".to_string(), // Assuming translator uses != for NotEquals
                ast::AstComparisonOperator::LessThan => "<".to_string(),
                ast::AstComparisonOperator::LessThanOrEquals => "<=".to_string(),
                ast::AstComparisonOperator::GreaterThan => ">".to_string(),
                ast::AstComparisonOperator::GreaterThanOrEquals => ">=".to_string(),
                ast::AstComparisonOperator::IsNull => "IS NULL".to_string(),
                ast::AstComparisonOperator::IsNotNull => "IS NOT NULL".to_string(),
            };

            // For SqlSimpleCondition, we need a column name on one side and a literal on the other.
            // Handle IS NULL / IS NOT NULL first
            if ast_condition.operator == ast::AstComparisonOperator::IsNull || ast_condition.operator == ast::AstComparisonOperator::IsNotNull {
                if let Some(column_name) = try_ast_expression_to_column_name(&ast_condition.left) {
                    // The 'right' side of IS NULL/IS NOT NULL is AstExpression::Literal(AstLiteralValue::Null) by parser convention
                    // commands::SqlSimpleCondition expects a DataType value. For "IS NULL", this value is typically DataType::Null.
                    Ok(commands::SqlConditionTree::Comparison(commands::SqlSimpleCondition {
                        column: column_name,
                        operator: op_str,
                        value: DataType::Null, // This is consistent with how IS NULL is often handled
                    }))
                } else {
                    Err(OxidbError::SqlParsing(format!(
                        "Left-hand side of {} operator must be a column name.", op_str
                    )))
                }
            } else {
                // Attempt to make it column <op> literal
                if let Some(column_name) = try_ast_expression_to_column_name(&ast_condition.left) {
                    if let Some(literal_value) = try_ast_expression_to_datatype_literal(&ast_condition.right)? {
                        Ok(commands::SqlConditionTree::Comparison(commands::SqlSimpleCondition {
                            column: column_name,
                            operator: op_str,
                            value: literal_value,
                        }))
                    } else {
                        Err(OxidbError::SqlParsing(format!(
                            "Right-hand side of comparison for column '{}' must be a literal. Found complex expression: {:?}",
                            column_name, ast_condition.right
                        )))
                    }
                }
                // Attempt to make it literal <op> column (and swap)
                else if let Some(column_name) = try_ast_expression_to_column_name(&ast_condition.right) {
                    if let Some(literal_value) = try_ast_expression_to_datatype_literal(&ast_condition.left)? {
                        let reversed_op_str = match ast_condition.operator {
                            ast::AstComparisonOperator::LessThan => ">".to_string(),
                            ast::AstComparisonOperator::LessThanOrEquals => ">=".to_string(),
                            ast::AstComparisonOperator::GreaterThan => "<".to_string(),
                            ast::AstComparisonOperator::GreaterThanOrEquals => "<=".to_string(),
                            // Equals and NotEquals are symmetric
                            _ => op_str,
                        };
                        Ok(commands::SqlConditionTree::Comparison(commands::SqlSimpleCondition {
                            column: column_name,
                            operator: reversed_op_str,
                            value: literal_value,
                        }))
                    } else {
                        Err(OxidbError::SqlParsing(format!(
                            "Left-hand side of comparison with column '{}' must be a literal. Found complex expression: {:?}",
                            column_name, ast_condition.left
                        )))
                    }
                }
                // Neither side could be simplified to a column name for SqlSimpleCondition with a literal on the other.
                else {
                    Err(OxidbError::SqlParsing(
                        "Comparison conditions must involve one column name and one literal for current translation capabilities.".to_string()
                    ))
                }
            }
        }
        ast::ConditionTree::And(left_ast, right_ast) => {
            let left_sql = translate_condition_tree_to_sql_condition_tree(left_ast)?;
            let right_sql = translate_condition_tree_to_sql_condition_tree(right_ast)?;
            Ok(commands::SqlConditionTree::And(Box::new(left_sql), Box::new(right_sql)))
        }
        ast::ConditionTree::Or(left_ast, right_ast) => {
            let left_sql = translate_condition_tree_to_sql_condition_tree(left_ast)?;
            let right_sql = translate_condition_tree_to_sql_condition_tree(right_ast)?;
            Ok(commands::SqlConditionTree::Or(Box::new(left_sql), Box::new(right_sql)))
        }
        ast::ConditionTree::Not(ast_cond) => {
            let sql_cond = translate_condition_tree_to_sql_condition_tree(ast_cond)?;
            Ok(commands::SqlConditionTree::Not(Box::new(sql_cond)))
        }
    }
}

fn translate_assignment_to_sql_assignment(
    ast_assignment: &ast::Assignment,
) -> Result<commands::SqlAssignment, OxidbError> {
    // The value in ast::Assignment is now an AstExpression.
    // commands::SqlAssignment expects a DataType.
    // We can only translate if the AstExpression is a literal.
    if let Some(data_type_value) = try_ast_expression_to_datatype_literal(&ast_assignment.value)? {
        Ok(commands::SqlAssignment {
            column: ast_assignment.column.clone(),
            value: data_type_value,
        })
    } else {
        Err(OxidbError::SqlParsing(format!(
            "Right-hand side of assignment for column '{}' must be a literal for current translation capabilities. Found: {:?}",
            ast_assignment.column, ast_assignment.value
        )))
    }
}

fn translate_select_columns(ast_columns: Vec<ast::SelectColumn>) -> commands::SelectColumnSpec {
    if ast_columns.iter().any(|col| matches!(col, ast::SelectColumn::Asterisk)) {
        return commands::SelectColumnSpec::All;
    }

    let specific_columns: Vec<String> = ast_columns
        .into_iter()
        .filter_map(|col_enum| match col_enum {
            ast::SelectColumn::Expression(expr) => {
                // Attempt to stringify the expression for the command layer
                // This is a simplification; command layer might need richer info later.
                match expr {
                    ast::AstExpression::ColumnIdentifier(name) => Some(name),
                    ast::AstExpression::FunctionCall { name, args } => {
                        let args_str: Vec<String> = args.iter().map(|arg| {
                            match arg {
                                ast::AstFunctionArg::Asterisk => "*".to_string(),
                                ast::AstFunctionArg::Expression(e) => format!("{:?}", e), // Debug format for now
                                ast::AstFunctionArg::Distinct(e_box) => format!("DISTINCT {:?}", e_box), // Debug format
                            }
                        }).collect();
                        Some(format!("{}({})", name, args_str.join(", ")))
                    }
                    // For other AstExpression types (Literal, BinaryOp, UnaryOp),
                    // translating them to a single string for SelectColumnSpec::Specific might not be ideal.
                    // For now, let's return their debug representation or error.
                    // This part of the translator might need more sophisticated handling if complex expressions
                    // in SELECT list need to be passed to the command layer in a structured way.
                    // For now, we'll allow stringification via debug for simplicity in this step.
                    _ => Some(format!("{:?}", expr)), // Fallback to debug string
                }
            }
            ast::SelectColumn::Asterisk => None, // Asterisk presence handled by the initial check
        })
        .collect();

    if specific_columns.is_empty() {
        commands::SelectColumnSpec::All
    } else {
        commands::SelectColumnSpec::Specific(specific_columns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::DataType;

    // Use aliased imports for ast items used in tests to avoid conflict with `super::ast`
    use crate::core::query::sql::ast::{
        Assignment as TestAssignment, AstLiteralValue as TestAstLiteralValue,
        Condition as TestCondition, SelectColumn as TestSelectColumn,
        SelectStatement as TestSelectStatement, Statement as TestStatement,
        UpdateStatement as TestUpdateStatement,
    };

    #[test]
    fn test_translate_literal_string() {
        let ast_literal = TestAstLiteralValue::String("hello".to_string());
        assert!(matches!(translate_literal(&ast_literal), Ok(DataType::String(s)) if s == "hello"));
    }

    #[test]
    fn test_translate_literal_integer() {
        let ast_literal = TestAstLiteralValue::Number("123".to_string());
        assert!(matches!(translate_literal(&ast_literal), Ok(DataType::Integer(123))));
    }

    #[test]
    fn test_translate_literal_float() {
        let ast_literal = TestAstLiteralValue::Number("123.45".to_string());
        assert!(
            matches!(translate_literal(&ast_literal), Ok(DataType::Float(f)) if (f - 123.45).abs() < f64::EPSILON)
        );
    }

    #[test]
    fn test_translate_literal_negative_integer() {
        let ast_literal = TestAstLiteralValue::Number("-50".to_string());
        assert!(matches!(translate_literal(&ast_literal), Ok(DataType::Integer(-50))));
    }

    #[test]
    fn test_translate_literal_negative_float() {
        let ast_literal = TestAstLiteralValue::Number("-50.75".to_string());
        assert!(
            matches!(translate_literal(&ast_literal), Ok(DataType::Float(f)) if (f - -50.75).abs() < f64::EPSILON)
        );
    }

    #[test]
    fn test_translate_literal_invalid_number() {
        let ast_literal = TestAstLiteralValue::Number("abc".to_string());
        match translate_literal(&ast_literal) {
            Err(OxidbError::SqlParsing(msg)) => {
                // Changed
                assert!(msg.contains("Cannot parse numeric literal 'abc'"));
            }
            _ => panic!("Expected SqlParsing error for unparsable number string."), // Changed
        }
    }

    #[test]
    fn test_translate_literal_number_with_alpha_suffix() {
        let ast_literal = TestAstLiteralValue::Number("123xyz".to_string());
        match translate_literal(&ast_literal) {
            Err(OxidbError::SqlParsing(msg)) => {
                // Changed
                assert!(msg.contains("Cannot parse numeric literal '123xyz'"));
            }
            _ => panic!("Expected SqlParsing error for unparsable number string."), // Changed
        }
    }

    #[test]
    fn test_translate_literal_boolean_true() {
        let ast_literal = TestAstLiteralValue::Boolean(true);
        assert!(matches!(translate_literal(&ast_literal), Ok(DataType::Boolean(true))));
    }

    #[test]
    fn test_translate_literal_boolean_false() {
        let ast_literal = TestAstLiteralValue::Boolean(false);
        assert!(matches!(translate_literal(&ast_literal), Ok(DataType::Boolean(false))));
    }

    #[test]
    fn test_translate_literal_null() {
        let ast_literal = TestAstLiteralValue::Null;
        assert!(matches!(translate_literal(&ast_literal), Ok(DataType::Null)));
    }

    #[test]
    fn test_translate_condition_simple_equals() {
        let ast_cond_tree = ast::ConditionTree::Comparison(TestCondition {
            left: ast::AstExpression::ColumnIdentifier("name".to_string()),
            operator: ast::AstComparisonOperator::Equals,
            right: ast::AstExpression::Literal(TestAstLiteralValue::String(
                "test_user".to_string(),
            )),
        });
        let expected_sql_cond_tree =
            commands::SqlConditionTree::Comparison(commands::SqlSimpleCondition {
                column: "name".to_string(),
                operator: "=".to_string(),
                value: DataType::String("test_user".to_string()),
            });
        match translate_condition_tree_to_sql_condition_tree(&ast_cond_tree) {
            Ok(res_cond_tree) => assert_eq!(res_cond_tree, expected_sql_cond_tree),
            Err(e) => panic!("Translation failed: {:?}", e),
        }
    }

    #[test]
    fn test_translate_condition_with_numeric_value() {
        let ast_cond_tree = ast::ConditionTree::Comparison(TestCondition {
            left: ast::AstExpression::ColumnIdentifier("age".to_string()),
            operator: ast::AstComparisonOperator::GreaterThan,
            right: ast::AstExpression::Literal(TestAstLiteralValue::Number("30".to_string())),
        });
        let expected_sql_cond_tree =
            commands::SqlConditionTree::Comparison(commands::SqlSimpleCondition {
                column: "age".to_string(),
                operator: ">".to_string(),
                value: DataType::Integer(30),
            });
        match translate_condition_tree_to_sql_condition_tree(&ast_cond_tree) {
            Ok(res_cond_tree) => assert_eq!(res_cond_tree, expected_sql_cond_tree),
            Err(e) => panic!("Translation failed: {:?}", e),
        }
    }

    #[test]
    fn test_translate_assignment_string() {
        let ast_assign = TestAssignment {
            column: "email".to_string(),
            value: ast::AstExpression::Literal(TestAstLiteralValue::String("new@example.com".to_string())),
        };
        let expected_sql_assign = commands::SqlAssignment {
            column: "email".to_string(),
            value: DataType::String("new@example.com".to_string()),
        };
        assert!(
            matches!(translate_assignment_to_sql_assignment(&ast_assign), Ok(ref res_assign) if *res_assign == expected_sql_assign)
        );
    }

    #[test]
    fn test_translate_assignment_boolean() {
        let ast_assign = TestAssignment {
            column: "is_active".to_string(),
            value: ast::AstExpression::Literal(TestAstLiteralValue::Boolean(true)),
        };
        let expected_sql_assign = commands::SqlAssignment {
            column: "is_active".to_string(),
            value: DataType::Boolean(true),
        };
        assert!(
            matches!(translate_assignment_to_sql_assignment(&ast_assign), Ok(ref res_assign) if *res_assign == expected_sql_assign)
        );
    }

    #[test]
    fn test_translate_select_columns_all() {
        let ast_cols = vec![TestSelectColumn::Asterisk];
        assert_eq!(translate_select_columns(ast_cols), commands::SelectColumnSpec::All);
    }

    #[test]
    fn test_translate_select_columns_specific() {
        let ast_cols = vec![
            TestSelectColumn::Expression(ast::AstExpression::ColumnIdentifier("id".to_string())),
            TestSelectColumn::Expression(ast::AstExpression::ColumnIdentifier("name".to_string())),
        ];
        let expected_spec =
            commands::SelectColumnSpec::Specific(vec!["id".to_string(), "name".to_string()]);
        assert_eq!(translate_select_columns(ast_cols), expected_spec);
    }

    #[test]
    fn test_translate_select_columns_specific_with_asterisk_first() {
        let ast_cols =
            vec![TestSelectColumn::Asterisk, TestSelectColumn::Expression(ast::AstExpression::ColumnIdentifier("id".to_string()))];
        assert_eq!(translate_select_columns(ast_cols), commands::SelectColumnSpec::All);
    }

    #[test]
    fn test_translate_select_columns_specific_with_asterisk_last() {
        let ast_cols =
            vec![TestSelectColumn::Expression(ast::AstExpression::ColumnIdentifier("id".to_string())), TestSelectColumn::Asterisk];
        assert_eq!(translate_select_columns(ast_cols), commands::SelectColumnSpec::All);
    }

    #[test]
    fn test_translate_select_columns_empty() {
        let ast_cols = vec![];
        assert_eq!(translate_select_columns(ast_cols), commands::SelectColumnSpec::All);
    }

    #[test]
    fn test_translate_ast_select_simple() {
        let ast_stmt = TestStatement::Select(TestSelectStatement {
            distinct: false, // Added default
            columns: vec![TestSelectColumn::Asterisk],
            from_clause: ast::TableReference {
                name: "users".to_string(),
                alias: None,
            },
            joins: Vec::new(),
            condition: None,
            group_by: None, // Added default
            having: None,   // Added default
            order_by: None, // Added
            limit: None,    // Added
        });
        let command = translate_ast_to_command(ast_stmt).unwrap();
        match command {
            Command::Select { columns, source, condition, order_by, limit } => {
                // Added
                assert_eq!(columns, commands::SelectColumnSpec::All);
                assert_eq!(source, "users");
                assert!(condition.is_none());
                assert!(order_by.is_none()); // Added
                assert!(limit.is_none()); // Added
            }
            _ => panic!("Expected Command::Select"),
        }
    }

    #[test]
    fn test_translate_ast_select_with_condition() {
        let ast_stmt = TestStatement::Select(TestSelectStatement {
            distinct: false, // Added default
            columns: vec![TestSelectColumn::Expression(ast::AstExpression::ColumnIdentifier("email".to_string()))],
            from_clause: ast::TableReference {
                name: "customers".to_string(),
                alias: None,
            },
            joins: Vec::new(),
            condition: Some(ast::ConditionTree::Comparison(TestCondition {
                left: ast::AstExpression::ColumnIdentifier("id".to_string()),
                operator: ast::AstComparisonOperator::Equals,
                right: ast::AstExpression::Literal(TestAstLiteralValue::Number(
                    "101".to_string(),
                )),
            })),
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
        });
        let command = translate_ast_to_command(ast_stmt).unwrap();
        match command {
            Command::Select { columns, source, condition, order_by, limit } => {
                assert_eq!(
                    columns,
                    commands::SelectColumnSpec::Specific(vec!["email".to_string()])
                );
                assert_eq!(source, "customers");
                assert!(condition.is_some());
                if let Some(commands::SqlConditionTree::Comparison(simple_cond)) = condition {
                    assert_eq!(simple_cond.column, "id");
                    assert_eq!(simple_cond.operator, "=");
                    assert_eq!(simple_cond.value, DataType::Integer(101));
                } else {
                    panic!("Expected Comparison condition tree variant");
                }
                assert!(order_by.is_none());
                assert!(limit.is_none());
            }
            _ => panic!("Expected Command::Select"),
        }
    }

    #[test]
    fn test_translate_ast_update_simple() {
        let ast_stmt = TestStatement::Update(TestUpdateStatement {
            source: "products".to_string(),
            assignments: vec![TestAssignment {
                column: "price".to_string(),
                value: ast::AstExpression::Literal(TestAstLiteralValue::Number("19.99".to_string())),
            }],
            condition: Some(ast::ConditionTree::Comparison(TestCondition {
                left: ast::AstExpression::ColumnIdentifier("product_id".to_string()),
                operator: ast::AstComparisonOperator::Equals,
                right: ast::AstExpression::Literal(TestAstLiteralValue::String(
                    "XYZ123".to_string(),
                )),
            })),
        });
        let command = translate_ast_to_command(ast_stmt).unwrap();
        match command {
            Command::Update { source, assignments, condition } => {
                assert_eq!(source, "products");
                assert_eq!(assignments.len(), 1);
                assert_eq!(assignments[0].column, "price");
                assert_eq!(assignments[0].value, DataType::Float(19.99));
                assert!(condition.is_some());
                if let Some(commands::SqlConditionTree::Comparison(simple_cond)) = condition {
                    assert_eq!(simple_cond.column, "product_id");
                    assert_eq!(simple_cond.value, DataType::String("XYZ123".to_string()));
                } else {
                    panic!("Expected Comparison condition tree variant for Update");
                }
            }
            _ => panic!("Expected Command::Update"),
        }
    }
}
