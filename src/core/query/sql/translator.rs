use super::ast;
use crate::core::common::OxidbError; // Changed
use crate::core::query::commands::{self, Command};
use crate::core::types::DataType;

pub fn translate_ast_to_command(ast_statement: ast::Statement) -> Result<Command, OxidbError> {
    // Changed
    match ast_statement {
        ast::Statement::Select(select_ast) => {
            let columns_spec = translate_select_columns(select_ast.columns);
            let condition_cmd = match select_ast.condition {
                Some(cond_ast) => Some(translate_condition_to_sql_condition(&cond_ast)?),
                None => None,
            };
            Ok(Command::Select {
                columns: columns_spec,
                source: select_ast.source,
                condition: condition_cmd,
            })
        }
        ast::Statement::Update(update_ast) => {
            let assignments_cmd = update_ast
                .assignments
                .iter()
                .map(translate_assignment_to_sql_assignment)
                .collect::<Result<Vec<_>, _>>()?;
            let condition_cmd = match update_ast.condition {
                Some(cond_ast) => Some(translate_condition_to_sql_condition(&cond_ast)?),
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
                // Basic type mapping, can be expanded
                // This mapping should align with types supported by DataType and schema system
                let uppercase_type_str = ast_col_def.data_type.to_uppercase();
                let data_type = if uppercase_type_str.starts_with("INTEGER")
                    || uppercase_type_str.starts_with("INT")
                {
                    DataType::Integer(0)
                } else if uppercase_type_str.starts_with("TEXT")
                    || uppercase_type_str.starts_with("STRING")
                    || uppercase_type_str.starts_with("VARCHAR")
                {
                    DataType::String("".to_string())
                } else if uppercase_type_str.starts_with("BOOLEAN")
                    || uppercase_type_str.starts_with("BOOL")
                {
                    DataType::Boolean(false)
                } else if uppercase_type_str.starts_with("FLOAT")
                    || uppercase_type_str.starts_with("REAL")
                    || uppercase_type_str.starts_with("DOUBLE")
                {
                    DataType::Float(0.0)
                } else {
                    return Err(OxidbError::SqlParsing(format!(
                        "Unsupported column type during CREATE TABLE translation: {}",
                        ast_col_def.data_type
                    )));
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
                Some(cond_ast) => Some(translate_condition_to_sql_condition(&cond_ast)?),
                None => None,
            };
            Ok(Command::SqlDelete { table_name: delete_stmt.table_name, condition: condition_cmd })
        }
    }
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
    }
}

fn translate_condition_to_sql_condition(
    ast_condition: &ast::Condition,
) -> Result<commands::SqlCondition, OxidbError> {
    // Changed
    let value = translate_literal(&ast_condition.value)?;
    Ok(commands::SqlCondition {
        column: ast_condition.column.clone(),
        operator: ast_condition.operator.clone(),
        value,
    })
}

fn translate_assignment_to_sql_assignment(
    ast_assignment: &ast::Assignment,
) -> Result<commands::SqlAssignment, OxidbError> {
    // Changed
    let value = translate_literal(&ast_assignment.value)?;
    Ok(commands::SqlAssignment { column: ast_assignment.column.clone(), value })
}

fn translate_select_columns(ast_columns: Vec<ast::SelectColumn>) -> commands::SelectColumnSpec {
    if ast_columns.iter().any(|col| matches!(col, ast::SelectColumn::Asterisk)) {
        return commands::SelectColumnSpec::All;
    }

    let specific_columns: Vec<String> = ast_columns
        .into_iter()
        .filter_map(|col| match col {
            ast::SelectColumn::ColumnName(name) => Some(name),
            ast::SelectColumn::Asterisk => None,
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
        let ast_cond = TestCondition {
            // Using aliased TestCondition
            column: "name".to_string(),
            operator: "=".to_string(),
            value: TestAstLiteralValue::String("test_user".to_string()),
        };
        let expected_sql_cond = commands::SqlCondition {
            column: "name".to_string(),
            operator: "=".to_string(),
            value: DataType::String("test_user".to_string()),
        };
        assert!(
            matches!(translate_condition_to_sql_condition(&ast_cond), Ok(ref res_cond) if *res_cond == expected_sql_cond)
        );
    }

    #[test]
    fn test_translate_condition_with_numeric_value() {
        let ast_cond = TestCondition {
            // Using aliased TestCondition
            column: "age".to_string(),
            operator: ">".to_string(),
            value: TestAstLiteralValue::Number("30".to_string()),
        };
        let expected_sql_cond = commands::SqlCondition {
            column: "age".to_string(),
            operator: ">".to_string(),
            value: DataType::Integer(30),
        };
        assert!(
            matches!(translate_condition_to_sql_condition(&ast_cond), Ok(ref res_cond) if *res_cond == expected_sql_cond)
        );
    }

    #[test]
    fn test_translate_assignment_string() {
        let ast_assign = TestAssignment {
            column: "email".to_string(),
            value: TestAstLiteralValue::String("new@example.com".to_string()),
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
            value: TestAstLiteralValue::Boolean(true),
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
            TestSelectColumn::ColumnName("id".to_string()),
            TestSelectColumn::ColumnName("name".to_string()),
        ];
        let expected_spec =
            commands::SelectColumnSpec::Specific(vec!["id".to_string(), "name".to_string()]);
        assert_eq!(translate_select_columns(ast_cols), expected_spec);
    }

    #[test]
    fn test_translate_select_columns_specific_with_asterisk_first() {
        let ast_cols =
            vec![TestSelectColumn::Asterisk, TestSelectColumn::ColumnName("id".to_string())];
        assert_eq!(translate_select_columns(ast_cols), commands::SelectColumnSpec::All);
    }

    #[test]
    fn test_translate_select_columns_specific_with_asterisk_last() {
        let ast_cols =
            vec![TestSelectColumn::ColumnName("id".to_string()), TestSelectColumn::Asterisk];
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
            columns: vec![TestSelectColumn::Asterisk],
            source: "users".to_string(),
            condition: None,
        });
        let command = translate_ast_to_command(ast_stmt).unwrap();
        match command {
            Command::Select { columns, source, condition } => {
                assert_eq!(columns, commands::SelectColumnSpec::All);
                assert_eq!(source, "users");
                assert!(condition.is_none());
            }
            _ => panic!("Expected Command::Select"),
        }
    }

    #[test]
    fn test_translate_ast_select_with_condition() {
        let ast_stmt = TestStatement::Select(TestSelectStatement {
            columns: vec![TestSelectColumn::ColumnName("email".to_string())],
            source: "customers".to_string(),
            condition: Some(TestCondition {
                column: "id".to_string(),
                operator: "=".to_string(),
                value: TestAstLiteralValue::Number("101".to_string()),
            }),
        });
        let command = translate_ast_to_command(ast_stmt).unwrap();
        match command {
            Command::Select { columns, source, condition } => {
                assert_eq!(
                    columns,
                    commands::SelectColumnSpec::Specific(vec!["email".to_string()])
                );
                assert_eq!(source, "customers");
                assert!(condition.is_some());
                let cond_val = condition.unwrap();
                assert_eq!(cond_val.column, "id");
                assert_eq!(cond_val.operator, "=");
                assert_eq!(cond_val.value, DataType::Integer(101));
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
                value: TestAstLiteralValue::Number("19.99".to_string()),
            }],
            condition: Some(TestCondition {
                column: "product_id".to_string(),
                operator: "=".to_string(),
                value: TestAstLiteralValue::String("XYZ123".to_string()),
            }),
        });
        let command = translate_ast_to_command(ast_stmt).unwrap();
        match command {
            Command::Update { source, assignments, condition } => {
                assert_eq!(source, "products");
                assert_eq!(assignments.len(), 1);
                assert_eq!(assignments[0].column, "price");
                assert_eq!(assignments[0].value, DataType::Float(19.99));
                assert!(condition.is_some());
                let cond_val = condition.unwrap();
                assert_eq!(cond_val.column, "product_id");
                assert_eq!(cond_val.value, DataType::String("XYZ123".to_string()));
            }
            _ => panic!("Expected Command::Update"),
        }
    }
}
