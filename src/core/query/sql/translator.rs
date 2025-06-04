use crate::core::common::error::DbError;
use crate::core::query::commands::{self, Command}; // Removed SelectColumnSpec, SqlCondition, SqlAssignment
use crate::core::types::DataType;
use super::ast;

// Main translation function
pub fn translate_ast_to_command(ast_statement: ast::Statement) -> Result<Command, DbError> {
    // Placeholder for now
    match ast_statement {
        ast::Statement::Select(select_ast) => {
            let columns = translate_select_columns(select_ast.columns);
            let condition = match select_ast.condition {
                Some(cond_ast) => Some(translate_condition(&cond_ast)?),
                None => None,
            };
            Ok(Command::Select {
                columns,
                source: select_ast.source,
                condition,
            })
        }
        ast::Statement::Update(update_ast) => {
            let assignments = update_ast
                .assignments
                .iter()
                .map(translate_assignment)
                .collect::<Result<Vec<_>, _>>()?;
            let condition = match update_ast.condition {
                Some(cond_ast) => Some(translate_condition(&cond_ast)?),
                None => None,
            };
            Ok(Command::Update {
                source: update_ast.source,
                assignments,
                condition,
            })
        }
        // Add other statement types like Insert, Delete if they get added to ast::Statement
        // _ => Err(DbError::NotImplemented(format!("Translation for AST statement type not implemented."))),
    }
}

// Helper function to translate AST literal values to DataType
fn translate_literal(literal: &ast::AstLiteralValue) -> Result<DataType, DbError> {
    match literal {
        ast::AstLiteralValue::String(s) => Ok(DataType::String(s.clone())),
        ast::AstLiteralValue::Number(n_str) => {
            // Try to parse as i64, then f64, then fallback to String if all fail.
            // A more robust solution would involve a dedicated Number type or more sophisticated parsing.
            if let Ok(i_val) = n_str.parse::<i64>() {
                Ok(DataType::Integer(i_val))
            } else if let Ok(f_val) = n_str.parse::<f64>() {
                Ok(DataType::Float(f_val)) // Assuming DataType::Float exists
            } else {
                // Fallback or error - for now, let's error if it looks like a number but doesn't parse.
                // Alternatively, could treat as DataType::String(n_str.clone()) if that's desired.
                Err(DbError::InvalidQuery(format!(
                    "Cannot parse numeric literal '{}' into a known number type (i64, f64).",
                    n_str
                )))
            }
        }
        ast::AstLiteralValue::Boolean(b) => Ok(DataType::Boolean(*b)),
        ast::AstLiteralValue::Null => Ok(DataType::Null),
    }
}

// Placeholder for other translation functions
fn translate_condition(ast_condition: &ast::Condition) -> Result<commands::SqlCondition, DbError> {
    let value = translate_literal(&ast_condition.value)?;
    Ok(commands::SqlCondition {
        column: ast_condition.column.clone(),
        operator: ast_condition.operator.clone(),
        value,
    })
}

fn translate_assignment(ast_assignment: &ast::Assignment) -> Result<commands::SqlAssignment, DbError> {
    let value = translate_literal(&ast_assignment.value)?;
    Ok(commands::SqlAssignment {
        column: ast_assignment.column.clone(),
        value,
    })
}

fn translate_select_columns(ast_columns: Vec<ast::SelectColumn>) -> commands::SelectColumnSpec {
    if ast_columns.iter().any(|col| match col {
        ast::SelectColumn::Asterisk => true,
        _ => false,
    }) {
        // If any column is Asterisk, it implies SELECT *
        // SQL parser should ideally ensure that if * is present, it's the only "column"
        // or handle semantic validation. For translation, this is a safe assumption.
        return commands::SelectColumnSpec::All;
    }

    let specific_columns: Vec<String> = ast_columns
        .into_iter()
        .filter_map(|col| match col {
            ast::SelectColumn::ColumnName(name) => Some(name),
            ast::SelectColumn::Asterisk => None, // Should have been caught above
        })
        .collect();

    if specific_columns.is_empty() {
        // This case should ideally be prevented by the parser (e.g., SELECT FROM table).
        // However, if it occurs, treat as Select All or define specific error.
        // For now, let's assume parser ensures non-empty column list if not *.
        // If specific_columns is empty and there was no Asterisk, it's like SELECT FROM table.
        // Depending on SQL dialect/strictness, this could be an error or implies all columns.
        // Let's default to All if somehow this state is reached, though parser should prevent it.
        // A stricter approach would be to return an error or ensure parser never allows this.
        // For now, if specific_columns is empty and no asterisk was found, it's an anomaly.
        // But to be safe and simple for translation:
        commands::SelectColumnSpec::All // Or perhaps an error: DbError::InvalidAst("Empty column list without asterisk")
    } else {
        commands::SelectColumnSpec::Specific(specific_columns)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::DataType;
    use super::super::ast; // To access ast types for test setup

    #[test]
    fn test_translate_literal_string() {
        let ast_literal = ast::AstLiteralValue::String("hello".to_string());
        assert_eq!(translate_literal(&ast_literal), Ok(DataType::String("hello".to_string())));
    }

    #[test]
    fn test_translate_literal_integer() {
        let ast_literal = ast::AstLiteralValue::Number("123".to_string());
        assert_eq!(translate_literal(&ast_literal), Ok(DataType::Integer(123)));
    }

    #[test]
    fn test_translate_literal_float() {
        // Assuming DataType::Float exists and is the intended type for numbers with decimals
        let ast_literal = ast::AstLiteralValue::Number("123.45".to_string());
        assert_eq!(translate_literal(&ast_literal), Ok(DataType::Float(123.45)));
    }

    #[test]
    fn test_translate_literal_negative_integer() {
        let ast_literal = ast::AstLiteralValue::Number("-50".to_string());
        assert_eq!(translate_literal(&ast_literal), Ok(DataType::Integer(-50)));
    }

    #[test]
    fn test_translate_literal_negative_float() {
        let ast_literal = ast::AstLiteralValue::Number("-50.75".to_string());
        assert_eq!(translate_literal(&ast_literal), Ok(DataType::Float(-50.75)));
    }


    #[test]
    fn test_translate_literal_invalid_number() {
        let ast_literal = ast::AstLiteralValue::Number("abc".to_string());
        match translate_literal(&ast_literal) {
            Err(DbError::InvalidQuery(msg)) => {
                assert!(msg.contains("Cannot parse numeric literal 'abc'"));
            }
            _ => panic!("Expected InvalidQuery error for unparsable number string."),
        }
    }

    #[test]
    fn test_translate_literal_number_with_alpha_suffix() {
        let ast_literal = ast::AstLiteralValue::Number("123xyz".to_string());
         match translate_literal(&ast_literal) {
            Err(DbError::InvalidQuery(msg)) => {
                assert!(msg.contains("Cannot parse numeric literal '123xyz'"));
            }
            _ => panic!("Expected InvalidQuery error for unparsable number string."),
        }
    }


    #[test]
    fn test_translate_literal_boolean_true() {
        let ast_literal = ast::AstLiteralValue::Boolean(true);
        assert_eq!(translate_literal(&ast_literal), Ok(DataType::Boolean(true)));
    }

    #[test]
    fn test_translate_literal_boolean_false() {
        let ast_literal = ast::AstLiteralValue::Boolean(false);
        assert_eq!(translate_literal(&ast_literal), Ok(DataType::Boolean(false)));
    }

    #[test]
    fn test_translate_literal_null() {
        let ast_literal = ast::AstLiteralValue::Null;
        assert_eq!(translate_literal(&ast_literal), Ok(DataType::Null));
    }

    // Tests for translate_condition
    #[test]
    fn test_translate_condition_simple_equals() {
        let ast_cond = ast::Condition {
            column: "name".to_string(),
            operator: "=".to_string(),
            value: ast::AstLiteralValue::String("test_user".to_string()),
        };
        let expected_sql_cond = commands::SqlCondition {
            column: "name".to_string(),
            operator: "=".to_string(),
            value: DataType::String("test_user".to_string()),
        };
        assert_eq!(translate_condition(&ast_cond), Ok(expected_sql_cond));
    }

    #[test]
    fn test_translate_condition_with_numeric_value() {
        let ast_cond = ast::Condition {
            column: "age".to_string(),
            operator: ">".to_string(),
            value: ast::AstLiteralValue::Number("30".to_string()),
        };
        let expected_sql_cond = commands::SqlCondition {
            column: "age".to_string(),
            operator: ">".to_string(),
            value: DataType::Integer(30),
        };
        assert_eq!(translate_condition(&ast_cond), Ok(expected_sql_cond));
    }

    // Tests for translate_assignment
    #[test]
    fn test_translate_assignment_string() {
        let ast_assign = ast::Assignment {
            column: "email".to_string(),
            value: ast::AstLiteralValue::String("new@example.com".to_string()),
        };
        let expected_sql_assign = commands::SqlAssignment {
            column: "email".to_string(),
            value: DataType::String("new@example.com".to_string()),
        };
        assert_eq!(translate_assignment(&ast_assign), Ok(expected_sql_assign));
    }

    #[test]
    fn test_translate_assignment_boolean() {
        let ast_assign = ast::Assignment {
            column: "is_active".to_string(),
            value: ast::AstLiteralValue::Boolean(true),
        };
        let expected_sql_assign = commands::SqlAssignment {
            column: "is_active".to_string(),
            value: DataType::Boolean(true),
        };
        assert_eq!(translate_assignment(&ast_assign), Ok(expected_sql_assign));
    }

    // Tests for translate_select_columns
    #[test]
    fn test_translate_select_columns_all() {
        let ast_cols = vec![ast::SelectColumn::Asterisk];
        assert_eq!(translate_select_columns(ast_cols), commands::SelectColumnSpec::All);
    }

    #[test]
    fn test_translate_select_columns_specific() {
        let ast_cols = vec![
            ast::SelectColumn::ColumnName("id".to_string()),
            ast::SelectColumn::ColumnName("name".to_string()),
        ];
        let expected_spec = commands::SelectColumnSpec::Specific(vec!["id".to_string(), "name".to_string()]);
        assert_eq!(translate_select_columns(ast_cols), expected_spec);
    }

    #[test]
    fn test_translate_select_columns_specific_with_asterisk_first() {
        // If Asterisk is present, it should always result in All, even if others are there
        // (SQL parser should validate this, translator just reacts)
        let ast_cols = vec![
            ast::SelectColumn::Asterisk,
            ast::SelectColumn::ColumnName("id".to_string()),
        ];
        assert_eq!(translate_select_columns(ast_cols), commands::SelectColumnSpec::All);
    }

    #[test]
    fn test_translate_select_columns_specific_with_asterisk_last() {
        let ast_cols = vec![
            ast::SelectColumn::ColumnName("id".to_string()),
            ast::SelectColumn::Asterisk,
        ];
        assert_eq!(translate_select_columns(ast_cols), commands::SelectColumnSpec::All);
    }


    #[test]
    fn test_translate_select_columns_empty() {
        // An empty column list from AST (e.g. "SELECT FROM table", if parser allowed)
        // Current implementation defaults to All, which might be okay or might need specific error.
        let ast_cols = vec![];
        assert_eq!(translate_select_columns(ast_cols), commands::SelectColumnSpec::All);
    }

    // Tests for translate_ast_to_command (main function)
    #[test]
    fn test_translate_ast_select_simple() {
        let ast_stmt = ast::Statement::Select(ast::SelectStatement {
            columns: vec![ast::SelectColumn::Asterisk],
            source: "users".to_string(),
            condition: None,
        });
        let command = translate_ast_to_command(ast_stmt).unwrap();
        match command {
            Command::Select { columns, source, condition } => {
                assert_eq!(columns, SelectColumnSpec::All);
                assert_eq!(source, "users");
                assert!(condition.is_none());
            }
            _ => panic!("Expected Command::Select"),
        }
    }

    #[test]
    fn test_translate_ast_select_with_condition() {
        let ast_stmt = ast::Statement::Select(ast::SelectStatement {
            columns: vec![ast::SelectColumn::ColumnName("email".to_string())],
            source: "customers".to_string(),
            condition: Some(ast::Condition {
                column: "id".to_string(),
                operator: "=".to_string(),
                value: ast::AstLiteralValue::Number("101".to_string()),
            }),
        });
        let command = translate_ast_to_command(ast_stmt).unwrap();
        match command {
            Command::Select { columns, source, condition } => {
                assert_eq!(columns, SelectColumnSpec::Specific(vec!["email".to_string()]));
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
        let ast_stmt = ast::Statement::Update(ast::UpdateStatement {
            source: "products".to_string(),
            assignments: vec![ast::Assignment {
                column: "price".to_string(),
                value: ast::AstLiteralValue::Number("19.99".to_string()),
            }],
            condition: Some(ast::Condition {
                column: "product_id".to_string(),
                operator: "=".to_string(),
                value: ast::AstLiteralValue::String("XYZ123".to_string()),
            }),
        });
        let command = translate_ast_to_command(ast_stmt).unwrap();
        match command {
            Command::Update { source, assignments, condition } => {
                assert_eq!(source, "products");
                assert_eq!(assignments.len(), 1);
                assert_eq!(assignments[0].column, "price");
                assert_eq!(assignments[0].value, DataType::Float(19.99)); // Assumes DataType::Float
                assert!(condition.is_some());
                let cond_val = condition.unwrap();
                assert_eq!(cond_val.column, "product_id");
                assert_eq!(cond_val.value, DataType::String("XYZ123".to_string()));
            }
            _ => panic!("Expected Command::Update"),
        }
    }
}
