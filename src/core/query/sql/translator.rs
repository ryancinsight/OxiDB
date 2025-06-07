use crate::core::common::error::DbError;
use crate::core::query::commands::{self, Command};
use crate::core::types::DataType;
use super::ast;

pub fn translate_ast_to_command(ast_statement: ast::Statement) -> Result<Command, DbError> {
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
    }
}

fn translate_literal(literal: &ast::AstLiteralValue) -> Result<DataType, DbError> {
    match literal {
        ast::AstLiteralValue::String(s) => Ok(DataType::String(s.clone())),
        ast::AstLiteralValue::Number(n_str) => {
            if let Ok(i_val) = n_str.parse::<i64>() { Ok(DataType::Integer(i_val)) }
            else if let Ok(f_val) = n_str.parse::<f64>() { Ok(DataType::Float(f_val)) }
            else { Err(DbError::InvalidQuery(format!("Cannot parse numeric literal '{}'", n_str))) }
        }
        ast::AstLiteralValue::Boolean(b) => Ok(DataType::Boolean(*b)),
        ast::AstLiteralValue::Null => Ok(DataType::Null),
    }
}

fn translate_condition_to_sql_condition(ast_condition: &ast::Condition) -> Result<commands::SqlCondition, DbError> {
    let value = translate_literal(&ast_condition.value)?;
    Ok(commands::SqlCondition {
        column: ast_condition.column.clone(),
        operator: ast_condition.operator.clone(),
        value,
    })
}

fn translate_assignment_to_sql_assignment(ast_assignment: &ast::Assignment) -> Result<commands::SqlAssignment, DbError> {
    let value = translate_literal(&ast_assignment.value)?;
    Ok(commands::SqlAssignment {
        column: ast_assignment.column.clone(),
        value,
    })
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
        AstLiteralValue as TestAstLiteralValue,
        SelectColumn as TestSelectColumn,
        Statement as TestStatement,
        SelectStatement as TestSelectStatement,
        UpdateStatement as TestUpdateStatement,
        Assignment as TestAssignment,
        Condition as TestCondition
    };


    #[test]
    fn test_translate_literal_string() {
        let ast_literal = TestAstLiteralValue::String("hello".to_string());
        assert_eq!(translate_literal(&ast_literal), Ok(DataType::String("hello".to_string())));
    }

    #[test]
    fn test_translate_literal_integer() {
        let ast_literal = TestAstLiteralValue::Number("123".to_string());
        assert_eq!(translate_literal(&ast_literal), Ok(DataType::Integer(123)));
    }

    #[test]
    fn test_translate_literal_float() {
        let ast_literal = TestAstLiteralValue::Number("123.45".to_string());
        assert_eq!(translate_literal(&ast_literal), Ok(DataType::Float(123.45)));
    }

    #[test]
    fn test_translate_literal_negative_integer() {
        let ast_literal = TestAstLiteralValue::Number("-50".to_string());
        assert_eq!(translate_literal(&ast_literal), Ok(DataType::Integer(-50)));
    }

    #[test]
    fn test_translate_literal_negative_float() {
        let ast_literal = TestAstLiteralValue::Number("-50.75".to_string());
        assert_eq!(translate_literal(&ast_literal), Ok(DataType::Float(-50.75)));
    }


    #[test]
    fn test_translate_literal_invalid_number() {
        let ast_literal = TestAstLiteralValue::Number("abc".to_string());
        match translate_literal(&ast_literal) {
            Err(DbError::InvalidQuery(msg)) => {
                assert!(msg.contains("Cannot parse numeric literal 'abc'"));
            }
            _ => panic!("Expected InvalidQuery error for unparsable number string."),
        }
    }

    #[test]
    fn test_translate_literal_number_with_alpha_suffix() {
        let ast_literal = TestAstLiteralValue::Number("123xyz".to_string());
         match translate_literal(&ast_literal) {
            Err(DbError::InvalidQuery(msg)) => {
                assert!(msg.contains("Cannot parse numeric literal '123xyz'"));
            }
            _ => panic!("Expected InvalidQuery error for unparsable number string."),
        }
    }


    #[test]
    fn test_translate_literal_boolean_true() {
        let ast_literal = TestAstLiteralValue::Boolean(true);
        assert_eq!(translate_literal(&ast_literal), Ok(DataType::Boolean(true)));
    }

    #[test]
    fn test_translate_literal_boolean_false() {
        let ast_literal = TestAstLiteralValue::Boolean(false);
        assert_eq!(translate_literal(&ast_literal), Ok(DataType::Boolean(false)));
    }

    #[test]
    fn test_translate_literal_null() {
        let ast_literal = TestAstLiteralValue::Null;
        assert_eq!(translate_literal(&ast_literal), Ok(DataType::Null));
    }

    #[test]
    fn test_translate_condition_simple_equals() {
        let ast_cond = TestCondition { // Using aliased TestCondition
            column: "name".to_string(),
            operator: "=".to_string(),
            value: TestAstLiteralValue::String("test_user".to_string()),
        };
        let expected_sql_cond = commands::SqlCondition {
            column: "name".to_string(),
            operator: "=".to_string(),
            value: DataType::String("test_user".to_string()),
        };
        assert_eq!(translate_condition_to_sql_condition(&ast_cond), Ok(expected_sql_cond));
    }

    #[test]
    fn test_translate_condition_with_numeric_value() {
         let ast_cond = TestCondition { // Using aliased TestCondition
            column: "age".to_string(),
            operator: ">".to_string(),
            value: TestAstLiteralValue::Number("30".to_string()),
        };
        let expected_sql_cond = commands::SqlCondition {
            column: "age".to_string(),
            operator: ">".to_string(),
            value: DataType::Integer(30),
        };
        assert_eq!(translate_condition_to_sql_condition(&ast_cond), Ok(expected_sql_cond));
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
        assert_eq!(translate_assignment_to_sql_assignment(&ast_assign), Ok(expected_sql_assign));
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
        assert_eq!(translate_assignment_to_sql_assignment(&ast_assign), Ok(expected_sql_assign));
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
        let expected_spec = commands::SelectColumnSpec::Specific(vec!["id".to_string(), "name".to_string()]);
        assert_eq!(translate_select_columns(ast_cols), expected_spec);
    }

    #[test]
    fn test_translate_select_columns_specific_with_asterisk_first() {
        let ast_cols = vec![
            TestSelectColumn::Asterisk,
            TestSelectColumn::ColumnName("id".to_string()),
        ];
        assert_eq!(translate_select_columns(ast_cols), commands::SelectColumnSpec::All);
    }

    #[test]
    fn test_translate_select_columns_specific_with_asterisk_last() {
        let ast_cols = vec![
            TestSelectColumn::ColumnName("id".to_string()),
            TestSelectColumn::Asterisk,
        ];
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
                assert_eq!(columns, SelectColumnSpec::All);
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
