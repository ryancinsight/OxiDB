// Imports needed for the tests
use crate::core::query::sql::ast::{
    AstLiteralValue,
    SelectColumn,
    // AST nodes for assertions
    Statement,
};
use crate::core::query::sql::errors::SqlParseError;
use crate::core::query::sql::parser::SqlParser; // The struct being tested
use crate::core::query::sql::tokenizer::{Token, Tokenizer}; // For tokenizing test strings // Error type for assertions

// Helper function to tokenize a string for tests
fn tokenize_str(input: &str) -> Vec<Token> {
    let mut tokenizer = Tokenizer::new(input);
    tokenizer.tokenize().unwrap_or_else(|e| panic!("Test tokenizer error: {}", e))
}

#[test]
fn test_update_missing_set_keyword() {
    let tokens = tokenize_str("UPDATE table field = 'value';");
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(
        matches!(result, Err(SqlParseError::UnexpectedToken { .. })),
        "Result was: {:?}",
        result
    );
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
        assert!(expected.to_lowercase().contains("set"));
        assert!(found.to_lowercase().contains("identifier(\"field\")"));
    } else {
        panic!("Wrong error type: {:?}", result);
    }
}

#[test]
fn test_update_empty_set_clause() {
    let tokens = tokenize_str("UPDATE table SET;");
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(
        matches!(
            result,
            Err(SqlParseError::UnexpectedToken { .. }) | Err(SqlParseError::UnexpectedEOF)
        ),
        "Result was: {:?}",
        result
    );
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
        assert!(expected.to_lowercase().contains("identifier"));
        assert!(found.to_lowercase().contains("semicolon"));
    } else if let Err(SqlParseError::UnexpectedEOF) = result {
        // also possible, if input is just "UPDATE table SET"
    } else {
        panic!("Wrong error type for empty SET clause: {:?}", result);
    }
}

#[test]
fn test_update_missing_value_in_assignment() {
    let tokens = tokenize_str("UPDATE table SET field =;");
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(
        matches!(
            result,
            Err(SqlParseError::UnexpectedToken { .. }) | Err(SqlParseError::UnexpectedEOF)
        ),
        "Result was: {:?}",
        result
    );
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
        assert!(
            expected.to_lowercase().contains("literal value")
                || expected.to_lowercase().contains("expected value for assignment")
        );
        assert!(found.to_lowercase().contains("semicolon"));
    } else if let Err(SqlParseError::UnexpectedEOF) = result {
        // also possible
    } else {
        panic!("Wrong error type for missing value in assignment: {:?}", result);
    }
}

#[test]
fn test_update_missing_equals_in_assignment() {
    let tokens = tokenize_str("UPDATE table SET field 'value';");
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(
        matches!(result, Err(SqlParseError::UnexpectedToken { .. })),
        "Result was: {:?}",
        result
    );
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
        assert!(
            expected.to_lowercase().contains("operator(\"=\")")
                || expected.to_lowercase().contains("operator '='")
        );
        assert!(found.to_lowercase().contains("stringliteral(\"value\")"));
    } else {
        panic!("Wrong error type for missing equals in assignment: {:?}", result);
    }
}

#[test]
fn test_update_trailing_comma_in_assignment_list() {
    let tokens = tokenize_str("UPDATE table SET field = 'val', ;");
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(
        matches!(
            result,
            Err(SqlParseError::UnexpectedToken { .. }) | Err(SqlParseError::UnexpectedEOF)
        ),
        "Result was: {:?}",
        result
    );
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
        assert!(expected.to_lowercase().contains("identifier"));
        assert!(found.to_lowercase().contains("semicolon"));
    } else if let Err(SqlParseError::UnexpectedEOF) = result {
        // also possible
    } else {
        panic!("Wrong error type for trailing comma in assignment: {:?}", result);
    }
}

#[test]
fn test_update_empty_where_clause() {
    let tokens = tokenize_str("UPDATE table SET field = 'val' WHERE;");
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(
        matches!(
            result,
            Err(SqlParseError::UnexpectedToken { .. }) | Err(SqlParseError::UnexpectedEOF)
        ),
        "Result was: {:?}",
        result
    );
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
        assert!(expected.to_lowercase().contains("identifier"));
        assert!(found.to_lowercase().contains("semicolon"));
    } else if let Err(SqlParseError::UnexpectedEOF) = result {
        // also possible
    } else {
        panic!("Wrong error type for empty WHERE clause (UPDATE): {:?}", result);
    }
}

#[test]
fn test_update_missing_value_in_condition() {
    let tokens = tokenize_str("UPDATE table SET field = 'val' WHERE id =;");
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(
        matches!(
            result,
            Err(SqlParseError::UnexpectedToken { .. }) | Err(SqlParseError::UnexpectedEOF)
        ),
        "Result was: {:?}",
        result
    );
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
        assert!(
            expected.to_lowercase().contains("literal value")
                || expected.to_lowercase().contains("expected value for condition")
        );
        assert!(found.to_lowercase().contains("semicolon"));
    } else if let Err(SqlParseError::UnexpectedEOF) = result {
        // also possible
    } else {
        panic!("Wrong error type for missing value in condition (UPDATE): {:?}", result);
    }
}

#[test]
fn test_update_missing_operator_in_condition() {
    let tokens = tokenize_str("UPDATE table SET field = 'val' WHERE id value;");
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(
        matches!(result, Err(SqlParseError::UnexpectedToken { .. })),
        "Result was: {:?}",
        result
    );
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
        assert!(expected.to_lowercase().contains("operator"));
        assert!(found.to_lowercase().contains("identifier(\"value\")"));
    } else {
        panic!("Wrong error type for missing operator in condition (UPDATE): {:?}", result);
    }
}

#[test]
fn test_update_extra_token_after_valid_statement_no_semicolon() {
    let tokens = tokenize_str("UPDATE table SET field = 'value' EXTRA_TOKEN");
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(
        matches!(result, Err(SqlParseError::UnexpectedToken { .. })),
        "Result was: {:?}",
        result
    );
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
        assert!(expected.to_lowercase().contains("end of statement or eof"));
        assert!(found.to_lowercase().contains("identifier(\"extra_token\")"));
    } else {
        panic!("Wrong error type for extra token (UPDATE, no semi): {:?}", result);
    }
}

#[test]
fn test_update_extra_token_after_semicolon() {
    let tokens = tokenize_str("UPDATE table SET field = 'value'; EXTRA_TOKEN");
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(
        matches!(result, Err(SqlParseError::UnexpectedToken { .. })),
        "Result was: {:?}",
        result
    );
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
        assert!(expected.to_lowercase().contains("end of statement or eof"));
        assert!(found.to_lowercase().contains("identifier(\"extra_token\")"));
    } else {
        panic!("Wrong error type for extra token (UPDATE, with semi): {:?}", result);
    }
}

#[test]
fn test_parse_empty_tokens() {
    let tokens = vec![Token::EOF];
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(matches!(result, Err(SqlParseError::UnexpectedEOF)), "Result was: {:?}", result);
}

#[test]
fn test_parse_empty_tokens_no_eof() {
    let tokens = vec![];
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(matches!(result, Err(SqlParseError::UnexpectedEOF)), "Result was: {:?}", result);
}

#[test]
fn test_parse_select_simple() {
    let tokens = tokenize_str("SELECT name FROM users;");
    let mut parser = SqlParser::new(tokens);
    let ast = parser.parse().unwrap();
    match ast {
        Statement::Select(select_stmt) => {
            assert_eq!(select_stmt.source, "users");
            assert_eq!(select_stmt.columns, vec![SelectColumn::ColumnName("name".to_string())]);
            assert!(select_stmt.condition.is_none());
        }
        _ => panic!("Expected SelectStatement"),
    }
}

#[test]
fn test_parse_select_asterisk() {
    let tokens = tokenize_str("SELECT * FROM orders;");
    let mut parser = SqlParser::new(tokens);
    let ast = parser.parse().unwrap();
    match ast {
        Statement::Select(select_stmt) => {
            assert_eq!(select_stmt.source, "orders");
            assert_eq!(select_stmt.columns, vec![SelectColumn::Asterisk]);
            assert!(select_stmt.condition.is_none());
        }
        _ => panic!("Expected SelectStatement"),
    }
}

#[test]
fn test_parse_select_multiple_columns() {
    let tokens = tokenize_str("SELECT id, name, email FROM customers;");
    let mut parser = SqlParser::new(tokens);
    let ast = parser.parse().unwrap();
    match ast {
        Statement::Select(select_stmt) => {
            assert_eq!(select_stmt.source, "customers");
            assert_eq!(
                select_stmt.columns,
                vec![
                    SelectColumn::ColumnName("id".to_string()),
                    SelectColumn::ColumnName("name".to_string()),
                    SelectColumn::ColumnName("email".to_string()),
                ]
            );
            assert!(select_stmt.condition.is_none());
        }
        _ => panic!("Expected SelectStatement"),
    }
}

#[test]
fn test_parse_select_with_where_clause() {
    let tokens = tokenize_str("SELECT id FROM products WHERE price = 10.99;");
    let mut parser = SqlParser::new(tokens);
    let ast = parser.parse().unwrap();
    match ast {
        Statement::Select(select_stmt) => {
            assert_eq!(select_stmt.source, "products");
            assert_eq!(select_stmt.columns, vec![SelectColumn::ColumnName("id".to_string())]);
            assert!(select_stmt.condition.is_some());
            let cond = select_stmt.condition.unwrap();
            assert_eq!(cond.column, "price");
            assert_eq!(cond.operator, "=");
            assert_eq!(cond.value, AstLiteralValue::Number("10.99".to_string()));
        }
        _ => panic!("Expected SelectStatement"),
    }
}

#[test]
fn test_parse_update_simple() {
    let tokens = tokenize_str("UPDATE users SET name = 'New Name' WHERE id = 1;");
    let mut parser = SqlParser::new(tokens);
    let ast = parser.parse().unwrap();
    match ast {
        Statement::Update(update_stmt) => {
            assert_eq!(update_stmt.source, "users");
            assert_eq!(update_stmt.assignments.len(), 1);
            assert_eq!(update_stmt.assignments[0].column, "name");
            assert_eq!(
                update_stmt.assignments[0].value,
                AstLiteralValue::String("New Name".to_string())
            );
            assert!(update_stmt.condition.is_some());
            let cond = update_stmt.condition.unwrap();
            assert_eq!(cond.column, "id");
            assert_eq!(cond.operator, "=");
            assert_eq!(cond.value, AstLiteralValue::Number("1".to_string()));
        }
        _ => panic!("Expected UpdateStatement"),
    }
}

#[test]
fn test_parse_update_multiple_assignments() {
    let tokens = tokenize_str(
        "UPDATE products SET price = 99.50, stock = 500 WHERE category = 'electronics';",
    );
    let mut parser = SqlParser::new(tokens);
    let ast = parser.parse().unwrap();
    match ast {
        Statement::Update(update_stmt) => {
            assert_eq!(update_stmt.source, "products");
            assert_eq!(update_stmt.assignments.len(), 2);
            assert_eq!(update_stmt.assignments[0].column, "price");
            assert_eq!(
                update_stmt.assignments[0].value,
                AstLiteralValue::Number("99.50".to_string())
            );
            assert_eq!(update_stmt.assignments[1].column, "stock");
            assert_eq!(
                update_stmt.assignments[1].value,
                AstLiteralValue::Number("500".to_string())
            );

            assert!(update_stmt.condition.is_some());
            let cond = update_stmt.condition.unwrap();
            assert_eq!(cond.column, "category");
            assert_eq!(cond.operator, "=");
            assert_eq!(cond.value, AstLiteralValue::String("electronics".to_string()));
        }
        _ => panic!("Expected UpdateStatement"),
    }
}

#[test]
fn test_parse_select_missing_from() {
    let tokens = tokenize_str("SELECT name users;");
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(
        matches!(result, Err(SqlParseError::UnexpectedToken { .. })),
        "Result was: {:?}",
        result
    );
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
        assert!(expected.to_lowercase().contains("from"));
        assert!(found.to_lowercase().contains("identifier(\"users\")"));
    } else {
        panic!("Wrong error type for select missing FROM: {:?}", result);
    }
}

#[test]
fn test_parse_update_missing_set() {
    let tokens = tokenize_str("UPDATE users name = 'Test';");
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(
        matches!(result, Err(SqlParseError::UnexpectedToken { .. })),
        "Result was: {:?}",
        result
    );
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
        assert!(expected.contains("Set")); // Token::Set
        assert!(found.contains("Identifier(\"name\")"));
    } else {
        panic!("Wrong error type");
    }
}

#[test]
fn test_unexpected_token_instead_of_literal() {
    let tokens = tokenize_str("SELECT name FROM users WHERE id = SELECT;");
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(
        matches!(result, Err(SqlParseError::UnexpectedToken { .. })),
        "Result was: {:?}",
        result
    );
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
        assert_eq!(expected, "Expected value for condition"); // Was "literal value..."
        assert!(found.contains("Select"));
    } else {
        panic!("Wrong error type for unexpected token: {:?}", result);
    }
}

#[test]
fn test_select_missing_columns() {
    let tokens = tokenize_str("SELECT FROM table;");
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(
        matches!(result, Err(SqlParseError::UnexpectedToken { .. })),
        "Result was: {:?}",
        result
    );
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
        assert_eq!(expected, "Identifier");
        assert_eq!(found, "From");
    } else {
        panic!("Wrong error type: {:?}", result);
    }
}

#[test]
fn test_select_missing_from_keyword() {
    let tokens = tokenize_str("SELECT * table;");
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(
        matches!(result, Err(SqlParseError::UnexpectedToken { .. })),
        "Result was: {:?}",
        result
    );
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
        assert!(expected.to_lowercase().contains("from"));
        assert!(found.to_lowercase().contains("identifier(\"table\")"));
    } else {
        panic!("Wrong error type for select missing FROM keyword: {:?}", result);
    }
}

#[test]
fn test_select_trailing_comma_in_column_list() {
    let tokens = tokenize_str("SELECT col, FROM table;");
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(
        matches!(result, Err(SqlParseError::UnexpectedToken { .. })),
        "Result was: {:?}",
        result
    );
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
        assert_eq!(expected, "Identifier");
        assert_eq!(found, "From");
    } else {
        panic!("Wrong error type: {:?}", result);
    }
}

#[test]
fn test_select_missing_table_name() {
    let tokens = tokenize_str("SELECT col FROM;");
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(
        matches!(
            result,
            Err(SqlParseError::UnexpectedToken { .. }) | Err(SqlParseError::UnexpectedEOF)
        ),
        "Result was: {:?}",
        result
    );

    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
        assert!(expected.contains("Identifier")); // Expecting column name for condition
        assert!(found.contains("Semicolon"));
    } else if let Err(SqlParseError::UnexpectedEOF) = result {
        // This case is also possible
    } else {
        panic!("Wrong error type for empty WHERE clause (SELECT): {:?}", result);
    }
}

#[test]
fn test_select_empty_where_clause() {
    let tokens = tokenize_str("SELECT col FROM table WHERE;");
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(
        matches!(
            result,
            Err(SqlParseError::UnexpectedToken { .. }) | Err(SqlParseError::UnexpectedEOF)
        ),
        "Result was: {:?}",
        result
    );
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
        assert!(expected.to_lowercase().contains("identifier"));
        assert!(found.to_lowercase().contains("semicolon"));
    } else if let Err(SqlParseError::UnexpectedEOF) = result {
        // This case is also possible
    } else {
        panic!("Wrong error type: {:?}", result); // Keep this panic for unexpected errors
    }
}

#[test]
fn test_select_missing_value_in_condition() {
    let tokens = tokenize_str("SELECT col FROM table WHERE field =;");
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(
        matches!(
            result,
            Err(SqlParseError::UnexpectedToken { .. }) | Err(SqlParseError::UnexpectedEOF)
        ),
        "Result was: {:?}",
        result
    );
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
        assert!(
            expected.to_lowercase().contains("literal value")
                || expected.to_lowercase().contains("expected value for condition")
        );
        assert!(found.to_lowercase().contains("semicolon"));
    } else if let Err(SqlParseError::UnexpectedEOF) = result {
        // This case is also possible
    } else {
        panic!("Wrong error type for select missing value in condition: {:?}", result);
    }
}

#[test]
fn test_select_missing_operator_in_condition() {
    let tokens = tokenize_str("SELECT col FROM table WHERE field value;");
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(
        matches!(result, Err(SqlParseError::UnexpectedToken { .. })),
        "Result was: {:?}",
        result
    );
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
        assert!(expected.to_lowercase().contains("operator"));
        assert!(found.to_lowercase().contains("identifier(\"value\")"));
    } else {
        panic!("Wrong error type for select missing operator: {:?}", result);
    }
}

#[test]
fn test_select_extra_token_after_valid_statement_no_semicolon() {
    let tokens = tokenize_str("SELECT col FROM table WHERE field = 1 EXTRA_TOKEN");
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(
        matches!(result, Err(SqlParseError::UnexpectedToken { .. })),
        "Result was: {:?}",
        result
    );
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
        assert!(expected.to_lowercase().contains("end of statement or eof"));
        assert!(found.to_lowercase().contains("identifier(\"extra_token\")"));
    } else {
        panic!("Wrong error type for select extra token (no semi): {:?}", result);
    }
}

#[test]
fn test_select_extra_token_after_semicolon() {
    let tokens = tokenize_str("SELECT col FROM table WHERE field = 1; EXTRA_TOKEN");
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(
        matches!(result, Err(SqlParseError::UnexpectedToken { .. })),
        "Result was: {:?}",
        result
    );
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
        assert!(expected.to_lowercase().contains("end of statement or eof"));
        assert!(found.to_lowercase().contains("identifier(\"extra_token\")"));
    } else {
        panic!("Wrong error type for select extra token (with semi): {:?}", result);
    }
}

#[test]
fn test_select_empty_string_literal() {
    let tokens = tokenize_str("SELECT * FROM test WHERE name = '';");
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse().unwrap();
    match result {
        Statement::Select(select_stmt) => {
            let cond = select_stmt.condition.unwrap();
            assert_eq!(cond.column, "name");
            assert_eq!(cond.operator, "=");
            assert_eq!(cond.value, AstLiteralValue::String("".to_string()));
        }
        _ => panic!("Expected SelectStatement"),
    }
}

#[test]
fn test_update_set_null_value() {
    let tokens = tokenize_str("UPDATE test SET value = NULL WHERE id = 1;");
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse().unwrap();
    match result {
        Statement::Update(update_stmt) => {
            assert_eq!(update_stmt.assignments[0].column, "value");
            assert_eq!(update_stmt.assignments[0].value, AstLiteralValue::Null);
            let cond = update_stmt.condition.unwrap();
            assert_eq!(cond.column, "id");
            assert_eq!(cond.operator, "=");
            assert_eq!(cond.value, AstLiteralValue::Number("1".to_string()));
        }
        _ => panic!("Expected UpdateStatement"),
    }
}

#[test]
fn test_select_where_null_value() {
    let tokens = tokenize_str("SELECT * FROM test WHERE data = NULL;");
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse().unwrap();
    match result {
        Statement::Select(select_stmt) => {
            let cond = select_stmt.condition.unwrap();
            assert_eq!(cond.column, "data");
            assert_eq!(cond.operator, "=");
            assert_eq!(cond.value, AstLiteralValue::Null);
        }
        _ => panic!("Expected SelectStatement"),
    }
}

#[test]
fn test_identifier_as_substring_of_keyword() {
    let tokens = tokenize_str("SELECT selector FROM selections WHERE selector_id = 1;");
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse().unwrap();
    match result {
        Statement::Select(select_stmt) => {
            assert_eq!(select_stmt.columns, vec![SelectColumn::ColumnName("selector".to_string())]);
            assert_eq!(select_stmt.source, "selections");
            let cond = select_stmt.condition.unwrap();
            assert_eq!(cond.column, "selector_id");
            assert_eq!(cond.value, AstLiteralValue::Number("1".to_string()));
        }
        _ => panic!("Expected SelectStatement"),
    }
}

#[test]
fn test_mixed_case_keywords() {
    let tokens = tokenize_str("SeLeCt * FrOm my_table WhErE value = TrUe;");
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse().unwrap();
    match result {
        Statement::Select(select_stmt) => {
            assert_eq!(select_stmt.columns, vec![SelectColumn::Asterisk]);
            assert_eq!(select_stmt.source, "my_table");
            let cond = select_stmt.condition.unwrap();
            assert_eq!(cond.column, "value");
            assert_eq!(cond.value, AstLiteralValue::Boolean(true));
        }
        _ => panic!("Expected SelectStatement"),
    }
}
