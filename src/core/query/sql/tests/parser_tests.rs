// Imports needed for the tests
use crate::core::query::sql::ast::{
    AstColumnConstraint, // Added this import
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
        assert_eq!(expected.to_lowercase(), "expected column name for assignment");
        assert_eq!(found.to_lowercase(), "semicolon");
    } else if let Err(SqlParseError::UnexpectedEOF) = result {
        // also possible, if input is just "UPDATE table SET"
        panic!("UnexpectedEOF, expected UnexpectedToken for 'UPDATE table SET;'");
    } else {
        panic!("Wrong error type for empty SET clause: {:?}, expected UnexpectedToken", result);
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
        assert_eq!(expected.to_lowercase(), "expected column name for assignment");
        assert_eq!(found.to_lowercase(), "semicolon");
    } else if let Err(SqlParseError::UnexpectedEOF) = result {
        // also possible
        panic!("UnexpectedEOF, expected UnexpectedToken for 'UPDATE table SET field = 'val', ;'");
    } else {
        panic!(
            "Wrong error type for trailing comma in assignment: {:?}, expected UnexpectedToken",
            result
        );
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
        assert_eq!(expected.to_lowercase(), "expected column name for condition");
        assert_eq!(found.to_lowercase(), "semicolon");
    } else if let Err(SqlParseError::UnexpectedEOF) = result {
        // also possible if input is "UPDATE table SET field = 'val' WHERE"
        panic!("UnexpectedEOF, expected UnexpectedToken for 'WHERE;'");
    } else {
        panic!(
            "Wrong error type for empty WHERE clause (UPDATE): {:?}, expected UnexpectedToken",
            result
        );
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

// --- Tests for CREATE TABLE with constraints ---

#[test]
fn test_parse_create_table_with_constraints() {
    let sql = "CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        email VARCHAR(255) NOT NULL UNIQUE,
        age INT NOT NULL,
        username TEXT UNIQUE,
        bio TEXT
    );";
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let ast = parser.parse().unwrap();

    match ast {
        Statement::CreateTable(create_stmt) => {
            assert_eq!(create_stmt.table_name, "users");
            assert_eq!(create_stmt.columns.len(), 5);

            // id INTEGER PRIMARY KEY
            let id_col = &create_stmt.columns[0];
            assert_eq!(id_col.name, "id");
            assert_eq!(id_col.data_type, "INTEGER");
            assert_eq!(id_col.constraints.len(), 1);
            assert!(id_col.constraints.contains(&AstColumnConstraint::PrimaryKey));

            // email VARCHAR(255) NOT NULL UNIQUE
            let email_col = &create_stmt.columns[1];
            assert_eq!(email_col.name, "email");
            assert_eq!(email_col.data_type, "VARCHAR(255)");
            assert_eq!(email_col.constraints.len(), 2);
            assert!(email_col.constraints.contains(&AstColumnConstraint::NotNull));
            assert!(email_col.constraints.contains(&AstColumnConstraint::Unique));
            // Check order if parser preserves it (optional, current parser likely does)
            assert_eq!(email_col.constraints[0], AstColumnConstraint::NotNull);
            assert_eq!(email_col.constraints[1], AstColumnConstraint::Unique);

            // age INT NOT NULL
            let age_col = &create_stmt.columns[2];
            assert_eq!(age_col.name, "age");
            assert_eq!(age_col.data_type, "INT");
            assert_eq!(age_col.constraints.len(), 1);
            assert!(age_col.constraints.contains(&AstColumnConstraint::NotNull));

            // username TEXT UNIQUE
            let username_col = &create_stmt.columns[3];
            assert_eq!(username_col.name, "username");
            assert_eq!(username_col.data_type, "TEXT");
            assert_eq!(username_col.constraints.len(), 1);
            assert!(username_col.constraints.contains(&AstColumnConstraint::Unique));

            // bio TEXT (no constraints)
            let bio_col = &create_stmt.columns[4];
            assert_eq!(bio_col.name, "bio");
            assert_eq!(bio_col.data_type, "TEXT");
            assert!(bio_col.constraints.is_empty());
        }
        _ => panic!("Expected CreateTableStatement"),
    }
}

#[test]
fn test_parse_create_table_primary_key_not_null_variants() {
    // PRIMARY KEY implies NOT NULL, but users might specify it.
    // The parser should capture what's specified. Validation/normalization is a later step.
    let test_cases = vec![
        (
            "id INTEGER PRIMARY KEY NOT NULL",
            vec![AstColumnConstraint::PrimaryKey, AstColumnConstraint::NotNull],
        ),
        (
            "id INTEGER NOT NULL PRIMARY KEY",
            vec![AstColumnConstraint::NotNull, AstColumnConstraint::PrimaryKey],
        ),
    ];

    for (col_sql, expected_constraints) in test_cases {
        let sql = format!("CREATE TABLE test_pk ( {} );", col_sql);
        let tokens = tokenize_str(&sql);
        let mut parser = SqlParser::new(tokens);
        let ast = parser.parse().unwrap_or_else(|e| panic!("Failed to parse '{}': {:?}", sql, e));

        match ast {
            Statement::CreateTable(create_stmt) => {
                assert_eq!(create_stmt.columns.len(), 1);
                let col_def = &create_stmt.columns[0];
                assert_eq!(
                    col_def.constraints, expected_constraints,
                    "Constraints mismatch for: {}",
                    col_sql
                );
            }
            _ => panic!("Expected CreateTableStatement for: {}", sql),
        }
    }
}

#[test]
fn test_parse_create_table_no_constraints() {
    let sql = "CREATE TABLE simple (id INT, name VARCHAR);"; // VARCHAR without length
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let ast = parser.parse().unwrap();
    match ast {
        Statement::CreateTable(create_stmt) => {
            assert_eq!(create_stmt.table_name, "simple");
            assert_eq!(create_stmt.columns.len(), 2);
            assert_eq!(create_stmt.columns[0].name, "id");
            assert_eq!(create_stmt.columns[0].data_type, "INT");
            assert!(create_stmt.columns[0].constraints.is_empty());
            assert_eq!(create_stmt.columns[1].name, "name");
            assert_eq!(create_stmt.columns[1].data_type, "VARCHAR");
            assert!(create_stmt.columns[1].constraints.is_empty());
        }
        _ => panic!("Expected CreateTableStatement"),
    }
}

#[test]
fn test_parse_create_table_invalid_constraint_sequence() {
    // Example: "id INTEGER NOT PRIMARY KEY" - "NOT" should be followed by "NULL"
    let sql = "CREATE TABLE bad_constraint (id INTEGER NOT PRIMARY);";
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(
        matches!(result, Err(SqlParseError::UnexpectedToken { .. })),
        "Result was: {:?}",
        result
    );
    if let Err(SqlParseError::UnexpectedToken { expected, found, position: _ }) = result {
        assert_eq!(expected.to_lowercase(), "null"); // Expecting NULL after NOT
        let lower_found = found.to_lowercase();
        // Debug format for Identifier("PRIMARY") results in something like "Identifier(PRIMARY)"
        // .to_lowercase() makes it "identifier(primary)"
        assert!(
            lower_found.starts_with("identifier("),
            "Expected found to start with 'identifier(', got: {}",
            lower_found
        );
        assert!(
            lower_found.contains("primary") && !lower_found.contains("\"primary\""),
            "Expected found to contain 'primary' (no quotes), got: {}",
            lower_found
        );
    } else {
        panic!("Expected UnexpectedToken for invalid 'NOT PRIMARY', got {:?}", result);
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
        assert_eq!(expected.to_lowercase(), "expected column name or '*'");
        assert_eq!(found.to_lowercase(), "from");
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
        assert_eq!(
            found, "Table",
            "Found token was {:?}, debug of Identifier(\"table\") seems to be 'Table'",
            found
        ); // Expect "Table"
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
        assert_eq!(expected.to_lowercase(), "expected column name or '*'");
        assert_eq!(found.to_lowercase(), "from");
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
        assert_eq!(expected.to_lowercase(), "expected table name after from");
        assert_eq!(found.to_lowercase(), "semicolon");
    } else if let Err(SqlParseError::UnexpectedEOF) = result {
        // This case is also possible if input is "SELECT col FROM"
        panic!("UnexpectedEOF, expected UnexpectedToken for 'FROM;'");
    } else {
        panic!("Wrong error type for missing table name: {:?}, expected UnexpectedToken", result);
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
        assert_eq!(expected.to_lowercase(), "expected column name for condition");
        assert_eq!(found.to_lowercase(), "semicolon");
    } else if let Err(SqlParseError::UnexpectedEOF) = result {
        // This case is also possible if input is "SELECT col FROM table WHERE"
        // For "SELECT col FROM table WHERE;", it should be UnexpectedToken
        // Let's assume the test aims for UnexpectedToken primarily.
        // If UnexpectedEOF is a valid outcome for some variant of this test,
        // the test should be more specific or split. Given current strictness,
        // "SELECT col FROM table WHERE;" should yield UnexpectedToken.
        panic!("UnexpectedEOF, expected UnexpectedToken for 'WHERE;'");
    } else {
        panic!("Wrong error type: {:?}, expected UnexpectedToken", result);
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
        let expected_lower = expected.to_lowercase();
        println!("Actual expected (with semi): '{}'", expected_lower); // Keep for logging during test run
        assert_eq!(
            expected_lower, "end of statement or eof",
            "Assertion failed: expected_lower was '{}', expected 'end of statement or eof'",
            expected_lower
        );
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
fn test_parse_insert_simple() {
    let tokens =
        tokenize_str("INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com');");
    let mut parser = SqlParser::new(tokens);
    let ast = parser.parse().unwrap();
    match ast {
        Statement::Insert(insert_stmt) => {
            assert_eq!(insert_stmt.table_name, "users");
            assert_eq!(insert_stmt.columns, Some(vec!["name".to_string(), "email".to_string()]));
            assert_eq!(insert_stmt.values.len(), 1);
            assert_eq!(insert_stmt.values[0].len(), 2);
            assert_eq!(insert_stmt.values[0][0], AstLiteralValue::String("Alice".to_string()));
            assert_eq!(
                insert_stmt.values[0][1],
                AstLiteralValue::String("alice@example.com".to_string())
            );
        }
        _ => panic!("Expected InsertStatement"),
    }
}

#[test]
fn test_parse_insert_multiple_values() {
    // Test case 1: Multiple VALUES sets with explicit columns
    let tokens1 = tokenize_str("INSERT INTO products (id, name, price) VALUES (1, 'Laptop', 1200.00), (2, 'Mouse', 25.00), (3, 'Keyboard', 75.50);");
    let mut parser1 = SqlParser::new(tokens1);
    let ast1 = parser1.parse().unwrap();
    match ast1 {
        Statement::Insert(insert_stmt) => {
            assert_eq!(insert_stmt.table_name, "products");
            assert_eq!(
                insert_stmt.columns,
                Some(vec!["id".to_string(), "name".to_string(), "price".to_string()])
            );
            assert_eq!(insert_stmt.values.len(), 3, "Expected 3 sets of values for TC1");
            // Check first set
            assert_eq!(insert_stmt.values[0].len(), 3);
            assert_eq!(insert_stmt.values[0][0], AstLiteralValue::Number("1".to_string()));
            assert_eq!(insert_stmt.values[0][1], AstLiteralValue::String("Laptop".to_string()));
            assert_eq!(insert_stmt.values[0][2], AstLiteralValue::Number("1200.00".to_string()));
            // Check second set
            assert_eq!(insert_stmt.values[1].len(), 3);
            assert_eq!(insert_stmt.values[1][0], AstLiteralValue::Number("2".to_string()));
            assert_eq!(insert_stmt.values[1][1], AstLiteralValue::String("Mouse".to_string()));
            assert_eq!(insert_stmt.values[1][2], AstLiteralValue::Number("25.00".to_string()));
            // Check third set
            assert_eq!(insert_stmt.values[2].len(), 3);
            assert_eq!(insert_stmt.values[2][0], AstLiteralValue::Number("3".to_string()));
            assert_eq!(insert_stmt.values[2][1], AstLiteralValue::String("Keyboard".to_string()));
            assert_eq!(insert_stmt.values[2][2], AstLiteralValue::Number("75.50".to_string()));
        }
        _ => panic!("Expected InsertStatement for TC1"),
    }

    // Test case 2: Multiple VALUES sets without explicit columns
    let tokens2 =
        tokenize_str("INSERT INTO locations VALUES ('USA', 'New York'), ('CAN', 'Toronto');");
    let mut parser2 = SqlParser::new(tokens2);
    let ast2 = parser2.parse().unwrap();
    match ast2 {
        Statement::Insert(insert_stmt) => {
            assert_eq!(insert_stmt.table_name, "locations");
            assert!(insert_stmt.columns.is_none());
            assert_eq!(insert_stmt.values.len(), 2, "Expected 2 sets of values for TC2");
            assert_eq!(insert_stmt.values[0].len(), 2);
            assert_eq!(insert_stmt.values[0][0], AstLiteralValue::String("USA".to_string()));
            assert_eq!(insert_stmt.values[0][1], AstLiteralValue::String("New York".to_string()));
            assert_eq!(insert_stmt.values[1].len(), 2);
            assert_eq!(insert_stmt.values[1][0], AstLiteralValue::String("CAN".to_string()));
            assert_eq!(insert_stmt.values[1][1], AstLiteralValue::String("Toronto".to_string()));
        }
        _ => panic!("Expected InsertStatement for TC2"),
    }

    // Test case 3: Single VALUES set (ensure loop handles this correctly)
    let tokens3 = tokenize_str("INSERT INTO tasks (description) VALUES ('Finish report');");
    let mut parser3 = SqlParser::new(tokens3);
    let ast3 = parser3.parse().unwrap();
    match ast3 {
        Statement::Insert(insert_stmt) => {
            assert_eq!(insert_stmt.table_name, "tasks");
            assert_eq!(insert_stmt.columns, Some(vec!["description".to_string()]));
            assert_eq!(insert_stmt.values.len(), 1, "Expected 1 set of values for TC3");
            assert_eq!(insert_stmt.values[0].len(), 1);
            assert_eq!(
                insert_stmt.values[0][0],
                AstLiteralValue::String("Finish report".to_string())
            );
        }
        _ => panic!("Expected InsertStatement for TC3"),
    }

    // Test case 4: Error - Trailing comma after last VALUES set
    let tokens4 = tokenize_str("INSERT INTO test VALUES (1, 'a'),;");
    let mut parser4 = SqlParser::new(tokens4);
    let result4 = parser4.parse();
    assert!(
        matches!(
            result4,
            Err(SqlParseError::UnexpectedToken { .. }) | Err(SqlParseError::UnexpectedEOF)
        ),
        "Result was: {:?}",
        result4
    );
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result4 {
        assert!(
            expected.to_lowercase().contains("lparen") || expected.to_lowercase().contains("(")
        ); // Expects start of new value set
        assert!(found.to_lowercase().contains("semicolon"));
    } else if let Err(SqlParseError::UnexpectedEOF) = result4 {
        // This might also be valid if the parser expects another ( but finds EOF after comma
        panic!("UnexpectedEOF, expected UnexpectedToken for trailing comma in VALUES");
    } else {
        panic!("Wrong error type for trailing comma in VALUES: {:?}", result4);
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
