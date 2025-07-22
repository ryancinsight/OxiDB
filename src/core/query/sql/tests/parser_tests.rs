// Imports needed for the tests
use crate::core::query::commands::Command;
use crate::core::query::sql::ast::{
    self, AstColumnConstraint, AstDataType, AstLiteralValue, ConditionTree, OrderDirection,
    SelectColumn, Statement,
};
use crate::core::query::sql::errors::SqlParseError;
use crate::core::query::sql::parser::SqlParser; // The struct being tested
use crate::core::query::sql::tokenizer::{Token, Tokenizer}; // For tokenizing test strings // Error type for assertions
use crate::core::query::sql::translator::translate_ast_to_command;
use crate::core::types::DataType;

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
        assert_eq!(expected.to_lowercase(), "column name for assignment");
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
        assert_eq!(expected.to_lowercase(), "column name for assignment after comma");
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
    // The following assertion should already be correct based on the file content from the previous turn.
    // If it's still failing as "left: X, right: Y", it implies the file state is not what read_files reports.
    // Forcing the known correct state again.
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
        assert_eq!(expected.to_lowercase(), "expected column name, 'not', or '(' for condition");
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
            expected
                .to_lowercase()
                .contains("expected literal, parameter (?), or column identifier for rhs of condition")
                || expected.to_lowercase().contains("expected identifier") // If parse_qualified_identifier fails early
        );
        assert!(found.to_lowercase().contains("semicolon"));
    } else if let Err(SqlParseError::UnexpectedEOF) = result {
        // also possible if input ends after "="
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
            assert_eq!(select_stmt.from_clause.name, "users");
            assert_eq!(select_stmt.columns, vec![SelectColumn::ColumnName("name".to_string())]);
            assert!(select_stmt.condition.is_none());
            assert!(select_stmt.joins.is_empty()); // New check
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
            assert_eq!(id_col.data_type, AstDataType::Integer);
            assert_eq!(id_col.constraints.len(), 1);
            assert!(id_col.constraints.contains(&AstColumnConstraint::PrimaryKey));

            // email VARCHAR(255) NOT NULL UNIQUE
            let email_col = &create_stmt.columns[1];
            assert_eq!(email_col.name, "email");
            // Parser simplifies VARCHAR(255) to Text as it doesn't store length parameter in AstDataType::Text
            assert_eq!(email_col.data_type, AstDataType::Text);
            assert_eq!(email_col.constraints.len(), 2);
            assert!(email_col.constraints.contains(&AstColumnConstraint::NotNull));
            assert!(email_col.constraints.contains(&AstColumnConstraint::Unique));
            // Check order if parser preserves it (optional, current parser likely does)
            assert_eq!(email_col.constraints[0], AstColumnConstraint::NotNull);
            assert_eq!(email_col.constraints[1], AstColumnConstraint::Unique);

            // age INT NOT NULL
            let age_col = &create_stmt.columns[2];
            assert_eq!(age_col.name, "age");
            assert_eq!(age_col.data_type, AstDataType::Integer);
            assert_eq!(age_col.constraints.len(), 1);
            assert!(age_col.constraints.contains(&AstColumnConstraint::NotNull));

            // username TEXT UNIQUE
            let username_col = &create_stmt.columns[3];
            assert_eq!(username_col.name, "username");
            assert_eq!(username_col.data_type, AstDataType::Text);
            assert_eq!(username_col.constraints.len(), 1);
            assert!(username_col.constraints.contains(&AstColumnConstraint::Unique));

            // bio TEXT (no constraints)
            let bio_col = &create_stmt.columns[4];
            assert_eq!(bio_col.name, "bio");
            assert_eq!(bio_col.data_type, AstDataType::Text);
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
            AstDataType::Integer,
            vec![AstColumnConstraint::PrimaryKey, AstColumnConstraint::NotNull],
        ),
        (
            "id INTEGER NOT NULL PRIMARY KEY",
            AstDataType::Integer,
            vec![AstColumnConstraint::NotNull, AstColumnConstraint::PrimaryKey],
        ),
    ];

    for (col_sql, expected_data_type, expected_constraints) in test_cases {
        let sql = format!("CREATE TABLE test_pk ( {} );", col_sql);
        let tokens = tokenize_str(&sql);
        let mut parser = SqlParser::new(tokens);
        let ast = parser.parse().unwrap_or_else(|e| panic!("Failed to parse '{}': {:?}", sql, e));

        match ast {
            Statement::CreateTable(create_stmt) => {
                assert_eq!(create_stmt.columns.len(), 1);
                let col_def = &create_stmt.columns[0];
                assert_eq!(
                    col_def.data_type, expected_data_type,
                    "Data type mismatch for: {}",
                    col_sql
                );
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
fn test_parse_create_table_with_autoincrement() {
    let sql = "CREATE TABLE users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL
    );";
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let ast = parser.parse().unwrap();

    match ast {
        Statement::CreateTable(create_stmt) => {
            assert_eq!(create_stmt.table_name, "users");
            assert_eq!(create_stmt.columns.len(), 2);

            // id INTEGER PRIMARY KEY AUTOINCREMENT
            let id_col = &create_stmt.columns[0];
            assert_eq!(id_col.name, "id");
            assert_eq!(id_col.data_type, AstDataType::Integer);
            assert_eq!(id_col.constraints.len(), 2);
            assert!(id_col.constraints.contains(&AstColumnConstraint::PrimaryKey));
            assert!(id_col.constraints.contains(&AstColumnConstraint::AutoIncrement));

            // name TEXT NOT NULL
            let name_col = &create_stmt.columns[1];
            assert_eq!(name_col.name, "name");
            assert_eq!(name_col.data_type, AstDataType::Text);
            assert_eq!(name_col.constraints.len(), 1);
            assert!(name_col.constraints.contains(&AstColumnConstraint::NotNull));
        }
        _ => panic!("Expected CreateTableStatement"),
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
            assert_eq!(create_stmt.columns[0].data_type, self::ast::AstDataType::Integer);
            assert!(create_stmt.columns[0].constraints.is_empty());
            assert_eq!(create_stmt.columns[1].name, "name");
            assert_eq!(create_stmt.columns[1].data_type, self::ast::AstDataType::Text); // Assuming VARCHAR maps to Text
            assert!(create_stmt.columns[1].constraints.is_empty());
        }
        _ => panic!("Expected CreateTableStatement"),
    }
}

#[test]
fn test_parse_create_table_with_vector_and_blob() {
    let sql = "CREATE TABLE items (
        id INT PRIMARY KEY,
        feature_vector VECTOR[128],
        image_data BLOB NOT NULL,
        description TEXT
    );";
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let ast = parser.parse().unwrap();

    match ast {
        Statement::CreateTable(create_stmt) => {
            assert_eq!(create_stmt.table_name, "items");
            assert_eq!(create_stmt.columns.len(), 4);

            let id_col = &create_stmt.columns[0];
            assert_eq!(id_col.name, "id");
            assert_eq!(id_col.data_type, self::ast::AstDataType::Integer);
            assert!(id_col.constraints.contains(&AstColumnConstraint::PrimaryKey));

            let vector_col = &create_stmt.columns[1];
            assert_eq!(vector_col.name, "feature_vector");
            assert_eq!(vector_col.data_type, self::ast::AstDataType::Vector { dimension: 128 });
            assert!(vector_col.constraints.is_empty());

            let blob_col = &create_stmt.columns[2];
            assert_eq!(blob_col.name, "image_data");
            assert_eq!(blob_col.data_type, self::ast::AstDataType::Blob);
            assert!(blob_col.constraints.contains(&AstColumnConstraint::NotNull));

            let desc_col = &create_stmt.columns[3];
            assert_eq!(desc_col.name, "description");
            assert_eq!(desc_col.data_type, self::ast::AstDataType::Text);
            assert!(desc_col.constraints.is_empty());
        }
        _ => panic!("Expected CreateTableStatement for vector/blob test"),
    }
}

#[test]
fn test_parse_create_table_invalid_vector_dimension() {
    let sql_invalid_dim = "CREATE TABLE t (v VECTOR[0]);"; // Dimension 0
    let tokens_invalid = tokenize_str(sql_invalid_dim);
    let mut parser_invalid = SqlParser::new(tokens_invalid);
    let result_invalid = parser_invalid.parse();
    assert!(matches!(result_invalid, Err(SqlParseError::InvalidDataTypeParameter { .. })));
    if let Err(SqlParseError::InvalidDataTypeParameter { type_name, parameter, reason, .. }) =
        result_invalid
    {
        assert_eq!(type_name, "VECTOR");
        assert_eq!(parameter, "0");
        assert!(reason.contains("greater than 0"));
    } else {
        panic!("Wrong error type for invalid vector dimension: {:?}", result_invalid);
    }

    let sql_non_numeric_dim = "CREATE TABLE t (v VECTOR[abc]);";
    let tokens_non_numeric = tokenize_str(sql_non_numeric_dim);
    let mut parser_non_numeric = SqlParser::new(tokens_non_numeric);
    let result_non_numeric = parser_non_numeric.parse();
    assert!(matches!(result_non_numeric, Err(SqlParseError::UnexpectedToken { .. })));
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result_non_numeric {
        assert!(expected.contains("numeric dimension"));
        assert!(found.to_lowercase().contains("identifier(\"abc\")"));
    } else {
        panic!("Wrong error type for non-numeric vector dimension: {:?}", result_non_numeric);
    }
}

#[test]
fn test_parse_delete_simple() {
    let sql = "DELETE FROM users WHERE id = 100;";
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let ast = parser.parse().unwrap();
    match ast {
        Statement::Delete(del_stmt) => {
            assert_eq!(del_stmt.table_name, "users");
            assert!(del_stmt.condition.is_some());
            match del_stmt.condition.unwrap() {
                ConditionTree::Comparison(cond) => {
                    assert_eq!(cond.column, "id");
                    assert_eq!(cond.operator, "=");
                    assert_eq!(
                        cond.value,
                        ast::AstExpressionValue::Literal(AstLiteralValue::Number(
                            "100".to_string()
                        ))
                    );
                }
                _ => panic!("Expected simple comparison in DELETE"),
            }
        }
        _ => panic!("Expected DeleteStatement"),
    }
}

#[test]
fn test_parse_delete_no_where() {
    let sql = "DELETE FROM logs;"; // Deletes all rows
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let ast = parser.parse().unwrap();
    match ast {
        Statement::Delete(del_stmt) => {
            assert_eq!(del_stmt.table_name, "logs");
            assert!(del_stmt.condition.is_none());
        }
        _ => panic!("Expected DeleteStatement"),
    }
}

#[test]
fn test_parse_delete_complex_where() {
    let sql =
        "DELETE FROM items WHERE (category = 'old' AND last_updated < 2020) OR stock_count = 0;";
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let ast = parser.parse().unwrap();
    match ast {
        Statement::Delete(del_stmt) => {
            assert_eq!(del_stmt.table_name, "items");
            assert!(del_stmt.condition.is_some());
            // Further checks on ConditionTree structure can be added, similar to SELECT test
        }
        _ => panic!("Expected DeleteStatement"),
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
            assert_eq!(select_stmt.from_clause.name, "orders");
            assert_eq!(select_stmt.columns, vec![SelectColumn::Asterisk]);
            assert!(select_stmt.condition.is_none());
            assert!(select_stmt.joins.is_empty());
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
            assert_eq!(select_stmt.from_clause.name, "customers");
            assert_eq!(
                select_stmt.columns,
                vec![
                    SelectColumn::ColumnName("id".to_string()),
                    SelectColumn::ColumnName("name".to_string()),
                    SelectColumn::ColumnName("email".to_string()),
                ]
            );
            assert!(select_stmt.condition.is_none());
            assert!(select_stmt.joins.is_empty());
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
            assert_eq!(select_stmt.from_clause.name, "products");
            assert_eq!(select_stmt.columns, vec![SelectColumn::ColumnName("id".to_string())]);
            assert!(select_stmt.condition.is_some());
            assert!(select_stmt.joins.is_empty());
            match select_stmt.condition.unwrap() {
                ConditionTree::Comparison(cond) => {
                    assert_eq!(cond.column, "price");
                    assert_eq!(cond.operator, "=");
                    assert_eq!(
                        cond.value,
                        ast::AstExpressionValue::Literal(AstLiteralValue::Number(
                            "10.99".to_string()
                        ))
                    );
                }
                _ => panic!("Expected ConditionTree::Comparison for price condition"),
            }
        }
        _ => panic!("Expected SelectStatement"),
    }
}

#[test]
fn test_parse_select_with_complex_where_clause_and_or_not_parens() {
    let sql = "SELECT * FROM data WHERE (col1 = 10 AND col2 = 'test') OR NOT (col3 < 5.5);";
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let ast = parser.parse().unwrap();

    match ast {
        Statement::Select(select_stmt) => {
            assert_eq!(select_stmt.from_clause.name, "data");
            assert!(select_stmt.joins.is_empty());
            assert!(select_stmt.condition.is_some());
            let condition_tree = select_stmt.condition.unwrap();

            // Expected: OR( AND( (col1=10), (col2='test') ), NOT( (col3<5.5) ) )
            match condition_tree {
                ConditionTree::Or(left_or_box, right_or_box) => {
                    // Left side of OR: AND( (col1=10), (col2='test') )
                    match *left_or_box {
                        ConditionTree::And(left_and_box, right_and_box) => {
                            match *left_and_box {
                                ConditionTree::Comparison(cond) => {
                                    assert_eq!(cond.column, "col1");
                                    assert_eq!(cond.operator, "=");
                                    assert_eq!(
                                        cond.value,
                                        ast::AstExpressionValue::Literal(AstLiteralValue::Number(
                                            "10".to_string()
                                        ))
                                    );
                                }
                                _ => panic!("Expected Comparison for col1=10"),
                            }
                            match *right_and_box {
                                ConditionTree::Comparison(cond) => {
                                    assert_eq!(cond.column, "col2");
                                    assert_eq!(cond.operator, "=");
                                    assert_eq!(
                                        cond.value,
                                        ast::AstExpressionValue::Literal(AstLiteralValue::String(
                                            "test".to_string()
                                        ))
                                    );
                                }
                                _ => panic!("Expected Comparison for col2='test'"),
                            }
                        }
                        _ => panic!("Expected AND on the left side of OR"),
                    }

                    // Right side of OR: NOT( (col3<5.5) )
                    match *right_or_box {
                        ConditionTree::Not(negated_condition_box) => match *negated_condition_box {
                            ConditionTree::Comparison(cond) => {
                                assert_eq!(cond.column, "col3");
                                assert_eq!(cond.operator, "<");
                                assert_eq!(
                                    cond.value,
                                    ast::AstExpressionValue::Literal(AstLiteralValue::Number(
                                        "5.5".to_string()
                                    ))
                                );
                            }
                            _ => panic!("Expected Comparison for col3<5.5 inside NOT"),
                        },
                        _ => panic!("Expected NOT on the right side of OR"),
                    }
                }
                _ => panic!("Expected top-level OR condition"),
            }
        }
        _ => panic!("Expected SelectStatement"),
    }
}

#[test]
fn test_parse_select_where_precedence() {
    // a = 1 OR b = 2 AND c = 3  =>  (a=1) OR ((b=2) AND (c=3))
    let sql = "SELECT * FROM test WHERE a = 1 OR b = 2 AND c = 3;";
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let ast = parser.parse().unwrap();
    match ast {
        Statement::Select(select_stmt) => {
            assert_eq!(select_stmt.from_clause.name, "test");
            assert!(select_stmt.joins.is_empty());
            let condition_tree = select_stmt.condition.unwrap();
            match condition_tree {
                ConditionTree::Or(left_or_box, right_or_box) => {
                    match *left_or_box {
                        ConditionTree::Comparison(cond) => {
                            assert_eq!(cond.column, "a");
                            assert_eq!(cond.operator, "="); // Added operator check
                            assert_eq!(
                                cond.value,
                                ast::AstExpressionValue::Literal(AstLiteralValue::Number(
                                    "1".to_string()
                                ))
                            );
                            // Added value check
                        }
                        _ => panic!("Expected comparison for 'a=1'"),
                    }
                    match *right_or_box {
                        ConditionTree::And(left_and_box, right_and_box) => {
                            match *left_and_box {
                                ConditionTree::Comparison(cond) => {
                                    assert_eq!(cond.column, "b");
                                    assert_eq!(cond.operator, "="); // Added operator check
                                    assert_eq!(
                                        cond.value,
                                        ast::AstExpressionValue::Literal(AstLiteralValue::Number(
                                            "2".to_string()
                                        ))
                                    ); // Added value check
                                }
                                _ => panic!("Expected comparison for 'b=2'"),
                            }
                            match *right_and_box {
                                ConditionTree::Comparison(cond) => {
                                    assert_eq!(cond.column, "c");
                                    assert_eq!(cond.operator, "="); // Added operator check
                                    assert_eq!(
                                        cond.value,
                                        ast::AstExpressionValue::Literal(AstLiteralValue::Number(
                                            "3".to_string()
                                        ))
                                    ); // Added value check
                                }
                                _ => panic!("Expected comparison for 'c=3'"),
                            }
                        }
                        _ => panic!("Expected AND for 'b=2 AND c=3'"),
                    }
                }
                _ => panic!("Expected OR at top level due to precedence"),
            }
        }
        _ => panic!("Expected SelectStatement"),
    }
}

#[test]
fn test_parse_select_where_is_null_and_is_not_null() {
    let sql = "SELECT * FROM test WHERE name IS NULL AND description IS NOT NULL;";
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let ast = parser.parse().unwrap();
    match ast {
        Statement::Select(select_stmt) => {
            assert_eq!(select_stmt.from_clause.name, "test");
            assert!(select_stmt.joins.is_empty());
            assert!(select_stmt.condition.is_some());
            let condition_tree = select_stmt.condition.unwrap();
            match condition_tree {
                ConditionTree::And(left_and_box, right_and_box) => {
                    match *left_and_box {
                        ConditionTree::Comparison(cond) => {
                            assert_eq!(cond.column, "name");
                            assert_eq!(cond.operator, "IS NULL");
                            assert_eq!(
                                cond.value,
                                ast::AstExpressionValue::Literal(AstLiteralValue::Null)
                            );
                        }
                        _ => panic!("Expected Comparison for name IS NULL"),
                    }
                    match *right_and_box {
                        ConditionTree::Comparison(cond) => {
                            assert_eq!(cond.column, "description");
                            assert_eq!(cond.operator, "IS NOT NULL");
                            assert_eq!(
                                cond.value,
                                ast::AstExpressionValue::Literal(AstLiteralValue::Null)
                            );
                        }
                        _ => panic!("Expected Comparison for description IS NOT NULL"),
                    }
                }
                _ => panic!("Expected AND at top level"),
            }
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
                ast::AstExpressionValue::Literal(AstLiteralValue::String("New Name".to_string()))
            );
            assert!(update_stmt.condition.is_some());
            match update_stmt.condition.unwrap() {
                ConditionTree::Comparison(cond) => {
                    assert_eq!(cond.column, "id");
                    assert_eq!(cond.operator, "=");
                    assert_eq!(
                        cond.value,
                        ast::AstExpressionValue::Literal(AstLiteralValue::Number("1".to_string()))
                    );
                }
                _ => panic!("Expected ConditionTree::Comparison for id condition"),
            }
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
                ast::AstExpressionValue::Literal(AstLiteralValue::Number("99.50".to_string()))
            );
            assert_eq!(update_stmt.assignments[1].column, "stock");
            assert_eq!(
                update_stmt.assignments[1].value,
                ast::AstExpressionValue::Literal(AstLiteralValue::Number("500".to_string()))
            );

            assert!(update_stmt.condition.is_some());
            match update_stmt.condition.unwrap() {
                ConditionTree::Comparison(cond) => {
                    assert_eq!(cond.column, "category");
                    assert_eq!(cond.operator, "=");
                    assert_eq!(
                        cond.value,
                        ast::AstExpressionValue::Literal(AstLiteralValue::String(
                            "electronics".to_string()
                        ))
                    );
                }
                _ => panic!("Expected ConditionTree::Comparison for category condition"),
            }
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
        assert_eq!(expected, "Expected literal, parameter (?), or column identifier for RHS of condition");
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
        assert_eq!(expected.to_lowercase(), "column name or '*'");
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
        assert_eq!(expected.to_lowercase(), "column name or '*' after comma");
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
        assert_eq!(expected.to_lowercase(), "expected column name, 'not', or '(' for condition");
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
            expected
                .to_lowercase()
                .contains("expected literal, parameter (?), or column identifier for rhs of condition")
                || expected.to_lowercase().contains("expected identifier") // If parse_qualified_identifier fails early
        );
        assert!(found.to_lowercase().contains("semicolon"));
    } else if let Err(SqlParseError::UnexpectedEOF) = result {
        // This case is also possible if input is "SELECT ... WHERE field =" (no semicolon)
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
            assert_eq!(select_stmt.from_clause.name, "test");
            assert!(select_stmt.joins.is_empty());
            match select_stmt.condition.unwrap() {
                ConditionTree::Comparison(cond) => {
                    assert_eq!(cond.column, "name");
                    assert_eq!(cond.operator, "=");
                    assert_eq!(
                        cond.value,
                        ast::AstExpressionValue::Literal(AstLiteralValue::String("".to_string()))
                    );
                }
                _ => panic!("Expected ConditionTree::Comparison for name condition"),
            }
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
            assert_eq!(update_stmt.assignments[0].value, ast::AstExpressionValue::Literal(AstLiteralValue::Null));
            match update_stmt.condition.unwrap() {
                ConditionTree::Comparison(cond) => {
                    assert_eq!(cond.column, "id");
                    assert_eq!(cond.operator, "=");
                    assert_eq!(
                        cond.value,
                        ast::AstExpressionValue::Literal(AstLiteralValue::Number("1".to_string()))
                    );
                }
                _ => panic!("Expected ConditionTree::Comparison for id condition"),
            }
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
            assert_eq!(select_stmt.from_clause.name, "test");
            assert!(select_stmt.joins.is_empty());
            match select_stmt.condition.unwrap() {
                ConditionTree::Comparison(cond) => {
                    assert_eq!(cond.column, "data");
                    assert_eq!(cond.operator, "=");
                    assert_eq!(cond.value, ast::AstExpressionValue::Literal(AstLiteralValue::Null));
                }
                _ => panic!("Expected ConditionTree::Comparison for data condition"),
            }
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
            assert_eq!(select_stmt.from_clause.name, "selections");
            assert!(select_stmt.joins.is_empty());
            match select_stmt.condition.unwrap() {
                ConditionTree::Comparison(cond) => {
                    assert_eq!(cond.column, "selector_id");
                    assert_eq!(cond.operator, "="); // Added operator check, was missing
                    assert_eq!(
                        cond.value,
                        ast::AstExpressionValue::Literal(AstLiteralValue::Number("1".to_string()))
                    );
                }
                _ => panic!("Expected ConditionTree::Comparison for selector_id condition"),
            }
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
            assert_eq!(insert_stmt.values[0][0], ast::AstExpressionValue::Literal(AstLiteralValue::String("Alice".to_string())));
            assert_eq!(
                insert_stmt.values[0][1],
                ast::AstExpressionValue::Literal(AstLiteralValue::String("alice@example.com".to_string()))
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
            assert_eq!(insert_stmt.values[0][0], ast::AstExpressionValue::Literal(AstLiteralValue::Number("1".to_string())));
            assert_eq!(insert_stmt.values[0][1], ast::AstExpressionValue::Literal(AstLiteralValue::String("Laptop".to_string())));
            assert_eq!(insert_stmt.values[0][2], ast::AstExpressionValue::Literal(AstLiteralValue::Number("1200.00".to_string())));
            // Check second set
            assert_eq!(insert_stmt.values[1].len(), 3);
            assert_eq!(insert_stmt.values[1][0], ast::AstExpressionValue::Literal(AstLiteralValue::Number("2".to_string())));
            assert_eq!(insert_stmt.values[1][1], ast::AstExpressionValue::Literal(AstLiteralValue::String("Mouse".to_string())));
            assert_eq!(insert_stmt.values[1][2], ast::AstExpressionValue::Literal(AstLiteralValue::Number("25.00".to_string())));
            // Check third set
            assert_eq!(insert_stmt.values[2].len(), 3);
            assert_eq!(insert_stmt.values[2][0], ast::AstExpressionValue::Literal(AstLiteralValue::Number("3".to_string())));
            assert_eq!(insert_stmt.values[2][1], ast::AstExpressionValue::Literal(AstLiteralValue::String("Keyboard".to_string())));
            assert_eq!(insert_stmt.values[2][2], ast::AstExpressionValue::Literal(AstLiteralValue::Number("75.50".to_string())));
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
            assert_eq!(insert_stmt.values[0][0], ast::AstExpressionValue::Literal(AstLiteralValue::String("USA".to_string())));
            assert_eq!(insert_stmt.values[0][1], ast::AstExpressionValue::Literal(AstLiteralValue::String("New York".to_string())));
            assert_eq!(insert_stmt.values[1].len(), 2);
            assert_eq!(insert_stmt.values[1][0], ast::AstExpressionValue::Literal(AstLiteralValue::String("CAN".to_string())));
            assert_eq!(insert_stmt.values[1][1], ast::AstExpressionValue::Literal(AstLiteralValue::String("Toronto".to_string())));
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
                ast::AstExpressionValue::Literal(AstLiteralValue::String("Finish report".to_string()))
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
            assert_eq!(select_stmt.from_clause.name, "my_table");
            assert!(select_stmt.joins.is_empty());
            let cond_tree = select_stmt.condition.unwrap();
            match cond_tree {
                ConditionTree::Comparison(cond) => {
                    assert_eq!(cond.column, "value");
                    assert_eq!(cond.operator, "="); // Added operator check
                    assert_eq!(
                        cond.value,
                        ast::AstExpressionValue::Literal(AstLiteralValue::Boolean(true))
                    );
                }
                _ => panic!("Expected simple comparison"),
            }
        }
        _ => panic!("Expected SelectStatement"),
    }
}

#[test]
fn test_parse_vector_literal_simple() {
    let sql = "SELECT * FROM vectors WHERE embedding = [1.0, 2.5, 3.0];";
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let ast = parser.parse().unwrap();
    match ast {
        Statement::Select(select_stmt) => {
            assert_eq!(select_stmt.from_clause.name, "vectors");
            assert!(select_stmt.joins.is_empty());
            match select_stmt.condition.unwrap() {
                ConditionTree::Comparison(cond) => {
                    assert_eq!(cond.column, "embedding");
                    assert_eq!(cond.operator, "=");
                    assert_eq!(
                        cond.value,
                        ast::AstExpressionValue::Literal(AstLiteralValue::Vector(vec![
                            AstLiteralValue::Number("1.0".to_string()),
                            AstLiteralValue::Number("2.5".to_string()),
                            AstLiteralValue::Number("3.0".to_string()),
                        ]))
                    );
                }
                _ => panic!("Expected simple comparison for vector literal"),
            }
        }
        _ => panic!("Expected SelectStatement for vector literal test"),
    }
}

#[test]
fn test_parse_vector_literal_mixed_types_and_nested() {
    // Assuming grammar allows nested vectors, though `AstLiteralValue::Vector` might not be used this way by engine.
    // Current parser should handle syntax: `[1, [2,3], 'text']`
    let sql = "INSERT INTO complex_data (val) VALUES ([1, ['nested_str', 2.0], true, NULL]);";
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let ast = parser.parse().unwrap();
    match ast {
        Statement::Insert(insert_stmt) => {
            assert_eq!(insert_stmt.values.len(), 1);
            assert_eq!(insert_stmt.values[0].len(), 1);
            let vector_val = &insert_stmt.values[0][0];
            match vector_val {
                ast::AstExpressionValue::Literal(AstLiteralValue::Vector(elements)) => {
                    assert_eq!(elements.len(), 4);
                    assert_eq!(elements[0], AstLiteralValue::Number("1".to_string()));
                    match &elements[1] {
                        // Nested vector
                        AstLiteralValue::Vector(nested_elements) => {
                            assert_eq!(nested_elements.len(), 2);
                            assert_eq!(
                                nested_elements[0],
                                AstLiteralValue::String("nested_str".to_string())
                            );
                            assert_eq!(
                                nested_elements[1],
                                AstLiteralValue::Number("2.0".to_string())
                            );
                        }
                        _ => panic!("Expected nested vector for second element"),
                    }
                    assert_eq!(elements[2], AstLiteralValue::Boolean(true));
                    assert_eq!(elements[3], AstLiteralValue::Null);
                }
                _ => panic!("Expected top-level vector literal"),
            }
        }
        _ => panic!("Expected InsertStatement for mixed/nested vector test"),
    }
}

#[test]
fn test_parse_vector_literal_empty() {
    let sql = "SELECT * FROM items WHERE tags = [];";
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let ast = parser.parse().unwrap();
    match ast {
        Statement::Select(select_stmt) => {
            assert_eq!(select_stmt.from_clause.name, "items");
            assert!(select_stmt.joins.is_empty());
            match select_stmt.condition.unwrap() {
                ConditionTree::Comparison(cond) => {
                    assert_eq!(cond.column, "tags");
                    assert_eq!(cond.operator, "=");
                    assert_eq!(
                        cond.value,
                        ast::AstExpressionValue::Literal(AstLiteralValue::Vector(vec![]))
                    );
                }
                _ => panic!("Expected simple comparison for empty vector"),
            }
        }
        _ => panic!("Expected SelectStatement for empty vector test"),
    }
}

#[test]
fn test_parse_vector_literal_trailing_comma_error() {
    let sql = "SELECT * FROM items WHERE tags = [1, 2,];";
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(matches!(result, Err(SqlParseError::UnexpectedToken { .. })));
    if let Err(SqlParseError::UnexpectedToken { expected, found, position: _ }) = result {
        assert_eq!(expected.to_lowercase(), "value after comma in vector literal");
        assert_eq!(found, "]");
    } else {
        panic!("Wrong error type for trailing comma in vector: {:?}", result);
    }
}

#[test]
fn test_parse_vector_literal_missing_comma_error() {
    let sql = "SELECT * FROM items WHERE tags = [1 2];";
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(matches!(result, Err(SqlParseError::UnexpectedToken { .. })));
    if let Err(SqlParseError::UnexpectedToken { expected, found, position: _ }) = result {
        assert_eq!(expected.to_lowercase(), "comma or ']' in vector literal");
        assert_eq!(found.to_lowercase(), "numericliteral(\"2\")"); // Match debug format
    } else {
        panic!("Wrong error type for missing comma in vector: {:?}", result);
    }
}

#[test]
fn test_parse_vector_literal_unclosed_error() {
    let sql = "SELECT * FROM items WHERE tags = [1, 2";
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse(); // This might be UnexpectedEOF or UnexpectedToken depending on semicolon
    if sql.ends_with(';') {
        assert!(matches!(result, Err(SqlParseError::UnexpectedToken { .. })));
        if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
            assert_eq!(expected.to_lowercase(), "comma or ']' in vector literal");
            assert_eq!(found.to_lowercase(), "semicolon");
        } else {
            panic!("Wrong error for unclosed vector with semicolon: {:?}", result);
        }
    } else {
        // SQL does not end with semicolon, e.g. "SELECT ... tags = [1, 2"
        if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
            assert!(
                expected.to_lowercase().contains("comma or ']'"),
                "Expected comma or ']' but got EOF"
            );
            assert!(
                found.to_lowercase().contains("eof"),
                "Found token should indicate EOF, was: {}",
                found
            );
        } else {
            // If it's not UnexpectedToken, then the original UnexpectedEOF might be relevant for other reasons
            // but for this specific case, we expect an EOF to be an "unexpected token".
            panic!(
                "Expected UnexpectedToken indicating EOF, but got different error or Ok: {:?}",
                result
            );
        }
    }
}

// --- Tests for specific error handling improvements from previous step ---

#[test]
fn test_select_trailing_comma_refined() {
    let sql = "SELECT col1, FROM mytable;";
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(matches!(result, Err(SqlParseError::UnexpectedToken { .. })));
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
        assert_eq!(expected.to_lowercase(), "column name or '*' after comma");
        assert_eq!(found.to_lowercase(), "from");
    } else {
        panic!("Wrong error type for trailing comma in SELECT: {:?}", result);
    }
}

#[test]
fn test_update_trailing_comma_in_set_refined() {
    let sql = "UPDATE mytable SET col1 = 1, WHERE id = 0;";
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(matches!(result, Err(SqlParseError::UnexpectedToken { .. })));
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
        assert_eq!(expected.to_lowercase(), "column name for assignment after comma");
        assert_eq!(found.to_lowercase(), "where");
    } else {
        panic!("Wrong error type for trailing comma in SET: {:?}", result);
    }
}

#[test]
fn test_create_table_trailing_comma_refined() {
    let sql = "CREATE TABLE mytable (col1 INT, );";
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(matches!(result, Err(SqlParseError::UnexpectedToken { .. })));
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
        assert_eq!(expected.to_lowercase(), "column definition");
        assert_eq!(found.to_lowercase(), ")");
    } else {
        panic!("Wrong error type for trailing comma in CREATE TABLE: {:?}", result);
    }
}

#[test]
fn test_create_table_empty_column_list_error() {
    let sql = "CREATE TABLE mytable ();";
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(matches!(result, Err(SqlParseError::UnexpectedToken { .. })));
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
        assert_eq!(expected.to_lowercase(), "column definition");
        assert_eq!(found.to_lowercase(), ")");
    } else {
        panic!("Wrong error type for empty column list in CREATE TABLE: {:?}", result);
    }
}

// --- Tests for DROP TABLE ---

#[test]
fn test_parse_drop_table_simple() {
    let sql = "DROP TABLE users;";
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let ast = parser.parse().unwrap();
    match ast {
        Statement::DropTable(drop_stmt) => {
            assert_eq!(drop_stmt.table_name, "users");
            assert!(!drop_stmt.if_exists);
        }
        _ => panic!("Expected DropTableStatement"),
    }
}

#[test]
fn test_parse_drop_table_if_exists() {
    let sql = "DROP TABLE IF EXISTS customers;";
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let ast = parser.parse().unwrap();
    match ast {
        Statement::DropTable(drop_stmt) => {
            assert_eq!(drop_stmt.table_name, "customers");
            assert!(drop_stmt.if_exists);
        }
        _ => panic!("Expected DropTableStatement with IF EXISTS"),
    }
}

#[test]
fn test_parse_drop_table_missing_table_keyword() {
    let sql = "DROP users;";
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(matches!(result, Err(SqlParseError::UnexpectedToken { .. })));
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
        assert!(expected.to_lowercase().contains("table"));
        assert!(found.to_lowercase().contains("identifier(\"users\")"));
    } else {
        panic!("Wrong error type for DROP missing TABLE: {:?}", result);
    }
}

#[test]
fn test_parse_drop_table_missing_table_name() {
    let sql = "DROP TABLE;";
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(matches!(
        result,
        Err(SqlParseError::UnexpectedToken { .. }) | Err(SqlParseError::UnexpectedEOF)
    ));
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
        assert!(expected.to_lowercase().contains("expected table name"));
        assert!(found.to_lowercase().contains("semicolon"));
    } else if let Err(SqlParseError::UnexpectedEOF) = result {
        // This can happen if input is "DROP TABLE"
    } else {
        panic!("Wrong error type for DROP TABLE missing name: {:?}", result);
    }
}

#[test]
fn test_parse_drop_table_if_exists_missing_table_name() {
    let sql = "DROP TABLE IF EXISTS;";
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(matches!(
        result,
        Err(SqlParseError::UnexpectedToken { .. }) | Err(SqlParseError::UnexpectedEOF)
    ));
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
        assert!(expected.to_lowercase().contains("expected table name"));
        assert!(found.to_lowercase().contains("semicolon"));
    } else if let Err(SqlParseError::UnexpectedEOF) = result {
        // This can happen if input is "DROP TABLE IF EXISTS"
    } else {
        panic!("Wrong error type for DROP TABLE IF EXISTS missing name: {:?}", result);
    }
}

#[test]
fn test_parse_drop_table_if_missing_exists() {
    let sql = "DROP TABLE IF customers;";
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(matches!(result, Err(SqlParseError::UnexpectedToken { .. })));
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
        assert!(expected.to_lowercase().contains("exists"));
        assert!(found.to_lowercase().contains("identifier(customers)")); // Removed escaped quotes
    } else {
        panic!("Wrong error type for DROP TABLE IF missing EXISTS: {:?}", result);
    }
}

// --- Tests for JOIN clauses ---

#[test]
fn test_parse_select_simple_inner_join() {
    let sql = "SELECT t1.name, t2.value FROM table1 t1 JOIN table2 t2 ON t1.id = t2.t1_id;";
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let ast_result = parser.parse();
    assert!(ast_result.is_ok(), "Parse failed: {:?}", ast_result.err());
    let ast = ast_result.unwrap();

    match ast {
        Statement::Select(select_stmt) => {
            assert_eq!(select_stmt.from_clause.name, "table1");
            assert_eq!(select_stmt.from_clause.alias, Some("t1".to_string()));
            assert_eq!(select_stmt.joins.len(), 1);

            let join_clause = &select_stmt.joins[0];
            assert_eq!(join_clause.join_type, ast::JoinType::Inner);
            assert_eq!(join_clause.right_source.name, "table2");
            assert_eq!(join_clause.right_source.alias, Some("t2".to_string()));
            assert!(join_clause.on_condition.is_some());

            match join_clause.on_condition.as_ref().unwrap() {
                ConditionTree::Comparison(cond) => {
                    assert_eq!(cond.column, "t1.id");
                    assert_eq!(cond.operator, "=");
                    assert_eq!(
                        cond.value,
                        ast::AstExpressionValue::ColumnIdentifier("t2.t1_id".to_string())
                    );
                }
                _ => panic!("Expected Comparison condition for ON clause"),
            }
        }
        _ => panic!("Expected SelectStatement"),
    }
}

#[test]
fn test_parse_select_simple_inner_join_original_on_fails() {
    // This test originally demonstrated a limitation with "col = col" in ON.
    // With AstExpressionValue supporting ColumnIdentifier, this should now parse.
    let sql = "SELECT t1.name, t2.value FROM table1 t1 JOIN table2 t2 ON t1.id = t2.t1_id;";
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let ast_result = parser.parse();
    assert!(
        ast_result.is_ok(),
        "Parse failed for 'col = col' ON condition: {:?}",
        ast_result.err()
    );
    let ast = ast_result.unwrap();

    match ast {
        Statement::Select(select_stmt) => {
            assert_eq!(select_stmt.from_clause.name, "table1");
            assert_eq!(select_stmt.from_clause.alias, Some("t1".to_string()));
            assert_eq!(select_stmt.joins.len(), 1);
            let join_clause = &select_stmt.joins[0];
            assert_eq!(join_clause.join_type, ast::JoinType::Inner);
            assert_eq!(join_clause.right_source.name, "table2");
            assert_eq!(join_clause.right_source.alias, Some("t2".to_string()));
            assert!(join_clause.on_condition.is_some());
            match join_clause.on_condition.as_ref().unwrap() {
                ConditionTree::Comparison(cond) => {
                    assert_eq!(cond.column, "t1.id");
                    assert_eq!(cond.operator, "=");
                    assert_eq!(
                        cond.value,
                        ast::AstExpressionValue::ColumnIdentifier("t2.t1_id".to_string())
                    );
                }
                _ => panic!("Expected Comparison condition for ON clause"),
            }
        }
        _ => panic!("Expected SelectStatement"),
    }
}

#[test]
fn test_parse_select_left_outer_join() {
    let sql = "SELECT * FROM tableA LEFT OUTER JOIN tableB ON tableA.id = 10;"; // Using literal for ON
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let ast = parser.parse().unwrap();
    match ast {
        Statement::Select(select_stmt) => {
            assert_eq!(select_stmt.from_clause.name, "tableA");
            assert_eq!(select_stmt.joins.len(), 1);
            let join = &select_stmt.joins[0];
            assert_eq!(join.join_type, ast::JoinType::LeftOuter);
            assert_eq!(join.right_source.name, "tableB");
            assert!(join.on_condition.is_some());
            match join.on_condition.as_ref().unwrap() {
                ConditionTree::Comparison(cond) => {
                    assert_eq!(cond.column, "tableA.id");
                    assert_eq!(cond.operator, "=");
                    assert_eq!(
                        cond.value,
                        ast::AstExpressionValue::Literal(AstLiteralValue::Number("10".to_string()))
                    );
                }
                _ => panic!("Expected Comparison condition for ON clause"),
            }
        }
        _ => panic!("Expected SelectStatement with LEFT OUTER JOIN"),
    }
}

#[test]
fn test_parse_select_right_join() {
    let sql = "SELECT * FROM tableA RIGHT JOIN tableB ON tableA.name = 'test';"; // Using literal
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let ast = parser.parse().unwrap();
    match ast {
        Statement::Select(select_stmt) => {
            assert_eq!(select_stmt.from_clause.name, "tableA");
            assert_eq!(select_stmt.joins.len(), 1);
            let join = &select_stmt.joins[0];
            assert_eq!(join.join_type, ast::JoinType::RightOuter); // RIGHT implies RIGHT OUTER
            assert_eq!(join.right_source.name, "tableB");
            assert!(join.on_condition.is_some());
        }
        _ => panic!("Expected SelectStatement with RIGHT JOIN"),
    }
}

#[test]
fn test_parse_select_full_outer_join() {
    let sql = "SELECT * FROM tableA FULL OUTER JOIN tableB ON tableA.flag = TRUE;"; // Using literal
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let ast = parser.parse().unwrap();
    match ast {
        Statement::Select(select_stmt) => {
            assert_eq!(select_stmt.from_clause.name, "tableA");
            assert_eq!(select_stmt.joins.len(), 1);
            let join = &select_stmt.joins[0];
            assert_eq!(join.join_type, ast::JoinType::FullOuter);
            assert_eq!(join.right_source.name, "tableB");
            assert!(join.on_condition.is_some());
        }
        _ => panic!("Expected SelectStatement with FULL OUTER JOIN"),
    }
}

#[test]
fn test_parse_select_cross_join() {
    let sql = "SELECT * FROM table1 CROSS JOIN table2;";
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let ast = parser.parse().unwrap();
    match ast {
        Statement::Select(select_stmt) => {
            assert_eq!(select_stmt.from_clause.name, "table1");
            assert_eq!(select_stmt.joins.len(), 1);
            let join = &select_stmt.joins[0];
            assert_eq!(join.join_type, ast::JoinType::Cross);
            assert_eq!(join.right_source.name, "table2");
            assert!(join.on_condition.is_none()); // CROSS JOIN has no ON
        }
        _ => panic!("Expected SelectStatement with CROSS JOIN"),
    }
}

#[test]
fn test_parse_select_multiple_joins() {
    let sql = "SELECT * FROM t1 JOIN t2 ON t1.id = 1 LEFT JOIN t3 ON t2.id = 2;"; // Using literals
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let ast = parser.parse().unwrap();
    match ast {
        Statement::Select(select_stmt) => {
            assert_eq!(select_stmt.from_clause.name, "t1");
            assert_eq!(select_stmt.joins.len(), 2);

            let join1 = &select_stmt.joins[0];
            assert_eq!(join1.join_type, ast::JoinType::Inner);
            assert_eq!(join1.right_source.name, "t2");
            assert!(join1.on_condition.is_some());
            match join1.on_condition.as_ref().unwrap() {
                ConditionTree::Comparison(cond) => assert_eq!(
                    cond.value,
                    ast::AstExpressionValue::Literal(AstLiteralValue::Number("1".to_string()))
                ),
                _ => panic!("Expected Comparison for first ON"),
            }

            let join2 = &select_stmt.joins[1];
            assert_eq!(join2.join_type, ast::JoinType::LeftOuter);
            assert_eq!(join2.right_source.name, "t3");
            assert!(join2.on_condition.is_some());
            match join2.on_condition.as_ref().unwrap() {
                ConditionTree::Comparison(cond) => assert_eq!(
                    cond.value,
                    ast::AstExpressionValue::Literal(AstLiteralValue::Number("2".to_string()))
                ),
                _ => panic!("Expected Comparison for second ON"),
            }
        }
        _ => panic!("Expected SelectStatement with multiple JOINs"),
    }
}

#[test]
fn test_parse_join_missing_on_condition_for_inner_join() {
    let sql = "SELECT * FROM t1 INNER JOIN t2;"; // Missing ON
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(matches!(result, Err(SqlParseError::UnexpectedToken { .. })));
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
        assert_eq!(expected.to_lowercase(), "on"); // Parser expects ON after table name for non-CROSS joins
        assert!(found.to_lowercase().contains("semicolon") || found.to_lowercase().contains("eof"));
    } else {
        panic!("Wrong error type for JOIN missing ON: {:?}", result);
    }
}

#[test]
fn test_parse_join_missing_table_name_after_on() {
    let sql = "SELECT * FROM t1 JOIN ON t1.id = t2.id;"; // Missing table after JOIN
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(matches!(result, Err(SqlParseError::UnexpectedToken { .. })));
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
        assert_eq!(expected.to_lowercase(), "expected table name after join clause");
        assert_eq!(found.to_lowercase(), "on");
    } else {
        panic!("Error expected for missing table name in JOIN: {:?}", result);
    }
}

#[test]
fn test_parse_join_cross_join_with_on_error() {
    let sql = "SELECT * FROM t1 CROSS JOIN t2 ON t1.id = 1;"; // Using literal for ON
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse(); // Should error because CROSS JOIN cannot have ON
                                 // The current parser consumes ON even for CROSS JOIN if it's there,
                                 // then fails later if it's not expecting more tokens.
                                 // A more specific error would be better.
    assert!(matches!(result, Err(SqlParseError::UnexpectedToken { .. })));
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
        // After "CROSS JOIN t2", it expects WHERE/ORDER/LIMIT/EOF. "ON" is unexpected.
        assert_eq!(expected.to_lowercase(), "end of statement or eof");
        assert_eq!(found.to_lowercase(), "on");
    } else {
        panic!("CROSS JOIN with ON should be an error: {:?}", result);
    }
}

// --- Tests for ORDER BY and LIMIT ---

#[test]
fn test_parse_select_order_by_simple() {
    let sql = "SELECT name FROM users ORDER BY name;";
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let ast = parser.parse().unwrap();
    match ast {
        Statement::Select(select_stmt) => {
            assert_eq!(select_stmt.from_clause.name, "users"); // Updated from source
            assert!(select_stmt.order_by.is_some());
            let order_by_list = select_stmt.order_by.unwrap();
            assert_eq!(order_by_list.len(), 1);
            assert_eq!(order_by_list[0].expression, "name");
            assert!(order_by_list[0].direction.is_none()); // Default ASC
            assert!(select_stmt.limit.is_none());
        }
        _ => panic!("Expected SelectStatement with ORDER BY"),
    }
}

#[test]
fn test_parse_select_order_by_asc_desc() {
    let sql = "SELECT id, score FROM results ORDER BY score DESC, id ASC;";
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let ast = parser.parse().unwrap();
    match ast {
        Statement::Select(select_stmt) => {
            assert!(select_stmt.order_by.is_some());
            let order_by_list = select_stmt.order_by.unwrap();
            assert_eq!(order_by_list.len(), 2);
            assert_eq!(order_by_list[0].expression, "score");
            assert_eq!(order_by_list[0].direction, Some(OrderDirection::Desc));
            assert_eq!(order_by_list[1].expression, "id");
            assert_eq!(order_by_list[1].direction, Some(OrderDirection::Asc));
        }
        _ => panic!("Expected SelectStatement with ORDER BY ASC/DESC"),
    }
}

#[test]
fn test_parse_select_limit_simple() {
    let sql = "SELECT * FROM products LIMIT 10;";
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let ast = parser.parse().unwrap();
    match ast {
        Statement::Select(select_stmt) => {
            assert_eq!(select_stmt.from_clause.name, "products"); // Updated
            assert!(select_stmt.limit.is_some());
            assert_eq!(select_stmt.limit.unwrap(), AstLiteralValue::Number("10".to_string()));
            assert!(select_stmt.order_by.is_none());
        }
        _ => panic!("Expected SelectStatement with LIMIT"),
    }
}

#[test]
fn test_parse_select_order_by_limit() {
    let sql = "SELECT name, age FROM people ORDER BY age DESC LIMIT 5;";
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let ast = parser.parse().unwrap();
    match ast {
        Statement::Select(select_stmt) => {
            assert_eq!(select_stmt.from_clause.name, "people"); // Updated
            assert!(select_stmt.order_by.is_some());
            let order_by_list = select_stmt.order_by.unwrap();
            assert_eq!(order_by_list.len(), 1);
            assert_eq!(order_by_list[0].expression, "age");
            assert_eq!(order_by_list[0].direction, Some(OrderDirection::Desc));

            assert!(select_stmt.limit.is_some());
            assert_eq!(select_stmt.limit.unwrap(), AstLiteralValue::Number("5".to_string()));
        }
        _ => panic!("Expected SelectStatement with ORDER BY and LIMIT"),
    }
}

#[test]
fn test_parse_select_limit_order_by_invalid_order() {
    // LIMIT must come after ORDER BY if both are present
    let sql = "SELECT name FROM people LIMIT 5 ORDER BY age DESC;";
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(matches!(result, Err(SqlParseError::UnexpectedToken { .. })));
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
        assert!(expected.to_lowercase().contains("end of statement or eof"));
        assert!(found.to_lowercase().contains("order"));
    } else {
        panic!("Wrong error type for LIMIT before ORDER BY: {:?}", result);
    }
}

#[test]
fn test_parse_order_by_missing_column() {
    let sql = "SELECT * FROM t ORDER BY ;";
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(matches!(result, Err(SqlParseError::UnexpectedToken { .. })));
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
        assert!(expected.to_lowercase().contains("at least one column for order by"));
        assert!(found.to_lowercase().contains("semicolon"));
    } else {
        panic!("Wrong error: {:?}", result);
    }
}

#[test]
fn test_parse_order_by_trailing_comma() {
    let sql = "SELECT * FROM t ORDER BY name, ;";
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(matches!(result, Err(SqlParseError::UnexpectedToken { .. })));
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
        assert!(expected.to_lowercase().contains("column name after comma in order by"));
        assert!(found.to_lowercase().contains("semicolon"));
    } else {
        panic!("Wrong error: {:?}", result);
    }
}

#[test]
fn test_parse_limit_missing_value() {
    let sql = "SELECT * FROM t LIMIT ;";
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(matches!(
        result,
        Err(SqlParseError::UnexpectedToken { .. }) | Err(SqlParseError::UnexpectedEOF)
    ));
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
        assert!(expected.to_lowercase().contains("numeric literal for limit"));
        assert!(found.to_lowercase().contains("semicolon"));
    } else if let Err(SqlParseError::UnexpectedEOF) = result {
        // ok if no semicolon
    } else {
        panic!("Wrong error: {:?}", result);
    }
}

#[test]
fn test_parse_limit_non_numeric_value() {
    let sql = "SELECT * FROM t LIMIT 'abc';";
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(matches!(result, Err(SqlParseError::UnexpectedToken { .. })));
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
        assert!(expected.to_lowercase().contains("numeric literal for limit"));
        assert!(found.to_lowercase().contains("stringliteral(\"abc\")"));
    } else {
        panic!("Wrong error: {:?}", result);
    }
}

#[test]
fn test_translate_create_table_with_autoincrement() {
    let sql = "CREATE TABLE users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL
    );";
    let tokens = tokenize_str(sql);
    let mut parser = SqlParser::new(tokens);
    let ast = parser.parse().unwrap();
    let command = translate_ast_to_command(ast).unwrap();

    match command {
        Command::CreateTable { table_name, columns } => {
            assert_eq!(table_name, "users");
            assert_eq!(columns.len(), 2);

            // id INTEGER PRIMARY KEY AUTOINCREMENT
            let id_col = &columns[0];
            assert_eq!(id_col.name, "id");
            assert_eq!(id_col.data_type, DataType::Integer(0));
            assert!(id_col.is_primary_key);
            assert!(id_col.is_unique);
            assert!(!id_col.is_nullable);
            assert!(id_col.is_auto_increment);

            // name TEXT NOT NULL
            let name_col = &columns[1];
            assert_eq!(name_col.name, "name");
            assert_eq!(name_col.data_type, DataType::String("".to_string()));
            assert!(!name_col.is_primary_key);
            assert!(!name_col.is_unique);
            assert!(!name_col.is_nullable);
            assert!(!name_col.is_auto_increment);
        }
        _ => panic!("Expected CreateTable command"),
    }
}

#[test]
fn test_autoincrement_insert_functionality() {
    use crate::core::query::executor::QueryExecutor;
    use crate::core::storage::engine::InMemoryKvStore;
    use crate::core::wal::log_manager::LogManager;
    use crate::core::wal::writer::{WalWriter, WalWriterConfig};
    use std::sync::Arc;
    use tempfile::TempDir;

    // Create a temporary directory for the test
    let temp_dir = TempDir::new().unwrap();
    let temp_path = temp_dir.path().to_path_buf();

    // Create WAL writer and log manager
    let wal_config = WalWriterConfig::default();
    let wal_writer = WalWriter::new(temp_path.join("test.wal"), wal_config);
    let log_manager = Arc::new(LogManager::new());

    // Create executor with in-memory store
    let store = InMemoryKvStore::new();
    let mut executor =
        QueryExecutor::new(store, temp_path.clone(), wal_writer, log_manager).unwrap();

    // Create table with auto-increment column
    let create_sql = "CREATE TABLE test_table (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL
    );";
    let tokens = tokenize_str(create_sql);
    let mut parser = SqlParser::new(tokens);
    let ast = parser.parse().unwrap();
    let command = translate_ast_to_command(ast).unwrap();

    // Execute CREATE TABLE
    executor.execute_command(command).unwrap();

    // Insert rows without specifying ID (should auto-generate)
    let insert_sql1 = "INSERT INTO test_table (name) VALUES ('Alice');";
    let tokens1 = tokenize_str(insert_sql1);
    let mut parser1 = SqlParser::new(tokens1);
    let ast1 = parser1.parse().unwrap();
    let command1 = translate_ast_to_command(ast1).unwrap();
    executor.execute_command(command1).unwrap();

    let insert_sql2 = "INSERT INTO test_table (name) VALUES ('Bob');";
    let tokens2 = tokenize_str(insert_sql2);
    let mut parser2 = SqlParser::new(tokens2);
    let ast2 = parser2.parse().unwrap();
    let command2 = translate_ast_to_command(ast2).unwrap();
    executor.execute_command(command2).unwrap();

    // Verify auto-increment state
    assert_eq!(executor.get_next_auto_increment_value("test_table", "id"), 3);
}
