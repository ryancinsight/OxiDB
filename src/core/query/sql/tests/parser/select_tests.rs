// SELECT statement parser tests
// Extracted from monolithic parser_tests.rs to enforce SLAP principle (<300 lines per module)
// Focus: Single Responsibility - SELECT statement parsing validation

use super::tokenize_str;
use crate::core::query::sql::errors::SqlParseError;
use crate::core::query::sql::parser::SqlParser;
use crate::core::query::sql::ast::{Statement, SelectColumn};

/// Test simple SELECT statement parsing
#[test]
fn test_parse_select_simple() {
    let tokens = tokenize_str("SELECT id FROM users;");
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(result.is_ok(), "Failed to parse simple SELECT: {:?}", result);
    
    if let Ok(Statement::Select(select_stmt)) = result {
        assert_eq!(select_stmt.from_clause.name, "users");
        if let SelectColumn::ColumnName(column_name) = &select_stmt.columns[0] {
            assert_eq!(column_name, "id");
        } else {
            panic!("Expected column name 'id'");
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

/// Test SELECT with asterisk (all columns)
#[test] 
fn test_parse_select_asterisk() {
    let tokens = tokenize_str("SELECT * FROM users;");
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(result.is_ok(), "Failed to parse SELECT *: {:?}", result);
    
    if let Ok(Statement::Select(select_stmt)) = result {
        assert_eq!(select_stmt.from_clause.name, "users");
        assert!(matches!(select_stmt.columns[0], SelectColumn::Asterisk));
    } else {
        panic!("Expected SELECT statement");
    }
}

/// Test SELECT with multiple columns
#[test]
fn test_parse_select_multiple_columns() {
    let tokens = tokenize_str("SELECT id, name, age FROM users;");
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(result.is_ok(), "Failed to parse multi-column SELECT: {:?}", result);
    
    if let Ok(Statement::Select(select_stmt)) = result {
        assert_eq!(select_stmt.from_clause.name, "users");
        assert_eq!(select_stmt.columns.len(), 3);
        
        let expected_columns = ["id", "name", "age"];
        for (i, expected) in expected_columns.iter().enumerate() {
            if let SelectColumn::ColumnName(column_name) = &select_stmt.columns[i] {
                assert_eq!(column_name, expected);
            } else {
                panic!("Expected column name '{}'", expected);
            }
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

/// Test SELECT with simple WHERE clause
#[test]
fn test_parse_select_with_where_clause() {
    let tokens = tokenize_str("SELECT id FROM users WHERE age > 18;");
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(result.is_ok(), "Failed to parse SELECT with WHERE: {:?}", result);
    
    if let Ok(Statement::Select(select_stmt)) = result {
        assert_eq!(select_stmt.from_clause.name, "users");
        assert!(select_stmt.condition.is_some(), "Expected WHERE clause");
    } else {
        panic!("Expected SELECT statement");
    }
}

/// Test SELECT with missing FROM keyword (should fail)
#[test]
fn test_parse_select_missing_from() {
    let tokens = tokenize_str("SELECT id users;");
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(
        matches!(result, Err(SqlParseError::UnexpectedToken { .. })),
        "Expected error for missing FROM: {:?}",
        result
    );
    
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
        assert!(
            expected.to_lowercase().contains("from") || expected.to_lowercase().contains("keyword"),
            "Expected 'from' in error message, got: {}",
            expected
        );
        assert!(
            found.to_lowercase().contains("users") || found.to_lowercase().contains("identifier"),
            "Expected 'users' in found token, got: {}",
            found
        );
    } else {
        panic!("Wrong error type: {:?}", result);
    }
}

/// Test SELECT with ORDER BY clause
#[test]
fn test_parse_select_with_order_by() {
    let tokens = tokenize_str("SELECT id FROM users ORDER BY age ASC;");
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(result.is_ok(), "Failed to parse SELECT with ORDER BY: {:?}", result);
    
    if let Ok(Statement::Select(select_stmt)) = result {
        assert!(select_stmt.order_by.is_some(), "Expected ORDER BY clause");
        // More detailed ORDER BY validation would require examining the OrderByExpr structure
    } else {
        panic!("Expected SELECT statement");
    }
}

/// Test SELECT with LIMIT clause
#[test]
fn test_parse_select_with_limit() {
    let tokens = tokenize_str("SELECT id FROM users LIMIT 10;");
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(result.is_ok(), "Failed to parse SELECT with LIMIT: {:?}", result);
    
    if let Ok(Statement::Select(select_stmt)) = result {
        assert!(select_stmt.limit.is_some(), "Expected LIMIT clause");
        // LIMIT value validation would require examining AstLiteralValue structure
    } else {
        panic!("Expected SELECT statement");
    }
}

/// Test SELECT with complex WHERE clause (AND/OR operators)
#[test]
fn test_parse_select_complex_where() {
    let tokens = tokenize_str("SELECT id FROM users WHERE age > 18 AND name = 'John';");
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(result.is_ok(), "Failed to parse complex WHERE: {:?}", result);
    
    if let Ok(Statement::Select(select_stmt)) = result {
        assert!(select_stmt.condition.is_some(), "Expected WHERE clause");
        // Complex WHERE validation would require more detailed AST inspection
    } else {
        panic!("Expected SELECT statement");
    }
}

/// Test SELECT with IS NULL condition  
#[test]
fn test_parse_select_is_null() {
    let tokens = tokenize_str("SELECT id FROM users WHERE name IS NULL;");
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(result.is_ok(), "Failed to parse IS NULL: {:?}", result);
    
    if let Ok(Statement::Select(select_stmt)) = result {
        assert!(select_stmt.condition.is_some(), "Expected WHERE clause with IS NULL");
    } else {
        panic!("Expected SELECT statement");
    }
}

/// Test SELECT with IS NOT NULL condition
#[test]
fn test_parse_select_is_not_null() {
    let tokens = tokenize_str("SELECT id FROM users WHERE name IS NOT NULL;");
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(result.is_ok(), "Failed to parse IS NOT NULL: {:?}", result);
    
    if let Ok(Statement::Select(select_stmt)) = result {
        assert!(select_stmt.condition.is_some(), "Expected WHERE clause with IS NOT NULL");
    } else {
        panic!("Expected SELECT statement");
    }
}