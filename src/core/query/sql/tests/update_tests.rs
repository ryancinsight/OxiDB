//! UPDATE statement parser tests

use super::common::tokenize_str;
use crate::core::query::sql::errors::SqlParseError;
use crate::core::query::sql::parser::SqlParser;

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
        assert!(false, "Wrong error type: {:?}", result);
    }
}

#[test]
fn test_update_empty_set_clause() {
    let tokens = tokenize_str("UPDATE table SET;");
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(
        matches!(result, Err(SqlParseError::UnexpectedToken { .. } | SqlParseError::UnexpectedEOF)),
        "Result was: {:?}",
        result
    );
    if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
        assert_eq!(expected.to_lowercase(), "column name for assignment");
        assert_eq!(found.to_lowercase(), "semicolon");
    } else if let Err(SqlParseError::UnexpectedEOF) = result {
        // also possible, if input is just "UPDATE table SET"
        assert!(false, "UnexpectedEOF, expected UnexpectedToken for 'UPDATE table SET;'");
    } else {
        assert!(
            false,
            "Wrong error type for empty SET clause: {:?}, expected UnexpectedToken",
            result
        );
    }
}

#[test]
fn test_update_missing_value_in_assignment() {
    let tokens = tokenize_str("UPDATE table SET field =;");
    let mut parser = SqlParser::new(tokens);
    let result = parser.parse();
    assert!(
        matches!(result, Err(SqlParseError::UnexpectedToken { .. } | SqlParseError::UnexpectedEOF)),
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
        assert!(false, "Wrong error type for missing value in assignment: {:?}", result);
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
        assert!(false, "Wrong error type for missing equals in assignment: {:?}", result);
    }
}

// Additional UPDATE tests will be extracted here...
