//! Query parsing module.
//!
//! This module provides SQL query parsing functionality.

use crate::core::common::OxidbError;
use crate::core::query::commands::Command;
use crate::core::query::sql::parser::SqlParser;
use crate::core::query::sql::tokenizer::Tokenizer;
use crate::core::query::sql::translator::translate_ast_to_command;

/// Parse a query string into a Command.
///
/// This function parses SQL queries into internal command representations.
///
/// # Arguments
///
/// * `query_str` - The query string to parse
///
/// # Returns
///
/// * `Ok(Command)` - The parsed command
/// * `Err(OxidbError)` - If parsing fails
///
/// # Errors
///
/// This function will return an error if:
/// - SQL tokenization fails
/// - SQL parsing fails due to invalid syntax
pub fn parse_query(query_str: &str) -> Result<Command, OxidbError> {
    // Tokenize the query
    let mut tokenizer = Tokenizer::new(query_str);
    let tokens = tokenizer.tokenize().map_err(|e| {
        OxidbError::SqlParsing(format!("SQL tokenizer error: {e}"))
    })?;

    // Parse SQL
    let mut parser = SqlParser::new(tokens);
    let statement = parser.parse().map_err(|e| {
        OxidbError::SqlParsing(format!("SQL parse error: {e}"))
    })?;

    // Translate AST to Command
    translate_ast_to_command(statement)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::query::commands::SelectColumnSpec;

    #[test]
    fn test_parse_sql_select() {
        let result = parse_query("SELECT * FROM users");
        match result {
            Ok(Command::Select { columns, source, condition, order_by: _, limit: _ }) => {
                assert_eq!(columns, SelectColumnSpec::All);
                assert_eq!(source, "users");
                assert!(condition.is_none());
            }
            _ => panic!("Expected SELECT command for simple SQL"),
        }
    }

    #[test]
    fn test_parse_sql_select_with_where() {
        let result = parse_query("SELECT name, age FROM users WHERE age > 18");
        match result {
            Ok(Command::Select { columns, source, condition, order_by: _, limit: _ }) => {
                assert_eq!(
                    columns,
                    SelectColumnSpec::Specific(vec!["name".to_string(), "age".to_string()])
                );
                assert_eq!(source, "users");
                assert!(condition.is_some());
            }
            _ => panic!("Expected SELECT command for SQL with WHERE"),
        }
    }

    #[test]
    fn test_parse_sql_update() {
        let result = parse_query("UPDATE users SET name = 'John', age = 30 WHERE id = 1");
        match result {
            Ok(Command::Update { source, assignments, condition }) => {
                assert_eq!(source, "users");
                assert_eq!(assignments.len(), 2);
                assert_eq!(assignments[0].column, "name");
                assert_eq!(assignments[1].column, "age");
                assert!(condition.is_some());
            }
            _ => panic!("Expected UPDATE command for SQL"),
        }
    }

    #[test]
    fn test_parse_sql_insert() {
        let result = parse_query("INSERT INTO users (name, age) VALUES ('Alice', 25)");
        match result {
            Ok(Command::SqlInsert { table_name, columns, values }) => {
                assert_eq!(table_name, "users");
                assert_eq!(columns, Some(vec!["name".to_string(), "age".to_string()]));
                assert_eq!(values.len(), 1);
            }
            _ => panic!("Expected SqlInsert command"),
        }
    }

    #[test]
    fn test_parse_sql_delete() {
        let result = parse_query("DELETE FROM users WHERE id = 1");
        match result {
            Ok(Command::SqlDelete { table_name, condition }) => {
                assert_eq!(table_name, "users");
                assert!(condition.is_some());
            }
            _ => panic!("Expected SqlDelete command"),
        }
    }

    #[test]
    fn test_parse_sql_create_table() {
        let result = parse_query("CREATE TABLE users (id INTEGER, name TEXT)");
        match result {
            Ok(Command::CreateTable { table_name, columns }) => {
                assert_eq!(table_name, "users");
                assert_eq!(columns.len(), 2);
            }
            _ => panic!("Expected CreateTable command"),
        }
    }

    #[test]
    fn test_parse_transaction_commands() {
        // Transaction commands are now handled through SQL
        let result = parse_query("BEGIN");
        assert!(matches!(result, Ok(Command::BeginTransaction)));

        let result = parse_query("COMMIT");
        assert!(matches!(result, Ok(Command::CommitTransaction)));

        let result = parse_query("ROLLBACK");
        assert!(matches!(result, Ok(Command::RollbackTransaction)));
    }

    #[test]
    fn test_parse_invalid_sql() {
        let result = parse_query("INVALID SQL QUERY");
        match result {
            Err(OxidbError::SqlParsing(msg)) => {
                assert!(msg.contains("SQL"));
            }
            _ => panic!("Expected SQL parsing error"),
        }
    }

    #[test]
    fn test_parse_empty_query() {
        let result = parse_query("");
        assert!(matches!(result, Err(OxidbError::SqlParsing(_))));
    }
}
