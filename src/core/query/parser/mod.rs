//! Query Parser Module
//!
//! This module is responsible for parsing raw string queries into the internal `Command` representation.
//! It handles tokenization, command identification, and argument extraction for supported database operations.
//! It now attempts to parse SQL-like queries first (SELECT, UPDATE) and falls back to a legacy
//! command parser for other command types (GET, INSERT, DELETE, BEGIN, COMMIT, ROLLBACK).

use crate::core::common::OxidbError; // Changed
use crate::core::query::commands::{Command, Key};
use crate::core::types::DataType;

// Imports for the new SQL parser integration
use crate::core::query::sql::{self, parser::SqlParser, tokenizer::Tokenizer};

pub fn parse_query_string(query_str: &str) -> Result<Command, OxidbError> { // Changed
    if query_str.is_empty() {
        return Err(OxidbError::SqlParsing("Input query string cannot be empty.".to_string())); // Changed
    }

    let first_word = query_str.split_whitespace().next().unwrap_or("").to_uppercase();

    match first_word.as_str() {
        "SELECT" | "UPDATE" => {
            let mut tokenizer = Tokenizer::new(query_str);
            match tokenizer.tokenize() {
                Ok(tokens) => {
                    let mut parser = SqlParser::new(tokens);
                    match parser.parse() {
                        Ok(ast_statement) => {
                            sql::translator::translate_ast_to_command(ast_statement)
                        }
                        Err(sql_parse_error) => Err(OxidbError::SqlParsing(format!( // Changed
                            "SQL parse error: {}",
                            sql_parse_error
                        ))),
                    }
                }
                Err(sql_tokenizer_error) => Err(OxidbError::SqlParsing(format!( // Changed
                    "SQL tokenizer error: {}",
                    sql_tokenizer_error
                ))),
            }
        }
        "GET" | "INSERT" | "DELETE" | "BEGIN" | "COMMIT" | "ROLLBACK" => {
            parse_legacy_command_string(query_str)
        }
        _ => {
            let mut tokenizer = Tokenizer::new(query_str);
            match tokenizer.tokenize() {
                Ok(tokens) => {
                    let mut parser = SqlParser::new(tokens);
                    match parser.parse() {
                        Ok(ast_statement) => {
                            sql::translator::translate_ast_to_command(ast_statement)
                        }
                        Err(sql_parse_error) => {
                             Err(OxidbError::SqlParsing(format!( // Changed
                                "SQL parse error: {}. If you intended a legacy command, ensure it's one of GET, INSERT, DELETE, BEGIN, COMMIT, ROLLBACK.",
                                sql_parse_error
                            )))
                        }
                    }
                }
                Err(sql_tokenizer_error) => {
                     Err(OxidbError::SqlParsing(format!( // Changed
                        "SQL tokenizer error: {}. If you intended a legacy command, ensure it's one of GET, INSERT, DELETE, BEGIN, COMMIT, ROLLBACK.",
                        sql_tokenizer_error
                    )))
                }
            }
        }
    }
}

fn parse_legacy_command_string(query_str: &str) -> Result<Command, OxidbError> { // Changed
    if query_str.is_empty() {
        return Err(OxidbError::SqlParsing("Input query string cannot be empty.".to_string())); // Changed
    }

    let mut tokens: Vec<String> = Vec::new();
    let mut current_token = String::new();
    let mut in_quotes = false;

    for c in query_str.chars() {
        match c {
            ' ' if !in_quotes => {
                if !current_token.is_empty() {
                    tokens.push(current_token.clone());
                    current_token.clear();
                }
            }
            '"' => {
                if in_quotes {
                    tokens.push(current_token.clone());
                    current_token.clear();
                    in_quotes = false;
                } else {
                    if !current_token.is_empty() {
                        return Err(OxidbError::SqlParsing(format!( // Changed
                            "Unexpected quote in token: {}",
                            current_token
                        )));
                    }
                    in_quotes = true;
                }
            }
            _ => {
                current_token.push(c);
            }
        }
    }

    if in_quotes {
        return Err(OxidbError::SqlParsing("Unclosed quotes in query string.".to_string())); // Changed
    }

    if !current_token.is_empty() {
        tokens.push(current_token);
    }

    if tokens.is_empty() {
        return Err(OxidbError::SqlParsing("Input query string resulted in no tokens.".to_string())); // Changed
    }

    let command_str = tokens[0].to_uppercase();

    match command_str.as_str() {
        "GET" => {
            if tokens.len() == 2 {
                let key: Key = tokens[1].as_bytes().to_vec();
                Ok(Command::Get { key })
            } else {
                Err(OxidbError::SqlParsing(format!( // Changed
                    "GET command expects 1 argument, got {}",
                    tokens.len() - 1
                )))
            }
        }
        "INSERT" => {
            if tokens.len() == 3 {
                let key: Key = tokens[1].as_bytes().to_vec();
                let value_str = &tokens[2];

                let data_type_value = if value_str.eq_ignore_ascii_case("true") {
                    DataType::Boolean(true)
                } else if value_str.eq_ignore_ascii_case("false") {
                    DataType::Boolean(false)
                } else if let Ok(num) = value_str.parse::<i64>() {
                    DataType::Integer(num)
                } else if let Ok(num_f) = value_str.parse::<f64>() {
                    DataType::Float(num_f)
                } else {
                    DataType::String(value_str.clone())
                };
                Ok(Command::Insert { key, value: data_type_value })
            } else {
                Err(OxidbError::SqlParsing(format!( // Changed
                    "INSERT command expects 2 arguments, got {}",
                    tokens.len() - 1
                )))
            }
        }
        "DELETE" => {
            if tokens.len() == 2 {
                let key: Key = tokens[1].as_bytes().to_vec();
                Ok(Command::Delete { key })
            } else {
                Err(OxidbError::SqlParsing(format!( // Changed
                    "DELETE command expects 1 argument, got {}",
                    tokens.len() - 1
                )))
            }
        }
        "BEGIN" => {
            if tokens.len() == 1 {
                Ok(Command::BeginTransaction)
            } else {
                Err(OxidbError::SqlParsing(format!( // Changed
                    "BEGIN command expects 0 arguments, got {}",
                    tokens.len() - 1
                )))
            }
        }
        "COMMIT" => {
            if tokens.len() == 1 {
                Ok(Command::CommitTransaction)
            } else {
                Err(OxidbError::SqlParsing(format!( // Changed
                    "COMMIT command expects 0 arguments, got {}",
                    tokens.len() - 1
                )))
            }
        }
        "ROLLBACK" => {
            if tokens.len() == 1 {
                Ok(Command::RollbackTransaction)
            } else {
                Err(OxidbError::SqlParsing(format!( // Changed
                    "ROLLBACK command expects 0 arguments, got {}",
                    tokens.len() - 1
                )))
            }
        }
        _ => Err(OxidbError::SqlParsing(format!("Unknown legacy command: {}", tokens[0]))), // Changed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::query::commands::SelectColumnSpec;

    #[test]
    fn test_legacy_parse_get() {
        let result = parse_legacy_command_string("GET mykey");
        match result {
            Ok(Command::Get { key }) => assert_eq!(key, "mykey".as_bytes().to_vec()),
            _ => panic!("Expected GET command"),
        }
    }

    #[test]
    fn test_legacy_parse_get_case_insensitive() {
        let result = parse_legacy_command_string("get mykey");
        match result {
            Ok(Command::Get { key }) => assert_eq!(key, "mykey".as_bytes().to_vec()),
            _ => panic!("Expected GET command"),
        }
    }

    #[test]
    fn test_legacy_parse_insert() {
        let result = parse_legacy_command_string("INSERT mykey myvalue");
        match result {
            Ok(Command::Insert { key, value }) => {
                assert_eq!(key, "mykey".as_bytes().to_vec());
                assert_eq!(value, DataType::String("myvalue".to_string()));
            }
            _ => panic!("Expected INSERT command"),
        }
    }

    #[test]
    fn test_legacy_parse_insert_float() {
        let result = parse_legacy_command_string("INSERT mykey 123.45");
        match result {
            Ok(Command::Insert { key, value }) => {
                assert_eq!(key, "mykey".as_bytes().to_vec());
                assert_eq!(value, DataType::Float(123.45));
            }
            _ => panic!("Expected INSERT command with Float"),
        }
    }

    #[test]
    fn test_legacy_parse_insert_with_quotes() {
        let result = parse_legacy_command_string("INSERT mykey \"my value with spaces\"");
        match result {
            Ok(Command::Insert { key, value }) => {
                assert_eq!(key, "mykey".as_bytes().to_vec());
                assert_eq!(value, DataType::String("my value with spaces".to_string()));
            }
            _ => panic!("Expected INSERT command"),
        }
    }

    #[test]
    fn test_legacy_parse_insert_with_quotes_case_insensitive_command() {
        let result = parse_legacy_command_string("insert mykey \"my value with spaces\"");
        match result {
            Ok(Command::Insert { key, value }) => {
                assert_eq!(key, "mykey".as_bytes().to_vec());
                assert_eq!(value, DataType::String("my value with spaces".to_string()));
            }
            _ => panic!("Expected INSERT command"),
        }
    }

    #[test]
    fn test_legacy_parse_insert_integer() {
        let result = parse_legacy_command_string("INSERT mykey 12345");
        match result {
            Ok(Command::Insert { key, value }) => {
                assert_eq!(key, "mykey".as_bytes().to_vec());
                assert_eq!(value, DataType::Integer(12345));
            }
            _ => panic!("Expected INSERT command with Integer"),
        }
    }

    #[test]
    fn test_legacy_parse_delete() {
        let result = parse_legacy_command_string("DELETE mykey");
        match result {
            Ok(Command::Delete { key }) => assert_eq!(key, "mykey".as_bytes().to_vec()),
            _ => panic!("Expected DELETE command"),
        }
    }

    #[test]
    fn test_legacy_invalid_command() {
        let result = parse_legacy_command_string("UNKNOWN mykey");
        match result {
            Err(OxidbError::SqlParsing(msg)) => { // Changed
                assert!(msg.contains("Unknown legacy command: UNKNOWN"))
            }
            _ => panic!("Expected SqlParsing error"), // Changed
        }
    }

    #[test]
    fn test_legacy_unclosed_quotes() {
        let result = parse_legacy_command_string("INSERT mykey \"my value with spaces");
        match result {
            Err(OxidbError::SqlParsing(msg)) => assert_eq!(msg, "Unclosed quotes in query string."), // Changed
            _ => panic!("Expected SqlParsing error for unclosed quotes"), // Changed
        }
    }

    #[test]
    fn test_legacy_parse_begin_transaction() {
        let result = parse_legacy_command_string("BEGIN");
        assert!(matches!(result, Ok(Command::BeginTransaction))); // Changed to use matches!
    }

    #[test]
    fn test_main_parse_get_routes_to_legacy() {
        let result = parse_query_string("GET mykey");
        match result {
            Ok(Command::Get { key }) => assert_eq!(key, "mykey".as_bytes().to_vec()),
            _ => panic!("Expected GET command via main parser"),
        }
    }

    #[test]
    fn test_main_parse_insert_routes_to_legacy() {
        let result = parse_query_string("INSERT key1 val1");
        match result {
            Ok(Command::Insert { key, value }) => {
                assert_eq!(key, "key1".as_bytes().to_vec());
                assert_eq!(value, DataType::String("val1".to_string()));
            }
            _ => panic!("Expected INSERT command via main parser"),
        }
    }

    #[test]
    fn test_main_parse_select_simple_sql() {
        let result = parse_query_string("SELECT name FROM users;");
        match result {
            Ok(Command::Select { columns, source, condition }) => {
                assert_eq!(columns, SelectColumnSpec::Specific(vec!["name".to_string()]));
                assert_eq!(source, "users");
                assert!(condition.is_none());
            }
            Err(e) => panic!("Expected simple SELECT to parse, got error: {:?}", e),
            _ => panic!("Expected Command::Select for simple SQL"),
        }
    }

    #[test]
    fn test_main_parse_select_star_sql() {
        let result = parse_query_string("SELECT * FROM products WHERE id = 10;");
        match result {
            Ok(Command::Select { columns, source, condition }) => {
                assert_eq!(columns, SelectColumnSpec::All);
                assert_eq!(source, "products");
                assert!(condition.is_some());
                let cond = condition.unwrap();
                assert_eq!(cond.column, "id");
                assert_eq!(cond.operator, "=");
                assert_eq!(cond.value, DataType::Integer(10));
            }
            Err(e) => panic!("Expected SELECT * to parse, got error: {:?}", e),
            _ => panic!("Expected Command::Select for SQL with WHERE"),
        }
    }

    #[test]
    fn test_main_parse_update_sql() {
        let result = parse_query_string(
            "UPDATE users SET email = 'new@example.com' WHERE name = \"old name\";",
        );
        match result {
            Ok(Command::Update { source, assignments, condition }) => {
                assert_eq!(source, "users");
                assert_eq!(assignments.len(), 1);
                assert_eq!(assignments[0].column, "email");
                assert_eq!(assignments[0].value, DataType::String("new@example.com".to_string()));
                assert!(condition.is_some());
                let cond = condition.unwrap();
                assert_eq!(cond.column, "name");
                assert_eq!(cond.value, DataType::String("old name".to_string()));
            }
            Err(e) => panic!("Expected UPDATE to parse, got error: {:?}", e),
            _ => panic!("Expected Command::Update for SQL"),
        }
    }

    #[test]
    fn test_main_parse_sql_syntax_error() {
        let result = parse_query_string("SELECT name FROM users WHERE id =;");
        match result {
            Err(OxidbError::SqlParsing(msg)) => { // Changed
                // Check for the specific error message propagated from the new parser logic
                assert!(msg.contains("Expected value for condition"));
            }
            Ok(cmd) => {
                panic!("Expected SqlParsing error for SQL syntax error, got Ok({:?})", cmd) // Changed
            }
            other_err => panic!("Expected SqlParsing error, got {:?}", other_err), // Changed
        }
    }

    #[test]
    fn test_main_parse_sql_tokenizer_error() {
        let result = parse_query_string("SELECT name FROM users WHERE id = #;");
        match result {
            Err(OxidbError::SqlParsing(msg)) => { // Changed
                // Reverting to expect 34 as per observed tokenizer output
                assert!(msg.contains("SQL tokenizer error: Invalid character '#' at position 34"));
            }
            _ => panic!("Expected SqlParsing error for SQL tokenizer error"), // Changed
        }
    }

    #[test]
    fn test_main_parse_unknown_command_tries_sql_first() {
        let result = parse_query_string("QUERY mydata");
        match result {
            Err(OxidbError::SqlParsing(msg)) => { // Changed
                assert!(
                    msg.contains("SQL parse error: Unknown statement type")
                        || msg.contains("SQL tokenizer error")
                );
            }
            _ => panic!("Expected SqlParsing error for unknown command"), // Changed
        }
    }

    #[test]
    fn test_empty_query_main_parser() {
        let result = parse_query_string("");
        match result {
            Err(OxidbError::SqlParsing(msg)) => { // Changed
                assert_eq!(msg, "Input query string cannot be empty.")
            }
            _ => panic!("Expected SqlParsing error for empty query"), // Changed
        }
    }

    #[test]
    fn test_whitespace_query_main_parser() {
        let result = parse_query_string("   ");
        match result {
            Err(OxidbError::SqlParsing(msg)) => { // Changed
                assert!(msg.contains("SQL parse error: Unexpected end of input"));
            }
            _ => panic!("Expected SqlParsing error for whitespace only query, got {:?}", result), // Changed
        }
    }
}
