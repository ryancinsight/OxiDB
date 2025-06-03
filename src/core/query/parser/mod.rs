//! Query Parser Module
//!
//! This module is responsible for parsing raw string queries into the internal `Command` representation.
//! It handles tokenization, command identification, and argument extraction for supported database operations.

use crate::core::common::error::DbError;
use crate::core::query::commands::{Command, Key, Value};

/// Parses a raw query string into a `Command` object.
///
/// This function takes a string slice representing a query, tokenizes it,
/// and attempts to match it against known command patterns.
///
/// # Supported Commands:
/// * `GET <key>`: Retrieves a value associated with `<key>`.
/// * `INSERT <key> <value>`: Stores a `<key>`-`<value>` pair.
/// * `DELETE <key>`: Removes the entry associated with `<key>`.
///
/// # Key and Value Handling:
/// Keys and values are treated as strings. They are converted to `Vec<u8>`
/// (aliased as `Key` and `Value` respectively) using `.as_bytes().to_vec()`.
///
/// # Quoted Values for `INSERT`:
/// For the `INSERT` command, the `<value>` can be enclosed in double quotes
/// to include spaces. The quotes themselves are removed from the final value.
/// For example, `INSERT user_name "John Doe"` will result in the value "John Doe".
///
/// # Return Value:
/// * `Ok(Command)`: If the query string is valid and successfully parsed, this
///   function returns the corresponding `Command` enum variant (`Command::Get`,
///   `Command::Insert`, or `Command::Delete`).
/// * `Err(DbError::InvalidQuery(String))`: If the query string is malformed
///   (e.g., unknown command, incorrect number of arguments, unclosed quotes, empty input),
///   an `InvalidQuery` error is returned with a descriptive message.
///
/// # Examples
/// ```rust
/// use oxidb::core::query::parser::parse_query_string;
/// use oxidb::core::query::commands::Command;
/// use oxidb::core::common::error::DbError;
///
/// // Example: GET command
/// let get_query = "GET my_key";
/// match parse_query_string(get_query) {
///     Ok(Command::Get { key }) => assert_eq!(key, "my_key".as_bytes().to_vec()),
///     _ => panic!("GET query failed"),
/// }
///
/// // Example: INSERT command
/// let insert_query = "INSERT my_key my_value";
/// match parse_query_string(insert_query) {
///     Ok(Command::Insert { key, value }) => {
///         assert_eq!(key, "my_key".as_bytes().to_vec());
///         assert_eq!(value, "my_value".as_bytes().to_vec());
///     },
///     _ => panic!("INSERT query failed"),
/// }
///
/// // Example: INSERT command with quoted value
/// let insert_quoted_query = "INSERT user_name \"Alice Wonderland\"";
/// match parse_query_string(insert_quoted_query) {
///     Ok(Command::Insert { key, value }) => {
///         assert_eq!(key, "user_name".as_bytes().to_vec());
///         assert_eq!(value, "Alice Wonderland".as_bytes().to_vec());
///     },
///     _ => panic!("INSERT with quoted value query failed"),
/// }
///
/// // Example: DELETE command
/// let delete_query = "DELETE my_key";
/// match parse_query_string(delete_query) {
///     Ok(Command::Delete { key }) => assert_eq!(key, "my_key".as_bytes().to_vec()),
///     _ => panic!("DELETE query failed"),
/// }
///
/// // Example: Invalid command
/// let invalid_query = "UPDATE my_key my_value";
/// match parse_query_string(invalid_query) {
///     Err(DbError::InvalidQuery(msg)) => assert!(msg.contains("Unknown command")),
///     _ => panic!("Invalid query test failed"),
/// }
///
/// // Example: Incorrect number of arguments
/// let wrong_args_query = "GET key1 key2";
/// match parse_query_string(wrong_args_query) {
///     Err(DbError::InvalidQuery(msg)) => assert!(msg.contains("expects 1 argument")),
///     _ => panic!("Wrong arguments test failed"),
/// }
/// ```
pub fn parse_query_string(query_str: &str) -> Result<Command, DbError> {
    if query_str.is_empty() {
        return Err(DbError::InvalidQuery("Input query string cannot be empty.".to_string()));
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
                    // Ending quote
                    tokens.push(current_token.clone());
                    current_token.clear();
                    in_quotes = false;
                } else {
                    // Starting quote
                    if !current_token.is_empty() {
                        // This case should ideally not happen if quotes are properly used,
                        // e.g. `INSERT key"value"`, but we'll treat it as part of the quoted string.
                        // Alternatively, one could error here.
                         return Err(DbError::InvalidQuery(format!("Unexpected quote in token: {}", current_token)));
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
        return Err(DbError::InvalidQuery("Unclosed quotes in query string.".to_string()));
    }

    if !current_token.is_empty() {
        tokens.push(current_token);
    }

    if tokens.is_empty() {
        // This can happen if the input was only spaces
        return Err(DbError::InvalidQuery("Input query string resulted in no tokens.".to_string()));
    }

    let command_str = tokens[0].to_uppercase();

    match command_str.as_str() {
        "GET" => {
            if tokens.len() == 2 {
                let key: Key = tokens[1].as_bytes().to_vec();
                Ok(Command::Get { key })
            } else {
                Err(DbError::InvalidQuery(format!("GET command expects 1 argument, got {}", tokens.len() - 1)))
            }
        }
        "INSERT" => {
            if tokens.len() == 3 {
                let key: Key = tokens[1].as_bytes().to_vec();
                let value: Value = tokens[2].as_bytes().to_vec();
                Ok(Command::Insert { key, value })
            } else {
                Err(DbError::InvalidQuery(format!("INSERT command expects 2 arguments, got {}", tokens.len() - 1)))
            }
        }
        "DELETE" => {
            if tokens.len() == 2 {
                let key: Key = tokens[1].as_bytes().to_vec();
                Ok(Command::Delete { key })
            } else {
                Err(DbError::InvalidQuery(format!("DELETE command expects 1 argument, got {}", tokens.len() - 1)))
            }
        }
        "BEGIN" => {
            if tokens.len() == 1 {
                Ok(Command::BeginTransaction)
            } else {
                Err(DbError::InvalidQuery(format!("BEGIN command expects 0 arguments, got {}", tokens.len() - 1)))
            }
        }
        "COMMIT" => {
            if tokens.len() == 1 {
                Ok(Command::CommitTransaction)
            } else {
                Err(DbError::InvalidQuery(format!("COMMIT command expects 0 arguments, got {}", tokens.len() - 1)))
            }
        }
        "ROLLBACK" => {
            if tokens.len() == 1 {
                Ok(Command::RollbackTransaction)
            } else {
                Err(DbError::InvalidQuery(format!("ROLLBACK command expects 0 arguments, got {}", tokens.len() - 1)))
            }
        }
        _ => Err(DbError::InvalidQuery(format!("Unknown command: {}", tokens[0]))),
    }
}

// Basic tests (can be moved to a test module later)
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_get() {
        let result = parse_query_string("GET mykey");
        match result {
            Ok(Command::Get { key }) => assert_eq!(key, "mykey".as_bytes().to_vec()),
            _ => panic!("Expected GET command"),
        }
    }

    #[test]
    fn test_parse_get_case_insensitive() {
        let result = parse_query_string("get mykey");
         match result {
            Ok(Command::Get { key }) => assert_eq!(key, "mykey".as_bytes().to_vec()),
            _ => panic!("Expected GET command"),
        }
    }

    #[test]
    fn test_parse_insert() {
        let result = parse_query_string("INSERT mykey myvalue");
        match result {
            Ok(Command::Insert { key, value }) => {
                assert_eq!(key, "mykey".as_bytes().to_vec());
                assert_eq!(value, "myvalue".as_bytes().to_vec());
            }
            _ => panic!("Expected INSERT command"),
        }
    }

    #[test]
    fn test_parse_insert_with_quotes() {
        let result = parse_query_string("INSERT mykey \"my value with spaces\"");
        match result {
            Ok(Command::Insert { key, value }) => {
                assert_eq!(key, "mykey".as_bytes().to_vec());
                assert_eq!(value, "my value with spaces".as_bytes().to_vec());
            }
            _ => panic!("Expected INSERT command"),
        }
    }
    
    #[test]
    fn test_parse_insert_with_quotes_case_insensitive() {
        let result = parse_query_string("insert mykey \"my value with spaces\"");
        match result {
            Ok(Command::Insert { key, value }) => {
                assert_eq!(key, "mykey".as_bytes().to_vec());
                assert_eq!(value, "my value with spaces".as_bytes().to_vec());
            }
            _ => panic!("Expected INSERT command"),
        }
    }

    #[test]
    fn test_parse_delete() {
        let result = parse_query_string("DELETE mykey");
        match result {
            Ok(Command::Delete { key }) => assert_eq!(key, "mykey".as_bytes().to_vec()),
            _ => panic!("Expected DELETE command"),
        }
    }

    #[test]
    fn test_parse_delete_case_insensitive() {
        let result = parse_query_string("delete mykey");
        match result {
            Ok(Command::Delete { key }) => assert_eq!(key, "mykey".as_bytes().to_vec()),
            _ => panic!("Expected DELETE command"),
        }
    }

    #[test]
    fn test_invalid_command() {
        let result = parse_query_string("UNKNOWN mykey");
        match result {
            Err(DbError::InvalidQuery(msg)) => assert_eq!(msg, "Unknown command: UNKNOWN"),
            _ => panic!("Expected InvalidQuery error"),
        }
    }

    #[test]
    fn test_get_incorrect_args() {
        let result = parse_query_string("GET key1 key2");
        match result {
            Err(DbError::InvalidQuery(msg)) => assert_eq!(msg, "GET command expects 1 argument, got 2"),
            _ => panic!("Expected InvalidQuery error"),
        }
    }
    
    #[test]
    fn test_insert_incorrect_args_too_few() {
        let result = parse_query_string("INSERT key1");
        match result {
            Err(DbError::InvalidQuery(msg)) => assert_eq!(msg, "INSERT command expects 2 arguments, got 1"),
            _ => panic!("Expected InvalidQuery error"),
        }
    }

    #[test]
    fn test_insert_incorrect_args_too_many() {
        let result = parse_query_string("INSERT key1 val1 val2");
        match result {
            Err(DbError::InvalidQuery(msg)) => assert_eq!(msg, "INSERT command expects 2 arguments, got 3"),
            _ => panic!("Expected InvalidQuery error"),
        }
    }
    
    #[test]
    fn test_delete_incorrect_args() {
        let result = parse_query_string("DELETE key1 key2");
        match result {
            Err(DbError::InvalidQuery(msg)) => assert_eq!(msg, "DELETE command expects 1 argument, got 2"),
            _ => panic!("Expected InvalidQuery error"),
        }
    }

    #[test]
    fn test_unclosed_quotes() {
        let result = parse_query_string("INSERT mykey \"my value with spaces");
        match result {
            Err(DbError::InvalidQuery(msg)) => assert_eq!(msg, "Unclosed quotes in query string."),
            _ => panic!("Expected InvalidQuery error for unclosed quotes"),
        }
    }

    #[test]
    fn test_empty_query() {
        let result = parse_query_string("");
        match result {
            Err(DbError::InvalidQuery(msg)) => assert_eq!(msg, "Input query string cannot be empty."),
            _ => panic!("Expected InvalidQuery error for empty query"),
        }
    }

    #[test]
    fn test_whitespace_query() {
        let result = parse_query_string("   ");
        match result {
            Err(DbError::InvalidQuery(msg)) => assert_eq!(msg, "Input query string resulted in no tokens."),
            _ => panic!("Expected InvalidQuery error for whitespace only query"),
        }
    }

    #[test]
    fn test_command_with_leading_whitespace() {
        let result = parse_query_string("  GET mykey");
         match result {
            Ok(Command::Get { key }) => assert_eq!(key, "mykey".as_bytes().to_vec()),
            _ => panic!("Expected GET command"),
        }
    }

    #[test]
    fn test_command_with_trailing_whitespace() {
        let result = parse_query_string("GET mykey  ");
        match result {
            Ok(Command::Get { key }) => assert_eq!(key, "mykey".as_bytes().to_vec()),
            _ => panic!("Expected GET command"),
        }
    }
    
    #[test]
    fn test_insert_value_starts_with_quote_char_but_not_quoted() {
        // This is a tricky case. "value should be treated as a single token if not part of a quoted string.
        // My current tokenization logic might fail this if it expects a space after key before "
        // Let's test `INSERT key "value`
        let result = parse_query_string("INSERT key \"value"); // This is unclosed
        match result {
            Err(DbError::InvalidQuery(msg)) => assert_eq!(msg, "Unclosed quotes in query string."),
            _ => panic!("Expected InvalidQuery error for unclosed quotes"),
        }
    }

    #[test]
    fn test_insert_key_then_nothing() {
        let result = parse_query_string("INSERT key");
        match result {
            Err(DbError::InvalidQuery(msg)) => assert_eq!(msg, "INSERT command expects 2 arguments, got 1"),
            _ => panic!("Expected InvalidQuery error"),
        }
    }
    
    #[test]
    fn test_insert_key_then_empty_quoted_string() {
        let result = parse_query_string("INSERT key \"\"");
        match result {
            Ok(Command::Insert { key, value }) => {
                assert_eq!(key, "key".as_bytes().to_vec());
                assert_eq!(value, "".as_bytes().to_vec());
            }
            _ => panic!("Expected INSERT command with empty value"),
        }
    }

     #[test]
    fn test_token_concatenation_error() {
        // This test case is designed to fail with the current tokenization logic.
        // `INSERT key"value"` where `key"value` is treated as one token, then the logic for quotes will fail.
        // The current code has:
        // if !current_token.is_empty() {
        //    return Err(DbError::InvalidQuery(format!("Unexpected quote in token: {}", current_token)));
        // }
        // So, if current_token is "key" and then a quote appears, it should error out.
        let result = parse_query_string("INSERT key\"value\" otherkey");
        match result {
            Err(DbError::InvalidQuery(msg)) => {
                assert!(msg.contains("Unexpected quote in token: key"));
            }
            _ => panic!("Expected InvalidQuery error for unexpected quote"),
        }
    }

    // Tests for transaction commands
    #[test]
    fn test_parse_begin_transaction() {
        let result = parse_query_string("BEGIN");
        assert_eq!(result, Ok(Command::BeginTransaction));
    }

    #[test]
    fn test_parse_begin_transaction_case_insensitive() {
        let result = parse_query_string("begin");
        assert_eq!(result, Ok(Command::BeginTransaction));
    }

    #[test]
    fn test_parse_commit_transaction() {
        let result = parse_query_string("COMMIT");
        assert_eq!(result, Ok(Command::CommitTransaction));
    }

    #[test]
    fn test_parse_commit_transaction_case_insensitive() {
        let result = parse_query_string("commit");
        assert_eq!(result, Ok(Command::CommitTransaction));
    }

    #[test]
    fn test_parse_rollback_transaction() {
        let result = parse_query_string("ROLLBACK");
        assert_eq!(result, Ok(Command::RollbackTransaction));
    }

    #[test]
    fn test_parse_rollback_transaction_case_insensitive() {
        let result = parse_query_string("rollback");
        assert_eq!(result, Ok(Command::RollbackTransaction));
    }

    #[test]
    fn test_parse_begin_with_args_error() {
        let result = parse_query_string("BEGIN WORK");
        match result {
            Err(DbError::InvalidQuery(msg)) => {
                assert_eq!(msg, "BEGIN command expects 0 arguments, got 1")
            }
            _ => panic!("Expected InvalidQuery error for BEGIN with arguments"),
        }
    }

    #[test]
    fn test_parse_commit_with_args_error() {
        let result = parse_query_string("COMMIT NOW");
        match result {
            Err(DbError::InvalidQuery(msg)) => {
                assert_eq!(msg, "COMMIT command expects 0 arguments, got 1")
            }
            _ => panic!("Expected InvalidQuery error for COMMIT with arguments"),
        }
    }

    #[test]
    fn test_parse_rollback_with_args_error() {
        let result = parse_query_string("ROLLBACK SAVEPOINT A");
        match result {
            Err(DbError::InvalidQuery(msg)) => {
                // The tokenizer will create "SAVEPOINT" and "A" as separate tokens
                assert_eq!(msg, "ROLLBACK command expects 0 arguments, got 2")
            }
            _ => panic!("Expected InvalidQuery error for ROLLBACK with arguments"),
        }
    }
}
