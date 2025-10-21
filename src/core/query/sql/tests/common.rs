//! Common test utilities for SQL parser tests

use crate::core::query::sql::tokenizer::{Token, Tokenizer};

/// Helper function to tokenize a string for tests
pub fn tokenize_str(input: &str) -> Vec<Token> {
    let mut tokenizer = Tokenizer::new(input);
    tokenizer.tokenize().unwrap_or_else(|e| {
        assert!(false, "Test tokenizer error: {}", e);
        unreachable!()
    })
}
