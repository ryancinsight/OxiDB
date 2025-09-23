// Common utilities for SQL parser tests
// Extracted from monolithic parser_tests.rs to enforce SLAP principle (<300 lines per module)

use crate::core::query::sql::tokenizer::{Token, Tokenizer};

// Modular test modules for focused testing (SOLID principle)
pub mod update_tests;

/// Helper function to tokenize a string for tests
/// 
/// # Errors
/// 
/// This function will return an error if tokenization fails, but panics for test clarity
pub fn tokenize_str(input: &str) -> Vec<Token> {
    let mut tokenizer = Tokenizer::new(input);
    tokenizer.tokenize().unwrap_or_else(|e| panic!("Test tokenizer error: {}", e))
}