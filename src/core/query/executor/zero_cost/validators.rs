//! Query validators for pre-execution validation
//!
//! Validates queries before execution to catch errors early.

use super::QueryValidator;
use crate::core::common::OxidbError;

/// Basic SQL syntax validator
pub struct SyntaxValidator;

impl QueryValidator for SyntaxValidator {
    fn validate(&self, query: &str) -> Result<(), OxidbError> {
        // TODO: Implement syntax validation
        if query.trim().is_empty() {
            return Err(OxidbError::InvalidQuery {
                message: "Empty query".to_string(),
            });
        }
        Ok(())
    }
}