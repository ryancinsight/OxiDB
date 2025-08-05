//! Result transformers for post-processing query results
//!
//! Transform query results for different output formats.

use super::{QueryResult, ResultTransformer};

/// Transformer that limits the number of results
pub struct LimitTransformer {
    limit: usize,
}

impl LimitTransformer {
    pub fn new(limit: usize) -> Self {
        Self { limit }
    }
}

impl ResultTransformer for LimitTransformer {
    fn transform<'a>(&self, mut result: QueryResult<'a>) -> QueryResult<'a> {
        // Apply limit by wrapping the iterator
        result.rows = Box::new(result.rows.take(self.limit));
        result
    }
}