//! Query processors for different query types
//!
//! Each processor handles a specific type of query (SELECT, INSERT, etc.)

use super::{QueryProcessor, QueryResult};
use crate::core::common::OxidbError;
use crate::core::types::DataType;

/// Processor for SELECT queries
pub struct SelectProcessor;

impl QueryProcessor for SelectProcessor {
    fn process<'a>(
        &self,
        _query: &'a str,
        _params: &[DataType],
    ) -> Result<QueryResult<'a>, OxidbError> {
        // TODO: Implement SELECT processing
        todo!("Implement SELECT processing")
    }
}