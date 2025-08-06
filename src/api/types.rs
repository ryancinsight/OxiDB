//! Public API types for Oxidb database

use crate::core::common::types::Value;
use std::collections::HashMap;

/// Represents the result of a query execution.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryResult {
    /// Number of rows affected by the query (for INSERT, UPDATE, DELETE)
    pub rows_affected: usize,
    /// The result set for SELECT queries
    pub rows: Option<RowSet>,
}

/// Represents a set of rows returned from a query
#[derive(Debug, Clone, PartialEq)]
pub struct RowSet {
    /// Column names in the result set
    pub columns: Vec<String>,
    /// The actual row data
    pub rows: Vec<Row>,
}

/// Represents a single row of data
pub type Row = HashMap<String, Value>;

impl QueryResult {
    /// Create a new QueryResult for non-SELECT queries
    pub fn affected(rows_affected: usize) -> Self {
        Self {
            rows_affected,
            rows: None,
        }
    }

    /// Create a new QueryResult for SELECT queries
    pub fn with_rows(columns: Vec<String>, rows: Vec<Row>) -> Self {
        Self {
            rows_affected: rows.len(),
            rows: Some(RowSet { columns, rows }),
        }
    }
}
