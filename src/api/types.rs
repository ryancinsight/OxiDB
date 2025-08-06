//! Public API types for Oxidb database

use crate::core::common::types::Value;

/// Represents the result of a query execution.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryResult {
    /// Column names in the result set
    pub columns: Vec<String>,
    /// The actual rows of data
    pub rows: Vec<Row>,
}

/// Represents a single row of data
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    /// Values in the row, ordered by column position
    pub values: Vec<Value>,
}

impl QueryResult {
    /// Create a new empty QueryResult
    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
        }
    }

    /// Create a new QueryResult with the given columns and rows
    pub fn new(columns: Vec<String>, rows: Vec<Row>) -> Self {
        Self { columns, rows }
    }

    /// Get the number of rows in the result
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Check if the result is empty
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

impl Row {
    /// Create a new row with the given values
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }

    /// Get a value by column index
    pub fn get(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }

    /// Get the number of values in the row
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if the row is empty
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}
