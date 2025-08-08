//! Public API types for Oxidb database

use crate::core::common::types::Value;

/// Represents the result of a query execution.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryResult {
    /// Tabular data (columns + rows)
    Data(DataSet),
    /// Number of rows affected (INSERT/UPDATE/DELETE)
    RowsAffected(u64),
    /// Command succeeded without returning data (e.g., BEGIN/COMMIT)
    Success,
}

/// A tabular result set with column names and rows
#[derive(Debug, Clone, PartialEq)]
pub struct DataSet {
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
    /// Convenience: construct a Data result
    pub fn new(columns: Vec<String>, rows: Vec<Row>) -> Self {
        Self::Data(DataSet { columns, rows })
    }

    /// Create an empty Data result
    pub fn empty() -> Self {
        Self::Data(DataSet { columns: Vec::new(), rows: Vec::new() })
    }

    /// Number of rows (0 for non-Data results)
    pub fn row_count(&self) -> usize {
        match self {
            Self::Data(ds) => ds.rows.len(),
            _ => 0,
        }
    }

    /// Compatibility helper used by some examples
    pub fn from_execution_result(result: QueryResult) -> QueryResult {
        result
    }
}

impl DataSet {
    /// Create a new DataSet
    pub fn new(columns: Vec<String>, rows: Vec<Row>) -> Self {
        Self { columns, rows }
    }

    /// Get an iterator over the rows
    pub fn rows(&self) -> impl Iterator<Item = &Row> {
        self.rows.iter()
    }

    /// Return a reference to columns (compatibility helper)
    pub fn columns(&self) -> &Vec<String> {
        &self.columns
    }

    /// Get the number of rows
    pub fn row_count(&self) -> usize {
        self.rows.len()
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

    /// Iterate over values (compatibility helper)
    pub fn iter(&self) -> std::slice::Iter<'_, Value> {
        self.values.iter()
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
