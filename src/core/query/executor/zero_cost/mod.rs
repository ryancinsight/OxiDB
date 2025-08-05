//! Zero-cost query executor with improved design principles
//!
//! This module provides a refactored query executor that:
//! - Follows SOLID principles with clear separation of concerns
//! - Uses zero-cost abstractions to minimize allocations
//! - Employs iterator combinators for efficient data processing
//! - Maintains single source of truth (SSOT) for data access

use std::borrow::Cow;
use crate::core::common::OxidbError;
use crate::core::types::DataType;

pub mod iterators;
pub mod processors;
pub mod validators;
pub mod transformers;

// Re-export commonly used types
pub use iterators::{WindowIterator, WindowRefIterator};

/// Zero-cost wrapper for query results that avoids unnecessary allocations
pub struct QueryResult<'a> {
    /// Column names - can be borrowed or owned
    pub columns: Cow<'a, [String]>,
    /// Rows of data - iterator-based for lazy evaluation
    pub rows: Box<dyn Iterator<Item = Row<'a>> + 'a>,
    /// Query metadata
    pub metadata: QueryMetadata,
}

/// Single row of query results with zero-copy semantics
#[derive(Debug)]
pub struct Row<'a> {
    /// Values in the row - can be borrowed when possible
    values: Cow<'a, [DataType]>,
}

impl<'a> Row<'a> {
    /// Create a new row from borrowed data
    #[inline]
    pub fn from_borrowed(values: &'a [DataType]) -> Self {
        Self {
            values: Cow::Borrowed(values),
        }
    }

    /// Create a new row from owned data
    #[inline]
    pub fn from_owned(values: Vec<DataType>) -> Self {
        Self {
            values: Cow::Owned(values),
        }
    }

    /// Get a value by index without cloning
    #[inline]
    pub fn get(&self, index: usize) -> Option<&DataType> {
        self.values.get(index)
    }

    /// Iterate over values
    #[inline]
    pub fn iter(&self) -> std::slice::Iter<'_, DataType> {
        self.values.iter()
    }

    /// Get the number of values
    #[inline]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

/// Query metadata for tracking execution details
#[derive(Debug, Default)]
pub struct QueryMetadata {
    /// Number of rows affected
    pub rows_affected: usize,
    /// Execution time in microseconds
    pub execution_time_us: u64,
    /// Whether the query used an index
    pub used_index: bool,
    /// Index name if used
    pub index_name: Option<String>,
}

/// Trait for query processors following Interface Segregation Principle
pub trait QueryProcessor: Send + Sync {
    /// Process a query and return results
    fn process<'a>(
        &self,
        query: &'a str,
        params: &[DataType],
    ) -> Result<QueryResult<'a>, OxidbError>;
}

/// Trait for query validators
pub trait QueryValidator: Send + Sync {
    /// Validate a query before execution
    fn validate(&self, query: &str) -> Result<(), OxidbError>;
}

/// Trait for result transformers
pub trait ResultTransformer: Send + Sync {
    /// Transform query results
    fn transform<'a>(&self, result: QueryResult<'a>) -> QueryResult<'a>;
}

/// Zero-cost string view for avoiding allocations
pub type StringView<'a> = Cow<'a, str>;

/// Zero-cost bytes view for binary data
pub type BytesView<'a> = Cow<'a, [u8]>;

/// Extension methods for efficient string handling
pub trait StringExt {
    /// Get a zero-copy view of the string
    fn as_string_view(&self) -> StringView<'_>;
}

impl StringExt for String {
    #[inline]
    fn as_string_view(&self) -> StringView<'_> {
        Cow::Borrowed(self.as_str())
    }
}

impl StringExt for &str {
    #[inline]
    fn as_string_view(&self) -> StringView<'_> {
        Cow::Borrowed(self)
    }
}

/// Extension methods for efficient byte handling
pub trait BytesExt {
    /// Get a zero-copy view of the bytes
    fn as_bytes_view(&self) -> BytesView<'_>;
}

impl BytesExt for Vec<u8> {
    #[inline]
    fn as_bytes_view(&self) -> BytesView<'_> {
        Cow::Borrowed(self.as_slice())
    }
}

impl BytesExt for &[u8] {
    #[inline]
    fn as_bytes_view(&self) -> BytesView<'_> {
        Cow::Borrowed(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_zero_copy() {
        let values = vec![
            DataType::Integer(42),
            DataType::String("test".to_string()),
        ];
        
        // Test borrowed row
        let borrowed_row = Row::from_borrowed(&values);
        assert_eq!(borrowed_row.len(), 2);
        assert_eq!(borrowed_row.get(0), Some(&DataType::Integer(42)));
        
        // Test owned row
        let owned_row = Row::from_owned(values.clone());
        assert_eq!(owned_row.len(), 2);
        assert_eq!(owned_row.get(1), Some(&DataType::String("test".to_string())));
    }

    #[test]
    fn test_string_view_zero_alloc() {
        let owned_string = String::from("hello");
        let string_view = owned_string.as_string_view();
        
        // Verify it's borrowed, not cloned
        match string_view {
            Cow::Borrowed(_) => (), // Expected
            Cow::Owned(_) => panic!("Expected borrowed, got owned"),
        }
    }
}