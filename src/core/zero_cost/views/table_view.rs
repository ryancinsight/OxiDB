//! Zero-copy table view for efficient table-level data access
//!
//! Provides a view over a collection of rows with column metadata,
//! enabling efficient table operations without data copying.

use crate::core::common::types::Row;
use crate::core::zero_cost::views::column_view::ColumnView;
use std::borrow::Cow;
use std::slice;

/// Zero-copy view over a table's rows and schema
/// 
/// Provides efficient access to table data including rows and column metadata.
/// Designed for high-performance database operations where memory allocation
/// should be minimized.
#[derive(Debug)]
pub struct TableView<'a> {
    rows: &'a [Row],
    column_names: Cow<'a, [String]>,
}

impl<'a> TableView<'a> {
    /// Create a new table view
    /// 
    /// # Arguments
    /// * `rows` - Slice of rows representing the table data
    /// * `column_names` - Column names (borrowed or owned)
    /// 
    /// # Examples
    /// ```
    /// use oxidb::core::zero_cost::views::TableView;
    /// use oxidb::core::common::types::Row;
    /// use std::borrow::Cow;
    /// 
    /// let rows = vec![Row::new(vec![])];
    /// let column_names = vec!["id".to_string(), "name".to_string()];
    /// let table_view = TableView::new(&rows, Cow::Borrowed(&column_names));
    /// assert_eq!(table_view.row_count(), 1);
    /// assert_eq!(table_view.column_count(), 2);
    /// ```
    #[inline]
    pub fn new(rows: &'a [Row], column_names: Cow<'a, [String]>) -> Self {
        Self { rows, column_names }
    }

    /// Get the number of rows
    /// 
    /// # Returns
    /// Number of rows in this table view
    #[inline]
    pub const fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Get the number of columns
    /// 
    /// # Returns
    /// Number of columns based on the column names
    #[inline]
    pub fn column_count(&self) -> usize {
        self.column_names.len()
    }

    /// Get a row by index
    /// 
    /// # Arguments
    /// * `index` - Row index to access
    /// 
    /// # Returns
    /// * `Some(&Row)` - If index is valid
    /// * `None` - If index is out of bounds
    #[inline]
    pub fn get_row(&self, index: usize) -> Option<&'a Row> {
        self.rows.get(index)
    }

    /// Get column index by name
    /// 
    /// # Arguments
    /// * `name` - Column name to find
    /// 
    /// # Returns
    /// * `Some(usize)` - Column index if found
    /// * `None` - If column name not found
    #[inline]
    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.column_names.iter().position(|col| col == name)
    }

    /// Create an iterator over rows
    /// 
    /// # Returns
    /// Iterator over the rows in this table
    #[inline]
    pub fn rows(&self) -> slice::Iter<'a, Row> {
        self.rows.iter()
    }

    /// Create a column view
    /// 
    /// # Arguments
    /// * `column_index` - Index of the column to create a view for
    /// 
    /// # Returns
    /// ColumnView for accessing values in the specified column
    /// 
    /// # Examples
    /// ```
    /// use oxidb::core::zero_cost::views::TableView;
    /// use oxidb::core::common::types::{Row, Value};
    /// use std::borrow::Cow;
    /// 
    /// let rows = vec![
    ///     Row::new(vec![Value::Integer(1), Value::Text("Alice".to_string())]),
    ///     Row::new(vec![Value::Integer(2), Value::Text("Bob".to_string())]),
    /// ];
    /// let column_names = vec!["id".to_string(), "name".to_string()];
    /// let table_view = TableView::new(&rows, Cow::Borrowed(&column_names));
    /// 
    /// let name_column = table_view.column(1);
    /// assert_eq!(name_column.len(), 2);
    /// ```
    pub fn column(&self, column_index: usize) -> ColumnView<'a> {
        ColumnView::new(self.rows, column_index)
    }

    /// Get column names
    /// 
    /// # Returns
    /// Reference to the column names
    #[inline]
    pub fn column_names(&self) -> &[String] {
        &self.column_names
    }

    /// Check if the table is empty
    /// 
    /// # Returns
    /// `true` if the table has no rows, `false` otherwise
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::common::types::{Row, Value};

    #[test]
    fn test_table_view_creation() {
        let rows = vec![
            Row::new(vec![Value::Integer(1)]),
            Row::new(vec![Value::Integer(2)]),
        ];
        let column_names = vec!["id".to_string()];
        let table_view = TableView::new(&rows, Cow::Borrowed(&column_names));
        
        assert_eq!(table_view.row_count(), 2);
        assert_eq!(table_view.column_count(), 1);
        assert!(!table_view.is_empty());
    }

    #[test]
    fn test_table_view_row_access() {
        let rows = vec![
            Row::new(vec![Value::Integer(1)]),
            Row::new(vec![Value::Integer(2)]),
        ];
        let column_names = vec!["id".to_string()];
        let table_view = TableView::new(&rows, Cow::Borrowed(&column_names));
        
        assert!(table_view.get_row(0).is_some());
        assert!(table_view.get_row(1).is_some());
        assert!(table_view.get_row(2).is_none());
    }

    #[test]
    fn test_table_view_column_lookup() {
        let rows = vec![Row::new(vec![Value::Integer(1), Value::Text("test".to_string())])];
        let column_names = vec!["id".to_string(), "name".to_string()];
        let table_view = TableView::new(&rows, Cow::Borrowed(&column_names));
        
        assert_eq!(table_view.get_column_index("id"), Some(0));
        assert_eq!(table_view.get_column_index("name"), Some(1));
        assert_eq!(table_view.get_column_index("nonexistent"), None);
    }

    #[test]
    fn test_table_view_iteration() {
        let rows = vec![
            Row::new(vec![Value::Integer(1)]),
            Row::new(vec![Value::Integer(2)]),
        ];
        let column_names = vec!["id".to_string()];
        let table_view = TableView::new(&rows, Cow::Borrowed(&column_names));
        
        let collected: Vec<&Row> = table_view.rows().collect();
        assert_eq!(collected.len(), 2);
    }

    #[test]
    fn test_empty_table_view() {
        let rows: Vec<Row> = vec![];
        let column_names: Vec<String> = vec![];
        let table_view = TableView::new(&rows, Cow::Borrowed(&column_names));
        
        assert_eq!(table_view.row_count(), 0);
        assert_eq!(table_view.column_count(), 0);
        assert!(table_view.is_empty());
    }
}