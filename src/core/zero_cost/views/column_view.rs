//! Zero-copy column view for efficient column-oriented data access
//!
//! Provides a view over a specific column across multiple rows,
//! enabling efficient column operations without data copying.

use crate::core::common::types::{Row, Value};

/// Zero-copy view over a column's values across rows
/// 
/// Provides efficient access to column data for analytical operations.
/// Designed for high-performance database operations where column-oriented
/// access patterns are needed.
#[derive(Debug)]
pub struct ColumnView<'a> {
    rows: &'a [Row],
    column_index: usize,
}

impl<'a> ColumnView<'a> {
    /// Create a new column view
    /// 
    /// # Arguments
    /// * `rows` - Slice of rows to access
    /// * `column_index` - Index of the column to view
    /// 
    /// # Examples
    /// ```
    /// use oxidb::core::zero_cost::views::ColumnView;
    /// use oxidb::core::common::types::{Row, Value};
    /// 
    /// let rows = vec![
    ///     Row::new(vec![Value::Integer(1), Value::Text("Alice".to_string())]),
    ///     Row::new(vec![Value::Integer(2), Value::Text("Bob".to_string())]),
    /// ];
    /// let column_view = ColumnView::new(&rows, 1); // Name column
    /// assert_eq!(column_view.len(), 2);
    /// ```
    #[inline]
    pub const fn new(rows: &'a [Row], column_index: usize) -> Self {
        Self { rows, column_index }
    }

    /// Get value at row index
    /// 
    /// # Arguments
    /// * `row_index` - Index of the row to access
    /// 
    /// # Returns
    /// * `Some(&Value)` - If both row and column indices are valid
    /// * `None` - If either index is out of bounds
    #[inline]
    pub fn get(&self, row_index: usize) -> Option<&'a Value> {
        self.rows.get(row_index).and_then(|row| row.values.get(self.column_index))
    }

    /// Get the number of rows in this column view
    /// 
    /// # Returns
    /// Number of rows in the underlying table
    #[inline]
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Check if the column view is empty
    /// 
    /// # Returns
    /// `true` if there are no rows, `false` otherwise
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Iterator over column values
    /// 
    /// Returns an iterator that yields `Option<&Value>` for each row.
    /// `None` is returned for rows that don't have this column index.
    /// 
    /// # Returns
    /// Iterator over column values (may contain None for missing values)
    pub fn iter(&self) -> impl Iterator<Item = Option<&'a Value>> + '_ {
        self.rows.iter().map(move |row| row.values.get(self.column_index))
    }

    /// Count non-null values
    /// 
    /// Counts the number of rows that have a value for this column.
    /// 
    /// # Returns
    /// Number of non-null values in this column
    pub fn count_non_null(&self) -> usize {
        self.iter().filter(|opt| opt.is_some()).count()
    }

    /// Check if all values match a predicate
    /// 
    /// Tests whether all non-null values in the column satisfy the predicate.
    /// Returns `true` for empty columns.
    /// 
    /// # Arguments
    /// * `predicate` - Function to test each value
    /// 
    /// # Returns
    /// `true` if all non-null values satisfy the predicate
    pub fn all<F>(&self, predicate: F) -> bool
    where
        F: Fn(&Value) -> bool,
    {
        self.iter().flatten().all(predicate)
    }

    /// Check if any value matches a predicate
    /// 
    /// Tests whether any non-null value in the column satisfies the predicate.
    /// Returns `false` for empty columns.
    /// 
    /// # Arguments
    /// * `predicate` - Function to test each value
    /// 
    /// # Returns
    /// `true` if any non-null value satisfies the predicate
    pub fn any<F>(&self, predicate: F) -> bool
    where
        F: Fn(&Value) -> bool,
    {
        self.iter().flatten().any(predicate)
    }

    /// Get the column index
    /// 
    /// # Returns
    /// The column index this view represents
    #[inline]
    pub const fn column_index(&self) -> usize {
        self.column_index
    }

    /// Collect all non-null values
    /// 
    /// Creates a vector containing all non-null values in this column.
    /// 
    /// # Returns
    /// Vector of references to non-null values
    pub fn collect_values(&self) -> Vec<&'a Value> {
        self.iter().flatten().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::common::types::{Row, Value};

    #[test]
    fn test_column_view_creation() {
        let rows = vec![
            Row::new(vec![Value::Integer(1), Value::Text("Alice".to_string())]),
            Row::new(vec![Value::Integer(2), Value::Text("Bob".to_string())]),
        ];
        let column_view = ColumnView::new(&rows, 1);
        
        assert_eq!(column_view.len(), 2);
        assert!(!column_view.is_empty());
        assert_eq!(column_view.column_index(), 1);
    }

    #[test]
    fn test_column_view_access() {
        let rows = vec![
            Row::new(vec![Value::Integer(1), Value::Text("Alice".to_string())]),
            Row::new(vec![Value::Integer(2), Value::Text("Bob".to_string())]),
        ];
        let column_view = ColumnView::new(&rows, 1);
        
        assert_eq!(column_view.get(0), Some(&Value::Text("Alice".to_string())));
        assert_eq!(column_view.get(1), Some(&Value::Text("Bob".to_string())));
        assert_eq!(column_view.get(2), None);
    }

    #[test]
    fn test_column_view_invalid_column() {
        let rows = vec![
            Row::new(vec![Value::Integer(1)]),
            Row::new(vec![Value::Integer(2)]),
        ];
        let column_view = ColumnView::new(&rows, 5); // Invalid column
        
        assert_eq!(column_view.get(0), None);
        assert_eq!(column_view.get(1), None);
        assert_eq!(column_view.count_non_null(), 0);
    }

    #[test]
    fn test_column_view_count_non_null() {
        let rows = vec![
            Row::new(vec![Value::Integer(1), Value::Text("Alice".to_string())]),
            Row::new(vec![Value::Integer(2)]), // Missing second column
            Row::new(vec![Value::Integer(3), Value::Text("Charlie".to_string())]),
        ];
        let column_view = ColumnView::new(&rows, 1);
        
        assert_eq!(column_view.count_non_null(), 2);
    }

    #[test]
    fn test_column_view_predicates() {
        let rows = vec![
            Row::new(vec![Value::Integer(10)]),
            Row::new(vec![Value::Integer(20)]),
            Row::new(vec![Value::Integer(30)]),
        ];
        let column_view = ColumnView::new(&rows, 0);
        
        // Test all
        assert!(column_view.all(|v| matches!(v, Value::Integer(i) if *i > 0)));
        assert!(!column_view.all(|v| matches!(v, Value::Integer(i) if *i > 25)));
        
        // Test any
        assert!(column_view.any(|v| matches!(v, Value::Integer(i) if *i > 25)));
        assert!(!column_view.any(|v| matches!(v, Value::Integer(i) if *i > 50)));
    }

    #[test]
    fn test_column_view_collect_values() {
        let rows = vec![
            Row::new(vec![Value::Integer(1)]),
            Row::new(vec![Value::Integer(2)]),
        ];
        let column_view = ColumnView::new(&rows, 0);
        
        let values = column_view.collect_values();
        assert_eq!(values.len(), 2);
        assert_eq!(values[0], &Value::Integer(1));
        assert_eq!(values[1], &Value::Integer(2));
    }

    #[test]
    fn test_empty_column_view() {
        let rows: Vec<Row> = vec![];
        let column_view = ColumnView::new(&rows, 0);
        
        assert_eq!(column_view.len(), 0);
        assert!(column_view.is_empty());
        assert_eq!(column_view.count_non_null(), 0);
        assert!(column_view.all(|_| false)); // Empty case
        assert!(!column_view.any(|_| true)); // Empty case
    }
}