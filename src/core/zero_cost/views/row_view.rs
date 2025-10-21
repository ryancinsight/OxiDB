//! Zero-copy row view for efficient data access without copying
//!
//! Provides a view over a row's values that allows accessing data efficiently
//! while maintaining zero-copy semantics.

use crate::core::common::types::Value;
use std::ops::Index;
use std::slice;

/// Zero-copy view over a row's values
///
/// Provides efficient access to row data without copying values.
/// Designed for high-performance database operations where memory allocation
/// should be minimized.
#[derive(Debug)]
pub struct RowView<'a> {
    values: &'a [Value],
}

impl<'a> RowView<'a> {
    /// Create a new row view
    ///
    /// # Arguments
    /// * `values` - Slice of values representing the row
    ///
    /// # Examples
    /// ```
    /// use oxidb::core::zero_cost::views::RowView;
    /// use oxidb::core::common::types::Value;
    ///
    /// let values = vec![Value::Integer(1), Value::Text("test".to_string())];
    /// let row_view = RowView::new(&values);
    /// assert_eq!(row_view.len(), 2);
    /// ```
    #[inline]
    pub const fn new(values: &'a [Value]) -> Self {
        Self { values }
    }

    /// Get a value by column index
    ///
    /// # Arguments
    /// * `index` - Column index to access
    ///
    /// # Returns
    /// * `Some(&Value)` - If index is valid
    /// * `None` - If index is out of bounds
    #[inline]
    pub fn get(&self, index: usize) -> Option<&'a Value> {
        self.values.get(index)
    }

    /// Get the number of columns
    ///
    /// # Returns
    /// Number of values in this row view
    #[inline]
    pub const fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if the row is empty
    ///
    /// # Returns
    /// `true` if the row has no values, `false` otherwise
    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Iterate over values
    ///
    /// # Returns
    /// Iterator over the values in this row
    #[inline]
    pub fn iter(&self) -> slice::Iter<'a, Value> {
        self.values.iter()
    }

    /// Project specific columns
    ///
    /// Creates a new collection containing only the values at the specified indices.
    /// Missing indices are filtered out.
    ///
    /// # Arguments
    /// * `indices` - Column indices to project
    ///
    /// # Returns
    /// Vector containing references to the projected values
    #[inline]
    pub fn project(&self, indices: &[usize]) -> Vec<&'a Value> {
        indices.iter().filter_map(|&idx| self.get(idx)).collect()
    }
}

impl Index<usize> for RowView<'_> {
    type Output = Value;

    /// Index into the row view
    ///
    /// # Panics
    /// Panics if the index is out of bounds
    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        &self.values[index]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::common::types::Value;

    #[test]
    fn test_row_view_creation() {
        let values = vec![Value::Integer(1), Value::Text("test".to_string())];
        let row_view = RowView::new(&values);

        assert_eq!(row_view.len(), 2);
        assert!(!row_view.is_empty());
    }

    #[test]
    fn test_row_view_access() {
        let values = vec![Value::Integer(42), Value::Text("hello".to_string())];
        let row_view = RowView::new(&values);

        assert_eq!(row_view.get(0), Some(&Value::Integer(42)));
        assert_eq!(row_view.get(1), Some(&Value::Text("hello".to_string())));
        assert_eq!(row_view.get(2), None);
    }

    #[test]
    fn test_row_view_index() {
        let values = vec![Value::Integer(100)];
        let row_view = RowView::new(&values);

        assert_eq!(&row_view[0], &Value::Integer(100));
    }

    #[test]
    fn test_row_view_projection() {
        let values = vec![Value::Integer(1), Value::Text("two".to_string()), Value::Integer(3)];
        let row_view = RowView::new(&values);

        let projected = row_view.project(&[0, 2, 5]); // Include invalid index
        assert_eq!(projected.len(), 2);
        assert_eq!(projected[0], &Value::Integer(1));
        assert_eq!(projected[1], &Value::Integer(3));
    }

    #[test]
    fn test_row_view_iteration() {
        let values = vec![Value::Integer(1), Value::Integer(2)];
        let row_view = RowView::new(&values);

        let collected: Vec<&Value> = row_view.iter().collect();
        assert_eq!(collected.len(), 2);
        assert_eq!(collected[0], &Value::Integer(1));
        assert_eq!(collected[1], &Value::Integer(2));
    }
}
