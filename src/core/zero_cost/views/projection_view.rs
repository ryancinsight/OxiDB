//! Zero-copy projection view for efficient column selection
//!
//! Provides view over specific columns of a row without data copying,
//! enabling efficient projection operations in query processing.

use crate::core::common::types::{Row, Value};

/// Projection view that provides access to specific columns
///
/// Allows efficient access to a subset of columns from a row without
/// copying data. Useful for SELECT clause processing and column filtering.
#[derive(Debug)]
pub struct ProjectionView<'a> {
    row: &'a Row,
    indices: &'a [usize],
}

impl<'a> ProjectionView<'a> {
    /// Create a new projection view
    ///
    /// # Arguments
    /// * `row` - Row to create projection for
    /// * `indices` - Column indices to include in projection
    ///
    /// # Examples
    /// ```
    /// use oxidb::core::zero_cost::views::ProjectionView;
    /// use oxidb::core::common::types::{Row, Value};
    ///
    /// let row = Row::new(vec![
    ///     Value::Integer(1),
    ///     Value::Text("Alice".to_string()),
    ///     Value::Integer(25),
    /// ]);
    /// let indices = [0, 2]; // Select id and age, skip name
    /// let projection = ProjectionView::new(&row, &indices);
    /// assert_eq!(projection.len(), 2);
    /// ```
    #[inline]
    pub const fn new(row: &'a Row, indices: &'a [usize]) -> Self {
        Self { row, indices }
    }

    /// Get projected value by index
    ///
    /// # Arguments
    /// * `index` - Index in the projection (not the original row)
    ///
    /// # Returns
    /// * `Some(&Value)` - If projection index is valid and points to valid column
    /// * `None` - If projection index is invalid or points to invalid column
    #[inline]
    pub fn get(&self, index: usize) -> Option<&'a Value> {
        self.indices.get(index).and_then(|&col_idx| self.row.values.get(col_idx))
    }

    /// Get the number of projected columns
    ///
    /// # Returns
    /// Number of columns in this projection
    #[inline]
    pub const fn len(&self) -> usize {
        self.indices.len()
    }

    /// Check if projection is empty
    ///
    /// # Returns
    /// `true` if no columns are projected, `false` otherwise
    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    /// Iterate over projected values
    ///
    /// Returns an iterator that yields `Option<&Value>` for each projected column.
    /// `None` is returned for invalid column indices.
    ///
    /// # Returns
    /// Iterator over projected values (may contain None for invalid indices)
    pub fn iter(&self) -> impl Iterator<Item = Option<&'a Value>> + '_ {
        self.indices.iter().map(move |&idx| self.row.values.get(idx))
    }

    /// Get the projection indices
    ///
    /// # Returns
    /// Slice of column indices being projected
    #[inline]
    pub fn indices(&self) -> &[usize] {
        self.indices
    }

    /// Collect all valid projected values
    ///
    /// Creates a vector containing all valid projected values, filtering out
    /// any invalid column indices.
    ///
    /// # Returns
    /// Vector of references to valid projected values
    pub fn collect_values(&self) -> Vec<&'a Value> {
        self.iter().flatten().collect()
    }

    /// Check if all projected columns are valid
    ///
    /// # Returns
    /// `true` if all projection indices are valid for the row, `false` otherwise
    pub fn all_valid(&self) -> bool {
        self.indices.iter().all(|&idx| idx < self.row.values.len())
    }

    /// Count valid projected columns
    ///
    /// # Returns
    /// Number of projection indices that are valid for the row
    pub fn count_valid(&self) -> usize {
        self.indices.iter().filter(|&&idx| idx < self.row.values.len()).count()
    }

    /// Create a new projection with additional columns
    ///
    /// # Arguments
    /// * `additional_indices` - Additional column indices to include
    ///
    /// # Returns
    /// Vector containing current indices plus additional indices
    pub fn extend_indices(&self, additional_indices: &[usize]) -> Vec<usize> {
        let mut extended = self.indices.to_vec();
        extended.extend_from_slice(additional_indices);
        extended
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::common::types::{Row, Value};

    #[test]
    fn test_projection_view_creation() {
        let row =
            Row::new(vec![Value::Integer(1), Value::Text("Alice".to_string()), Value::Integer(25)]);
        let indices = [0, 2]; // Select id and age
        let projection = ProjectionView::new(&row, &indices);

        assert_eq!(projection.len(), 2);
        assert!(!projection.is_empty());
        assert_eq!(projection.indices(), &[0, 2]);
    }

    #[test]
    fn test_projection_view_access() {
        let row =
            Row::new(vec![Value::Integer(1), Value::Text("Alice".to_string()), Value::Integer(25)]);
        let indices = [0, 2]; // Select id and age
        let projection = ProjectionView::new(&row, &indices);

        assert_eq!(projection.get(0), Some(&Value::Integer(1))); // First projected column (id)
        assert_eq!(projection.get(1), Some(&Value::Integer(25))); // Second projected column (age)
        assert_eq!(projection.get(2), None); // Out of projection bounds
    }

    #[test]
    fn test_projection_view_invalid_indices() {
        let row = Row::new(vec![Value::Integer(1), Value::Text("Alice".to_string())]);
        let indices = [0, 5]; // Second index is invalid
        let projection = ProjectionView::new(&row, &indices);

        assert_eq!(projection.get(0), Some(&Value::Integer(1)));
        assert_eq!(projection.get(1), None); // Invalid column index
        assert!(!projection.all_valid());
        assert_eq!(projection.count_valid(), 1);
    }

    #[test]
    fn test_projection_view_iteration() {
        let row =
            Row::new(vec![Value::Integer(1), Value::Text("Alice".to_string()), Value::Integer(25)]);
        let indices = [2, 0]; // Age, then id (reversed order)
        let projection = ProjectionView::new(&row, &indices);

        let values: Vec<Option<&Value>> = projection.iter().collect();
        assert_eq!(values.len(), 2);
        assert_eq!(values[0], Some(&Value::Integer(25))); // Age
        assert_eq!(values[1], Some(&Value::Integer(1))); // ID
    }

    #[test]
    fn test_projection_view_collect_values() {
        let row =
            Row::new(vec![Value::Integer(1), Value::Text("Alice".to_string()), Value::Integer(25)]);
        let indices = [0, 2, 5]; // Include invalid index
        let projection = ProjectionView::new(&row, &indices);

        let values = projection.collect_values();
        assert_eq!(values.len(), 2); // Only valid values
        assert_eq!(values[0], &Value::Integer(1));
        assert_eq!(values[1], &Value::Integer(25));
    }

    #[test]
    fn test_empty_projection() {
        let row = Row::new(vec![Value::Integer(1)]);
        let indices: [usize; 0] = [];
        let projection = ProjectionView::new(&row, &indices);

        assert_eq!(projection.len(), 0);
        assert!(projection.is_empty());
        assert!(projection.all_valid()); // Vacuously true
        assert_eq!(projection.count_valid(), 0);
    }

    #[test]
    fn test_projection_extend_indices() {
        let row =
            Row::new(vec![Value::Integer(1), Value::Text("Alice".to_string()), Value::Integer(25)]);
        let indices = [0]; // Just id
        let projection = ProjectionView::new(&row, &indices);

        let extended = projection.extend_indices(&[1, 2]);
        assert_eq!(extended, vec![0, 1, 2]);
    }

    #[test]
    fn test_projection_view_all_valid() {
        let row = Row::new(vec![Value::Integer(1), Value::Text("Alice".to_string())]);

        // All valid indices
        let valid_indices = [0, 1];
        let valid_projection = ProjectionView::new(&row, &valid_indices);
        assert!(valid_projection.all_valid());
        assert_eq!(valid_projection.count_valid(), 2);

        // Some invalid indices
        let invalid_indices = [0, 5];
        let invalid_projection = ProjectionView::new(&row, &invalid_indices);
        assert!(!invalid_projection.all_valid());
        assert_eq!(invalid_projection.count_valid(), 1);
    }

    #[test]
    fn test_projection_view_duplicate_indices() {
        let row = Row::new(vec![Value::Integer(1), Value::Text("Alice".to_string())]);
        let indices = [0, 0, 1]; // Duplicate index
        let projection = ProjectionView::new(&row, &indices);

        assert_eq!(projection.len(), 3);
        assert_eq!(projection.get(0), Some(&Value::Integer(1)));
        assert_eq!(projection.get(1), Some(&Value::Integer(1))); // Same value again
        assert_eq!(projection.get(2), Some(&Value::Text("Alice".to_string())));
    }
}
