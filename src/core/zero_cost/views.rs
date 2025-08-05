//! Zero-copy views for database data structures
//! 
//! This module provides view types that allow accessing data without copying,
//! enabling efficient data processing with minimal memory overhead.

use std::borrow::Cow;
use std::ops::Index;
use std::slice;
use crate::core::common::types::Value;
use crate::api::types::Row;

/// Zero-copy view over a row's values
#[derive(Debug)]
pub struct RowView<'a> {
    values: &'a [Value],
}

impl<'a> RowView<'a> {
    /// Create a new row view
    #[inline]
    pub const fn new(values: &'a [Value]) -> Self {
        Self { values }
    }
    
    /// Get a value by column index
    #[inline]
    pub fn get(&self, index: usize) -> Option<&'a Value> {
        self.values.get(index)
    }
    
    /// Get the number of columns
    #[inline]
    pub const fn len(&self) -> usize {
        self.values.len()
    }
    
    /// Check if the row is empty
    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
    
    /// Iterate over values
    #[inline]
    pub fn iter(&self) -> slice::Iter<'a, Value> {
        self.values.iter()
    }
    
    /// Project specific columns
    #[inline]
    pub fn project(&self, indices: &[usize]) -> Vec<&'a Value> {
        indices
            .iter()
            .filter_map(|&idx| self.get(idx))
            .collect()
    }
}

impl<'a> Index<usize> for RowView<'a> {
    type Output = Value;
    
    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        &self.values[index]
    }
}

/// Zero-copy table view for efficient data access
pub struct TableView<'a> {
    rows: &'a [Row],
    column_names: Cow<'a, [String]>,
}

impl<'a> TableView<'a> {
    /// Create a new table view
    #[inline]
    pub fn new(rows: &'a [Row], column_names: Cow<'a, [String]>) -> Self {
        Self { rows, column_names }
    }
    
    /// Get the number of rows
    #[inline]
    pub const fn row_count(&self) -> usize {
        self.rows.len()
    }
    
    /// Get the number of columns
    #[inline]
    pub fn column_count(&self) -> usize {
        self.column_names.len()
    }
    
    /// Get a row by index
    #[inline]
    pub fn get_row(&self, index: usize) -> Option<&'a Row> {
        self.rows.get(index)
    }
    
    /// Get column index by name
    #[inline]
    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.column_names
            .iter()
            .position(|col| col == name)
    }
    
    /// Create an iterator over rows
    #[inline]
    pub fn rows(&self) -> slice::Iter<'a, Row> {
        self.rows.iter()
    }
    
    /// Create a column view
    pub fn column(&self, column_index: usize) -> ColumnView<'a> {
        ColumnView::new(self.rows, column_index)
    }
}

/// Zero-copy column view for vertical data access
pub struct ColumnView<'a> {
    rows: &'a [Row],
    column_index: usize,
}

impl<'a> ColumnView<'a> {
    /// Create a new column view
    #[inline]
    pub const fn new(rows: &'a [Row], column_index: usize) -> Self {
        Self { rows, column_index }
    }
    
    /// Get value at row index
    #[inline]
    pub fn get(&self, row_index: usize) -> Option<&'a Value> {
        self.rows
            .get(row_index)
            .and_then(|row| row.get(self.column_index))
    }
    
    /// Iterator over column values
    pub fn iter(&self) -> impl Iterator<Item = Option<&'a Value>> + '_ {
        self.rows
            .iter()
            .map(move |row| row.get(self.column_index))
    }
    
    /// Count non-null values
    pub fn count_non_null(&self) -> usize {
        self.iter()
            .filter(|opt| opt.is_some())
            .count()
    }
    
    /// Check if all values match a predicate
    pub fn all<F>(&self, predicate: F) -> bool
    where
        F: Fn(&Value) -> bool,
    {
        self.iter()
            .filter_map(|opt| opt)
            .all(predicate)
    }
    
    /// Check if any value matches a predicate
    pub fn any<F>(&self, predicate: F) -> bool
    where
        F: Fn(&Value) -> bool,
    {
        self.iter()
            .filter_map(|opt| opt)
            .any(predicate)
    }
}

/// Zero-copy string view that can be either borrowed or owned
pub type StringView<'a> = Cow<'a, str>;

/// Zero-copy bytes view that can be either borrowed or owned
pub type BytesView<'a> = Cow<'a, [u8]>;

/// Value view that provides zero-copy access to Value contents
pub enum ValueView<'a> {
    Integer(i64),
    Float(f64),
    Text(StringView<'a>),
    Boolean(bool),
    Blob(BytesView<'a>),
    Vector(&'a [f32]),
    Null,
}

impl<'a> ValueView<'a> {
    /// Create a value view from a Value reference
    pub fn from_value(value: &'a Value) -> Self {
        match value {
            Value::Integer(i) => ValueView::Integer(*i),
            Value::Float(f) => ValueView::Float(*f),
            Value::Text(s) => ValueView::Text(Cow::Borrowed(s)),
            Value::Boolean(b) => ValueView::Boolean(*b),
            Value::Blob(b) => ValueView::Blob(Cow::Borrowed(b)),
            Value::Vector(v) => ValueView::Vector(v),
            Value::Null => ValueView::Null,
        }
    }
    
    /// Check if the value is null
    #[inline]
    pub const fn is_null(&self) -> bool {
        matches!(self, ValueView::Null)
    }
    
    /// Try to get as integer
    #[inline]
    pub const fn as_integer(&self) -> Option<i64> {
        match self {
            ValueView::Integer(i) => Some(*i),
            _ => None,
        }
    }
    
    /// Try to get as float
    #[inline]
    pub const fn as_float(&self) -> Option<f64> {
        match self {
            ValueView::Float(f) => Some(*f),
            _ => None,
        }
    }
    
    /// Try to get as string
    #[inline]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            ValueView::Text(s) => Some(s),
            _ => None,
        }
    }
    
    /// Try to get as boolean
    #[inline]
    pub const fn as_bool(&self) -> Option<bool> {
        match self {
            ValueView::Boolean(b) => Some(*b),
            _ => None,
        }
    }
    
    /// Try to get as bytes
    #[inline]
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            ValueView::Blob(b) => Some(b),
            _ => None,
        }
    }
    
    /// Try to get as vector
    #[inline]
    pub const fn as_vector(&self) -> Option<&[f32]> {
        match self {
            ValueView::Vector(v) => Some(v),
            _ => None,
        }
    }
}

/// Projection view that provides access to specific columns
pub struct ProjectionView<'a> {
    row: &'a Row,
    indices: &'a [usize],
}

impl<'a> ProjectionView<'a> {
    /// Create a new projection view
    #[inline]
    pub const fn new(row: &'a Row, indices: &'a [usize]) -> Self {
        Self { row, indices }
    }
    
    /// Get projected value by index
    #[inline]
    pub fn get(&self, index: usize) -> Option<&'a Value> {
        self.indices
            .get(index)
            .and_then(|&col_idx| self.row.get(col_idx))
    }
    
    /// Get the number of projected columns
    #[inline]
    pub const fn len(&self) -> usize {
        self.indices.len()
    }
    
    /// Check if projection is empty
    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }
    
    /// Iterate over projected values
    pub fn iter(&self) -> impl Iterator<Item = Option<&'a Value>> + '_ {
        self.indices
            .iter()
            .map(move |&idx| self.row.get(idx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_row_view() {
        let values = vec![
            Value::Integer(42),
            Value::Text("hello".to_string()),
            Value::Boolean(true),
        ];
        
        let view = RowView::new(&values);
        assert_eq!(view.len(), 3);
        assert_eq!(view.get(0), Some(&Value::Integer(42)));
        assert_eq!(view.get(1), Some(&Value::Text("hello".to_string())));
        assert_eq!(view.get(2), Some(&Value::Boolean(true)));
        assert_eq!(view.get(3), None);
    }
    
    #[test]
    fn test_value_view() {
        let text_value = Value::Text("test".to_string());
        let view = ValueView::from_value(&text_value);
        
        assert_eq!(view.as_str(), Some("test"));
        assert_eq!(view.as_integer(), None);
        assert!(!view.is_null());
    }
    
    #[test]
    fn test_column_view() {
        let rows = vec![
            Row::new(vec![Value::Integer(1), Value::Text("a".to_string())]),
            Row::new(vec![Value::Integer(2), Value::Text("b".to_string())]),
            Row::new(vec![Value::Integer(3), Value::Text("c".to_string())]),
        ];
        
        let col_view = ColumnView::new(&rows, 0);
        assert_eq!(col_view.get(0), Some(&Value::Integer(1)));
        assert_eq!(col_view.get(1), Some(&Value::Integer(2)));
        assert_eq!(col_view.count_non_null(), 3);
        
        assert!(col_view.all(|v| matches!(v, Value::Integer(_))));
        assert!(col_view.any(|v| matches!(v, Value::Integer(2))));
    }
}