// src/core/zero_cost/views.rs
//! Zero-copy view abstractions for database data structures


use std::ops::{Index, Range};
use crate::core::types::DataType;

/// Zero-copy view of a database row
#[derive(Debug)]
pub struct RowView<'a> {
    data: &'a [DataType],
    column_names: &'a [String],
}

impl<'a> RowView<'a> {
    /// Create a new row view
    #[inline]
    pub const fn new(data: &'a [DataType], column_names: &'a [String]) -> Self {
        Self { data, column_names }
    }
    
    /// Get column count
    #[inline]
    pub fn column_count(&self) -> usize {
        self.data.len()
    }
    
    /// Get column by index
    #[inline]
    pub fn get_column(&self, index: usize) -> Option<&'a DataType> {
        self.data.get(index)
    }
    
    /// Get column by name
    pub fn get_column_by_name(&self, name: &str) -> Option<&'a DataType> {
        self.column_names
            .iter()
            .position(|col_name| col_name == name)
            .and_then(|index| self.data.get(index))
    }
    
    /// Get column names
    #[inline]
    pub const fn column_names(&self) -> &'a [String] {
        self.column_names
    }
    
    /// Iterate over columns
    #[inline]
    pub fn columns(&self) -> impl Iterator<Item = (&'a String, &'a DataType)> {
        self.column_names.iter().zip(self.data.iter())
    }
    
    /// Create a projection view with selected columns
    pub fn project<'b>(&'b self, column_indices: &'b [usize]) -> ProjectedRowView<'b>
    where
        'a: 'b,
    {
        ProjectedRowView::new(self, column_indices)
    }
}

impl<'a> Index<usize> for RowView<'a> {
    type Output = DataType;
    
    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        &self.data[index]
    }
}

/// Zero-copy projected view of a row with selected columns
#[derive(Debug)]
pub struct ProjectedRowView<'a> {
    row: &'a RowView<'a>,
    column_indices: &'a [usize],
}

impl<'a> ProjectedRowView<'a> {
    /// Create a new projected row view
    #[inline]
    pub const fn new(row: &'a RowView<'a>, column_indices: &'a [usize]) -> Self {
        Self { row, column_indices }
    }
    
    /// Get column count in projection
    #[inline]
    pub fn column_count(&self) -> usize {
        self.column_indices.len()
    }
    
    /// Get column by projection index
    #[inline]
    pub fn get_column(&self, proj_index: usize) -> Option<&'a DataType> {
        self.column_indices
            .get(proj_index)
            .and_then(|&actual_index| self.row.get_column(actual_index))
    }
    
    /// Iterate over projected columns
    pub fn columns(&self) -> impl Iterator<Item = (&String, &DataType)> + '_ {
        self.column_indices.iter().filter_map(move |&index| {
            self.row.column_names.get(index)
                .zip(self.row.data.get(index))
        })
    }
}

/// Zero-copy view of multiple rows (table view)
#[derive(Debug)]
pub struct TableView<'a> {
    rows: &'a [Vec<DataType>],
    column_names: &'a [String],
}

impl<'a> TableView<'a> {
    /// Create a new table view
    #[inline]
    pub const fn new(rows: &'a [Vec<DataType>], column_names: &'a [String]) -> Self {
        Self { rows, column_names }
    }
    
    /// Get row count
    #[inline]
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }
    
    /// Get column count
    #[inline]
    pub fn column_count(&self) -> usize {
        self.column_names.len()
    }
    
    /// Get row by index
    pub fn get_row(&self, index: usize) -> Option<RowView<'a>> {
        self.rows.get(index).map(|row_data| {
            RowView::new(row_data, self.column_names)
        })
    }
    
    /// Iterate over rows
    pub fn rows(&self) -> impl Iterator<Item = RowView<'a>> + '_ {
        self.rows.iter().map(move |row_data| {
            RowView::new(row_data, self.column_names)
        })
    }
    
    /// Get column names
    #[inline]
    pub const fn column_names(&self) -> &'a [String] {
        self.column_names
    }
    
    /// Create a slice view of rows
    pub fn slice(&'a self, range: Range<usize>) -> SlicedTableView<'a> {
        SlicedTableView::new(self, range)
    }
    
    /// Create a filtered view
    pub fn filter<F>(&'a self, predicate: F) -> FilteredTableView<'a, F>
    where
        F: Fn(&RowView<'a>) -> bool,
    {
        FilteredTableView::new(self, predicate)
    }
    
    /// Create a projected view with selected columns
    pub fn project(&'a self, column_indices: &'a [usize]) -> ProjectedTableView<'a> {
        ProjectedTableView::new(self, column_indices)
    }
}

/// Zero-copy sliced view of a table
#[derive(Debug)]
pub struct SlicedTableView<'a> {
    table: &'a TableView<'a>,
    range: Range<usize>,
}

impl<'a> SlicedTableView<'a> {
    /// Create a new sliced table view
    #[inline]
    pub const fn new(table: &'a TableView<'a>, range: Range<usize>) -> Self {
        Self { table, range }
    }
    
    /// Get row count in slice
    #[inline]
    pub fn row_count(&self) -> usize {
        (self.range.end - self.range.start).min(self.table.row_count() - self.range.start)
    }
    
    /// Get row by slice index
    pub fn get_row(&self, slice_index: usize) -> Option<RowView<'a>> {
        if slice_index < self.row_count() {
            self.table.get_row(self.range.start + slice_index)
        } else {
            None
        }
    }
    
    /// Iterate over rows in slice
    pub fn rows(&self) -> impl Iterator<Item = RowView<'a>> + '_ {
        let start = self.range.start;
        let count = self.row_count();
        (0..count).filter_map(move |i| {
            self.table.get_row(start + i)
        })
    }
}

/// Zero-copy filtered view of a table
#[derive(Debug)]
pub struct FilteredTableView<'a, F> {
    table: &'a TableView<'a>,
    predicate: F,
}

impl<'a, F> FilteredTableView<'a, F>
where
    F: Fn(&RowView<'a>) -> bool,
{
    /// Create a new filtered table view
    #[inline]
    pub const fn new(table: &'a TableView<'a>, predicate: F) -> Self {
        Self { table, predicate }
    }
    
    /// Iterate over filtered rows
    pub fn rows(&self) -> impl Iterator<Item = RowView<'a>> + '_ {
        self.table.rows().filter(&self.predicate)
    }
    
    /// Count filtered rows
    pub fn count(&self) -> usize {
        self.rows().count()
    }
}

/// Zero-copy projected view of a table with selected columns
#[derive(Debug)]
pub struct ProjectedTableView<'a> {
    table: &'a TableView<'a>,
    column_indices: &'a [usize],
}

impl<'a> ProjectedTableView<'a> {
    /// Create a new projected table view
    #[inline]
    pub const fn new(table: &'a TableView<'a>, column_indices: &'a [usize]) -> Self {
        Self { table, column_indices }
    }
    
    /// Get row count
    #[inline]
    pub fn row_count(&self) -> usize {
        self.table.row_count()
    }
    
    /// Get projected column count
    #[inline]
    pub fn column_count(&self) -> usize {
        self.column_indices.len()
    }
    
    /// Get projected row by index
    pub fn get_row(&self, index: usize) -> Option<ProjectedRowView<'a>> {
        if let Some(_row) = self.table.get_row(index) {
            // We need to create a ProjectedRowView that owns the row data
            // Since RowView is created on-the-fly, we can't borrow it
            // Instead, we should access the underlying data directly
            None // TODO: This needs a different approach - direct data access
        } else {
            None
        }
    }
    
    /// Iterate over projected rows
    pub fn rows(&self) -> impl Iterator<Item = ProjectedRowView<'a>> {
        // TODO: This needs a different approach - direct data access
        std::iter::empty()
    }
}

/// Zero-copy view for string data with optional interning
#[derive(Debug, Clone)]
pub enum StringView<'a> {
    Borrowed(&'a str),
    Owned(String),
    Interned(&'static str),
}

impl<'a> StringView<'a> {
    /// Create from borrowed string
    #[inline]
    pub const fn borrowed(s: &'a str) -> Self {
        Self::Borrowed(s)
    }
    
    /// Create from owned string
    #[inline]
    pub fn owned(s: String) -> Self {
        Self::Owned(s)
    }
    
    /// Create from interned string
    #[inline]
    pub const fn interned(s: &'static str) -> Self {
        Self::Interned(s)
    }
    
    /// Get string slice
    pub fn as_str(&self) -> &str {
        match self {
            Self::Borrowed(s) => s,
            Self::Owned(s) => s,
            Self::Interned(s) => s,
        }
    }
    
    /// Convert to owned string
    pub fn into_owned(self) -> String {
        match self {
            Self::Borrowed(s) => s.to_owned(),
            Self::Owned(s) => s,
            Self::Interned(s) => s.to_owned(),
        }
    }
    
    /// Check if the view owns the data
    #[inline]
    pub const fn is_owned(&self) -> bool {
        matches!(self, Self::Owned(_))
    }
}

impl<'a> AsRef<str> for StringView<'a> {
    #[inline]
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl<'a> std::fmt::Display for StringView<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Zero-copy view for binary data
#[derive(Debug, Clone)]
pub enum BytesView<'a> {
    Borrowed(&'a [u8]),
    Owned(Vec<u8>),
}

impl<'a> BytesView<'a> {
    /// Create from borrowed bytes
    #[inline]
    pub const fn borrowed(bytes: &'a [u8]) -> Self {
        Self::Borrowed(bytes)
    }
    
    /// Create from owned bytes
    #[inline]
    pub fn owned(bytes: Vec<u8>) -> Self {
        Self::Owned(bytes)
    }
    
    /// Get byte slice
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Borrowed(bytes) => bytes,
            Self::Owned(bytes) => bytes,
        }
    }
    
    /// Convert to owned bytes
    pub fn into_owned(self) -> Vec<u8> {
        match self {
            Self::Borrowed(bytes) => bytes.to_vec(),
            Self::Owned(bytes) => bytes,
        }
    }
    
    /// Get length
    #[inline]
    pub fn len(&self) -> usize {
        self.as_bytes().len()
    }
    
    /// Check if empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.as_bytes().is_empty()
    }
}

impl<'a> AsRef<[u8]> for BytesView<'a> {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

/// Zero-copy column-oriented view for analytical queries
#[derive(Debug)]
pub struct ColumnView<'a, T> {
    data: &'a [T],
    name: &'a str,
}

impl<'a, T> ColumnView<'a, T> {
    /// Create a new column view
    #[inline]
    pub const fn new(data: &'a [T], name: &'a str) -> Self {
        Self { data, name }
    }
    
    /// Get column name
    #[inline]
    pub const fn name(&self) -> &'a str {
        self.name
    }
    
    /// Get column data
    #[inline]
    pub const fn data(&self) -> &'a [T] {
        self.data
    }
    
    /// Get value count
    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }
    
    /// Check if empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
    
    /// Get value by index
    #[inline]
    pub fn get(&self, index: usize) -> Option<&'a T> {
        self.data.get(index)
    }
    
    /// Iterate over values
    #[inline]
    pub fn iter(&self) -> std::slice::Iter<'a, T> {
        self.data.iter()
    }
}

impl<'a, T> Index<usize> for ColumnView<'a, T> {
    type Output = T;
    
    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        &self.data[index]
    }
}

/// Zero-copy columnar table view for analytical processing
#[derive(Debug)]
pub struct ColumnarView<'a> {
    columns: Vec<ColumnView<'a, DataType>>,
    row_count: usize,
}

impl<'a> ColumnarView<'a> {
    /// Create a new columnar view
    pub fn new(columns: Vec<ColumnView<'a, DataType>>) -> Self {
        let row_count = columns.first().map_or(0, |col| col.len());
        Self { columns, row_count }
    }
    
    /// Get column count
    #[inline]
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }
    
    /// Get row count
    #[inline]
    pub const fn row_count(&self) -> usize {
        self.row_count
    }
    
    /// Get column by index
    #[inline]
    pub fn get_column(&self, index: usize) -> Option<&ColumnView<'a, DataType>> {
        self.columns.get(index)
    }
    
    /// Get column by name
    pub fn get_column_by_name(&self, name: &str) -> Option<&ColumnView<'a, DataType>> {
        self.columns.iter().find(|col| col.name() == name)
    }
    
    /// Iterate over columns
    #[inline]
    pub fn columns(&self) -> impl Iterator<Item = &ColumnView<'a, DataType>> {
        self.columns.iter()
    }
    
    /// Get value at row and column
    pub fn get_value(&self, row: usize, col: usize) -> Option<&'a DataType> {
        self.columns.get(col)?.get(row)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::DataType;
    
    #[test]
    fn test_row_view() {
        let data = vec![
            DataType::Integer(1),
            DataType::String("Alice".to_string()),
            DataType::Integer(25),
        ];
        let column_names = vec![
            "id".to_string(),
            "name".to_string(),
            "age".to_string(),
        ];
        
        let row = RowView::new(&data, &column_names);
        
        assert_eq!(row.column_count(), 3);
        assert_eq!(row.get_column_by_name("name"), Some(&DataType::String("Alice".to_string())));
        
        let mut iter = row.columns();
        assert_eq!(iter.next(), Some((&"id".to_string(), &DataType::Integer(1))));
    }
    
    #[test]
    fn test_projected_row_view() {
        let data = vec![
            DataType::Integer(1),
            DataType::String("Alice".to_string()),
            DataType::Integer(25),
        ];
        let column_names = vec![
            "id".to_string(),
            "name".to_string(),
            "age".to_string(),
        ];
        let column_indices = vec![0, 2]; // id and age only
        
        let row = RowView::new(&data, &column_names);
        let projected = row.project(&column_indices);
        
        assert_eq!(projected.column_count(), 2);
        assert_eq!(projected.get_column(0), Some(&DataType::Integer(1)));
        assert_eq!(projected.get_column(1), Some(&DataType::Integer(25)));
    }
    
    #[test]
    fn test_table_view() {
        let rows = vec![
            vec![DataType::Integer(1), DataType::String("Alice".to_string())],
            vec![DataType::Integer(2), DataType::String("Bob".to_string())],
        ];
        let column_names = vec!["id".to_string(), "name".to_string()];
        
        let table = TableView::new(&rows, &column_names);
        
        assert_eq!(table.row_count(), 2);
        assert_eq!(table.column_count(), 2);
        
        let first_row = table.get_row(0).unwrap();
        assert_eq!(first_row.get_column_by_name("name"), Some(&DataType::String("Alice".to_string())));
    }
    
    #[test]
    fn test_sliced_table_view() {
        let rows = vec![
            vec![DataType::Integer(1)],
            vec![DataType::Integer(2)],
            vec![DataType::Integer(3)],
            vec![DataType::Integer(4)],
        ];
        let column_names = vec!["id".to_string()];
        
        let table = TableView::new(&rows, &column_names);
        let slice = table.slice(1..3);
        
        assert_eq!(slice.row_count(), 2);
        let first_in_slice = slice.get_row(0).unwrap();
        assert_eq!(first_in_slice.get_column(0), Some(&DataType::Integer(2)));
    }
    
    #[test]
    fn test_string_view() {
        let borrowed = StringView::borrowed("hello");
        let owned = StringView::owned("world".to_string());
        let interned = StringView::interned("static");
        
        assert_eq!(borrowed.as_str(), "hello");
        assert_eq!(owned.as_str(), "world");
        assert_eq!(interned.as_str(), "static");
        
        assert!(!borrowed.is_owned());
        assert!(owned.is_owned());
        assert!(!interned.is_owned());
    }
    
    #[test]
    fn test_bytes_view() {
        let data = vec![1, 2, 3, 4];
        let borrowed = BytesView::borrowed(&data);
        let owned = BytesView::owned(vec![5, 6, 7, 8]);
        
        assert_eq!(borrowed.as_bytes(), &[1, 2, 3, 4]);
        assert_eq!(owned.as_bytes(), &[5, 6, 7, 8]);
        assert_eq!(borrowed.len(), 4);
        assert!(!borrowed.is_empty());
    }
    
    #[test]
    fn test_column_view() {
        let data = vec![
            DataType::Integer(1),
            DataType::Integer(2),
            DataType::Integer(3),
        ];
        
        let column = ColumnView::new(&data, "numbers");
        
        assert_eq!(column.name(), "numbers");
        assert_eq!(column.len(), 3);
        assert_eq!(column.get(1), Some(&DataType::Integer(2)));
        
        let sum: i64 = column.iter()
            .filter_map(|dt| match dt {
                DataType::Integer(n) => Some(*n),
                _ => None,
            })
            .sum();
        assert_eq!(sum, 6);
    }
}