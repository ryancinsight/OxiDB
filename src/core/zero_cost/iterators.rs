//! Zero-cost iterator abstractions for database operations
//! 
//! This module provides efficient, allocation-free iterators that leverage
//! Rust's zero-cost abstractions for maximum performance.

use std::marker::PhantomData;
use crate::core::common::types::Value;
use crate::api::types::Row;

/// Zero-cost iterator over database rows that avoids allocations
pub struct RowRefIterator<'a> {
    rows: &'a [Row],
    position: usize,
}

impl<'a> RowRefIterator<'a> {
    /// Create a new row reference iterator
    #[inline]
    pub const fn new(rows: &'a [Row]) -> Self {
        Self {
            rows,
            position: 0,
        }
    }
}

impl<'a> Iterator for RowRefIterator<'a> {
    type Item = &'a Row;
    
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.position < self.rows.len() {
            let row = &self.rows[self.position];
            self.position += 1;
            Some(row)
        } else {
            None
        }
    }
    
    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.rows.len().saturating_sub(self.position);
        (remaining, Some(remaining))
    }
    
    #[inline]
    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.position = self.position.saturating_add(n);
        self.next()
    }
}

impl<'a> ExactSizeIterator for RowRefIterator<'a> {
    #[inline]
    fn len(&self) -> usize {
        self.rows.len().saturating_sub(self.position)
    }
}

impl<'a> DoubleEndedIterator for RowRefIterator<'a> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.position < self.rows.len() {
            let row = &self.rows[self.rows.len() - 1];
            Some(row)
        } else {
            None
        }
    }
}

/// Zero-cost column projection iterator
pub struct ColumnProjection<'a, I> {
    iter: I,
    column_indices: &'a [usize],
    _phantom: PhantomData<&'a ()>,
}

impl<'a, I> ColumnProjection<'a, I> 
where
    I: Iterator<Item = &'a Row>,
{
    /// Create a new column projection iterator
    #[inline]
    pub fn new(iter: I, column_indices: &'a [usize]) -> Self {
        Self {
            iter,
            column_indices,
            _phantom: PhantomData,
        }
    }
}

impl<'a, I> Iterator for ColumnProjection<'a, I>
where
    I: Iterator<Item = &'a Row>,
{
    type Item = Vec<&'a Value>;
    
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|row| {
            self.column_indices
                .iter()
                .filter_map(|&idx| row.get(idx))
                .collect()
        })
    }
}

/// Zero-cost filter iterator that avoids allocations
pub struct FilterIterator<'a, I, F> {
    iter: I,
    predicate: F,
    _phantom: PhantomData<&'a ()>,
}

impl<'a, I, F> FilterIterator<'a, I, F>
where
    I: Iterator<Item = &'a Row>,
    F: Fn(&'a Row) -> bool,
{
    /// Create a new filter iterator
    #[inline]
    pub fn new(iter: I, predicate: F) -> Self {
        Self {
            iter,
            predicate,
            _phantom: PhantomData,
        }
    }
}

impl<'a, I, F> Iterator for FilterIterator<'a, I, F>
where
    I: Iterator<Item = &'a Row>,
    F: Fn(&'a Row) -> bool,
{
    type Item = &'a Row;
    
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.find(|row| (self.predicate)(row))
    }
    
    fn size_hint(&self) -> (usize, Option<usize>) {
        let (_, upper) = self.iter.size_hint();
        (0, upper)
    }
}

/// Window iterator for sliding window operations
pub struct WindowIterator<'a, T> {
    data: &'a [T],
    window_size: usize,
    position: usize,
}

impl<'a, T> WindowIterator<'a, T> {
    /// Create a new window iterator
    #[inline]
    pub const fn new(data: &'a [T], window_size: usize) -> Self {
        Self {
            data,
            window_size,
            position: 0,
        }
    }
}

impl<'a, T> Iterator for WindowIterator<'a, T> {
    type Item = &'a [T];
    
    fn next(&mut self) -> Option<Self::Item> {
        if self.position + self.window_size <= self.data.len() {
            let window = &self.data[self.position..self.position + self.window_size];
            self.position += 1;
            Some(window)
        } else {
            None
        }
    }
    
    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.data.len()
            .saturating_sub(self.position)
            .saturating_sub(self.window_size - 1);
        (remaining, Some(remaining))
    }
}

/// Batched iterator for processing rows in chunks
pub struct BatchedIterator<'a, I> {
    iter: I,
    batch_size: usize,
    _phantom: PhantomData<&'a ()>,
}

impl<'a, I> BatchedIterator<'a, I>
where
    I: Iterator<Item = &'a Row>,
{
    /// Create a new batched iterator
    #[inline]
    pub fn new(iter: I, batch_size: usize) -> Self {
        Self {
            iter,
            batch_size,
            _phantom: PhantomData,
        }
    }
}

impl<'a, I> Iterator for BatchedIterator<'a, I>
where
    I: Iterator<Item = &'a Row>,
{
    type Item = Vec<&'a Row>;
    
    fn next(&mut self) -> Option<Self::Item> {
        let batch: Vec<_> = self.iter
            .by_ref()
            .take(self.batch_size)
            .collect();
            
        if batch.is_empty() {
            None
        } else {
            Some(batch)
        }
    }
}

/// Chain multiple iterators without allocation
pub struct ChainedIterator<'a, I1, I2> {
    first: Option<I1>,
    second: I2,
    _phantom: PhantomData<&'a ()>,
}

impl<'a, I1, I2> ChainedIterator<'a, I1, I2>
where
    I1: Iterator<Item = &'a Row>,
    I2: Iterator<Item = &'a Row>,
{
    /// Create a new chained iterator
    #[inline]
    pub fn new(first: I1, second: I2) -> Self {
        Self {
            first: Some(first),
            second,
            _phantom: PhantomData,
        }
    }
}

impl<'a, I1, I2> Iterator for ChainedIterator<'a, I1, I2>
where
    I1: Iterator<Item = &'a Row>,
    I2: Iterator<Item = &'a Row>,
{
    type Item = &'a Row;
    
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(ref mut first) = self.first {
            if let Some(item) = first.next() {
                return Some(item);
            }
            self.first = None;
        }
        self.second.next()
    }
}

/// Extension trait for zero-cost iterator operations
pub trait ZeroCostIteratorExt<'a>: Iterator<Item = &'a Row> + Sized {
    /// Project specific columns without allocation
    fn project_columns(self, column_indices: &'a [usize]) -> ColumnProjection<'a, Self> {
        ColumnProjection::new(self, column_indices)
    }
    
    /// Filter rows without allocation
    fn filter_rows<F>(self, predicate: F) -> FilterIterator<'a, Self, F>
    where
        F: Fn(&'a Row) -> bool,
    {
        FilterIterator::new(self, predicate)
    }
    
    /// Process rows in batches
    fn batched(self, batch_size: usize) -> BatchedIterator<'a, Self> {
        BatchedIterator::new(self, batch_size)
    }
    
    /// Chain with another iterator
    fn chain_with<I2>(self, other: I2) -> ChainedIterator<'a, Self, I2>
    where
        I2: Iterator<Item = &'a Row>,
    {
        ChainedIterator::new(self, other)
    }
}

impl<'a, I> ZeroCostIteratorExt<'a> for I where I: Iterator<Item = &'a Row> + Sized {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::common::types::Value;
    
    #[test]
    fn test_row_ref_iterator() {
        let rows = vec![
            Row::new(vec![Value::Integer(1), Value::Text("a".to_string())]),
            Row::new(vec![Value::Integer(2), Value::Text("b".to_string())]),
        ];
        
        let mut iter = RowRefIterator::new(&rows);
        assert_eq!(iter.len(), 2);
        assert!(iter.next().is_some());
        assert_eq!(iter.len(), 1);
        assert!(iter.next().is_some());
        assert_eq!(iter.len(), 0);
        assert!(iter.next().is_none());
    }
    
    #[test]
    fn test_window_iterator() {
        let data = vec![1, 2, 3, 4, 5];
        let windows: Vec<_> = WindowIterator::new(&data, 3).collect();
        assert_eq!(windows.len(), 3);
        assert_eq!(windows[0], &[1, 2, 3]);
        assert_eq!(windows[1], &[2, 3, 4]);
        assert_eq!(windows[2], &[3, 4, 5]);
    }
}