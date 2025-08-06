//! Zero-cost iterator abstractions for database operations
//! 
//! This module provides efficient, allocation-free iterators that leverage
//! Rust's zero-cost abstractions for maximum performance.

use std::marker::PhantomData;
use crate::core::common::types::Value;
use crate::core::common::types::Row;

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
                .filter_map(|&idx| row.values.get(idx))
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
    
    /// Map rows to another type without allocation
    fn map_rows<F, T>(self, map_fn: F) -> MapIterator<'a, Self, F, T>
    where
        F: FnMut(&'a Row) -> T,
    {
        MapIterator::new(self, map_fn)
    }
    
    /// Flat map rows for expanding transformations
    fn flat_map_rows<F, U>(self, flat_map_fn: F) -> FlatMapIterator<'a, Self, F, U>
    where
        F: FnMut(&'a Row) -> U,
        U: IntoIterator,
    {
        FlatMapIterator::new(self, flat_map_fn)
    }
    
    /// Scan with state for running computations
    fn scan_rows<St, F, T>(self, initial_state: St, scan_fn: F) -> ScanIterator<'a, Self, St, F>
    where
        F: FnMut(&mut St, &'a Row) -> Option<T>,
    {
        ScanIterator::new(self, initial_state, scan_fn)
    }
    
    /// Take rows while predicate is true
    fn take_while_rows<P>(self, predicate: P) -> TakeWhileIterator<'a, Self, P>
    where
        P: FnMut(&&'a Row) -> bool,
    {
        TakeWhileIterator::new(self, predicate)
    }
    
    /// Skip rows while predicate is true
    fn skip_while_rows<P>(self, predicate: P) -> SkipWhileIterator<'a, Self, P>
    where
        P: FnMut(&&'a Row) -> bool,
    {
        SkipWhileIterator::new(self, predicate)
    }
    
    /// Make iterator peekable
    fn peekable_rows(self) -> PeekableIterator<'a, Self> {
        PeekableIterator::new(self)
    }
}

impl<'a, I> ZeroCostIteratorExt<'a> for I where I: Iterator<Item = &'a Row> + Sized {}

/// Zero-cost map iterator that transforms rows without allocation
pub struct MapIterator<'a, I, F, T> {
    iter: I,
    map_fn: F,
    _phantom: PhantomData<(&'a (), T)>,
}

impl<'a, I, F, T> MapIterator<'a, I, F, T>
where
    I: Iterator<Item = &'a Row>,
    F: FnMut(&'a Row) -> T,
{
    #[inline]
    pub fn new(iter: I, map_fn: F) -> Self {
        Self {
            iter,
            map_fn,
            _phantom: PhantomData,
        }
    }
}

impl<'a, I, F, T> Iterator for MapIterator<'a, I, F, T>
where
    I: Iterator<Item = &'a Row>,
    F: FnMut(&'a Row) -> T,
{
    type Item = T;
    
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(&mut self.map_fn)
    }
    
    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

/// Zero-cost flat map iterator for expanding rows
pub struct FlatMapIterator<'a, I, F, U: IntoIterator> {
    iter: I,
    flat_map_fn: F,
    current: Option<U::IntoIter>,
    _phantom: PhantomData<&'a ()>,
}

impl<'a, I, F, U> FlatMapIterator<'a, I, F, U>
where
    I: Iterator<Item = &'a Row>,
    F: FnMut(&'a Row) -> U,
    U: IntoIterator,
{
    #[inline]
    pub fn new(iter: I, flat_map_fn: F) -> Self {
        Self {
            iter,
            flat_map_fn,
            current: None,
            _phantom: PhantomData,
        }
    }
}

impl<'a, I, F, U> Iterator for FlatMapIterator<'a, I, F, U>
where
    I: Iterator<Item = &'a Row>,
    F: FnMut(&'a Row) -> U,
    U: IntoIterator,
{
    type Item = U::Item;
    
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(ref mut inner) = self.current {
                if let Some(item) = inner.next() {
                    return Some(item);
                }
            }
            
            match self.iter.next() {
                Some(row) => self.current = Some((self.flat_map_fn)(row).into_iter()),
                None => return None,
            }
        }
    }
}

/// Zero-cost scan iterator for stateful transformations
pub struct ScanIterator<'a, I, St, F> {
    iter: I,
    state: St,
    scan_fn: F,
    _phantom: PhantomData<&'a ()>,
}

impl<'a, I, St, F, T> ScanIterator<'a, I, St, F>
where
    I: Iterator<Item = &'a Row>,
    F: FnMut(&mut St, &'a Row) -> Option<T>,
{
    #[inline]
    pub fn new(iter: I, initial_state: St, scan_fn: F) -> Self {
        Self {
            iter,
            state: initial_state,
            scan_fn,
            _phantom: PhantomData,
        }
    }
}

impl<'a, I, St, F, T> Iterator for ScanIterator<'a, I, St, F>
where
    I: Iterator<Item = &'a Row>,
    F: FnMut(&mut St, &'a Row) -> Option<T>,
{
    type Item = T;
    
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().and_then(|row| (self.scan_fn)(&mut self.state, row))
    }
}

/// Zero-cost take while iterator
pub struct TakeWhileIterator<'a, I, P> {
    iter: I,
    predicate: P,
    done: bool,
    _phantom: PhantomData<&'a ()>,
}

impl<'a, I, P> TakeWhileIterator<'a, I, P>
where
    I: Iterator<Item = &'a Row>,
    P: FnMut(&&'a Row) -> bool,
{
    #[inline]
    pub fn new(iter: I, predicate: P) -> Self {
        Self {
            iter,
            predicate,
            done: false,
            _phantom: PhantomData,
        }
    }
}

impl<'a, I, P> Iterator for TakeWhileIterator<'a, I, P>
where
    I: Iterator<Item = &'a Row>,
    P: FnMut(&&'a Row) -> bool,
{
    type Item = &'a Row;
    
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            None
        } else {
            self.iter.next().and_then(|row| {
                if (self.predicate)(&row) {
                    Some(row)
                } else {
                    self.done = true;
                    None
                }
            })
        }
    }
}

/// Zero-cost skip while iterator
pub struct SkipWhileIterator<'a, I, P> {
    iter: I,
    predicate: Option<P>,
    _phantom: PhantomData<&'a ()>,
}

impl<'a, I, P> SkipWhileIterator<'a, I, P>
where
    I: Iterator<Item = &'a Row>,
    P: FnMut(&&'a Row) -> bool,
{
    #[inline]
    pub fn new(iter: I, predicate: P) -> Self {
        Self {
            iter,
            predicate: Some(predicate),
            _phantom: PhantomData,
        }
    }
}

impl<'a, I, P> Iterator for SkipWhileIterator<'a, I, P>
where
    I: Iterator<Item = &'a Row>,
    P: FnMut(&&'a Row) -> bool,
{
    type Item = &'a Row;
    
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(mut predicate) = self.predicate.take() {
            // Skip elements while predicate is true
            while let Some(row) = self.iter.next() {
                if !predicate(&row) {
                    return Some(row);
                }
            }
            None
        } else {
            // Predicate already failed, just pass through
            self.iter.next()
        }
    }
}

/// Zero-cost peekable iterator
pub struct PeekableIterator<'a, I> {
    iter: I,
    peeked: Option<Option<&'a Row>>,
}

impl<'a, I> PeekableIterator<'a, I>
where
    I: Iterator<Item = &'a Row>,
{
    #[inline]
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            peeked: None,
        }
    }
    
    /// Peek at the next element without consuming it
    #[inline]
    pub fn peek(&mut self) -> Option<&&'a Row> {
        if self.peeked.is_none() {
            self.peeked = Some(self.iter.next());
        }
        self.peeked.as_ref().unwrap().as_ref()
    }
}

impl<'a, I> Iterator for PeekableIterator<'a, I>
where
    I: Iterator<Item = &'a Row>,
{
    type Item = &'a Row;
    
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match self.peeked.take() {
            Some(v) => v,
            None => self.iter.next(),
        }
    }
}

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
    
    #[test]
    fn test_map_iterator() {
        let rows = vec![
            Row::new(vec![Value::Integer(1)]),
            Row::new(vec![Value::Integer(2)]),
            Row::new(vec![Value::Integer(3)]),
        ];
        
        let iter = RowRefIterator::new(&rows);
        let mapped: Vec<_> = iter.map_rows(|row| {
            match row.values.first() {
                Some(Value::Integer(n)) => *n * 2,
                _ => 0,
            }
        }).collect();
        
        assert_eq!(mapped, vec![2, 4, 6]);
    }
    
    #[test]
    fn test_filter_iterator() {
        let rows = vec![
            Row::new(vec![Value::Integer(1)]),
            Row::new(vec![Value::Integer(2)]),
            Row::new(vec![Value::Integer(3)]),
            Row::new(vec![Value::Integer(4)]),
        ];
        
        let iter = RowRefIterator::new(&rows);
        let filtered: Vec<_> = iter.filter_rows(|row| {
            match row.values.first() {
                Some(Value::Integer(n)) => *n % 2 == 0,
                _ => false,
            }
        }).collect();
        
        assert_eq!(filtered.len(), 2);
    }
    
    #[test]
    fn test_scan_iterator() {
        let rows = vec![
            Row::new(vec![Value::Integer(1)]),
            Row::new(vec![Value::Integer(2)]),
            Row::new(vec![Value::Integer(3)]),
        ];
        
        let iter = RowRefIterator::new(&rows);
        let sums: Vec<_> = iter.scan_rows(0i64, |sum, row| {
            match row.values.first() {
                Some(Value::Integer(n)) => {
                    *sum += n;
                    Some(*sum)
                },
                _ => None,
            }
        }).collect();
        
        assert_eq!(sums, vec![1, 3, 6]);
    }
    
    #[test]
    fn test_take_while_iterator() {
        let rows = vec![
            Row::new(vec![Value::Integer(1)]),
            Row::new(vec![Value::Integer(2)]),
            Row::new(vec![Value::Integer(3)]),
            Row::new(vec![Value::Integer(1)]),
        ];
        
        let iter = RowRefIterator::new(&rows);
        let taken: Vec<_> = iter.take_while_rows(|row| {
            match row.values.first() {
                Some(Value::Integer(n)) => *n < 3,
                _ => false,
            }
        }).collect();
        
        assert_eq!(taken.len(), 2);
    }
    
    #[test]
    fn test_peekable_iterator() {
        let rows = vec![
            Row::new(vec![Value::Integer(1)]),
            Row::new(vec![Value::Integer(2)]),
        ];
        
        let iter = RowRefIterator::new(&rows);
        let mut peekable = iter.peekable_rows();
        
        // Peek doesn't consume
        assert!(peekable.peek().is_some());
        assert!(peekable.peek().is_some());
        
        // Next consumes
        assert!(peekable.next().is_some());
        assert!(peekable.peek().is_some());
        assert!(peekable.next().is_some());
        assert!(peekable.peek().is_none());
        assert!(peekable.next().is_none());
    }
}