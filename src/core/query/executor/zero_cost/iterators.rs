//! Zero-cost iterator adapters for query processing
//!
//! Provides efficient, allocation-free iteration over query results
//! using Rust's iterator ecosystem.

use super::Row;
use crate::core::types::DataType;
use std::marker::PhantomData;

/// Iterator over query results with zero-copy semantics
pub struct RowIterator<'a, I> {
    inner: I,
    _phantom: PhantomData<&'a ()>,
}

impl<'a, I> RowIterator<'a, I>
where
    I: Iterator<Item = Vec<DataType>>,
{
    /// Create a new row iterator
    #[inline]
    pub fn new(inner: I) -> Self {
        Self {
            inner,
            _phantom: PhantomData,
        }
    }
}

impl<'a, I> Iterator for RowIterator<'a, I>
where
    I: Iterator<Item = Vec<DataType>>,
{
    type Item = Row<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(Row::from_owned)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

/// Filter iterator that avoids allocations
pub struct FilterIterator<I, P> {
    inner: I,
    predicate: P,
}

impl<I, P> FilterIterator<I, P> {
    #[inline]
    pub fn new(inner: I, predicate: P) -> Self {
        Self { inner, predicate }
    }
}

impl<'a, I, P> Iterator for FilterIterator<I, P>
where
    I: Iterator<Item = Row<'a>>,
    P: FnMut(&Row<'a>) -> bool,
{
    type Item = Row<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.find(&mut self.predicate)
    }
}

/// Map iterator for transforming rows without allocation
pub struct MapIterator<I, F> {
    inner: I,
    mapper: F,
}

impl<I, F> MapIterator<I, F> {
    #[inline]
    pub fn new(inner: I, mapper: F) -> Self {
        Self { inner, mapper }
    }
}

impl<'a, I, F, T> Iterator for MapIterator<I, F>
where
    I: Iterator<Item = Row<'a>>,
    F: FnMut(Row<'a>) -> T,
{
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(&mut self.mapper)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

/// Window iterator for sliding window operations
pub struct WindowIterator<I> {
    inner: I,
    window_size: usize,
    buffer: Vec<Vec<DataType>>,
    #[allow(dead_code)]
    index: usize,
}

impl<I> WindowIterator<I>
where
    I: Iterator<Item = Vec<DataType>>,
{
    pub fn new(mut inner: I, window_size: usize) -> Self {
        let mut buffer = Vec::with_capacity(window_size);
        
        // Pre-fill buffer
        for _ in 0..window_size {
            if let Some(row) = inner.next() {
                buffer.push(row);
            } else {
                break;
            }
        }
        
        Self {
            inner,
            window_size,
            buffer,
            index: 0,
        }
    }
}

impl<I> Iterator for WindowIterator<I>
where
    I: Iterator<Item = Vec<DataType>>,
{
    type Item = Vec<Vec<DataType>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buffer.len() < self.window_size {
            return None;
        }
        
        // Return current window as a clone
        let window = self.buffer.clone();
        
        // Advance window
        if let Some(next_row) = self.inner.next() {
            self.buffer.remove(0);
            self.buffer.push(next_row);
            Some(window)
        } else {
            None
        }
    }
}

/// Chunk iterator for batch processing
pub struct ChunkIterator<'a, I> {
    inner: I,
    chunk_size: usize,
    _phantom: PhantomData<&'a ()>,
}

impl<'a, I> ChunkIterator<'a, I>
where
    I: Iterator<Item = Row<'a>>,
{
    #[inline]
    pub fn new(inner: I, chunk_size: usize) -> Self {
        Self {
            inner,
            chunk_size,
            _phantom: PhantomData,
        }
    }
}

impl<'a, I> Iterator for ChunkIterator<'a, I>
where
    I: Iterator<Item = Row<'a>>,
{
    type Item = Vec<Row<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut chunk = Vec::with_capacity(self.chunk_size);
        
        for _ in 0..self.chunk_size {
            match self.inner.next() {
                Some(row) => chunk.push(row),
                None => break,
            }
        }
        
        if chunk.is_empty() {
            None
        } else {
            Some(chunk)
        }
    }
}

/// Extension trait for query iterators
pub trait QueryIteratorExt<'a>: Iterator<Item = Row<'a>> + Sized {
    /// Filter rows by predicate
    #[inline]
    fn filter_rows<P>(self, predicate: P) -> FilterIterator<Self, P>
    where
        P: FnMut(&Row<'a>) -> bool,
    {
        FilterIterator::new(self, predicate)
    }
    
    /// Map rows to another type
    #[inline]
    fn map_rows<F, T>(self, mapper: F) -> MapIterator<Self, F>
    where
        F: FnMut(Row<'a>) -> T,
    {
        MapIterator::new(self, mapper)
    }
    
    /// Process rows in chunks
    #[inline]
    fn chunks(self, chunk_size: usize) -> ChunkIterator<'a, Self> {
        ChunkIterator::new(self, chunk_size)
    }
    
    /// Take only the first n rows
    #[inline]
    fn limit(self, n: usize) -> std::iter::Take<Self> {
        self.take(n)
    }
    
    /// Skip the first n rows
    #[inline]
    fn offset(self, n: usize) -> std::iter::Skip<Self> {
        self.skip(n)
    }
}

impl<'a, I> QueryIteratorExt<'a> for I where I: Iterator<Item = Row<'a>> + Sized {}

/// Aggregate iterator for computing aggregates without collecting
#[allow(dead_code)]
pub struct AggregateIterator<I, A> {
    inner: I,
    aggregator: A,
}

/// Trait for aggregate functions
pub trait Aggregator<'a> {
    type Output;
    
    /// Update the aggregate with a new row
    fn update(&mut self, row: &Row<'a>);
    
    /// Finalize and return the result
    fn finalize(self) -> Self::Output;
}

/// Count aggregator
#[derive(Default)]
pub struct CountAggregator {
    count: usize,
}

impl<'a> Aggregator<'a> for CountAggregator {
    type Output = usize;
    
    #[inline]
    fn update(&mut self, _row: &Row<'a>) {
        self.count += 1;
    }
    
    #[inline]
    fn finalize(self) -> Self::Output {
        self.count
    }
}

/// Sum aggregator for numeric columns
pub struct SumAggregator {
    column_index: usize,
    sum: f64,
}

impl SumAggregator {
    #[inline]
    pub fn new(column_index: usize) -> Self {
        Self {
            column_index,
            sum: 0.0,
        }
    }
}

impl<'a> Aggregator<'a> for SumAggregator {
    type Output = f64;
    
    #[inline]
    fn update(&mut self, row: &Row<'a>) {
        if let Some(value) = row.get(self.column_index) {
            match value {
                DataType::Integer(i) => self.sum += *i as f64,
                DataType::Float(ordered_float) => self.sum += ordered_float.0,
                _ => {} // Ignore non-numeric values
            }
        }
    }
    
    #[inline]
    fn finalize(self) -> Self::Output {
        self.sum
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_filter_iterator() {
        let rows = vec![
            Row::from_owned(vec![DataType::Integer(1), DataType::String("a".to_string())]),
            Row::from_owned(vec![DataType::Integer(2), DataType::String("b".to_string())]),
            Row::from_owned(vec![DataType::Integer(3), DataType::String("c".to_string())]),
        ];
        
        let filtered: Vec<_> = rows
            .into_iter()
            .filter_rows(|row| {
                matches!(row.get(0), Some(DataType::Integer(i)) if *i > 1)
            })
            .collect();
        
        assert_eq!(filtered.len(), 2);
    }
    
    #[test]
    fn test_count_aggregator() {
        let rows = vec![
            Row::from_owned(vec![DataType::Integer(1)]),
            Row::from_owned(vec![DataType::Integer(2)]),
            Row::from_owned(vec![DataType::Integer(3)]),
        ];
        
        let mut count = CountAggregator::default();
        for row in &rows {
            count.update(row);
        }
        
        assert_eq!(count.finalize(), 3);
    }
}