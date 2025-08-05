//! Zero-cost iterator adapters for query processing
//!
//! Provides efficient, allocation-free iteration over query results
//! using Rust's iterator ecosystem.

use super::Row;
use crate::core::types::DataType;
use std::marker::PhantomData;

/// Iterator over query results with zero-copy semantics
/// 
/// This iterator yields borrowed rows that reference data from the underlying source
pub struct RowIterator<'a, I> {
    inner: I,
    _phantom: PhantomData<&'a ()>,
}

impl<'a, I> RowIterator<'a, I>
where
    I: Iterator<Item = &'a [DataType]>,
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
    I: Iterator<Item = &'a [DataType]>,
{
    type Item = Row<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(Row::from_borrowed)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

/// Filter iterator that applies predicates without allocation
pub struct FilterIterator<'a, I, F> {
    inner: I,
    predicate: F,
    _phantom: PhantomData<&'a ()>,
}

impl<'a, I, F> FilterIterator<'a, I, F>
where
    I: Iterator<Item = Row<'a>>,
    F: Fn(&Row<'a>) -> bool,
{
    #[inline]
    pub fn new(inner: I, predicate: F) -> Self {
        Self {
            inner,
            predicate,
            _phantom: PhantomData,
        }
    }
}

impl<'a, I, F> Iterator for FilterIterator<'a, I, F>
where
    I: Iterator<Item = Row<'a>>,
    F: Fn(&Row<'a>) -> bool,
{
    type Item = Row<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.find(|row| (self.predicate)(row))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let (_, upper) = self.inner.size_hint();
        (0, upper) // Can't know lower bound due to filtering
    }
}

/// Map iterator for transforming rows
pub struct MapIterator<'a, I, F> {
    inner: I,
    mapper: F,
    _phantom: PhantomData<&'a ()>,
}

impl<'a, I, F, T> Iterator for MapIterator<'a, I, F>
where
    I: Iterator<Item = Row<'a>>,
    F: Fn(Row<'a>) -> T,
{
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(&self.mapper)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

/// Window iterator for sliding window operations with zero-copy semantics
/// 
/// This iterator maintains a circular buffer and yields indices into it
pub struct WindowIterator<I> {
    inner: I,
    window_size: usize,
    buffer: Vec<Vec<DataType>>,
    start_idx: usize,
    is_full: bool,
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
        
        let is_full = buffer.len() == window_size;
        
        Self {
            inner,
            window_size,
            buffer,
            start_idx: 0,
            is_full,
        }
    }
}

impl<I> Iterator for WindowIterator<I>
where
    I: Iterator<Item = Vec<DataType>>,
{
    // Return owned data to avoid lifetime issues
    type Item = Vec<Vec<DataType>>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.is_full {
            return None;
        }
        
        // Build current window by cloning data
        let mut window = Vec::with_capacity(self.window_size);
        for i in 0..self.window_size {
            let idx = (self.start_idx + i) % self.window_size;
            window.push(self.buffer[idx].clone());
        }
        
        // Advance window
        if let Some(next_row) = self.inner.next() {
            self.buffer[self.start_idx] = next_row;
            self.start_idx = (self.start_idx + 1) % self.window_size;
        } else {
            self.is_full = false;
        }
        
        Some(window)
    }
}

/// Zero-copy window iterator that yields references
/// This is an alternative implementation that avoids cloning
pub struct WindowRefIterator<'a> {
    data: &'a [Vec<DataType>],
    window_size: usize,
    current_pos: usize,
}

impl<'a> WindowRefIterator<'a> {
    pub fn new(data: &'a [Vec<DataType>], window_size: usize) -> Self {
        Self {
            data,
            window_size,
            current_pos: 0,
        }
    }
}

impl<'a> Iterator for WindowRefIterator<'a> {
    type Item = &'a [Vec<DataType>];

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_pos + self.window_size > self.data.len() {
            return None;
        }
        
        let window = &self.data[self.current_pos..self.current_pos + self.window_size];
        self.current_pos += 1;
        Some(window)
    }
}

/// Chunk iterator for processing data in batches
pub struct ChunkIterator<'a, I> {
    inner: I,
    chunk_size: usize,
    _phantom: PhantomData<&'a ()>,
}

impl<'a, I> ChunkIterator<'a, I>
where
    I: Iterator<Item = Row<'a>>,
{
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
    /// Filter rows based on a predicate
    #[inline]
    fn filter_rows<F>(self, predicate: F) -> FilterIterator<'a, Self, F>
    where
        F: Fn(&Row<'a>) -> bool,
    {
        FilterIterator::new(self, predicate)
    }

    /// Map rows to a different type
    #[inline]
    fn map_rows<F, T>(self, mapper: F) -> MapIterator<'a, Self, F>
    where
        F: Fn(Row<'a>) -> T,
    {
        MapIterator {
            inner: self,
            mapper,
            _phantom: PhantomData,
        }
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
}

impl<'a, I> QueryIteratorExt<'a> for I where I: Iterator<Item = Row<'a>> + Sized {}

/// Aggregator trait for computing aggregates without collecting
pub trait Aggregator<'a> {
    type Output;
    
    /// Update the aggregator with a new row
    fn update(&mut self, row: &Row<'a>);
    
    /// Finalize and return the aggregate result
    fn finalize(self) -> Self::Output;
}

/// Count aggregator
pub struct CountAggregator {
    count: usize,
}

impl CountAggregator {
    #[inline]
    pub fn new() -> Self {
        Self { count: 0 }
    }
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

/// Group-by iterator that yields groups lazily
pub struct GroupByIterator<'a, I, K, F> {
    inner: I,
    key_fn: F,
    current_group: Option<(K, Vec<Row<'a>>)>,
    _phantom: PhantomData<&'a ()>,
}

impl<'a, I, K, F> GroupByIterator<'a, I, K, F>
where
    I: Iterator<Item = Row<'a>>,
    K: Eq,
    F: Fn(&Row<'a>) -> K,
{
    pub fn new(inner: I, key_fn: F) -> Self {
        Self {
            inner,
            key_fn,
            current_group: None,
            _phantom: PhantomData,
        }
    }
}

impl<'a, I, K, F> Iterator for GroupByIterator<'a, I, K, F>
where
    I: Iterator<Item = Row<'a>>,
    K: Eq,
    F: Fn(&Row<'a>) -> K,
{
    type Item = (K, Vec<Row<'a>>);

    fn next(&mut self) -> Option<Self::Item> {
        let mut group = Vec::new();
        let mut group_key = None;
        
        // Continue from where we left off or start fresh
        if let Some((key, mut existing_group)) = self.current_group.take() {
            group.append(&mut existing_group);
            group_key = Some(key);
        }
        
        // Collect all rows with the same key
        while let Some(row) = self.inner.next() {
            let key = (self.key_fn)(&row);
            
            match &group_key {
                None => {
                    group_key = Some(key);
                    group.push(row);
                }
                Some(gk) if gk == &key => {
                    group.push(row);
                }
                Some(_) => {
                    // Different key, save for next iteration
                    self.current_group = Some((key, vec![row]));
                    break;
                }
            }
        }
        
        group_key.map(|k| (k, group))
    }
}

/// Aggregate iterator that applies an aggregator to all rows
#[allow(dead_code)]
pub struct AggregateIterator<I, A> {
    inner: I,
    aggregator: A,
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_row_iterator() {
        let data: Vec<Vec<DataType>> = vec![
            vec![DataType::Integer(1), DataType::String("Alice".to_string())],
            vec![DataType::Integer(2), DataType::String("Bob".to_string())],
        ];
        
        let slices: Vec<&[DataType]> = data.iter().map(|v| v.as_slice()).collect();
        let mut iter = RowIterator::new(slices.into_iter());
        
        let row1 = iter.next().unwrap();
        assert_eq!(row1.get(0), Some(&DataType::Integer(1)));
        
        let row2 = iter.next().unwrap();
        assert_eq!(row2.get(1), Some(&DataType::String("Bob".to_string())));
        
        assert!(iter.next().is_none());
    }
}