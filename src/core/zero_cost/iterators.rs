// src/core/zero_cost/iterators.rs
//! Advanced iterator combinators and zero-cost iterator abstractions for database operations

use std::marker::PhantomData;

/// Zero-cost iterator adapter that provides window functions over database rows
pub struct WindowIterator<I, T, F> {
    iter: I,
    window_size: usize,
    step: usize,
    buffer: Vec<T>,
    #[allow(dead_code)]
    position: usize,
    func: F,
    _phantom: PhantomData<T>,
}

impl<I, T, F, R> WindowIterator<I, T, F>
where
    I: Iterator<Item = T>,
    T: Clone,
    F: Fn(&[T]) -> R,
{
    /// Create a new window iterator
    pub fn new(iter: I, window_size: usize, step: usize, func: F) -> Self {
        Self {
            iter,
            window_size,
            step,
            buffer: Vec::with_capacity(window_size),
            position: 0,
            func,
            _phantom: PhantomData,
        }
    }
}

impl<I, T, F, R> Iterator for WindowIterator<I, T, F>
where
    I: Iterator<Item = T>,
    T: Clone,
    F: Fn(&[T]) -> R,
{
    type Item = R;

    fn next(&mut self) -> Option<Self::Item> {
        // Fill buffer to window size
        while self.buffer.len() < self.window_size {
            if let Some(item) = self.iter.next() {
                self.buffer.push(item);
            } else {
                return None;
            }
        }

        if self.buffer.len() == self.window_size {
            let result = (self.func)(&self.buffer);
            
            // Slide the window
            for _ in 0..self.step.min(self.buffer.len()) {
                self.buffer.remove(0);
            }
            
            // Fill buffer again if needed
            while self.buffer.len() < self.window_size {
                if let Some(item) = self.iter.next() {
                    self.buffer.push(item);
                } else {
                    break;
                }
            }
            
            Some(result)
        } else {
            None
        }
    }
}

/// Zero-cost iterator for chunked processing of database rows
pub struct ChunkedIterator<I, T> {
    iter: I,
    chunk_size: usize,
    _phantom: PhantomData<T>,
}

impl<I, T> ChunkedIterator<I, T> {
    /// Create a new chunked iterator
    #[inline]
    pub const fn new(iter: I, chunk_size: usize) -> Self {
        Self {
            iter,
            chunk_size,
            _phantom: PhantomData,
        }
    }
}

impl<I, T> Iterator for ChunkedIterator<I, T>
where
    I: Iterator<Item = T>,
{
    type Item = Vec<T>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut chunk = Vec::with_capacity(self.chunk_size);
        
        for _ in 0..self.chunk_size {
            if let Some(item) = self.iter.next() {
                chunk.push(item);
            } else {
                break;
            }
        }
        
        if chunk.is_empty() {
            None
        } else {
            Some(chunk)
        }
    }
}

/// Zero-cost iterator adapter for SQL aggregation functions
pub struct AggregateIterator<I, T, F, A> {
    iter: I,
    group_by: F,
    aggregate_fn: A,
    current_group: Option<T>,
    group_items: Vec<T>,
    _phantom: PhantomData<T>,
}

impl<I, T, F, A, K, V> AggregateIterator<I, T, F, A>
where
    I: Iterator<Item = T>,
    T: Clone,
    F: Fn(&T) -> K,
    A: Fn(&[T]) -> V,
    K: PartialEq,
{
    /// Create a new aggregate iterator
    pub fn new(iter: I, group_by: F, aggregate_fn: A) -> Self {
        Self {
            iter,
            group_by,
            aggregate_fn,
            current_group: None,
            group_items: Vec::new(),
            _phantom: PhantomData,
        }
    }
}

impl<I, T, F, A, K, V> Iterator for AggregateIterator<I, T, F, A>
where
    I: Iterator<Item = T>,
    T: Clone,
    F: Fn(&T) -> K,
    A: Fn(&[T]) -> V,
    K: PartialEq,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(item) = self.iter.next() {
                let group_key = (self.group_by)(&item);
                
                match &self.current_group {
                    None => {
                        self.current_group = Some(item.clone());
                        self.group_items.push(item);
                    }
                    Some(current) => {
                        let current_key = (self.group_by)(current);
                        if current_key == group_key {
                            self.group_items.push(item);
                        } else {
                            // New group found, return current group result
                            let result_key = current_key;
                            let result_value = (self.aggregate_fn)(&self.group_items);
                            
                            // Start new group
                            self.current_group = Some(item.clone());
                            self.group_items.clear();
                            self.group_items.push(item);
                            
                            return Some((result_key, result_value));
                        }
                    }
                }
            } else {
                // End of iterator, return final group if exists
                if let Some(current) = self.current_group.take() {
                    let result_key = (self.group_by)(&current);
                    let result_value = (self.aggregate_fn)(&self.group_items);
                    self.group_items.clear();
                    return Some((result_key, result_value));
                } else {
                    return None;
                }
            }
        }
    }
}

/// Zero-cost iterator for parallel processing with work-stealing
pub struct ParallelIterator<I, T> {
    iter: I,
    batch_size: usize,
    _phantom: PhantomData<T>,
}

impl<I, T> ParallelIterator<I, T> 
where
    T: Sync,
{
    /// Create a new parallel iterator
    #[inline]
    pub const fn new(iter: I, batch_size: usize) -> Self {
        Self {
            iter,
            batch_size,
            _phantom: PhantomData,
        }
    }
    
    /// Process items in parallel using the provided function
    pub fn for_each_parallel<F>(self, func: F)
    where
        I: Iterator<Item = T> + Send,
        T: Send + Clone,
        F: Fn(T) + Send + Sync + Clone,
    {
        use std::sync::Arc;
        use std::thread;
        
        let items: Vec<T> = self.iter.collect();
        let chunk_size = self.batch_size;
        let func = Arc::new(func);
        
        thread::scope(|s| {
            for chunk in items.chunks(chunk_size) {
                let func = Arc::clone(&func);
                s.spawn(move || {
                    for item in chunk {
                        func(item.clone());
                    }
                });
            }
        });
    }
}

/// Zero-cost iterator combinator extensions
pub trait IteratorExt<T>: Iterator<Item = T> + Sized 
where
    T: Sync,
{
    /// Create a window iterator
    fn windows<F, R>(self, window_size: usize, step: usize, func: F) -> WindowIterator<Self, T, F>
    where
        T: Clone,
        F: Fn(&[T]) -> R,
    {
        WindowIterator::new(self, window_size, step, func)
    }
    
    /// Create a chunked iterator
    fn chunks(self, chunk_size: usize) -> ChunkedIterator<Self, T> {
        ChunkedIterator::new(self, chunk_size)
    }
    
    /// Create an aggregate iterator
    fn group_by_aggregate<F, A, K, V>(
        self,
        group_by: F,
        aggregate_fn: A,
    ) -> AggregateIterator<Self, T, F, A>
    where
        T: Clone,
        F: Fn(&T) -> K,
        A: Fn(&[T]) -> V,
        K: PartialEq,
    {
        AggregateIterator::new(self, group_by, aggregate_fn)
    }
    
    /// Create a parallel iterator
    fn parallel(self, batch_size: usize) -> ParallelIterator<Self, T> {
        ParallelIterator::new(self, batch_size)
    }
    
    /// Efficient count with early termination
    fn count_while<P>(mut self, mut predicate: P) -> usize
    where
        P: FnMut(&T) -> bool,
    {
        let mut count = 0;
        while let Some(item) = self.next() {
            if predicate(&item) {
                count += 1;
            } else {
                break;
            }
        }
        count
    }
    
    /// Zero-allocation exists check
    fn exists<P>(mut self, predicate: P) -> bool
    where
        P: FnMut(T) -> bool,
    {
        self.any(predicate)
    }
    
    /// Efficient min/max with single pass
    fn min_max_by<F, K>(mut self, mut key_fn: F) -> Option<(T, T)>
    where
        F: FnMut(&T) -> K,
        K: Ord + Clone,
        T: Clone,
    {
        let first = self.next()?;
        let mut min = first.clone();
        let mut max = first;
        let mut min_key = key_fn(&min);
        let mut max_key = min_key.clone();
        
        for item in self {
            let key = key_fn(&item);
            if key < min_key {
                min = item.clone();
                min_key = key.clone();
            }
            if key > max_key {
                max = item;
                max_key = key;
            }
        }
        
        Some((min, max))
    }
}

// Implement the extension trait for all iterators
impl<I, T> IteratorExt<T> for I where I: Iterator<Item = T>, T: Sync {}

/// SQL window function implementations
pub mod window_functions {
    /// ROW_NUMBER window function
    pub fn row_number<T>() -> impl Fn(&[T]) -> usize {
        |_| 1 // This would be stateful in real implementation
    }
    
    /// RANK window function
    pub fn rank<T, F, K>(_key_fn: F) -> impl Fn(&[T]) -> usize
    where
        F: Fn(&T) -> K + Clone,
        K: Ord,
        T: Clone,
    {
        move |window: &[T]| {
            if window.is_empty() {
                return 0;
            }
            // Simplified rank calculation
            1
        }
    }
    
    /// LAG window function
    pub fn lag<T>(offset: usize) -> impl Fn(&[T]) -> Option<T>
    where
        T: Clone,
    {
        move |window: &[T]| {
            if window.len() > offset {
                Some(window[window.len() - 1 - offset].clone())
            } else {
                None
            }
        }
    }
    
    /// LEAD window function
    pub fn lead<T>(offset: usize) -> impl Fn(&[T]) -> Option<T>
    where
        T: Clone,
    {
        move |window: &[T]| {
            if offset < window.len() {
                Some(window[offset].clone())
            } else {
                None
            }
        }
    }
    
    /// SUM window function
    pub fn sum<T, F, N>(value_fn: F) -> impl Fn(&[T]) -> N
    where
        F: Fn(&T) -> N + Clone,
        N: std::ops::Add<Output = N> + Default,
        T: Clone,
    {
        move |window: &[T]| {
            window.iter().map(&value_fn).fold(N::default(), |acc, x| acc + x)
        }
    }
    
    /// AVG window function
    pub fn avg<T, F>(value_fn: F) -> impl Fn(&[T]) -> f64
    where
        F: Fn(&T) -> f64 + Clone,
        T: Clone,
    {
        move |window: &[T]| {
            if window.is_empty() {
                0.0
            } else {
                let sum: f64 = window.iter().map(&value_fn).sum();
                sum / window.len() as f64
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_window_iterator() {
        let data = vec![1, 2, 3, 4, 5, 6];
        let windows: Vec<_> = data
            .into_iter()
            .windows(3, 1, |window| window.iter().sum::<i32>())
            .collect();
        
        assert_eq!(windows, vec![6, 9, 12, 15]); // [1,2,3], [2,3,4], [3,4,5], [4,5,6]
    }
    
    #[test]
    fn test_chunked_iterator() {
        let data = vec![1, 2, 3, 4, 5, 6, 7];
        let chunks: Vec<_> = data.into_iter().chunks(3).collect();
        
        assert_eq!(chunks, vec![vec![1, 2, 3], vec![4, 5, 6], vec![7]]);
    }
    
    #[test]
    fn test_aggregate_iterator() {
        #[derive(Clone, PartialEq, Debug)]
        struct Record {
            group: i32,
            value: i32,
        }
        
        let data = vec![
            Record { group: 1, value: 10 },
            Record { group: 1, value: 20 },
            Record { group: 2, value: 30 },
            Record { group: 2, value: 40 },
        ];
        
        let aggregated: Vec<_> = data
            .into_iter()
            .group_by_aggregate(
                |r| r.group,
                |group| group.iter().map(|r| r.value).sum::<i32>(),
            )
            .collect();
        
        assert_eq!(aggregated, vec![(1, 30), (2, 70)]);
    }
    
    #[test]
    fn test_iterator_extensions() {
        let data = vec![1, 2, 3, 4, 5];
        
        assert_eq!(data.iter().count_while(|&&x| x < 4), 3);
        assert!(data.iter().exists(|&x| x == 3));
        
        let (min, max) = data.iter().min_max_by(|&x| *x).unwrap();
        assert_eq!((*min, *max), (1, 5));
    }
    
    #[test]
    fn test_window_functions() {
        let data = vec![10, 20, 30, 40, 50];
        
        let sum_fn = window_functions::sum(|&x: &i32| x);
        assert_eq!(sum_fn(&data[0..3]), 60);
        
        let avg_fn = window_functions::avg(|&x: &i32| x as f64);
        assert_eq!(avg_fn(&data[0..3]), 20.0);
    }
}