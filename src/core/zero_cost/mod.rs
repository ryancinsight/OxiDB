// src/core/zero_cost/mod.rs
//! Zero-cost abstractions and zero-copy operations for OxiDB
//! 
//! This module provides efficient, compile-time optimized abstractions that minimize
//! runtime overhead while maximizing performance and safety.

use std::borrow::Cow;
use std::marker::PhantomData;
use std::ops::Deref;

pub mod iterators;
pub mod views;
pub mod borrowed;

// Re-export key zero-cost types
pub use iterators::*;
pub use views::*;
pub use borrowed::*;

/// Zero-cost wrapper for borrowed data that prevents unnecessary allocations
#[derive(Debug)]
pub struct ZeroCopyView<'a, T> {
    data: &'a T,
    _phantom: PhantomData<&'a T>,
}

impl<'a, T> ZeroCopyView<'a, T> {
    /// Create a new zero-copy view
    #[inline]
    pub const fn new(data: &'a T) -> Self {
        Self {
            data,
            _phantom: PhantomData,
        }
    }
    
    /// Get the underlying data reference
    #[inline]
    pub const fn get(&self) -> &'a T {
        self.data
    }
}

impl<'a, T> Deref for ZeroCopyView<'a, T> {
    type Target = T;
    
    #[inline]
    fn deref(&self) -> &Self::Target {
        self.data
    }
}

impl<'a, T: Clone> ZeroCopyView<'a, T> {
    /// Clone the underlying data only when necessary
    #[inline]
    pub fn into_owned(self) -> T {
        self.data.clone()
    }
}

/// Zero-cost abstraction for string data that can be either borrowed or owned
pub type StringView<'a> = Cow<'a, str>;

/// Zero-cost abstraction for byte data that can be either borrowed or owned
pub type BytesView<'a> = Cow<'a, [u8]>;

/// Compile-time string interning for SQL keywords and common identifiers
pub struct InternedString<const N: usize> {
    data: [u8; N],
    len: usize,
}

impl<const N: usize> InternedString<N> {
    /// Create a new interned string at compile time
    pub const fn new(s: &str) -> Self {
        let bytes = s.as_bytes();
        let mut data = [0u8; N];
        let mut i = 0;
        while i < bytes.len() && i < N {
            data[i] = bytes[i];
            i += 1;
        }
        Self {
            data,
            len: bytes.len(),
        }
    }
    
    /// Get the string slice
    #[inline]
    pub fn as_str(&self) -> &str {
        // Safe because we validated the string during construction
        std::str::from_utf8(&self.data[..self.len]).unwrap_or("")
    }
}

/// Zero-cost iterator adapter for database rows
pub struct RowIterator<'a, T> {
    data: &'a [T],
    index: usize,
}

impl<'a, T> RowIterator<'a, T> {
    /// Create a new row iterator
    #[inline]
    pub const fn new(data: &'a [T]) -> Self {
        Self { data, index: 0 }
    }
}

impl<'a, T> Iterator for RowIterator<'a, T> {
    type Item = &'a T;
    
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.data.len() {
            let item = &self.data[self.index];
            self.index += 1;
            Some(item)
        } else {
            None
        }
    }
    
    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.data.len() - self.index;
        (remaining, Some(remaining))
    }
}

impl<'a, T> ExactSizeIterator for RowIterator<'a, T> {}

/// Zero-cost column access abstraction
pub struct ColumnView<'a, T> {
    data: &'a [T],
    column_index: usize,
    row_count: usize,
}

impl<'a, T> ColumnView<'a, T> {
    /// Create a new column view
    #[inline]
    pub const fn new(data: &'a [T], column_index: usize, row_count: usize) -> Self {
        Self {
            data,
            column_index,
            row_count,
        }
    }
    
    /// Get value at row index
    #[inline]
    pub fn get(&self, row_index: usize) -> Option<&'a T> {
        if row_index < self.row_count {
            self.data.get(row_index * self.row_count + self.column_index)
        } else {
            None
        }
    }
}

/// Compile-time SQL keyword constants
pub mod sql_keywords {
    use super::InternedString;
    
    pub const SELECT: InternedString<6> = InternedString::new("SELECT");
    pub const INSERT: InternedString<6> = InternedString::new("INSERT");
    pub const UPDATE: InternedString<6> = InternedString::new("UPDATE");
    pub const DELETE: InternedString<6> = InternedString::new("DELETE");
    pub const CREATE: InternedString<6> = InternedString::new("CREATE");
    pub const DROP: InternedString<4> = InternedString::new("DROP");
    pub const ALTER: InternedString<5> = InternedString::new("ALTER");
    pub const INDEX: InternedString<5> = InternedString::new("INDEX");
    pub const VIEW: InternedString<4> = InternedString::new("VIEW");
    pub const TRIGGER: InternedString<7> = InternedString::new("TRIGGER");
    pub const PROCEDURE: InternedString<9> = InternedString::new("PROCEDURE");
    pub const FUNCTION: InternedString<8> = InternedString::new("FUNCTION");
    pub const WITH: InternedString<4> = InternedString::new("WITH");
    pub const RECURSIVE: InternedString<9> = InternedString::new("RECURSIVE");
    pub const WINDOW: InternedString<6> = InternedString::new("WINDOW");
    pub const PARTITION: InternedString<9> = InternedString::new("PARTITION");
    pub const OVER: InternedString<4> = InternedString::new("OVER");
    pub const ROW_NUMBER: InternedString<10> = InternedString::new("ROW_NUMBER");
    pub const RANK: InternedString<4> = InternedString::new("RANK");
    pub const DENSE_RANK: InternedString<10> = InternedString::new("DENSE_RANK");
    pub const LAG: InternedString<3> = InternedString::new("LAG");
    pub const LEAD: InternedString<4> = InternedString::new("LEAD");
}

/// Zero-cost abstraction for SQL execution plans
#[derive(Debug)]
pub struct ExecutionPlan<'a> {
    steps: &'a [ExecutionStep<'a>],
}

#[derive(Debug)]
pub enum ExecutionStep<'a> {
    Scan { table: &'a str },
    Filter { condition: &'a str },
    Project { columns: &'a [&'a str] },
    Join { 
        left: &'a str, 
        right: &'a str, 
        condition: &'a str 
    },
    Aggregate { 
        functions: &'a [&'a str],
        group_by: &'a [&'a str] 
    },
    Sort { columns: &'a [&'a str] },
    Limit { count: usize, offset: usize },
}

impl<'a> ExecutionPlan<'a> {
    /// Create a new execution plan
    #[inline]
    pub const fn new(steps: &'a [ExecutionStep<'a>]) -> Self {
        Self { steps }
    }
    
    /// Get an iterator over execution steps
    #[inline]
    pub fn steps(&self) -> impl Iterator<Item = &ExecutionStep<'a>> {
        self.steps.iter()
    }
    
    /// Estimate execution cost (zero-cost at compile time)
    #[inline]
    pub fn estimate_cost(&self) -> u64 {
        self.steps.iter().map(|step| match step {
            ExecutionStep::Scan { .. } => 100,
            ExecutionStep::Filter { .. } => 10,
            ExecutionStep::Project { .. } => 5,
            ExecutionStep::Join { .. } => 1000,
            ExecutionStep::Aggregate { .. } => 500,
            ExecutionStep::Sort { .. } => 200,
            ExecutionStep::Limit { .. } => 1,
        }).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_zero_copy_view() {
        let data = vec![1, 2, 3, 4, 5];
        let view = ZeroCopyView::new(&data);
        assert_eq!(view.len(), 5);
        assert_eq!(view[0], 1);
    }
    
    #[test]
    fn test_interned_string() {
        let select = sql_keywords::SELECT;
        assert_eq!(select.as_str(), "SELECT");
    }
    
    #[test]
    fn test_row_iterator() {
        let data = vec![1, 2, 3, 4, 5];
        let mut iter = RowIterator::new(&data);
        assert_eq!(iter.next(), Some(&1));
        assert_eq!(iter.next(), Some(&2));
        assert_eq!(iter.size_hint(), (3, Some(3)));
    }
    
    #[test]
    fn test_execution_plan_cost() {
        let steps = [
            ExecutionStep::Scan { table: "users" },
            ExecutionStep::Filter { condition: "age > 25" },
            ExecutionStep::Project { columns: &["name", "email"] },
        ];
        let plan = ExecutionPlan::new(&steps);
        assert_eq!(plan.estimate_cost(), 115);
    }
}