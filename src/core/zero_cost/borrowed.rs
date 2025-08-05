//! Borrowed data structures for zero-cost database operations
//! 
//! This module provides borrowed versions of common database structures
//! that avoid allocations and enable efficient data processing.

use std::borrow::Cow;
use std::marker::PhantomData;
use crate::core::common::types::Value;

/// Borrowed row that avoids allocating a Vec for values
#[derive(Debug)]
pub struct BorrowedRow<'a> {
    values: &'a [Value],
    _phantom: PhantomData<&'a ()>,
}

impl<'a> BorrowedRow<'a> {
    /// Create a new borrowed row
    #[inline]
    pub const fn new(values: &'a [Value]) -> Self {
        Self {
            values,
            _phantom: PhantomData,
        }
    }
    
    /// Get a value by index
    #[inline]
    pub fn get(&self, index: usize) -> Option<&'a Value> {
        self.values.get(index)
    }
    
    /// Get the number of values
    #[inline]
    pub const fn len(&self) -> usize {
        self.values.len()
    }
    
    /// Check if empty
    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
    
    /// Iterate over values
    #[inline]
    pub fn iter(&self) -> std::slice::Iter<'a, Value> {
        self.values.iter()
    }
}

/// Borrowed schema that avoids string allocations
#[derive(Debug)]
pub struct BorrowedSchema<'a> {
    table_name: Cow<'a, str>,
    column_names: Cow<'a, [Cow<'a, str>]>,
    column_types: &'a [crate::core::common::types::DataType],
}

impl<'a> BorrowedSchema<'a> {
    /// Create a new borrowed schema
    pub fn new(
        table_name: Cow<'a, str>,
        column_names: Cow<'a, [Cow<'a, str>]>,
        column_types: &'a [crate::core::common::types::DataType],
    ) -> Self {
        Self {
            table_name,
            column_names,
            column_types,
        }
    }
    
    /// Get table name
    #[inline]
    pub fn table_name(&self) -> &str {
        &self.table_name
    }
    
    /// Get column count
    #[inline]
    pub fn column_count(&self) -> usize {
        self.column_names.len()
    }
    
    /// Get column name by index
    #[inline]
    pub fn column_name(&self, index: usize) -> Option<&str> {
        self.column_names.get(index).map(|cow| cow.as_ref())
    }
    
    /// Get column type by index
    #[inline]
    pub fn column_type(&self, index: usize) -> Option<&crate::core::common::types::DataType> {
        self.column_types.get(index)
    }
    
    /// Find column index by name
    pub fn find_column(&self, name: &str) -> Option<usize> {
        self.column_names
            .iter()
            .position(|col| col == name)
    }
}

/// Borrowed predicate that avoids cloning expressions
#[derive(Debug)]
pub struct BorrowedPredicate<'a> {
    column: Cow<'a, str>,
    operator: ComparisonOp,
    value: BorrowedValue<'a>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComparisonOp {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Like,
    NotLike,
    In,
    NotIn,
}

/// Borrowed value that can reference existing data
#[derive(Debug)]
pub enum BorrowedValue<'a> {
    Integer(i64),
    Float(f64),
    Text(Cow<'a, str>),
    Boolean(bool),
    Blob(Cow<'a, [u8]>),
    Vector(Cow<'a, [f32]>),
    List(Cow<'a, [BorrowedValue<'a>]>),
    Null,
}

impl<'a> BorrowedPredicate<'a> {
    /// Create a new borrowed predicate
    pub fn new(column: Cow<'a, str>, operator: ComparisonOp, value: BorrowedValue<'a>) -> Self {
        Self {
            column,
            operator,
            value,
        }
    }
    
    /// Evaluate predicate against a value
    pub fn evaluate(&self, row_value: &Value) -> bool {
        match (&self.value, row_value) {
            (BorrowedValue::Integer(a), Value::Integer(b)) => {
                self.compare_ordered(a, b)
            }
            (BorrowedValue::Float(a), Value::Float(b)) => {
                self.compare_ordered(a, b)
            }
            (BorrowedValue::Text(a), Value::Text(b)) => {
                match self.operator {
                    ComparisonOp::Like => self.pattern_match(a, b),
                    ComparisonOp::NotLike => !self.pattern_match(a, b),
                    _ => self.compare_ordered(a.as_ref(), b.as_str()),
                }
            }
            (BorrowedValue::Boolean(a), Value::Boolean(b)) => {
                self.compare_equality(a, b)
            }
            (BorrowedValue::Null, Value::Null) => {
                matches!(self.operator, ComparisonOp::Equal)
            }
            _ => false,
        }
    }
    
    fn compare_ordered<T: Ord + ?Sized>(&self, a: &T, b: &T) -> bool {
        match self.operator {
            ComparisonOp::Equal => a == b,
            ComparisonOp::NotEqual => a != b,
            ComparisonOp::LessThan => a < b,
            ComparisonOp::LessThanOrEqual => a <= b,
            ComparisonOp::GreaterThan => a > b,
            ComparisonOp::GreaterThanOrEqual => a >= b,
            _ => false,
        }
    }
    
    fn compare_equality<T: PartialEq>(&self, a: &T, b: &T) -> bool {
        match self.operator {
            ComparisonOp::Equal => a == b,
            ComparisonOp::NotEqual => a != b,
            _ => false,
        }
    }
    
    fn pattern_match(&self, pattern: &str, text: &str) -> bool {
        // Simple LIKE pattern matching (% for any chars, _ for single char)
        let pattern = pattern.replace('%', ".*").replace('_', ".");
        regex::Regex::new(&format!("^{}$", pattern))
            .map(|re| re.is_match(text))
            .unwrap_or(false)
    }
}

/// Borrowed query plan that references existing nodes
#[derive(Debug)]
pub struct BorrowedQueryPlan<'a> {
    root: BorrowedPlanNode<'a>,
}

#[derive(Debug)]
pub enum BorrowedPlanNode<'a> {
    Scan {
        table: Cow<'a, str>,
        projection: Option<Cow<'a, [usize]>>,
    },
    Filter {
        input: Box<BorrowedPlanNode<'a>>,
        predicate: BorrowedPredicate<'a>,
    },
    Join {
        left: Box<BorrowedPlanNode<'a>>,
        right: Box<BorrowedPlanNode<'a>>,
        join_type: JoinType,
        on: BorrowedPredicate<'a>,
    },
    Aggregate {
        input: Box<BorrowedPlanNode<'a>>,
        group_by: Cow<'a, [usize]>,
        aggregates: Cow<'a, [AggregateFunc]>,
    },
    Sort {
        input: Box<BorrowedPlanNode<'a>>,
        order_by: Cow<'a, [(usize, SortOrder)]>,
    },
    Limit {
        input: Box<BorrowedPlanNode<'a>>,
        limit: usize,
        offset: usize,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortOrder {
    Ascending,
    Descending,
}

impl<'a> BorrowedQueryPlan<'a> {
    /// Create a new borrowed query plan
    #[inline]
    pub const fn new(root: BorrowedPlanNode<'a>) -> Self {
        Self { root }
    }
    
    /// Get the root node
    #[inline]
    pub const fn root(&self) -> &BorrowedPlanNode<'a> {
        &self.root
    }
    
    /// Estimate the cost of this plan (simplified)
    pub fn estimate_cost(&self) -> u64 {
        self.estimate_node_cost(&self.root)
    }
    
    fn estimate_node_cost(&self, node: &BorrowedPlanNode<'a>) -> u64 {
        match node {
            BorrowedPlanNode::Scan { .. } => 100,
            BorrowedPlanNode::Filter { input, .. } => {
                10 + self.estimate_node_cost(input)
            }
            BorrowedPlanNode::Join { left, right, .. } => {
                1000 + self.estimate_node_cost(left) + self.estimate_node_cost(right)
            }
            BorrowedPlanNode::Aggregate { input, .. } => {
                500 + self.estimate_node_cost(input)
            }
            BorrowedPlanNode::Sort { input, .. } => {
                200 + self.estimate_node_cost(input)
            }
            BorrowedPlanNode::Limit { input, .. } => {
                1 + self.estimate_node_cost(input)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_borrowed_row() {
        let values = vec![
            Value::Integer(42),
            Value::Text("test".to_string()),
        ];
        
        let row = BorrowedRow::new(&values);
        assert_eq!(row.len(), 2);
        assert_eq!(row.get(0), Some(&Value::Integer(42)));
        assert_eq!(row.get(1), Some(&Value::Text("test".to_string())));
    }
    
    #[test]
    fn test_borrowed_predicate() {
        let pred = BorrowedPredicate::new(
            Cow::Borrowed("age"),
            ComparisonOp::GreaterThan,
            BorrowedValue::Integer(25),
        );
        
        assert!(pred.evaluate(&Value::Integer(30)));
        assert!(!pred.evaluate(&Value::Integer(20)));
        assert!(!pred.evaluate(&Value::Text("30".to_string())));
    }
    
    #[test]
    fn test_borrowed_query_plan() {
        let plan = BorrowedQueryPlan::new(
            BorrowedPlanNode::Filter {
                input: Box::new(BorrowedPlanNode::Scan {
                    table: Cow::Borrowed("users"),
                    projection: None,
                }),
                predicate: BorrowedPredicate::new(
                    Cow::Borrowed("active"),
                    ComparisonOp::Equal,
                    BorrowedValue::Boolean(true),
                ),
            }
        );
        
        assert_eq!(plan.estimate_cost(), 110); // 100 for scan + 10 for filter
    }
}