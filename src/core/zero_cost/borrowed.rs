//! Zero-copy borrowed data structures for efficient data access
//! 
//! This module provides borrowed versions of common data structures used in database
//! operations, avoiding allocations and copies wherever possible.

use std::borrow::Cow;
use std::marker::PhantomData;
use crate::core::common::types::Value;
use crate::core::common::types::Row;

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

/// Comparison predicate for efficient filtering
#[derive(Debug)]
pub struct BorrowedPredicate<'a> {
    column_index: usize,
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
    /// List of values
    List(Vec<BorrowedValue<'a>>),
    Null,
}

impl<'a> BorrowedPredicate<'a> {
    /// Create a new borrowed predicate
    pub fn new(column_index: usize, operator: ComparisonOp, value: BorrowedValue<'a>) -> Self {
        Self {
            column_index,
            operator,
            value,
        }
    }
    
    /// Evaluate predicate against a row
    pub fn evaluate(&self, row: &Row) -> bool {
        // Get column value by index
        let column_value = match row.values.get(self.column_index) {
            Some(val) => val,
            None => return false,
        };
        
        // Compare with predicate value
        match (&self.value, column_value) {
            (BorrowedValue::Integer(a), Value::Integer(b)) => {
                // Note: We compare b (column value) against a (predicate value)
                // For example: "age > 25" means column_value > 25
                match self.operator {
                    ComparisonOp::Equal => b == a,
                    ComparisonOp::NotEqual => b != a,
                    ComparisonOp::LessThan => b < a,
                    ComparisonOp::LessThanOrEqual => b <= a,
                    ComparisonOp::GreaterThan => b > a,
                    ComparisonOp::GreaterThanOrEqual => b >= a,
                    _ => false,
                }
            }
            (BorrowedValue::Float(a), Value::Float(b)) => {
                // For floats, use partial comparison with NaN handling
                match (b.partial_cmp(a), self.operator) {
                    (Some(std::cmp::Ordering::Less), ComparisonOp::LessThan) => true,
                    (Some(std::cmp::Ordering::Less), ComparisonOp::LessThanOrEqual) => true,
                    (Some(std::cmp::Ordering::Equal), ComparisonOp::LessThanOrEqual) => true,
                    (Some(std::cmp::Ordering::Equal), ComparisonOp::GreaterThanOrEqual) => true,
                    (Some(std::cmp::Ordering::Equal), ComparisonOp::Equal) => true,
                    (Some(std::cmp::Ordering::Greater), ComparisonOp::GreaterThan) => true,
                    (Some(std::cmp::Ordering::Greater), ComparisonOp::GreaterThanOrEqual) => true,
                    (None, ComparisonOp::NotEqual) => true, // NaN != anything
                    _ => false,
                }
            }
            (BorrowedValue::Text(a), Value::Text(b)) => {
                match self.operator {
                    ComparisonOp::Like => self.pattern_match(a, b),
                    ComparisonOp::NotLike => !self.pattern_match(a, b),
                    ComparisonOp::Equal => b.as_str() == a.as_ref(),
                    ComparisonOp::NotEqual => b.as_str() != a.as_ref(),
                    ComparisonOp::LessThan => b.as_str() < a.as_ref(),
                    ComparisonOp::LessThanOrEqual => b.as_str() <= a.as_ref(),
                    ComparisonOp::GreaterThan => b.as_str() > a.as_ref(),
                    ComparisonOp::GreaterThanOrEqual => b.as_str() >= a.as_ref(),
                    _ => false,
                }
            }
            (BorrowedValue::Boolean(a), Value::Boolean(b)) => {
                match self.operator {
                    ComparisonOp::Equal => b == a,
                    ComparisonOp::NotEqual => b != a,
                    _ => false,
                }
            }
            (BorrowedValue::Null, Value::Null) => {
                matches!(self.operator, ComparisonOp::Equal)
            }
            _ => false,
        }
    }
    
    fn pattern_match(&self, pattern: &str, text: &str) -> bool {
        // Efficient SQL LIKE pattern matching without regex
        // % matches zero or more characters
        // _ matches exactly one character
        
        let pattern_chars: Vec<char> = pattern.chars().collect();
        let text_chars: Vec<char> = text.chars().collect();
        
        self.match_pattern(&pattern_chars, 0, &text_chars, 0)
    }
    
    fn match_pattern(&self, pattern: &[char], p_idx: usize, text: &[char], t_idx: usize) -> bool {
        // Base case: reached end of both pattern and text
        if p_idx >= pattern.len() && t_idx >= text.len() {
            return true;
        }
        
        // Pattern exhausted but text remains
        if p_idx >= pattern.len() {
            return false;
        }
        
        match pattern.get(p_idx) {
            Some('%') => {
                // Handle consecutive % by skipping them
                let mut next_p_idx = p_idx;
                while next_p_idx < pattern.len() && pattern[next_p_idx] == '%' {
                    next_p_idx += 1;
                }
                
                // If % is at the end, it matches everything remaining
                if next_p_idx >= pattern.len() {
                    return true;
                }
                
                // Try matching the rest of the pattern at each position in text
                for i in t_idx..=text.len() {
                    if self.match_pattern(pattern, next_p_idx, text, i) {
                        return true;
                    }
                }
                
                false
            }
            Some('_') => {
                // _ must match exactly one character
                if t_idx >= text.len() {
                    return false;
                }
                self.match_pattern(pattern, p_idx + 1, text, t_idx + 1)
            }
            Some('\\') if p_idx + 1 < pattern.len() => {
                // Handle escaped characters
                if t_idx >= text.len() || text[t_idx] != pattern[p_idx + 1] {
                    return false;
                }
                self.match_pattern(pattern, p_idx + 2, text, t_idx + 1)
            }
            Some(&c) => {
                // Regular character must match exactly
                if t_idx >= text.len() || text[t_idx] != c {
                    return false;
                }
                self.match_pattern(pattern, p_idx + 1, text, t_idx + 1)
            }
            None => {
                // Should not happen due to bounds check above
                false
            }
        }
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
    
    /// Estimate the cost of this plan
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
            0, // age column index
            ComparisonOp::GreaterThan,
            BorrowedValue::Integer(25),
        );
        
        // Create test rows
        let row1 = Row::new(vec![Value::Integer(30)]);
        let row2 = Row::new(vec![Value::Integer(20)]);
        let row3 = Row::new(vec![Value::Text("30".to_string())]);
        
        assert!(pred.evaluate(&row1));
        assert!(!pred.evaluate(&row2));
        assert!(!pred.evaluate(&row3));
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
                    0, // active column index
                    ComparisonOp::Equal,
                    BorrowedValue::Boolean(true),
                ),
            }
        );
        
        assert_eq!(plan.estimate_cost(), 110); // 100 for scan + 10 for filter
    }
    
    #[test]
    fn test_pattern_matching() {
        // Test exact matches
        let pred = BorrowedPredicate::new(0, ComparisonOp::Like, BorrowedValue::Text("hello".into()));
        // Note: pattern_match(pattern, text)
        assert!(pred.pattern_match("hello", "hello"));
        assert!(!pred.pattern_match("hello", "world"));
        
        // Test % wildcard (matches zero or more characters)
        assert!(pred.pattern_match("h%", "hello"));
        assert!(pred.pattern_match("h%", "h"));
        assert!(pred.pattern_match("%ello", "hello"));
        assert!(pred.pattern_match("%ello", "jello"));  // Fixed: jello ends with ello
        assert!(pred.pattern_match("h%o", "hello"));
        assert!(pred.pattern_match("%", "anything"));
        assert!(pred.pattern_match("%%", "anything"));
        assert!(pred.pattern_match("h%l%o", "hello"));
        assert!(pred.pattern_match("%low", "yellow"));  // This matches: yellow ends with low
        
        // Test _ wildcard (matches exactly one character)
        assert!(pred.pattern_match("h_llo", "hello"));
        assert!(!pred.pattern_match("h_llo", "hllo"));
        assert!(!pred.pattern_match("h_llo", "heello"));
        assert!(pred.pattern_match("_ello", "hello"));
        assert!(pred.pattern_match("hell_", "hello"));
        assert!(pred.pattern_match("_____", "hello"));
        assert!(!pred.pattern_match("______", "hello"));
        
        // Test combinations
        assert!(pred.pattern_match("h_l%", "hello"));
        assert!(pred.pattern_match("h_l%", "help"));
        assert!(pred.pattern_match("%l_o", "hello"));
        assert!(pred.pattern_match("%l_w", "yellow"));  // Fixed: yellow has l_w pattern
        assert!(!pred.pattern_match("%l_o", "helo"));
        
        // Test escaped characters
        assert!(pred.pattern_match("h\\%llo", "h%llo"));
        assert!(!pred.pattern_match("h\\%llo", "hello"));
        assert!(pred.pattern_match("h\\_llo", "h_llo"));
        assert!(!pred.pattern_match("h\\_llo", "hello"));
        assert!(pred.pattern_match("h\\\\llo", "h\\llo"));
        
        // Test edge cases
        assert!(pred.pattern_match("", ""));
        assert!(!pred.pattern_match("", "hello"));
        assert!(!pred.pattern_match("hello", ""));
        assert!(pred.pattern_match("%", ""));
        
        // Test Unicode
        assert!(pred.pattern_match("h%", "héllo"));
        assert!(pred.pattern_match("h_llo", "héllo"));
        assert!(pred.pattern_match("%世界", "你好世界"));
        assert!(pred.pattern_match("你_世界", "你好世界"));
    }
    
    #[test]
    fn test_like_predicate_evaluation() {
        let pred = BorrowedPredicate::new(
            0,
            ComparisonOp::Like,
            BorrowedValue::Text("John%".into()),
        );
        
        let row1 = Row::new(vec![Value::Text("John Doe".to_string())]);
        let row2 = Row::new(vec![Value::Text("Jane Doe".to_string())]);
        let row3 = Row::new(vec![Value::Text("John".to_string())]);
        let row4 = Row::new(vec![Value::Integer(42)]);
        
        assert!(pred.evaluate(&row1));
        assert!(!pred.evaluate(&row2));
        assert!(pred.evaluate(&row3));
        assert!(!pred.evaluate(&row4)); // Type mismatch
        
        // Test NOT LIKE
        let not_pred = BorrowedPredicate::new(
            0,
            ComparisonOp::NotLike,
            BorrowedValue::Text("John%".into()),
        );
        
        assert!(!not_pred.evaluate(&row1));
        assert!(not_pred.evaluate(&row2));
    }
}