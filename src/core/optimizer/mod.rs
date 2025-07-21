//! Query Optimization Module
//! 
//! This module provides query optimization capabilities including cost-based
//! optimization, rule-based optimization, and query planning.

use crate::core::types::DataType;

pub mod planner; // Cost-based query planner
pub mod rule; // Optimization rule trait
pub mod rules;

// Re-exports for easier access
pub use planner::CostBasedPlanner;
pub use rule::{OptimizationRule, RuleManager};

/// Main optimizer interface
/// Follows SOLID's Single Responsibility Principle - coordinates optimization
pub struct Optimizer {
    planner: CostBasedPlanner,
    rule_manager: RuleManager,
}

impl Optimizer {
    /// Create a new optimizer
    pub fn new() -> Self {
        Self {
            planner: CostBasedPlanner::new(),
            rule_manager: RuleManager::new(),
        }
    }
    
    /// Get the cost-based planner
    pub fn planner(&self) -> &CostBasedPlanner {
        &self.planner
    }
    
    /// Get the rule manager
    pub fn rule_manager(&self) -> &RuleManager {
        &self.rule_manager
    }
    
    /// Get mutable access to the planner
    pub fn planner_mut(&mut self) -> &mut CostBasedPlanner {
        &mut self.planner
    }
    
    /// Get mutable access to the rule manager
    pub fn rule_manager_mut(&mut self) -> &mut RuleManager {
        &mut self.rule_manager
    }
}

impl Default for Optimizer {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum QueryPlanNode {
    TableScan {
        table_name: String,
        alias: Option<String>,
    },
    IndexScan {
        index_name: String,
        table_name: String,
        alias: Option<String>,
        scan_condition: Option<SimplePredicate>,
    },
    Filter {
        input: Box<QueryPlanNode>,
        predicate: Expression,
    },
    Project {
        input: Box<QueryPlanNode>,
        columns: Vec<String>,
    },
    NestedLoopJoin {
        left: Box<QueryPlanNode>,
        right: Box<QueryPlanNode>,
        join_predicate: Option<JoinPredicate>,
    },
    DeleteNode {
        input: Box<QueryPlanNode>,
        table_name: String,
    },
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct SimplePredicate {
    pub column: String,
    pub operator: String,
    pub value: DataType,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    Literal(DataType),
    Column(String),
    BinaryOp {
        left: Box<Expression>,
        op: String,
        right: Box<Expression>,
    },
    CompareOp {
        left: Box<Expression>,
        op: String,
        right: Box<Expression>,
    },
    UnaryOp {
        // Added for NOT
        op: String, // e.g., "NOT"
        expr: Box<Expression>,
    },
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct JoinPredicate {
    pub left_column: String,
    pub right_column: String,
}
