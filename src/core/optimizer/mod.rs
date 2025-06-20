// This is the optimizer module
// Initially, it will define the QueryPlanNode enum and related structs.
// More optimization logic will be added here in the future.

pub mod optimizer; // Added optimizer module
pub mod rules; // Added rules module
pub use optimizer::Optimizer; // Re-export Optimizer

use crate::core::types::DataType; // Added import for DataType

#[allow(dead_code)] // TODO: Remove this when QueryPlanNode is used
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
        // Added DeleteNode
        input: Box<QueryPlanNode>,
        table_name: String,
    },
}

#[allow(dead_code)] // TODO: Remove this when SimplePredicate is used
#[derive(Debug, Clone)]
pub struct SimplePredicate {
    pub column: String,
    pub operator: String, // e.g., "=", "<", ">"
    pub value: DataType,
}

#[allow(dead_code)] // TODO: Remove this when Expression is used
#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    Literal(DataType),
    Column(String),
    BinaryOp {
        left: Box<Expression>,
        op: String, // e.g., "+", "-", "AND", "OR"
        right: Box<Expression>,
    },
    CompareOp {
        // Renamed from Predicate
        left: Box<Expression>,
        op: String, // e.g., "=", "<", ">"
        right: Box<Expression>,
    },
    // Add other expression types as needed, e.g.
    // And(Box<Expression>, Box<Expression>),
    // Or(Box<Expression>, Box<Expression>),
    // Not(Box<Expression>),
}

#[allow(dead_code)] // TODO: Remove this when JoinPredicate is used
#[derive(Debug, Clone)]
pub struct JoinPredicate {
    // Example: Compare two columns, or a column to a literal
    // For simplicity, can start with comparing two columns from left and right inputs
    pub left_column: String,
    pub right_column: String,
    // pub operator: String, // e.g., "="
    // pub right_value: Option<DataType>, // If comparing with a literal
}
