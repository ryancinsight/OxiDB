use crate::core::optimizer::{Expression, QueryPlanNode};
use crate::core::types::DataType;

/// Applies the no-op filter removal optimization rule.
///
/// This rule removes filter nodes that have predicates that always evaluate to true,
/// such as "1 = 1" or "true". These filters don't actually filter any rows and just
/// add unnecessary overhead.
#[must_use]
pub fn apply_noop_filter_removal_rule(plan: QueryPlanNode) -> QueryPlanNode {
    match plan {
        QueryPlanNode::Filter { input, predicate } => {
            // Check if the predicate is a no-op (always true)
            if is_always_true(&predicate) {
                // Remove the filter and return the input directly
                apply_noop_filter_removal_rule(*input)
            } else {
                // Keep the filter but recursively optimize the input
                QueryPlanNode::Filter {
                    input: Box::new(apply_noop_filter_removal_rule(*input)),
                    predicate,
                }
            }
        }
        // For other node types, recursively apply the rule to children
        QueryPlanNode::NestedLoopJoin { left, right, join_predicate } => {
            QueryPlanNode::NestedLoopJoin {
                left: Box::new(apply_noop_filter_removal_rule(*left)),
                right: Box::new(apply_noop_filter_removal_rule(*right)),
                join_predicate,
            }
        }
        QueryPlanNode::Project { input, columns } => QueryPlanNode::Project {
            input: Box::new(apply_noop_filter_removal_rule(*input)),
            columns,
        },
        QueryPlanNode::DeleteNode { input, table_name } => QueryPlanNode::DeleteNode {
            input: Box::new(apply_noop_filter_removal_rule(*input)),
            table_name,
        },
        QueryPlanNode::Aggregate { input, group_by, aggregates } => QueryPlanNode::Aggregate {
            input: Box::new(apply_noop_filter_removal_rule(*input)),
            group_by,
            aggregates,
        },
        // Leaf nodes have no children to optimize
        node @ (QueryPlanNode::TableScan { .. } | QueryPlanNode::IndexScan { .. }) => node,
    }
}

/// Checks if an expression always evaluates to true
fn is_always_true(expr: &Expression) -> bool {
    match expr {
        // Check for literal true
        Expression::Literal(DataType::Boolean(true)) => true,
        // Check for expressions like "1 = 1"
        Expression::BinaryOp { left, op, right } if op == "=" => match (&**left, &**right) {
            (Expression::Literal(l), Expression::Literal(r)) => l == r,
            _ => false,
        },
        // Check for "OR" expressions where at least one side is always true
        Expression::BinaryOp { left, op, right } if op == "OR" => {
            is_always_true(left) || is_always_true(right)
        },
        // Check for "NOT" expressions where the inner expression is always false
        Expression::UnaryOp { op, expr } if op == "NOT" => {
            is_always_false(expr)
        },
        _ => false,
    }
}

/// Checks if an expression always evaluates to false
fn is_always_false(expr: &Expression) -> bool {
    match expr {
        // Check for literal false
        Expression::Literal(DataType::Boolean(false)) => true,
        // Check for expressions like "1 = 2"
        Expression::BinaryOp { left, op, right } if op == "=" => match (&**left, &**right) {
            (Expression::Literal(l), Expression::Literal(r)) => l != r,
            _ => false,
        },
        // Check for "AND" expressions where at least one side is always false
        Expression::BinaryOp { left, op, right } if op == "AND" => {
            is_always_false(left) || is_always_false(right)
        },
        // Check for "OR" expressions where both sides are always false
        Expression::BinaryOp { left, op, right } if op == "OR" => {
            is_always_false(left) && is_always_false(right)
        },
        // Check for "NOT" expressions where the inner expression is always true
        Expression::UnaryOp { op, expr } if op == "NOT" => {
            is_always_true(expr)
        },
        _ => false,
    }
}
