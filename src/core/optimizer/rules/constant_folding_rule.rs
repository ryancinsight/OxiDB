use crate::core::optimizer::{Expression, QueryPlanNode};
use crate::core::types::DataType; // Correct DataType that holds values

// Helper function to perform the actual constant folding on expressions
fn fold_expression(expression: Expression) -> Expression {
    match expression {
        Expression::BinaryOp { left, op, right } => {
            let folded_left = Box::new(fold_expression(*left));
            let folded_right = Box::new(fold_expression(*right));

            match (&*folded_left, &*folded_right) {
                (Expression::Literal(left_val), Expression::Literal(right_val)) => {
                    // Both operands are literals, try to evaluate
                    match op.as_str() {
                        // Arithmetic Operations
                        "+" => match (left_val, right_val) {
                            (DataType::Integer(l), DataType::Integer(r)) => Expression::Literal(DataType::Integer(l + r)),
                            (DataType::Float(l), DataType::Float(r)) => Expression::Literal(DataType::Float(l + r)),
                            (DataType::Integer(l), DataType::Float(r)) => Expression::Literal(DataType::Float(*l as f64 + r)),
                            (DataType::Float(l), DataType::Integer(r)) => Expression::Literal(DataType::Float(l + *r as f64)),
                            _ => Expression::BinaryOp { left: folded_left, op, right: folded_right }, // Type mismatch
                        },
                        "-" => match (left_val, right_val) {
                            (DataType::Integer(l), DataType::Integer(r)) => Expression::Literal(DataType::Integer(l - r)),
                            (DataType::Float(l), DataType::Float(r)) => Expression::Literal(DataType::Float(l - r)),
                            (DataType::Integer(l), DataType::Float(r)) => Expression::Literal(DataType::Float(*l as f64 - r)),
                            (DataType::Float(l), DataType::Integer(r)) => Expression::Literal(DataType::Float(l - *r as f64)),
                            _ => Expression::BinaryOp { left: folded_left, op, right: folded_right },
                        },
                        "*" => match (left_val, right_val) {
                            (DataType::Integer(l), DataType::Integer(r)) => Expression::Literal(DataType::Integer(l * r)),
                            (DataType::Float(l), DataType::Float(r)) => Expression::Literal(DataType::Float(l * r)),
                            (DataType::Integer(l), DataType::Float(r)) => Expression::Literal(DataType::Float(*l as f64 * r)),
                            (DataType::Float(l), DataType::Integer(r)) => Expression::Literal(DataType::Float(l * *r as f64)),
                            _ => Expression::BinaryOp { left: folded_left, op, right: folded_right },
                        },
                        "/" => match (left_val, right_val) {
                            (DataType::Integer(l), DataType::Integer(r)) => {
                                if *r == 0 { Expression::BinaryOp { left: folded_left, op, right: folded_right } } // Division by zero
                                else { Expression::Literal(DataType::Integer(l / r)) }
                            },
                            (DataType::Float(l), DataType::Float(r)) => {
                                if *r == 0.0 { Expression::BinaryOp { left: folded_left, op, right: folded_right } } // Division by zero
                                else { Expression::Literal(DataType::Float(l / r)) }
                            },
                            (DataType::Integer(l), DataType::Float(r)) => {
                                if *r == 0.0 { Expression::BinaryOp { left: folded_left, op, right: folded_right } }
                                else { Expression::Literal(DataType::Float(*l as f64 / r)) }
                            },
                            (DataType::Float(l), DataType::Integer(r)) => {
                                if *r == 0 { Expression::BinaryOp { left: folded_left, op, right: folded_right } }
                                else { Expression::Literal(DataType::Float(l / *r as f64)) }
                            },
                            _ => Expression::BinaryOp { left: folded_left, op, right: folded_right },
                        },
                        // Logical Operations
                        "AND" => match (left_val, right_val) {
                            (DataType::Boolean(l), DataType::Boolean(r)) => Expression::Literal(DataType::Boolean(*l && *r)),
                            _ => Expression::BinaryOp { left: folded_left, op, right: folded_right }, // Type mismatch
                        },
                        "OR" => match (left_val, right_val) {
                            (DataType::Boolean(l), DataType::Boolean(r)) => Expression::Literal(DataType::Boolean(*l || *r)),
                            _ => Expression::BinaryOp { left: folded_left, op, right: folded_right },
                        },
                        _ => Expression::BinaryOp { left: folded_left, op, right: folded_right }, // Unknown operator
                    }
                }
                _ => Expression::BinaryOp { left: folded_left, op, right: folded_right }, // One or both not literals
            }
        }
        Expression::CompareOp { left, op, right } => {
            let folded_left = Box::new(fold_expression(*left));
            let folded_right = Box::new(fold_expression(*right));

            match (&*folded_left, &*folded_right) {
                (Expression::Literal(left_val), Expression::Literal(right_val)) => {
                    // Both operands are literals, try to evaluate comparison
                    // Note: Null propagation for comparisons: any comparison with NULL is NULL (or false in boolean context)
                    // For simplicity here, we'll treat Nulls as type mismatches for direct comparisons,
                    // unless specifically comparing for IS NULL / IS NOT NULL (which are not standard CompareOps here).
                    if left_val == &DataType::Null || right_val == &DataType::Null {
                         // Standard SQL comparison with NULL yields NULL.
                         // Here, we could return Literal(DataType::Null) if it should propagate,
                         // or Literal(DataType::Boolean(false)) if NULLs in comparisons are treated as false.
                         // Returning the original op for now if we want to be conservative or if Expression can't hold Null result for a boolean op.
                         // Let's assume for now it evaluates to Boolean(false) for simplicity in this context.
                         // However, a more robust way might be to return Literal(DataType::Boolean(false)) or handle specific SQL ways.
                         // For now, returning the original op if nulls are involved and not directly handled.
                         return Expression::CompareOp { left: folded_left, op, right: folded_right };
                    }

                    let result = match op.as_str() {
                        "=" | "==" => match (left_val, right_val) { // Added "=="
                            (DataType::Integer(l), DataType::Integer(r)) => DataType::Boolean(l == r),
                            (DataType::Float(l), DataType::Float(r)) => DataType::Boolean(l == r),
                            (DataType::String(l), DataType::String(r)) => DataType::Boolean(l == r),
                            (DataType::Boolean(l), DataType::Boolean(r)) => DataType::Boolean(l == r),
                            // Basic cross-type comparison for equality (e.g., Int and Float)
                            (DataType::Integer(l), DataType::Float(r)) => DataType::Boolean((*l as f64) == *r),
                            (DataType::Float(l), DataType::Integer(r)) => DataType::Boolean(*l == (*r as f64)),
                            _ => return Expression::CompareOp { left: folded_left, op, right: folded_right }, // Type mismatch
                        },
                        "!=" | "<>" => match (left_val, right_val) { // Added "<>" as an alias for "!="
                            (DataType::Integer(l), DataType::Integer(r)) => DataType::Boolean(l != r),
                            (DataType::Float(l), DataType::Float(r)) => DataType::Boolean(l != r),
                            (DataType::String(l), DataType::String(r)) => DataType::Boolean(l != r),
                            (DataType::Boolean(l), DataType::Boolean(r)) => DataType::Boolean(l != r),
                            (DataType::Integer(l), DataType::Float(r)) => DataType::Boolean((*l as f64) != *r),
                            (DataType::Float(l), DataType::Integer(r)) => DataType::Boolean(*l != (*r as f64)),
                            _ => return Expression::CompareOp { left: folded_left, op, right: folded_right },
                        },
                        "<" => match (left_val, right_val) {
                            (DataType::Integer(l), DataType::Integer(r)) => DataType::Boolean(l < r),
                            (DataType::Float(l), DataType::Float(r)) => DataType::Boolean(l < r),
                            (DataType::String(l), DataType::String(r)) => DataType::Boolean(l < r),
                            (DataType::Integer(l), DataType::Float(r)) => DataType::Boolean((*l as f64) < *r),
                            (DataType::Float(l), DataType::Integer(r)) => DataType::Boolean(*l < (*r as f64)),
                            _ => return Expression::CompareOp { left: folded_left, op, right: folded_right },
                        },
                        "<=" => match (left_val, right_val) {
                            (DataType::Integer(l), DataType::Integer(r)) => DataType::Boolean(l <= r),
                            (DataType::Float(l), DataType::Float(r)) => DataType::Boolean(l <= r),
                            (DataType::String(l), DataType::String(r)) => DataType::Boolean(l <= r),
                            (DataType::Integer(l), DataType::Float(r)) => DataType::Boolean((*l as f64) <= *r),
                            (DataType::Float(l), DataType::Integer(r)) => DataType::Boolean(*l <= (*r as f64)),
                            _ => return Expression::CompareOp { left: folded_left, op, right: folded_right },
                        },
                        ">" => match (left_val, right_val) {
                            (DataType::Integer(l), DataType::Integer(r)) => DataType::Boolean(l > r),
                            (DataType::Float(l), DataType::Float(r)) => DataType::Boolean(l > r),
                            (DataType::String(l), DataType::String(r)) => DataType::Boolean(l > r),
                            (DataType::Integer(l), DataType::Float(r)) => DataType::Boolean((*l as f64) > *r),
                            (DataType::Float(l), DataType::Integer(r)) => DataType::Boolean(*l > (*r as f64)),
                            _ => return Expression::CompareOp { left: folded_left, op, right: folded_right },
                        },
                        ">=" => match (left_val, right_val) {
                            (DataType::Integer(l), DataType::Integer(r)) => DataType::Boolean(l >= r),
                            (DataType::Float(l), DataType::Float(r)) => DataType::Boolean(l >= r),
                            (DataType::String(l), DataType::String(r)) => DataType::Boolean(l >= r),
                            (DataType::Integer(l), DataType::Float(r)) => DataType::Boolean((*l as f64) >= *r),
                            (DataType::Float(l), DataType::Integer(r)) => DataType::Boolean(*l >= (*r as f64)),
                            _ => return Expression::CompareOp { left: folded_left, op, right: folded_right },
                        },
                        _ => return Expression::CompareOp { left: folded_left, op, right: folded_right }, // Unknown operator
                    };
                    Expression::Literal(result)
                }
                _ => Expression::CompareOp { left: folded_left, op, right: folded_right }, // One or both not literals
            }
        }
        // Base cases: Literals and Columns are already "folded"
        Expression::Literal(_) => expression,
        Expression::Column(_) => expression,
    }
}

// Main function to apply the constant folding rule to a query plan
pub fn apply_constant_folding_rule(plan: QueryPlanNode) -> QueryPlanNode {
    match plan {
        QueryPlanNode::Filter { input, predicate } => {
            let new_input = Box::new(apply_constant_folding_rule(*input));
            let folded_predicate = fold_expression(predicate);
            QueryPlanNode::Filter { input: new_input, predicate: folded_predicate }
        }
        QueryPlanNode::Project { input, columns } => {
            // If columns could contain expressions in the future, they should be folded here.
            // For now, columns are just names (Vec<String>).
            let new_input = Box::new(apply_constant_folding_rule(*input));
            QueryPlanNode::Project { input: new_input, columns }
        }
        QueryPlanNode::NestedLoopJoin { left, right, join_predicate } => {
            // If join_predicate were an Expression, it would be folded here.
            // Currently, JoinPredicate is a simpler struct.
            let new_left = Box::new(apply_constant_folding_rule(*left));
            let new_right = Box::new(apply_constant_folding_rule(*right));
            QueryPlanNode::NestedLoopJoin { left: new_left, right: new_right, join_predicate }
        }
        QueryPlanNode::TableScan { .. } => plan, // No expressions to fold, no inputs to recurse
        QueryPlanNode::IndexScan { ref scan_condition, .. } => {
            // The scan_condition is Option<SimplePredicate>. SimplePredicate contains a DataType literal.
            // It's not an Expression, so fold_expression() doesn't directly apply.
            // If SimplePredicate.value could somehow be an expression (it can't by current def),
            // or if scan_condition became Option<Expression>, then folding would apply.
            // For now, IndexScan is a leaf in terms of expression folding for its direct conditions.
            // However, if it had an 'input' QueryPlanNode, we'd recurse. It does not.
            plan
        }
        // Add other QueryPlanNode variants here if they have expressions or inputs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::DataType;

    #[test]
    fn test_fold_literal_and_column() {
        let lit_expr = Expression::Literal(DataType::Integer(10));
        assert_eq!(fold_expression(lit_expr.clone()), lit_expr);

        let col_expr = Expression::Column("my_col".to_string());
        assert_eq!(fold_expression(col_expr.clone()), col_expr);
    }

    #[test]
    fn test_fold_binary_op_integers() {
        // 10 + 5
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Literal(DataType::Integer(10))),
            op: "+".to_string(),
            right: Box::new(Expression::Literal(DataType::Integer(5))),
        };
        let folded = fold_expression(expr);
        assert_eq!(folded, Expression::Literal(DataType::Integer(15)));

        // 10 / 0 (should not fold)
        let expr_div_zero = Expression::BinaryOp {
            left: Box::new(Expression::Literal(DataType::Integer(10))),
            op: "/".to_string(),
            right: Box::new(Expression::Literal(DataType::Integer(0))),
        };
        let folded_div_zero = fold_expression(expr_div_zero.clone());
         match folded_div_zero {
            Expression::BinaryOp { left, op, right } => {
                assert_eq!(*left, Expression::Literal(DataType::Integer(10)));
                assert_eq!(op, "/".to_string());
                assert_eq!(*right, Expression::Literal(DataType::Integer(0)));
            }
            _ => panic!("Expected BinaryOp for division by zero"),
        }
    }

    #[test]
    fn test_fold_binary_op_floats() {
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Literal(DataType::Float(10.5))),
            op: "*".to_string(),
            right: Box::new(Expression::Literal(DataType::Float(2.0))),
        };
        let folded = fold_expression(expr);
        assert_eq!(folded, Expression::Literal(DataType::Float(21.0)));
    }

    #[test]
    fn test_fold_binary_op_mixed_types_arithmetic() {
        // Integer + Float
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Literal(DataType::Integer(10))),
            op: "+".to_string(),
            right: Box::new(Expression::Literal(DataType::Float(5.5))),
        };
        let folded = fold_expression(expr);
        assert_eq!(folded, Expression::Literal(DataType::Float(15.5)));

        // Float + Integer
        let expr_rev = Expression::BinaryOp {
            left: Box::new(Expression::Literal(DataType::Float(5.5))),
            op: "+".to_string(),
            right: Box::new(Expression::Literal(DataType::Integer(10))),
        };
        let folded_rev = fold_expression(expr_rev);
        assert_eq!(folded_rev, Expression::Literal(DataType::Float(15.5)));
    }


    #[test]
    fn test_fold_binary_op_logical() {
        // true AND false
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Literal(DataType::Boolean(true))),
            op: "AND".to_string(),
            right: Box::new(Expression::Literal(DataType::Boolean(false))),
        };
        let folded = fold_expression(expr);
        assert_eq!(folded, Expression::Literal(DataType::Boolean(false)));
    }

    #[test]
    fn test_fold_binary_op_type_mismatch() {
        // Integer + Boolean (should not fold)
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Literal(DataType::Integer(10))),
            op: "+".to_string(),
            right: Box::new(Expression::Literal(DataType::Boolean(false))),
        };
        let folded_expr = fold_expression(expr.clone()); // Clone for assertion
        match folded_expr {
            Expression::BinaryOp { left, op, right } => {
                assert_eq!(*left, Expression::Literal(DataType::Integer(10)));
                assert_eq!(op, "+".to_string());
                assert_eq!(*right, Expression::Literal(DataType::Boolean(false)));
            }
            _ => panic!("Expected BinaryOp for type mismatch"),
        }
    }

    #[test]
    fn test_fold_compare_op_literals() {
        // 10 > 5
        let expr = Expression::CompareOp {
            left: Box::new(Expression::Literal(DataType::Integer(10))),
            op: ">".to_string(),
            right: Box::new(Expression::Literal(DataType::Integer(5))),
        };
        let folded = fold_expression(expr);
        assert_eq!(folded, Expression::Literal(DataType::Boolean(true)));

        // "apple" == "banana"
        let expr_str = Expression::CompareOp {
            left: Box::new(Expression::Literal(DataType::String("apple".to_string()))),
            op: "==".to_string(), // Using == for equality, as per typical SQL/Rust if op is flexible
            right: Box::new(Expression::Literal(DataType::String("banana".to_string()))),
        };
        // If your op was "=", change "==" to "=" above and here.
        let folded_str = fold_expression(expr_str);
        assert_eq!(folded_str, Expression::Literal(DataType::Boolean(false)));
    }

    #[test]
    fn test_fold_compare_op_type_mismatch() {
        // 10 > "apple" (should not fold)
        let expr = Expression::CompareOp {
            left: Box::new(Expression::Literal(DataType::Integer(10))),
            op: ">".to_string(),
            right: Box::new(Expression::Literal(DataType::String("apple".to_string()))),
        };
        let folded_expr = fold_expression(expr.clone());
        match folded_expr {
            Expression::CompareOp { left, op, right } => {
                assert_eq!(*left, Expression::Literal(DataType::Integer(10)));
                assert_eq!(op, ">".to_string());
                assert_eq!(*right, Expression::Literal(DataType::String("apple".to_string())));
            }
            _ => panic!("Expected CompareOp for type mismatch"),
        }
    }

    #[test]
    fn test_fold_compare_op_mixed_numeric_types() {
        // 10 == 10.0
        let expr = Expression::CompareOp {
            left: Box::new(Expression::Literal(DataType::Integer(10))),
            op: "=".to_string(),
            right: Box::new(Expression::Literal(DataType::Float(10.0))),
        };
        let folded = fold_expression(expr);
        assert_eq!(folded, Expression::Literal(DataType::Boolean(true)));

        // 5 < 5.5
        let expr2 = Expression::CompareOp {
            left: Box::new(Expression::Literal(DataType::Integer(5))),
            op: "<".to_string(),
            right: Box::new(Expression::Literal(DataType::Float(5.5))),
        };
        let folded2 = fold_expression(expr2);
        assert_eq!(folded2, Expression::Literal(DataType::Boolean(true)));
    }


    #[test]
    fn test_fold_nested_expression() {
        // (2 + 3) * 4  => 5 * 4 => 20
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::Literal(DataType::Integer(2))),
                op: "+".to_string(),
                right: Box::new(Expression::Literal(DataType::Integer(3))),
            }),
            op: "*".to_string(),
            right: Box::new(Expression::Literal(DataType::Integer(4))),
        };
        let folded = fold_expression(expr);
        assert_eq!(folded, Expression::Literal(DataType::Integer(20)));
    }

    #[test]
    fn test_fold_expression_with_column() {
        // col + 5 (should not fold to a single literal)
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Column("my_col".to_string())),
            op: "+".to_string(),
            right: Box::new(Expression::Literal(DataType::Integer(5))),
        };
        let folded_expr = fold_expression(expr.clone());
         match folded_expr {
            Expression::BinaryOp { left, op, right } => {
                assert_eq!(*left, Expression::Column("my_col".to_string()));
                assert_eq!(op, "+".to_string());
                assert_eq!(*right, Expression::Literal(DataType::Integer(5)));
            }
            _ => panic!("Expected BinaryOp when a column is involved"),
        }

        // (2 + 3) + col => 5 + col
        let expr_complex = Expression::BinaryOp {
            left: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::Literal(DataType::Integer(2))),
                op: "+".to_string(),
                right: Box::new(Expression::Literal(DataType::Integer(3))),
            }),
            op: "+".to_string(),
            right: Box::new(Expression::Column("my_col".to_string())),
        };
        let folded_complex = fold_expression(expr_complex);
        match folded_complex {
            Expression::BinaryOp { left, op, right } => {
                assert_eq!(*left, Expression::Literal(DataType::Integer(5)));
                assert_eq!(op, "+".to_string());
                assert_eq!(*right, Expression::Column("my_col".to_string()));
            }
            _ => panic!("Expected partially folded BinaryOp"),
        }
    }

    // Tests for apply_constant_folding_rule (plan traversal)
    #[test]
    fn test_apply_to_filter_node() {
        let plan = QueryPlanNode::Filter {
            input: Box::new(QueryPlanNode::TableScan { table_name: "t".to_string(), alias: None }),
            predicate: Expression::BinaryOp {
                left: Box::new(Expression::Literal(DataType::Integer(2))),
                op: "+".to_string(),
                right: Box::new(Expression::Literal(DataType::Integer(3))),
            },
        };
        let optimized_plan = apply_constant_folding_rule(plan);
        match optimized_plan {
            QueryPlanNode::Filter { predicate, .. } => {
                assert_eq!(predicate, Expression::Literal(DataType::Integer(5)));
            }
            _ => panic!("Expected Filter node"),
        }
    }

    #[test]
    fn test_compare_op_with_null_returns_original() {
        // Test with one Null operand
        let expr_null_left = Expression::CompareOp {
            left: Box::new(Expression::Literal(DataType::Null)),
            op: "=".to_string(),
            right: Box::new(Expression::Literal(DataType::Integer(5))),
        };
        let folded_null_left = fold_expression(expr_null_left.clone());
        assert_eq!(folded_null_left, expr_null_left, "Comparison with Null on left should return original expression");

        // Test with both Null operands
        let expr_null_both = Expression::CompareOp {
            left: Box::new(Expression::Literal(DataType::Null)),
            op: "=".to_string(),
            right: Box::new(Expression::Literal(DataType::Null)),
        };
        let folded_null_both = fold_expression(expr_null_both.clone());
        assert_eq!(folded_null_both, expr_null_both, "Comparison with Null on both sides should return original expression");
    }
}
