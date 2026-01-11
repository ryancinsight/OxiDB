#[cfg(test)]
mod tests {
    use crate::core::optimizer::rules::apply_noop_filter_removal_rule;
    use crate::core::optimizer::{Expression, QueryPlanNode};
    use crate::core::types::DataType;

    #[test]
    fn test_noop_filter_removal_removes_true_filters() {
        // Create a filter with predicate "1 = 1" (always true)
        let true_predicate = Expression::BinaryOp {
            left: Box::new(Expression::Literal(DataType::Integer(1))),
            op: "=".to_string(),
            right: Box::new(Expression::Literal(DataType::Integer(1))),
        };

        let filter_node = QueryPlanNode::Filter {
            input: Box::new(QueryPlanNode::TableScan {
                table_name: "test_table".to_string(),
                alias: None,
            }),
            predicate: true_predicate,
        };

        let optimized = apply_noop_filter_removal_rule(filter_node);

        // The filter should be removed, leaving just the table scan
        match optimized {
            QueryPlanNode::TableScan { table_name, .. } => {
                assert_eq!(table_name, "test_table");
            }
            _ => assert!(false, "Expected TableScan after removing no-op filter"),
        }
    }

    #[test]
    fn test_noop_filter_removal_keeps_meaningful_filters() {
        // Create a filter with predicate "id = 5" (meaningful filter)
        let meaningful_predicate = Expression::BinaryOp {
            left: Box::new(Expression::Column("id".to_string())),
            op: "=".to_string(),
            right: Box::new(Expression::Literal(DataType::Integer(5))),
        };

        let filter_node = QueryPlanNode::Filter {
            input: Box::new(QueryPlanNode::TableScan {
                table_name: "test_table".to_string(),
                alias: None,
            }),
            predicate: meaningful_predicate.clone(),
        };

        let optimized = apply_noop_filter_removal_rule(filter_node);

        // The filter should remain
        match optimized {
            QueryPlanNode::Filter { predicate, .. } => {
                assert!(matches!(predicate, Expression::BinaryOp { .. }));
            }
            _ => assert!(false, "Expected Filter to remain for meaningful predicate"),
        }
    }

    #[test]
    fn test_noop_filter_removal_or_true() {
        // "x = 5 OR true" should be always true
        let predicate = Expression::BinaryOp {
            left: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::Column("x".to_string())),
                op: "=".to_string(),
                right: Box::new(Expression::Literal(DataType::Integer(5))),
            }),
            op: "OR".to_string(),
            right: Box::new(Expression::Literal(DataType::Boolean(true))),
        };

        let filter_node = QueryPlanNode::Filter {
            input: Box::new(QueryPlanNode::TableScan {
                table_name: "test_table".to_string(),
                alias: None,
            }),
            predicate,
        };

        let optimized = apply_noop_filter_removal_rule(filter_node);

        match optimized {
            QueryPlanNode::TableScan { table_name, .. } => {
                assert_eq!(table_name, "test_table");
            }
            _ => assert!(false, "Expected TableScan after removing 'OR true' filter"),
        }
    }

    #[test]
    fn test_noop_filter_removal_true_or() {
        // "true OR x = 5" should be always true
        let predicate = Expression::BinaryOp {
            left: Box::new(Expression::Literal(DataType::Boolean(true))),
            op: "OR".to_string(),
            right: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::Column("x".to_string())),
                op: "=".to_string(),
                right: Box::new(Expression::Literal(DataType::Integer(5))),
            }),
        };

        let filter_node = QueryPlanNode::Filter {
            input: Box::new(QueryPlanNode::TableScan {
                table_name: "test_table".to_string(),
                alias: None,
            }),
            predicate,
        };

        let optimized = apply_noop_filter_removal_rule(filter_node);

        match optimized {
            QueryPlanNode::TableScan { table_name, .. } => {
                assert_eq!(table_name, "test_table");
            }
            _ => assert!(false, "Expected TableScan after removing 'true OR' filter"),
        }
    }

    #[test]
    fn test_noop_filter_removal_not_false() {
        // "NOT false" should be always true
        let predicate = Expression::UnaryOp {
            op: "NOT".to_string(),
            expr: Box::new(Expression::Literal(DataType::Boolean(false))),
        };

        let filter_node = QueryPlanNode::Filter {
            input: Box::new(QueryPlanNode::TableScan {
                table_name: "test_table".to_string(),
                alias: None,
            }),
            predicate,
        };

        let optimized = apply_noop_filter_removal_rule(filter_node);

        match optimized {
            QueryPlanNode::TableScan { table_name, .. } => {
                assert_eq!(table_name, "test_table");
            }
            _ => assert!(false, "Expected TableScan after removing 'NOT false' filter"),
        }
    }

    #[test]
    fn test_noop_filter_recursive_true() {
        // "(1=1) OR (x=5)" -> true OR (x=5) -> true
        let one_eq_one = Expression::BinaryOp {
            left: Box::new(Expression::Literal(DataType::Integer(1))),
            op: "=".to_string(),
            right: Box::new(Expression::Literal(DataType::Integer(1))),
        };

        let predicate = Expression::BinaryOp {
            left: Box::new(one_eq_one),
            op: "OR".to_string(),
            right: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::Column("x".to_string())),
                op: "=".to_string(),
                right: Box::new(Expression::Literal(DataType::Integer(5))),
            }),
        };

        let filter_node = QueryPlanNode::Filter {
            input: Box::new(QueryPlanNode::TableScan {
                table_name: "test_table".to_string(),
                alias: None,
            }),
            predicate,
        };

        let optimized = apply_noop_filter_removal_rule(filter_node);

        match optimized {
            QueryPlanNode::TableScan { table_name, .. } => {
                assert_eq!(table_name, "test_table");
            }
            _ => assert!(false, "Expected TableScan after removing recursive true filter"),
        }
    }
}
