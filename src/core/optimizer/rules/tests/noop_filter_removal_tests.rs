#[cfg(test)]
mod tests {
    use crate::core::optimizer::{QueryPlanNode, Expression};
    use crate::core::optimizer::rules::apply_noop_filter_removal_rule;

    #[test]
    fn test_noop_filter_removal_removes_true_filters() {
        // Create a filter with predicate "1 = 1" (always true)
        let true_predicate = Expression::BinaryOp {
            left: Box::new(Expression::Literal(crate::core::types::DataType::Integer(1))),
            op: "=".to_string(),
            right: Box::new(Expression::Literal(crate::core::types::DataType::Integer(1))),
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
            _ => panic!("Expected TableScan after removing no-op filter"),
        }
    }

    #[test]
    fn test_noop_filter_removal_keeps_meaningful_filters() {
        // Create a filter with predicate "id = 5" (meaningful filter)
        let meaningful_predicate = Expression::BinaryOp {
            left: Box::new(Expression::Column("id".to_string())),
            op: "=".to_string(),
            right: Box::new(Expression::Literal(crate::core::types::DataType::Integer(5))),
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
            _ => panic!("Expected Filter to remain for meaningful predicate"),
        }
    }
}
