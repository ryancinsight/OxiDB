#[cfg(test)]
mod tests {
    use crate::core::query::sql::ast::{
        AstExpressionValue, Condition, ConditionTree, SelectStatement, Statement,
        TableReference, SelectColumn,
    };
    use crate::core::query::sql::translator::translate_ast_to_command;
    use crate::core::query::commands::{Command, SqlConditionTree, ConditionValue, SelectColumnSpec};

    #[test]
    fn test_translate_column_comparison_command() {
        // SELECT * FROM users WHERE salary > avg_salary
        let ast_stmt = Statement::Select(SelectStatement {
            columns: vec![SelectColumn::Asterisk],
            from_clause: TableReference {
                name: "users".to_string(),
                alias: None,
            },
            joins: Vec::new(),
            condition: Some(ConditionTree::Comparison(Condition {
                column: "salary".to_string(),
                operator: ">".to_string(),
                value: AstExpressionValue::ColumnIdentifier("avg_salary".to_string()),
            })),
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
        });

        let command = translate_ast_to_command(ast_stmt).unwrap();

        match command {
            Command::Select { columns, source, condition, .. } => {
                assert_eq!(columns, SelectColumnSpec::All);
                assert_eq!(source, "users");
                assert!(condition.is_some());

                if let Some(SqlConditionTree::Comparison(simple_cond)) = condition {
                    assert_eq!(simple_cond.column, "salary");
                    assert_eq!(simple_cond.operator, ">");
                    match simple_cond.value {
                        ConditionValue::Column(col_name) => {
                            assert_eq!(col_name, "avg_salary");
                        },
                        _ => panic!("Expected ConditionValue::Column"),
                    }
                } else {
                    panic!("Expected Comparison condition tree variant");
                }
            }
            _ => panic!("Expected Command::Select"),
        }
    }
}
