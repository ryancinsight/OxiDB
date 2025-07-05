// src/core/optimizer/tests/optimizer_tests.rs

use crate::core::common::types::DataType as CommonDataType; // Renamed to avoid conflict
use crate::core::common::OxidbError;
use crate::core::indexing::manager::IndexManager;
use crate::core::optimizer::{Expression as OptimizerExpression, Optimizer, QueryPlanNode};
use crate::core::query::sql::ast::{
    AstExpression, AstFunctionArg, AstLiteralValue, Condition, ConditionTree, SelectColumn,
    SelectStatement, Statement, TableReference, AstComparisonOperator
};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

fn create_test_optimizer() -> Optimizer {
    let temp_dir = tempfile::tempdir().unwrap();
    let index_manager = Arc::new(RwLock::new(
        IndexManager::new(temp_dir.path().to_path_buf()).unwrap(),
    ));
    Optimizer::new(index_manager)
}

#[test]
fn test_translate_ast_function_call_to_optimizer_expression() {
    let optimizer = create_test_optimizer();
    let ast_expr = AstExpression::FunctionCall {
        name: "COSINE_SIMILARITY".to_string(),
        args: vec![
            AstFunctionArg::Expression(AstExpression::ColumnIdentifier("v1".to_string())),
            AstFunctionArg::Expression(AstExpression::Literal(AstLiteralValue::Vector(vec![
                AstLiteralValue::Number("1.0".to_string()),
                AstLiteralValue::Number("2.0".to_string()),
            ]))),
        ],
    };

    let result = optimizer.ast_expression_to_optimizer_expression(&ast_expr);
    assert!(result.is_ok());
    if let Ok(OptimizerExpression::FunctionCall { name, args }) = result {
        assert_eq!(name, "COSINE_SIMILARITY");
        assert_eq!(args.len(), 2);
        assert!(matches!(args[0], OptimizerExpression::Column(col_name) if col_name == "v1"));
        assert!(matches!(args[1], OptimizerExpression::Literal(CommonDataType::Vector(Some(2)))));
    } else {
        panic!("Expected OptimizerExpression::FunctionCall, got {:?}", result);
    }
}

#[test]
fn test_build_initial_plan_select_with_vector_func_in_where() {
    let optimizer = create_test_optimizer();
    let select_ast = SelectStatement {
        distinct: false,
        columns: vec![SelectColumn::Asterisk],
        from_clause: TableReference { name: "my_table".to_string(), alias: None },
        joins: vec![],
        condition: Some(ConditionTree::Comparison(Condition {
            left: AstExpression::FunctionCall {
                name: "DOT_PRODUCT".to_string(),
                args: vec![
                    AstFunctionArg::Expression(AstExpression::ColumnIdentifier("embedding".to_string())),
                    AstFunctionArg::Expression(AstExpression::Literal(AstLiteralValue::Vector(vec![
                        AstLiteralValue::Number("0.1".to_string()),
                        AstLiteralValue::Number("0.2".to_string()),
                    ]))),
                ],
            },
            operator: AstComparisonOperator::GreaterThan,
            right: AstExpression::Literal(AstLiteralValue::Number("0.9".to_string())),
        })),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
    };
    let ast_stmt = Statement::Select(select_ast);
    let plan_result = optimizer.build_initial_plan(&ast_stmt);
    assert!(plan_result.is_ok());

    if let Ok(QueryPlanNode::Project { input, .. }) = plan_result {
        if let QueryPlanNode::Filter { predicate, .. } = *input {
            match predicate {
                OptimizerExpression::CompareOp { left, op, right } => {
                    assert_eq!(op, ">");
                    assert!(matches!(*right, OptimizerExpression::Literal(CommonDataType::Float64(_)))); // 0.9 becomes Float64
                    match *left {
                        OptimizerExpression::FunctionCall { name, args } => {
                            assert_eq!(name, "DOT_PRODUCT");
                            assert_eq!(args.len(), 2);
                            assert!(matches!(args[0], OptimizerExpression::Column(col) if col == "embedding"));
                            assert!(matches!(args[1], OptimizerExpression::Literal(CommonDataType::Vector(Some(2)))));
                        }
                        _ => panic!("Expected FunctionCall on left side of comparison"),
                    }
                }
                _ => panic!("Expected CompareOp as predicate, got {:?}", predicate),
            }
        } else {
            panic!("Expected Filter node under Project");
        }
    } else {
        panic!("Expected Project node at top, got {:?}", plan_result);
    }
}

#[test]
fn test_build_initial_plan_select_with_vector_func_in_projection() {
    let optimizer = create_test_optimizer();
    let select_ast = SelectStatement {
        distinct: false,
        columns: vec![SelectColumn::Expression(AstExpression::FunctionCall {
            name: "COSINE_SIMILARITY".to_string(),
            args: vec![
                AstFunctionArg::Expression(AstExpression::ColumnIdentifier("v1".to_string())),
                AstFunctionArg::Expression(AstExpression::ColumnIdentifier("v2".to_string())),
            ],
        })],
        from_clause: TableReference { name: "vectors".to_string(), alias: None },
        joins: vec![],
        condition: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
    };
    let ast_stmt = Statement::Select(select_ast);
    let plan_result = optimizer.build_initial_plan(&ast_stmt);
    assert!(plan_result.is_ok());

    if let Ok(QueryPlanNode::Project { columns, .. }) = plan_result {
        assert_eq!(columns.len(), 1);
        // Current projection stringification is basic. This test verifies it contains the function call.
        // A more robust test would require Project node to hold Vec<OptimizerExpression>.
        let proj_string = &columns[0];
        assert!(proj_string.starts_with("COSINE_SIMILARITY("));
        assert!(proj_string.contains("Column(\"v1\")")); // Based on debug output of OptimizerExpression::Column
        assert!(proj_string.contains("Column(\"v2\")"));
        assert!(proj_string.ends_with(")"));
    } else {
        panic!("Expected Project node at top, got {:?}", plan_result);
    }
}

#[test]
fn test_ast_expr_to_optimizer_expr_unsupported_function() {
    let optimizer = create_test_optimizer();
    let ast_expr = AstExpression::FunctionCall {
        name: "UNSUPPORTED_FUNC".to_string(),
        args: vec![],
    };
    let result = optimizer.ast_expression_to_optimizer_expression(&ast_expr);
    assert!(matches!(result, Err(OxidbError::NotImplemented { .. })));
    if let Err(OxidbError::NotImplemented { feature }) = result {
        assert!(feature.contains("UNSUPPORTED_FUNC"));
    }
}

#[test]
fn test_ast_expr_to_optimizer_expr_vector_func_wrong_arg_count() {
    let optimizer = create_test_optimizer();
    let ast_expr = AstExpression::FunctionCall {
        name: "DOT_PRODUCT".to_string(),
        args: vec![AstFunctionArg::Expression(AstExpression::ColumnIdentifier("v1".to_string()))], // Only 1 arg
    };
    let result = optimizer.ast_expression_to_optimizer_expression(&ast_expr);
    assert!(matches!(result, Err(OxidbError::SqlParsing(_))));
     if let Err(OxidbError::SqlParsing(msg)) = result {
        assert!(msg.contains("Incorrect number of arguments for DOT_PRODUCT"));
    }
}

#[test]
fn test_ast_expr_to_optimizer_expr_vector_func_non_vector_arg() {
    let optimizer = create_test_optimizer();
    let ast_expr = AstExpression::FunctionCall {
        name: "COSINE_SIMILARITY".to_string(),
        args: vec![
            AstFunctionArg::Expression(AstExpression::ColumnIdentifier("v1".to_string())), // Assume this is vector
            AstFunctionArg::Expression(AstExpression::Literal(AstLiteralValue::Number("123".to_string()))), // Not a vector
        ],
    };
    let result = optimizer.ast_expression_to_optimizer_expression(&ast_expr);
    assert!(matches!(result, Err(OxidbError::SqlParsing(_))));
    if let Err(OxidbError::SqlParsing(msg)) = result {
        assert!(msg.contains("must be a vector column or vector literal"));
        assert!(msg.contains("Literal(Integer(123))"));
    }
}

// Test for COUNT(*) specifically
#[test]
fn test_ast_expr_to_optimizer_expr_count_asterisk() {
    let optimizer = create_test_optimizer();
    let ast_expr = AstExpression::FunctionCall {
        name: "COUNT".to_string(),
        args: vec![AstFunctionArg::Asterisk],
    };
    let result = optimizer.ast_expression_to_optimizer_expression(&ast_expr);
    assert!(result.is_ok());
    if let Ok(OptimizerExpression::FunctionCall { name, args }) = result {
        assert_eq!(name, "COUNT");
        assert!(args.is_empty(), "COUNT(*) should translate to FunctionCall with empty args in this model");
    } else {
        panic!("Expected OptimizerExpression::FunctionCall for COUNT(*), got {:?}", result);
    }
}

// Test for COUNT(column)
#[test]
fn test_ast_expr_to_optimizer_expr_count_column() {
    let optimizer = create_test_optimizer();
    let ast_expr = AstExpression::FunctionCall {
        name: "COUNT".to_string(),
        args: vec![AstFunctionArg::Expression(AstExpression::ColumnIdentifier("my_col".to_string()))],
    };
    let result = optimizer.ast_expression_to_optimizer_expression(&ast_expr);
    assert!(result.is_ok());
    if let Ok(OptimizerExpression::FunctionCall { name, args }) = result {
        assert_eq!(name, "COUNT");
        assert_eq!(args.len(), 1);
        assert!(matches!(args[0], OptimizerExpression::Column(col_name) if col_name == "my_col"));
    } else {
        panic!("Expected OptimizerExpression::FunctionCall for COUNT(column), got {:?}", result);
    }
}

// Test for SUM(column)
#[test]
fn test_ast_expr_to_optimizer_expr_sum_column() {
    let optimizer = create_test_optimizer();
    let ast_expr = AstExpression::FunctionCall {
        name: "SUM".to_string(),
        args: vec![AstFunctionArg::Expression(AstExpression::ColumnIdentifier("amount".to_string()))],
    };
    let result = optimizer.ast_expression_to_optimizer_expression(&ast_expr);
    assert!(result.is_ok());
    if let Ok(OptimizerExpression::FunctionCall { name, args }) = result {
        assert_eq!(name, "SUM");
        assert_eq!(args.len(), 1);
        assert!(matches!(args[0], OptimizerExpression::Column(col_name) if col_name == "amount"));
    } else {
        panic!("Expected OptimizerExpression::FunctionCall for SUM(column), got {:?}", result);
    }
}

// Test for AstSqlLiteralValue::Vector translation
#[test]
fn test_ast_literal_vector_to_optimizer_expression() {
    let optimizer = create_test_optimizer();
    let ast_expr = AstExpression::Literal(AstLiteralValue::Vector(vec![
        AstLiteralValue::Number("1.0".to_string()),
        AstLiteralValue::Number("2.5".to_string()),
        AstLiteralValue::Number("-3.0".to_string()),
    ]));
    let result = optimizer.ast_expression_to_optimizer_expression(&ast_expr);
    assert!(result.is_ok(), "Translation failed: {:?}", result.err());
    match result.unwrap() {
        OptimizerExpression::Literal(CommonDataType::Vector(Some(dim))) => {
            assert_eq!(dim, 3, "Dimension mismatch for translated vector literal");
            // Note: The actual f32 values are not stored in DataType::Vector, only the dimension.
            // The `Value` enum, when constructed from this, would hold the f32s.
            // The optimizer::Expression::Literal only holds the DataType.
        }
        other => panic!("Expected OptimizerExpression::Literal(DataType::Vector(Some(3))), got {:?}", other),
    }
}
