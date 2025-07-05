// src/core/query/binder/binder.rs
use super::expression::{bind_expression_entry, BoundExpression};
use crate::core::common::types::{ColumnDef, DataType, Schema}; // Added DataType back
use crate::core::query::sql::ast::{AstExpression, Statement as AstStatement};
use thiserror::Error;

#[derive(Error, Debug, PartialEq, Clone)]
pub enum BindError {
    #[error("Binding not yet implemented for statement: {statement_type}")]
    NotImplemented { statement_type: String },
    #[error("Binding not yet implemented for expression: {expression_type}")]
    ExpressionNotImplemented { expression_type: String },
    #[error("Function '{name}' not found")]
    FunctionNotFound { name: String },
    #[error("Incorrect number of arguments for function '{name}': expected {expected}, got {got}")]
    IncorrectArgumentCount { name: String, expected: usize, got: usize },
    #[error("Type mismatch for argument {arg_index} of function '{name}': expected {expected_type}, got {actual_type}")]
    TypeMismatch {
        name: String,
        arg_index: usize,
        expected_type: String,
        actual_type: String,
    },
    #[error("Column '{name}' not found in schema")]
    ColumnNotFound { name: String },
    #[error("Invalid literal value: {0}")]
    InvalidLiteral(String),
    #[error("Vector dimension mismatch for function '{name}': dim1={dim1}, dim2={dim2}")]
    VectorDimensionMismatch { name: String, dim1: usize, dim2: usize },
    #[error("Unsupported literal type in expression: {literal_type:?}")]
    UnsupportedLiteralInExpression { literal_type: String },
    #[error("Argument {arg_index} of function {name} has unknown vector dimension")]
    UnknownVectorDimension { name: String, arg_index: usize },
}

#[derive(Debug, PartialEq, Clone)]
pub struct BoundStatement {
    pub message: String,
}

#[derive(Debug)]
pub struct Binder<'a> {
    schema: Option<&'a Schema>,
}

impl<'a> Binder<'a> {
    pub fn new(schema: Option<&'a Schema>) -> Self {
        Binder { schema }
    }

    pub fn bind_expression(&mut self, expr: &AstExpression) -> Result<BoundExpression, BindError> {
        bind_expression_entry(self, expr)
    }

    pub fn bind_statement(&mut self, statement: &AstStatement) -> Result<BoundStatement, BindError> {
        let stmt_type = match statement {
            AstStatement::Select(_) => "Select",
            AstStatement::Update(_) => "Update",
            AstStatement::CreateTable(_) => "CreateTable",
            AstStatement::Insert(_) => "Insert",
            AstStatement::Delete(_) => "Delete",
            AstStatement::DropTable(_) => "DropTable",
        };
        eprintln!("[Binder] Attempting to bind statement: {:?}", stmt_type);
        Err(BindError::NotImplemented { statement_type: stmt_type.to_string() })
    }

    pub fn get_schema(&self) -> Option<&Schema> {
        self.schema
    }
}

impl Default for Binder<'_> {
    fn default() -> Self {
        Self::new(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // Removed unused Value import
    use crate::core::query::sql::ast::{AstFunctionArg, AstLiteralValue};

    fn get_test_schema() -> Schema {
        Schema { // Construct Schema directly
            columns: vec![
                ColumnDef { name: "col_int".to_string(), data_type: DataType::Integer },
                ColumnDef { name: "col_text".to_string(), data_type: DataType::Text },
                ColumnDef { name: "col_vec1".to_string(), data_type: DataType::Vector(Some(3)) },
                ColumnDef { name: "col_vec2".to_string(), data_type: DataType::Vector(Some(3)) },
                ColumnDef { name: "col_vec_dim_mismatch".to_string(), data_type: DataType::Vector(Some(4)) },
                ColumnDef { name: "col_vec_unk_dim".to_string(), data_type: DataType::Vector(None) },
            ],
        }
    }

    #[test]
    fn test_bind_cosine_similarity_success() {
        let schema = get_test_schema();
        let mut binder = Binder::new(Some(&schema));
        let expr = AstExpression::FunctionCall {
            name: "COSINE_SIMILARITY".to_string(),
            args: vec![
                AstFunctionArg::Expression(AstExpression::ColumnIdentifier("col_vec1".to_string())),
                AstFunctionArg::Expression(AstExpression::ColumnIdentifier("col_vec2".to_string())),
            ],
        };
        let bound_expr = binder.bind_expression(&expr).unwrap();
        match bound_expr {
            BoundExpression::FunctionCall { name, args, return_type } => {
                assert_eq!(name.to_uppercase(), "COSINE_SIMILARITY");
                assert_eq!(args.len(), 2);
                assert_eq!(args[0].get_type(), DataType::Vector(Some(3))); // Corrected syntax
                assert_eq!(args[1].get_type(), DataType::Vector(Some(3))); // Corrected syntax
                assert_eq!(return_type, DataType::Float64); // Corrected type
            }
            _ => panic!("Expected BoundExpression::FunctionCall"),
        }
    }

    #[test]
    fn test_bind_dot_product_success_with_literal() {
        let schema = get_test_schema();
        let mut binder = Binder::new(Some(&schema));
        let expr = AstExpression::FunctionCall {
            name: "DOT_PRODUCT".to_string(),
            args: vec![
                AstFunctionArg::Expression(AstExpression::Literal(AstLiteralValue::Vector(vec![
                    AstLiteralValue::Number("1.0".to_string()),
                    AstLiteralValue::Number("2.0".to_string()),
                    AstLiteralValue::Number("3.0".to_string()),
                ]))),
                AstFunctionArg::Expression(AstExpression::ColumnIdentifier("col_vec1".to_string())),
            ],
        };
        let bound_expr = binder.bind_expression(&expr).unwrap();
        match bound_expr {
            BoundExpression::FunctionCall { name, args, return_type } => {
                assert_eq!(name.to_uppercase(), "DOT_PRODUCT");
                assert_eq!(args.len(), 2);
                assert_eq!(args[0].get_type(), DataType::Vector(Some(3))); // Literal vector's type
                assert_eq!(args[1].get_type(), DataType::Vector(Some(3))); // Corrected syntax
                assert_eq!(return_type, DataType::Float64); // Corrected type
            }
            _ => panic!("Expected BoundExpression::FunctionCall, got {:?}", bound_expr),
        }
    }

    #[test]
    fn test_bind_vector_func_incorrect_arg_count() {
        let schema = get_test_schema();
        let mut binder = Binder::new(Some(&schema));
        let expr = AstExpression::FunctionCall {
            name: "COSINE_SIMILARITY".to_string(),
            args: vec![AstFunctionArg::Expression(AstExpression::ColumnIdentifier("col_vec1".to_string()))],
        };
        let result = binder.bind_expression(&expr);
        assert_eq!(
            result.unwrap_err(),
            BindError::IncorrectArgumentCount { name: "COSINE_SIMILARITY".to_string(), expected: 2, got: 1 }
        );
    }

    #[test]
    fn test_bind_vector_func_type_mismatch() {
        let schema = get_test_schema();
        let mut binder = Binder::new(Some(&schema));
        let expr = AstExpression::FunctionCall {
            name: "DOT_PRODUCT".to_string(),
            args: vec![
                AstFunctionArg::Expression(AstExpression::ColumnIdentifier("col_vec1".to_string())),
                AstFunctionArg::Expression(AstExpression::ColumnIdentifier("col_int".to_string())),
            ],
        };
        let result = binder.bind_expression(&expr);
        assert_eq!(
            result.unwrap_err(),
            BindError::TypeMismatch {
                name: "DOT_PRODUCT".to_string(),
                arg_index: 1,
                expected_type: "Vector".to_string(),
                actual_type: "Integer".to_string(),
            }
        );
    }

    #[test]
    fn test_bind_vector_func_dimension_mismatch_columns() {
        let schema = get_test_schema();
        let mut binder = Binder::new(Some(&schema));
        let expr = AstExpression::FunctionCall {
            name: "COSINE_SIMILARITY".to_string(),
            args: vec![
                AstFunctionArg::Expression(AstExpression::ColumnIdentifier("col_vec1".to_string())),
                AstFunctionArg::Expression(AstExpression::ColumnIdentifier("col_vec_dim_mismatch".to_string())),
            ],
        };
        let result = binder.bind_expression(&expr);
        assert_eq!(
            result.unwrap_err(),
            BindError::VectorDimensionMismatch {
                name: "COSINE_SIMILARITY".to_string(),
                dim1: 3,
                dim2: 4,
            }
        );
    }

    #[test]
    fn test_bind_vector_func_dimension_mismatch_literal_column() {
        let schema = get_test_schema();
        let mut binder = Binder::new(Some(&schema));
        let expr = AstExpression::FunctionCall {
            name: "DOT_PRODUCT".to_string(),
            args: vec![
                AstFunctionArg::Expression(AstExpression::Literal(AstLiteralValue::Vector(vec![
                    AstLiteralValue::Number("1.0".to_string()),
                    AstLiteralValue::Number("2.0".to_string()),
                ]))), // Literal vector of dim 2
                AstFunctionArg::Expression(AstExpression::ColumnIdentifier("col_vec1".to_string())), // dim 3
            ],
        };
        let result = binder.bind_expression(&expr);
        assert_eq!(
            result.unwrap_err(),
            BindError::VectorDimensionMismatch {
                name: "DOT_PRODUCT".to_string(),
                dim1: 2,
                dim2: 3,
            }
        );
    }

    #[test]
    fn test_bind_vector_func_unknown_dimension_arg1() {
        let schema = get_test_schema();
        let mut binder = Binder::new(Some(&schema));
        let expr = AstExpression::FunctionCall {
            name: "COSINE_SIMILARITY".to_string(),
            args: vec![
                AstFunctionArg::Expression(AstExpression::ColumnIdentifier("col_vec_unk_dim".to_string())),
                AstFunctionArg::Expression(AstExpression::ColumnIdentifier("col_vec1".to_string())),
            ],
        };
        let result = binder.bind_expression(&expr);
        assert_eq!(
            result.unwrap_err(),
            BindError::UnknownVectorDimension {
                name: "COSINE_SIMILARITY".to_string(),
                arg_index: 0
            }
        );
    }

    #[test]
    fn test_bind_vector_func_unknown_dimension_arg2() {
        let schema = get_test_schema();
        let mut binder = Binder::new(Some(&schema));
        let expr = AstExpression::FunctionCall {
            name: "DOT_PRODUCT".to_string(),
            args: vec![
                AstFunctionArg::Expression(AstExpression::ColumnIdentifier("col_vec1".to_string())),
                AstFunctionArg::Expression(AstExpression::ColumnIdentifier("col_vec_unk_dim".to_string())),
            ],
        };
        let result = binder.bind_expression(&expr);
         assert_eq!(
            result.unwrap_err(),
            BindError::UnknownVectorDimension {
                name: "DOT_PRODUCT".to_string(),
                arg_index: 1
            }
        );
    }

    #[test]
    fn test_bind_vector_func_unknown_dimension_both_args_ok() {
        // If both have unknown dimensions, we might allow it at bind time,
        // assuming the executor will handle it (or error if they are incompatible at runtime).
        let schema = get_test_schema();
        let mut binder = Binder::new(Some(&schema));
        let expr = AstExpression::FunctionCall {
            name: "COSINE_SIMILARITY".to_string(),
            args: vec![
                AstFunctionArg::Expression(AstExpression::ColumnIdentifier("col_vec_unk_dim".to_string())),
                AstFunctionArg::Expression(AstExpression::ColumnIdentifier("col_vec_unk_dim".to_string())),
            ],
        };
        let bound_expr = binder.bind_expression(&expr).unwrap();
         match bound_expr {
            BoundExpression::FunctionCall { name, args, return_type } => {
                assert_eq!(name.to_uppercase(), "COSINE_SIMILARITY");
                assert_eq!(args.len(), 2);
                assert_eq!(args[0].get_type(), DataType::Vector(None));
                assert_eq!(args[1].get_type(), DataType::Vector(None));
                assert_eq!(return_type, DataType::Float64);
            }
            _ => panic!("Expected BoundExpression::FunctionCall"),
        }
    }


    #[test]
    fn test_bind_unknown_function() {
        let schema = get_test_schema();
        let mut binder = Binder::new(Some(&schema));
        let expr = AstExpression::FunctionCall {
            name: "UNKNOWN_FUNCTION".to_string(),
            args: vec![],
        };
        let result = binder.bind_expression(&expr);
        assert_eq!(result.unwrap_err(), BindError::FunctionNotFound { name: "UNKNOWN_FUNCTION".to_string() });
    }
}
