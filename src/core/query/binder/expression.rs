// src/core/query/binder/expression.rs

use super::binder::{Binder, BindError};
use crate::core::common::types::{DataType, Value};
use crate::core::query::sql::ast::{AstExpression, AstFunctionArg, AstLiteralValue};

#[derive(Debug, PartialEq, Clone)]
pub enum BoundExpression {
    Literal {
        value: Value,
        return_type: DataType,
    },
    ColumnRef {
        name: String,
        return_type: DataType,
    },
    FunctionCall {
        name: String,
        args: Vec<BoundExpression>,
        return_type: DataType,
    },
}

impl BoundExpression {
    pub fn get_type(&self) -> DataType {
        match self {
            BoundExpression::Literal { return_type, .. } => return_type.clone(),
            BoundExpression::ColumnRef { return_type, .. } => return_type.clone(),
            BoundExpression::FunctionCall { return_type, .. } => return_type.clone(),
        }
    }
}

pub fn bind_expression_entry(binder: &mut Binder, expr: &AstExpression) -> Result<BoundExpression, BindError> {
    match expr {
        AstExpression::Literal(literal_val) => bind_literal(literal_val),
        AstExpression::ColumnIdentifier(name) => bind_column_ref(binder, name),
        AstExpression::FunctionCall { name, args } => bind_function_call(binder, name, args),
        AstExpression::BinaryOp { .. } => Err(BindError::ExpressionNotImplemented {
            expression_type: "BinaryOp".to_string(),
        }),
        AstExpression::UnaryOp { .. } => Err(BindError::ExpressionNotImplemented {
            expression_type: "UnaryOp".to_string(),
        }),
    }
}

fn bind_literal(literal_ast: &AstLiteralValue) -> Result<BoundExpression, BindError> {
    match literal_ast {
        AstLiteralValue::Number(s) => {
            if s.contains('.') {
                s.parse::<f64>()
                    .map_err(|e| BindError::InvalidLiteral(format!("Invalid float literal '{}': {}", s, e)))
                    .map(|f| BoundExpression::Literal {
                        value: Value::Float64(f), // Corrected Value variant
                        return_type: DataType::Float64, // Corrected DataType variant
                    })
            } else {
                s.parse::<i64>()
                    .map_err(|e| BindError::InvalidLiteral(format!("Invalid integer literal '{}': {}", s, e)))
                    .map(|i| BoundExpression::Literal {
                        value: Value::Integer(i),
                        return_type: DataType::Integer,
                    })
            }
        }
        AstLiteralValue::String(s) => Ok(BoundExpression::Literal {
            value: Value::Text(s.clone()),
            return_type: DataType::Text,
        }),
        AstLiteralValue::Boolean(b) => Ok(BoundExpression::Literal {
            value: Value::Boolean(*b),
            return_type: DataType::Boolean,
        }),
        AstLiteralValue::Null => Ok(BoundExpression::Literal {
            value: Value::Null,
            return_type: DataType::Null,
        }),
        AstLiteralValue::Vector(elements) => {
            let mut bound_elements = Vec::with_capacity(elements.len());
            let inferred_dim = elements.len(); // Dimension is just the number of elements

            for (i, el_ast) in elements.iter().enumerate() {
                match el_ast {
                    AstLiteralValue::Number(n_str) => {
                        match n_str.parse::<f32>() {
                            Ok(f_val) => bound_elements.push(f_val),
                            Err(_) => return Err(BindError::InvalidLiteral(format!(
                                "Invalid f32 literal '{}' for vector element {} in vector literal",
                                n_str, i
                            ))),
                        }
                    }
                    _ => return Err(BindError::InvalidLiteral(format!(
                        "Vector literals can only contain numeric (f32 compatible) values. Found {:?} at index {}.",
                        el_ast, i
                    ))),
                }
            }
            Ok(BoundExpression::Literal {
                value: Value::Vector(bound_elements),
                return_type: DataType::Vector(Some(inferred_dim)), // Corrected syntax and type for dimension
            })
        }
    }
}

fn bind_column_ref(binder: &Binder, name: &str) -> Result<BoundExpression, BindError> {
    if let Some(schema) = binder.get_schema() {
        // Correctly use get_column_index and then access columns vector
        if let Some(idx) = schema.get_column_index(name) {
            if let Some(col_def) = schema.columns.get(idx) {
                Ok(BoundExpression::ColumnRef {
                    name: name.to_string(),
                    return_type: col_def.data_type.clone(),
                })
            } else {
                // This case should ideally not happen if get_column_index returns a valid index
                Err(BindError::ColumnNotFound { name: format!("{} (index out of bounds after schema lookup)", name) })
            }
        } else {
            Err(BindError::ColumnNotFound { name: name.to_string() })
        }
    } else {
        Err(BindError::ColumnNotFound { name: format!("{} (no schema context)", name) })
    }
}

fn bind_function_call(
    binder: &mut Binder,
    name: &str,
    ast_args: &[AstFunctionArg],
) -> Result<BoundExpression, BindError> {
    let upper_name = name.to_uppercase();
    let mut bound_args = Vec::new();

    for ast_arg in ast_args {
        match ast_arg {
            AstFunctionArg::Expression(expr) => {
                bound_args.push(binder.bind_expression(expr)?);
            }
            AstFunctionArg::Asterisk => {
                return Err(BindError::ExpressionNotImplemented {
                    expression_type: "Asterisk argument for non-COUNT function".to_string(),
                });
            }
            AstFunctionArg::Distinct(_expr) => { // Marked _expr as unused
                return Err(BindError::ExpressionNotImplemented {
                    expression_type: format!("DISTINCT argument for function {}", name),
                });
            }
        }
    }

    match upper_name.as_str() {
        "COSINE_SIMILARITY" | "DOT_PRODUCT" => {
            if bound_args.len() != 2 {
                return Err(BindError::IncorrectArgumentCount {
                    name: upper_name,
                    expected: 2,
                    got: bound_args.len(),
                });
            }

            let arg1_type = bound_args[0].get_type();
            let arg2_type = bound_args[1].get_type();

            let dim1_opt = match arg1_type {
                DataType::Vector(d) => d,
                _ => return Err(BindError::TypeMismatch {
                    name: upper_name.clone(),
                    arg_index: 0,
                    expected_type: "Vector".to_string(),
                    actual_type: format!("{:?}", arg1_type),
                }),
            };

            let dim2_opt = match arg2_type {
                DataType::Vector(d) => d,
                _ => return Err(BindError::TypeMismatch {
                    name: upper_name.clone(),
                    arg_index: 1,
                    expected_type: "Vector".to_string(),
                    actual_type: format!("{:?}", arg2_type),
                }),
            };

            // Dimension check
            match (dim1_opt, dim2_opt) {
                (Some(d1), Some(d2)) => {
                    if d1 != d2 {
                        return Err(BindError::VectorDimensionMismatch {
                            name: upper_name,
                            dim1: d1,
                            dim2: d2,
                        });
                    }
                }
                (None, Some(_)) => return Err(BindError::UnknownVectorDimension { name: upper_name, arg_index: 0 }),
                (Some(_), None) => return Err(BindError::UnknownVectorDimension { name: upper_name, arg_index: 1 }),
                (None, None) => {
                    // Both dimensions unknown. Allow for now, executor must handle.
                }
            }

            Ok(BoundExpression::FunctionCall {
                name: upper_name,
                args: bound_args,
                return_type: DataType::Float64, // Corrected return type
            })
        }
        "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" => {
            let return_type = match upper_name.as_str() {
                "COUNT" => DataType::Integer,
                "SUM" | "AVG" => bound_args.get(0).map_or(DataType::Null, |arg| arg.get_type()),
                "MIN" | "MAX" => bound_args.get(0).map_or(DataType::Null, |arg| arg.get_type()),
                _ => DataType::Null, // Should be unreachable given outer match
            };
            Ok(BoundExpression::FunctionCall {
                name: upper_name,
                args: bound_args,
                return_type,
            })
        }
        _ => Err(BindError::FunctionNotFound { name: upper_name }),
    }
}
