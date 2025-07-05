// src/core/execution/expression_evaluator.rs

use crate::core::common::error::OxidbError;
use crate::core::common::types::{Schema, Value}; // Added Schema
use crate::core::execution::Tuple;
use crate::core::query::binder::expression::BoundExpression;
use crate::core::vector::similarity; // For cosine_similarity, dot_product

#[derive(Debug, thiserror::Error)]
pub enum EvaluationError {
    #[error("Column '{name}' not found in tuple for expression evaluation. Schema may be mismatched or column index is out of bounds.")]
    ColumnNotFoundInTuple { name: String },
    #[error("Function execution not implemented: {name}")]
    FunctionNotImplemented { name: String },
    #[error("Type mismatch during evaluation: expected {expected}, got {found} for {item}")]
    TypeMismatch { expected: String, found: String, item: String },
    #[error("Vector dimension mismatch during evaluation for function '{func_name}': dim1={dim1}, dim2={dim2}")]
    VectorDimensionMismatch { func_name: String, dim1: usize, dim2: usize },
    #[error("Vector magnitude is zero during evaluation for function '{func_name}'")]
    VectorMagnitudeZero { func_name: String },
    #[error("Invalid arguments for function '{func_name}': {reason}")]
    InvalidFunctionArguments { func_name: String, reason: String },
    #[error("An internal error occurred during expression evaluation: {0}")]
    Internal(String),
}

impl From<EvaluationError> for OxidbError {
    fn from(eval_err: EvaluationError) -> Self {
        OxidbError::Execution(eval_err.to_string())
    }
}

/// Evaluates a bound expression against a given tuple and schema.
///
/// # Arguments
///
/// * `expression` - The bound expression to evaluate.
/// * `current_tuple` - The current tuple providing values for column references.
/// * `schema` - The schema corresponding to the `current_tuple`. Used to find column indices by name.
///
/// # Returns
///
/// * `Result<Value, EvaluationError>` - The computed value of the expression or an error.
pub fn evaluate_expression(
    expression: &BoundExpression,
    current_tuple: &Tuple,
    schema: &Schema, // Schema of the input_tuple
) -> Result<Value, EvaluationError> {
    match expression {
        BoundExpression::Literal { value, .. } => Ok(value.clone()),
        BoundExpression::ColumnRef { name, .. } => {
            // Find the index of the column in the schema
            if let Some(index) = schema.get_column_index(name) {
                // Access the value from the tuple using the index
                current_tuple.get(index).cloned().ok_or_else(|| {
                    EvaluationError::ColumnNotFoundInTuple {
                        name: format!("{} (index {} out of bounds for tuple len {})", name, index, current_tuple.len()),
                    }
                })
            } else {
                Err(EvaluationError::ColumnNotFoundInTuple { name: name.clone() })
            }
        }
        BoundExpression::FunctionCall { name, args, .. } => {
            evaluate_function_call(name, args, current_tuple, schema)
        }
    }
}

fn evaluate_function_call(
    name: &str,
    bound_args: &[BoundExpression],
    current_tuple: &Tuple,
    schema: &Schema,
) -> Result<Value, EvaluationError> {
    let mut arg_values: Vec<Value> = Vec::with_capacity(bound_args.len());
    for arg_expr in bound_args {
        arg_values.push(evaluate_expression(arg_expr, current_tuple, schema)?);
    }

    match name.to_uppercase().as_str() {
        "COSINE_SIMILARITY" | "DOT_PRODUCT" => {
            if arg_values.len() != 2 {
                return Err(EvaluationError::InvalidFunctionArguments {
                    func_name: name.to_string(),
                    reason: format!("Expected 2 arguments, got {}", arg_values.len()),
                });
            }

            let v1 = match &arg_values[0] {
                Value::Vector(vec_val) => vec_val,
                other => return Err(EvaluationError::TypeMismatch {
                    expected: "Vector".to_string(),
                    found: format!("{:?}", other.get_type()),
                    item: format!("Argument 1 for function {}", name),
                }),
            };

            let v2 = match &arg_values[1] {
                Value::Vector(vec_val) => vec_val,
                other => return Err(EvaluationError::TypeMismatch {
                    expected: "Vector".to_string(),
                    found: format!("{:?}", other.get_type()),
                    item: format!("Argument 2 for function {}", name),
                }),
            };

            if name.to_uppercase().as_str() == "COSINE_SIMILARITY" {
                match similarity::cosine_similarity(v1, v2) {
                    Ok(val) => Ok(Value::Float64(val as f64)), // Similarity functions return f32
                    Err(OxidbError::VectorDimensionMismatch { dim1, dim2 }) => {
                        Err(EvaluationError::VectorDimensionMismatch { func_name: name.to_string(), dim1, dim2 })
                    }
                    Err(OxidbError::VectorMagnitudeZero) => {
                        Err(EvaluationError::VectorMagnitudeZero { func_name: name.to_string() })
                    }
                    Err(e) => Err(EvaluationError::Internal(format!("Error in cosine_similarity: {}", e))),
                }
            } else { // DOT_PRODUCT
                match similarity::dot_product(v1, v2) {
                    Ok(val) => Ok(Value::Float64(val as f64)), // Similarity functions return f32
                    Err(OxidbError::VectorDimensionMismatch { dim1, dim2 }) => {
                        Err(EvaluationError::VectorDimensionMismatch { func_name: name.to_string(), dim1, dim2 })
                    }
                    Err(e) => Err(EvaluationError::Internal(format!("Error in dot_product: {}", e))),
                }
            }
        }
        _ => Err(EvaluationError::FunctionNotImplemented { name: name.to_string() }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::common::types::{ColumnDef, DataType, Value};
    use crate::core::query::binder::expression::BoundExpression; // Corrected import

    fn get_test_schema() -> Schema {
        Schema {
            columns: vec![
                ColumnDef { name: "id".to_string(), data_type: DataType::Integer },
                ColumnDef { name: "name".to_string(), data_type: DataType::Text },
                ColumnDef { name: "vec1".to_string(), data_type: DataType::Vector(Some(3)) },
                ColumnDef { name: "vec2".to_string(), data_type: DataType::Vector(Some(3)) },
                ColumnDef { name: "vec_other_dim".to_string(), data_type: DataType::Vector(Some(4)) },
            ],
        }
    }

    #[test]
    fn test_evaluate_literal() {
        let schema = get_test_schema();
        let tuple = vec![Value::Integer(1)]; // Dummy tuple, not used for literal

        let expr = BoundExpression::Literal {
            value: Value::Integer(123),
            return_type: DataType::Integer,
        };
        assert_eq!(evaluate_expression(&expr, &tuple, &schema).unwrap(), Value::Integer(123));

        let expr_text = BoundExpression::Literal {
            value: Value::Text("hello".to_string()),
            return_type: DataType::Text,
        };
        assert_eq!(evaluate_expression(&expr_text, &tuple, &schema).unwrap(), Value::Text("hello".to_string()));

        let vec_val = vec![1.0f32, 2.0f32];
        let expr_vec = BoundExpression::Literal {
            value: Value::Vector(vec_val.clone()),
            return_type: DataType::Vector(Some(2)),
        };
        assert_eq!(evaluate_expression(&expr_vec, &tuple, &schema).unwrap(), Value::Vector(vec_val));
    }

    #[test]
    fn test_evaluate_column_ref() {
        let schema = get_test_schema();
        let tuple = vec![
            Value::Integer(1),
            Value::Text("Alice".to_string()),
            Value::Vector(vec![1.0, 2.0, 3.0]),
            Value::Vector(vec![4.0, 5.0, 6.0]),
            Value::Vector(vec![7.0, 8.0, 9.0, 10.0]),
        ];

        let expr_id = BoundExpression::ColumnRef {
            name: "id".to_string(),
            return_type: DataType::Integer,
        };
        assert_eq!(evaluate_expression(&expr_id, &tuple, &schema).unwrap(), Value::Integer(1));

        let expr_name = BoundExpression::ColumnRef {
            name: "name".to_string(),
            return_type: DataType::Text,
        };
        assert_eq!(evaluate_expression(&expr_name, &tuple, &schema).unwrap(), Value::Text("Alice".to_string()));

        let expr_vec1 = BoundExpression::ColumnRef {
            name: "vec1".to_string(),
            return_type: DataType::Vector(Some(3)),
        };
        assert_eq!(evaluate_expression(&expr_vec1, &tuple, &schema).unwrap(), Value::Vector(vec![1.0, 2.0, 3.0]));
    }

    #[test]
    fn test_evaluate_column_ref_not_found_in_schema() {
        let schema = get_test_schema();
        let tuple = vec![Value::Integer(1)];
        let expr = BoundExpression::ColumnRef {
            name: "nonexistent".to_string(),
            return_type: DataType::Null, // Type doesn't matter much here
        };
        match evaluate_expression(&expr, &tuple, &schema) {
            Err(EvaluationError::ColumnNotFoundInTuple { name }) => assert_eq!(name, "nonexistent"),
            other => panic!("Expected ColumnNotFoundInTuple, got {:?}", other),
        }
    }

     #[test]
    fn test_evaluate_column_ref_out_of_bounds_in_tuple() {
        // Schema has 'id', but tuple is empty
        let schema = Schema { columns: vec![ColumnDef { name: "id".to_string(), data_type: DataType::Integer }]};
        let tuple = vec![];
        let expr = BoundExpression::ColumnRef {
            name: "id".to_string(),
            return_type: DataType::Integer,
        };
        match evaluate_expression(&expr, &tuple, &schema) {
            Err(EvaluationError::ColumnNotFoundInTuple { name }) => {
                assert!(name.contains("id (index 0 out of bounds for tuple len 0)"));
            }
            other => panic!("Expected ColumnNotFoundInTuple due to bounds, got {:?}", other),
        }
    }

    #[test]
    fn test_evaluate_cosine_similarity_with_column_refs() {
        let schema = get_test_schema();
        let tuple = vec![
            Value::Integer(1), // id
            Value::Text("dummy".to_string()), // name
            Value::Vector(vec![1.0, 0.0, 0.0]), // vec1 (orthogonal to vec2 if vec2 is [0,1,0])
            Value::Vector(vec![0.0, 1.0, 0.0]), // vec2
            Value::Vector(vec![1.0,2.0,3.0,4.0]), // vec_other_dim
        ];

        let func_expr = BoundExpression::FunctionCall {
            name: "COSINE_SIMILARITY".to_string(),
            args: vec![
                BoundExpression::ColumnRef { name: "vec1".to_string(), return_type: DataType::Vector(Some(3)) },
                BoundExpression::ColumnRef { name: "vec2".to_string(), return_type: DataType::Vector(Some(3)) },
            ],
            return_type: DataType::Float64,
        };

        let result = evaluate_expression(&func_expr, &tuple, &schema).unwrap();
        match result {
            Value::Float64(val) => assert!((val - 0.0).abs() < 1e-6, "Expected 0.0, got {}", val),
            _ => panic!("Expected Float64 result"),
        }
    }

    #[test]
    fn test_evaluate_dot_product_with_literals() {
        let schema = get_test_schema(); // Not strictly needed for literal-only test
        let tuple = vec![]; // Empty tuple

        let func_expr = BoundExpression::FunctionCall {
            name: "DOT_PRODUCT".to_string(),
            args: vec![
                BoundExpression::Literal { value: Value::Vector(vec![1.0, 2.0, 3.0]), return_type: DataType::Vector(Some(3)) },
                BoundExpression::Literal { value: Value::Vector(vec![4.0, 5.0, 6.0]), return_type: DataType::Vector(Some(3)) },
            ],
            return_type: DataType::Float64,
        };
        // 1*4 + 2*5 + 3*6 = 4 + 10 + 18 = 32
        let result = evaluate_expression(&func_expr, &tuple, &schema).unwrap();
        match result {
            Value::Float64(val) => assert!((val - 32.0).abs() < 1e-6, "Expected 32.0, got {}", val),
            _ => panic!("Expected Float64 result"),
        }
    }

    #[test]
    fn test_evaluate_cosine_similarity_col_literal() {
        let schema = get_test_schema();
        let tuple = vec![
            Value::Integer(1), Value::Text("dummy".to_string()),
            Value::Vector(vec![1.0, 2.0, 3.0]), // vec1
            Value::Vector(vec![0.0, 0.0, 0.0]), Value::Vector(vec![0.0,0.0,0.0,0.0]) // Unused
        ];

        let func_expr = BoundExpression::FunctionCall {
            name: "COSINE_SIMILARITY".to_string(),
            args: vec![
                BoundExpression::ColumnRef { name: "vec1".to_string(), return_type: DataType::Vector(Some(3)) },
                BoundExpression::Literal { value: Value::Vector(vec![2.0, 4.0, 6.0]), return_type: DataType::Vector(Some(3)) },
            ],
            return_type: DataType::Float64,
        };
        // vec1 and literal are collinear, similarity should be 1.0
        let result = evaluate_expression(&func_expr, &tuple, &schema).unwrap();
         match result {
            Value::Float64(val) => assert!((val - 1.0).abs() < 1e-6, "Expected 1.0, got {}", val),
            _ => panic!("Expected Float64 result"),
        }
    }


    #[test]
    fn test_evaluate_function_dimension_mismatch() {
        let schema = get_test_schema();
        let tuple = vec![
            Value::Integer(1), Value::Text("dummy".to_string()),
            Value::Vector(vec![1.0, 0.0, 0.0]),     // vec1 (dim 3)
            Value::Vector(vec![0.0, 1.0, 0.0, 0.0]), // Incorrectly bound as vec2 for test, but content is dim 4 for vec_other_dim
            Value::Vector(vec![1.0,2.0,3.0,4.0]), // vec_other_dim (dim 4)
        ];

        let func_expr = BoundExpression::FunctionCall {
            name: "COSINE_SIMILARITY".to_string(),
            args: vec![
                BoundExpression::ColumnRef { name: "vec1".to_string(), return_type: DataType::Vector(Some(3)) },
                BoundExpression::ColumnRef { name: "vec_other_dim".to_string(), return_type: DataType::Vector(Some(4)) },
            ],
            return_type: DataType::Float64,
        };

        match evaluate_expression(&func_expr, &tuple, &schema) {
            Err(EvaluationError::VectorDimensionMismatch { func_name, dim1, dim2 }) => {
                assert_eq!(func_name, "COSINE_SIMILARITY");
                assert_eq!(dim1, 3);
                assert_eq!(dim2, 4);
            }
            other => panic!("Expected VectorDimensionMismatch, got {:?}", other),
        }
    }

    #[test]
    fn test_evaluate_function_magnitude_zero() {
        let schema = get_test_schema();
         let tuple = vec![
            Value::Integer(1), Value::Text("dummy".to_string()),
            Value::Vector(vec![0.0, 0.0, 0.0]),     // vec1 (zero magnitude)
            Value::Vector(vec![1.0, 2.0, 3.0]),     // vec2
            Value::Vector(vec![0.0,0.0,0.0,0.0]), // Unused
        ];


        let func_expr = BoundExpression::FunctionCall {
            name: "COSINE_SIMILARITY".to_string(),
            args: vec![
                BoundExpression::ColumnRef { name: "vec1".to_string(), return_type: DataType::Vector(Some(3)) },
                BoundExpression::ColumnRef { name: "vec2".to_string(), return_type: DataType::Vector(Some(3)) },
            ],
            return_type: DataType::Float64,
        };

        match evaluate_expression(&func_expr, &tuple, &schema) {
            Err(EvaluationError::VectorMagnitudeZero { func_name }) => {
                assert_eq!(func_name, "COSINE_SIMILARITY");
            }
            other => panic!("Expected VectorMagnitudeZero, got {:?}", other),
        }
    }

    #[test]
    fn test_evaluate_function_wrong_arg_type() {
        let schema = get_test_schema();
        let tuple = vec![ Value::Integer(1), Value::Text("not_a_vector".to_string()), Value::Vector(vec![]), Value::Vector(vec![]), Value::Vector(vec![])]; // name is Text

        let func_expr = BoundExpression::FunctionCall {
            name: "DOT_PRODUCT".to_string(),
            args: vec![
                BoundExpression::ColumnRef { name: "name".to_string(), return_type: DataType::Text }, // Passing Text
                BoundExpression::Literal { value: Value::Vector(vec![1.0, 2.0]), return_type: DataType::Vector(Some(2)) },
            ],
            return_type: DataType::Float64,
        };

        match evaluate_expression(&func_expr, &tuple, &schema) {
            Err(EvaluationError::TypeMismatch { expected, found, item }) => {
                assert_eq!(expected, "Vector");
                assert_eq!(found, "Text");
                assert_eq!(item, "Argument 1 for function DOT_PRODUCT");
            }
            other => panic!("Expected TypeMismatch, got {:?}", other),
        }
    }

    #[test]
    fn test_evaluate_unknown_function() {
        let schema = get_test_schema();
        let tuple = vec![];
        let func_expr = BoundExpression::FunctionCall {
            name: "UNKNOWN_FUNC".to_string(),
            args: vec![],
            return_type: DataType::Null,
        };
        match evaluate_expression(&func_expr, &tuple, &schema) {
            Err(EvaluationError::FunctionNotImplemented { name }) => {
                assert_eq!(name, "UNKNOWN_FUNC");
            }
            other => panic!("Expected FunctionNotImplemented, got {:?}", other),
        }
    }
}
