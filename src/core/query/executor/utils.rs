use crate::core::common::error::DbError;
use crate::core::types::DataType;
use crate::core::query::sql::ast::AstLiteralValue; // Ensure this path is correct

// This function was already here from previous refactoring, ensure it's pub
pub fn compare_data_types(val1: &DataType, val2: &DataType, operator: &str) -> Result<bool, DbError> {
    match operator {
        "=" => Ok(val1 == val2),
        "!=" => Ok(val1 != val2),
        "<" | "<=" | ">" | ">=" => {
            match (val1, val2) {
                (DataType::Integer(i1), DataType::Integer(i2)) => match operator {
                    "<" => Ok(i1 < i2),
                    "<=" => Ok(i1 <= i2),
                    ">" => Ok(i1 > i2),
                    ">=" => Ok(i1 >= i2),
                    _ => unreachable!(),
                },
                (DataType::Float(f1), DataType::Float(f2)) => match operator {
                    "<" => Ok(f1 < f2),
                    "<=" => Ok(f1 <= f2),
                    ">" => Ok(f1 > f2),
                    ">=" => Ok(f1 >= f2),
                    _ => unreachable!(),
                },
                (DataType::Integer(i1), DataType::Float(f2)) => {
                    let f1 = *i1 as f64;
                    match operator {
                        "<" => Ok(f1 < *f2),
                        "<=" => Ok(f1 <= *f2),
                        ">" => Ok(f1 > *f2),
                        ">=" => Ok(f1 >= *f2),
                        _ => unreachable!(),
                    }
                }
                (DataType::Float(f1), DataType::Integer(i2)) => {
                    let f2 = *i2 as f64;
                    match operator {
                        "<" => Ok(*f1 < f2),
                        "<=" => Ok(*f1 <= f2),
                        ">" => Ok(*f1 > f2),
                        ">=" => Ok(*f1 >= f2),
                        _ => unreachable!(),
                    }
                }
                (DataType::String(s1), DataType::String(s2)) => match operator {
                    "<" => Ok(s1 < s2),
                    "<=" => Ok(s1 <= s2),
                    ">" => Ok(s1 > s2),
                    ">=" => Ok(s1 >= s2),
                    _ => unreachable!(),
                },
                (DataType::Null, _) | (_, DataType::Null) => Err(DbError::InvalidQuery(format!(
                    "Ordered comparison ('{}') with NULL is not supported directly. Use IS NULL or IS NOT NULL.",
                    operator
                ))),
                _ => Err(DbError::TypeError(format!(
                    "Cannot apply ordered operator '{}' between {:?} and {:?}",
                    operator, val1, val2
                ))),
            }
        }
        _ => Err(DbError::InvalidQuery(format!("Unsupported operator: {}", operator))),
    }
}

// New helper function as planned
pub fn datatype_to_ast_literal(data_type: &DataType) -> Result<AstLiteralValue, DbError> {
    match data_type {
        DataType::Integer(i) => Ok(AstLiteralValue::Number(i.to_string())),
        DataType::String(s) => Ok(AstLiteralValue::String(s.clone())),
        DataType::Boolean(b) => Ok(AstLiteralValue::Boolean(*b)),
        DataType::Float(f) => Ok(AstLiteralValue::Number(f.to_string())), // Consider precision if needed
        DataType::Null => Ok(AstLiteralValue::Null),
        DataType::Map(_) => Err(DbError::NotImplemented(
            "Cannot convert Map DataType to AstLiteralValue for SQL conditions".to_string(),
        )),
        DataType::JsonBlob(json_val) => {
            // Convert serde_json::Value to a string representation for AstLiteralValue::String
            // This might need refinement based on how JSON literals are handled in SQL (e.g., direct JSON type vs. string)
            Ok(AstLiteralValue::String(json_val.to_string()))
        }
    }
}
