use crate::core::common::OxidbError; // Changed
use crate::core::query::sql::ast::AstLiteralValue;
use crate::core::types::DataType; // Ensure this path is correct

// This function was already here from previous refactoring, ensure it's pub
pub fn compare_data_types(
    val1: &DataType,
    val2: &DataType,
    operator: &str,
) -> Result<bool, OxidbError> {
    // Changed
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
                    #[allow(clippy::cast_precision_loss)]
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
                    #[allow(clippy::cast_precision_loss)]
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
                (DataType::Null, _) | (_, DataType::Null) => Err(OxidbError::SqlParsing(format!( // Changed
                    "Ordered comparison ('{operator}') with NULL is not supported directly. Use IS NULL or IS NOT NULL."
                ))),
                _ => Err(OxidbError::Type(format!( // Changed
                    "Cannot apply ordered operator '{operator}' between {val1:?} and {val2:?}"
                ))),
            }
        }
        _ => Err(OxidbError::SqlParsing(format!("Unsupported operator: {operator}"))), // Changed
    }
}

// New helper function as planned
pub fn datatype_to_ast_literal(data_type: &DataType) -> Result<AstLiteralValue, OxidbError> {
    // Changed
    match data_type {
        DataType::Integer(i) => Ok(AstLiteralValue::Number(i.to_string())),
        DataType::String(s) => Ok(AstLiteralValue::String(s.clone())),
        DataType::Boolean(b) => Ok(AstLiteralValue::Boolean(*b)),
        DataType::Float(f) => Ok(AstLiteralValue::Number(f.to_string())), // Consider precision if needed
        DataType::Null => Ok(AstLiteralValue::Null),
        DataType::Map(_) => Err(OxidbError::NotImplemented{feature: // Changed
            "Cannot convert Map DataType to AstLiteralValue for SQL conditions".to_string(),
        }),
        DataType::JsonBlob(json_val) => {
            // Convert serde_json::Value to a string representation for AstLiteralValue::String
            // This might need refinement based on how JSON literals are handled in SQL (e.g., direct JSON type vs. string)
            Ok(AstLiteralValue::String(json_val.to_string()))
        }
        DataType::RawBytes(bytes) => {
            // Represent bytes as a hex string literal or handle as an error
            // For now, let's convert to a hex string, assuming it might be used in some contexts.
            // This might not be directly usable in all SQL condition contexts without specific function calls.
            Ok(AstLiteralValue::String(hex::encode(bytes)))
        }
        DataType::Vector(vec) => {
            // Convert vector to a string representation for AST compatibility
            let vec_str = format!(
                "[{}]",
                vec.data.iter().map(std::string::ToString::to_string).collect::<Vec<_>>().join(",")
            );
            Ok(AstLiteralValue::String(vec_str))
        }
    }
}
