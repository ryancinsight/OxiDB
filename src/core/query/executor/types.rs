// src/core/query/executor/types.rs
//! Core types and structures for query execution

use crate::core::common::OxidbError;
use crate::core::query::sql::ast::AstLiteralValue;
use crate::core::types::DataType;

/// Maximum recursion depth for parameter resolution to prevent stack overflow
pub const MAX_PARAMETER_RECURSION_DEPTH: usize = 100;

/// Context for resolving parameter placeholders during execution
#[derive(Debug)]
pub struct ParameterContext<'a> {
    parameters: &'a [crate::core::common::types::Value],
}

impl<'a> ParameterContext<'a> {
    #[must_use]
    pub const fn new(parameters: &'a [crate::core::common::types::Value]) -> Self {
        Self { parameters }
    }

    /// Resolve a parameter by its index
    pub fn resolve_parameter(
        &self,
        index: u32,
    ) -> Result<&crate::core::common::types::Value, OxidbError> {
        let idx = index as usize;
        self.parameters.get(idx).ok_or_else(|| OxidbError::ParameterIndexOutOfBounds {
            index: idx,
            max: self.parameters.len(),
        })
    }

    /// Get total number of parameters
    #[must_use]
    pub const fn parameter_count(&self) -> usize {
        self.parameters.len()
    }
}

/// Conversion utilities for execution values
pub struct ValueConverter;

impl ValueConverter {
    /// Convert AST literal to DataType
    pub fn convert_ast_literal_to_datatype(
        literal: &AstLiteralValue,
    ) -> Result<DataType, OxidbError> {
        match literal {
            AstLiteralValue::String(s) => Ok(DataType::String(s.clone())),
            AstLiteralValue::Number(n) => {
                // Parse the number string as either integer or float
                if let Ok(i) = n.parse::<i64>() {
                    Ok(DataType::Integer(i))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(DataType::Float(crate::core::types::OrderedFloat(f)))
                } else {
                    Err(OxidbError::Parsing(format!("Invalid number format: {}", n)))
                }
            }
            AstLiteralValue::Boolean(b) => Ok(DataType::Boolean(*b)),
            AstLiteralValue::Null => Ok(DataType::Null),
            AstLiteralValue::Vector(_) => {
                Err(OxidbError::NotImplemented { feature: "Vector literal conversion".to_string() })
            }
        }
    }

    /// Convert common Value type to DataType
    pub fn convert_value_to_datatype(value: &crate::core::common::types::Value) -> DataType {
        use crate::core::common::types::Value;
        match value {
            Value::Integer(i) => DataType::Integer(*i),
            Value::Float(f) => DataType::Float(crate::core::types::OrderedFloat(*f)),
            Value::Text(s) => DataType::String(s.clone()),
            Value::Boolean(b) => DataType::Boolean(*b),
            Value::Blob(b) => DataType::RawBytes(b.clone()),
            Value::Vector(v) => Self::convert_vector_to_datatype(v),
            Value::Null => DataType::Null,
        }
    }

    /// Convert vector to DataType with proper error handling
    fn convert_vector_to_datatype(v: &[f32]) -> DataType {
        let dimension = v.len() as u32;
        if let Some(vector_data) = crate::core::types::VectorData::new(dimension, v.to_vec()) {
            DataType::Vector(crate::core::types::HashableVectorData(vector_data))
        } else {
            // Fallback to raw bytes if vector creation fails
            DataType::RawBytes(v.iter().flat_map(|f| f.to_le_bytes().to_vec()).collect())
        }
    }
}

/// Result of query execution
#[derive(Debug, PartialEq)]
pub enum ExecutionResult {
    Value(Option<DataType>),
    Success,
    Deleted(bool),
    Values(Vec<DataType>),
    Updated { count: usize },
    RankedResults(Vec<(f32, Vec<DataType>)>), // For similarity search results (distance, row_data)
    Query { columns: Vec<String>, rows: Vec<Vec<DataType>> },
}

impl ExecutionResult {
    /// Check if the result represents success
    #[must_use]
    pub const fn is_success(&self) -> bool {
        matches!(self, Self::Success)
    }

    /// Get the number of affected rows for update/delete operations
    #[must_use]
    pub const fn affected_rows(&self) -> usize {
        match self {
            Self::Updated { count } => *count,
            Self::Deleted(true) => 1,
            Self::Deleted(false) => 0,
            _ => 0,
        }
    }

    /// Check if the result contains data
    #[must_use]
    pub const fn has_data(&self) -> bool {
        matches!(self, Self::Query { .. } | Self::Values(_) | Self::RankedResults(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::common::types::Value;

    #[test]
    fn test_parameter_context_creation() {
        let params = vec![Value::Integer(42), Value::Text("test".to_string())];
        let ctx = ParameterContext::new(&params);
        assert_eq!(ctx.parameter_count(), 2);
    }

    #[test]
    fn test_parameter_resolution() {
        let params = vec![Value::Integer(42), Value::Text("test".to_string())];
        let ctx = ParameterContext::new(&params);

        let result = ctx.resolve_parameter(0).unwrap();
        assert_eq!(result, &Value::Integer(42));

        let result = ctx.resolve_parameter(1).unwrap();
        assert_eq!(result, &Value::Text("test".to_string()));

        assert!(ctx.resolve_parameter(2).is_err());
    }

    #[test]
    fn test_value_converter() {
        let value = Value::Integer(42);
        let datatype = ValueConverter::convert_value_to_datatype(&value);
        assert_eq!(datatype, DataType::Integer(42));
    }

    #[test]
    fn test_execution_result_helpers() {
        let success = ExecutionResult::Success;
        assert!(success.is_success());
        assert_eq!(success.affected_rows(), 0);
        assert!(!success.has_data());

        let updated = ExecutionResult::Updated { count: 5 };
        assert!(!updated.is_success());
        assert_eq!(updated.affected_rows(), 5);
        assert!(!updated.has_data());

        let query = ExecutionResult::Query {
            columns: vec!["id".to_string()],
            rows: vec![vec![DataType::Integer(1)]],
        };
        assert!(!query.is_success());
        assert_eq!(query.affected_rows(), 0);
        assert!(query.has_data());
    }
}
