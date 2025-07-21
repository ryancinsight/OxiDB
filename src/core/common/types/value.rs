use super::data_type::DataType;
use std::cmp::Ordering; // Required for Ordering::Equal

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)] // Removed PartialOrd from derive
pub enum Value {
    Integer(i64),
    Float(f64), // Added Float variant for floating-point numbers
    Text(String),
    Boolean(bool),
    Blob(Vec<u8>),
    Vector(Vec<f32>), // Represents a vector of f32
    Null,
}

impl Value {
    pub fn get_type(&self) -> DataType {
        match self {
            Value::Integer(_) => DataType::Integer,
            Value::Float(_) => DataType::Integer, // For now, map Float to Integer type
            Value::Text(_) => DataType::Text,
            Value::Boolean(_) => DataType::Boolean,
            Value::Blob(_) => DataType::Blob,
            Value::Vector(_) => DataType::Vector(None), // Or determine dimension if stored
            Value::Null => DataType::Null,
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::Integer(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
            (Value::Float(a), Value::Integer(b)) => a.partial_cmp(&(*b as f64)),
            (Value::Text(a), Value::Text(b)) => a.partial_cmp(b),
            (Value::Boolean(a), Value::Boolean(b)) => a.partial_cmp(b),
            (Value::Blob(a), Value::Blob(b)) => a.partial_cmp(b),
            // Vector comparison is not straightforward (e.g., lexicographical, magnitude)
            // For now, let's say they are not comparable for ordering purposes
            // Or define a specific comparison logic if needed by the application
            (Value::Vector(_), Value::Vector(_)) => None,
            (Value::Null, Value::Null) => Some(Ordering::Equal),
            // All other combinations are non-compatible
            _ => None,
        }
    }
}
