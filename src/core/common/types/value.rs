use super::data_type::DataType;
use std::cmp::Ordering; // Required for Ordering::Equal

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)] // Removed PartialOrd from derive
pub enum Value {
    Integer(i64),
    Text(String),
    Boolean(bool),
    Blob(Vec<u8>),
    Float64(f64),      // Added for floating point numbers
    Vector(Vec<f32>), // Represents a vector of f32
    Map(std::collections::HashMap<String, Value>), // Added for structured data/rows
    Null,
}

impl Value {
    pub fn get_type(&self) -> DataType {
        match self {
            Value::Integer(_) => DataType::Integer,
            Value::Text(_) => DataType::Text,
            Value::Boolean(_) => DataType::Boolean,
            Value::Blob(_) => DataType::Blob,
            Value::Float64(_) => DataType::Float64,
            Value::Vector(v) => DataType::Vector(Some(v.len())),
            // DataType::Map is DataType::Map(JsonSafeMap), which is different.
            // This implies we might need a DataType::Record or similar if we want a direct type mapping.
            // For now, a Value::Map will likely correspond to a DataType::Map(None) or a generic object type.
            // Let's assume for now it corresponds to a generic Map type for simplicity during this refactor.
            // The actual DataType::Map(JsonSafeMap) is a storage format.
            Value::Map(_) => DataType::Map(None), // Placeholder: This mapping needs refinement.
            Value::Null => DataType::Null,
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),
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
