use super::data_type::DataType;
use std::cmp::Ordering; // Required for Ordering::Equal

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)] // Removed PartialOrd from derive
pub enum Value {
    Integer(i64),
    Text(String),
    Boolean(bool),
    Blob(Vec<u8>),
    Null,
}

impl Value {
    pub fn get_type(&self) -> DataType {
        match self {
            Value::Integer(_) => DataType::Integer,
            Value::Text(_) => DataType::Text,
            Value::Boolean(_) => DataType::Boolean,
            Value::Blob(_) => DataType::Blob,
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
            (Value::Null, Value::Null) => Some(Ordering::Equal),
            // All other combinations are non-compatible
            _ => None,
        }
    }
}
