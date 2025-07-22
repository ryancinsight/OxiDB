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
    #[must_use]
    pub const fn get_type(&self) -> DataType {
        match self {
            Self::Integer(_) => DataType::Integer,
            Self::Float(_) => DataType::Float,
            Self::Text(_) => DataType::Text,
            Self::Boolean(_) => DataType::Boolean,
            Self::Blob(_) => DataType::Blob,
            Self::Vector(_) => DataType::Vector(None), // Or determine dimension if stored
            Self::Null => DataType::Null,
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Integer(a), Self::Integer(b)) => a.partial_cmp(b),
            (Self::Float(a), Self::Float(b)) => a.partial_cmp(b),
            (Self::Integer(a), Self::Float(b)) => (*a as f64).partial_cmp(b),
            (Self::Float(a), Self::Integer(b)) => a.partial_cmp(&(*b as f64)),
            (Self::Text(a), Self::Text(b)) => a.partial_cmp(b),
            (Self::Boolean(a), Self::Boolean(b)) => a.partial_cmp(b),
            (Self::Blob(a), Self::Blob(b)) => a.partial_cmp(b),
            // Vector comparison is not straightforward (e.g., lexicographical, magnitude)
            // For now, let's say they are not comparable for ordering purposes
            // Or define a specific comparison logic if needed by the application
            (Self::Vector(_), Self::Vector(_)) => None,
            (Self::Null, Self::Null) => Some(Ordering::Equal),
            // All other combinations are non-compatible
            _ => None,
        }
    }
}
