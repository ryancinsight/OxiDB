// src/core/types/mod.rs

use serde::{Deserialize, Serialize};
use serde_json; // For JsonBlob
use std::collections::HashMap; // Added for SimpleMap

// Define SimpleMap type alias
pub type SimpleMap = HashMap<Vec<u8>, DataType>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    Integer(i64),
    String(String),
    Boolean(bool),
    Float(f64), // Added Float variant
    Null, // Added Null variant
    Map(SimpleMap), // Added Map variant
    JsonBlob(serde_json::Value),
    // Potentially other types like Timestamp, etc. could be added later
}

// Optional: Helper methods for DataType if needed, e.g., for type checking
impl DataType {
    pub fn type_name(&self) -> &'static str {
        match self {
            DataType::Integer(_) => "Integer",
            DataType::String(_) => "String",
            DataType::Boolean(_) => "Boolean",
            DataType::Float(_) => "Float",
            DataType::Null => "Null",
            DataType::Map(_) => "Map",
            DataType::JsonBlob(_) => "JsonBlob",
        }
    }
}

// Example of how to use it (mainly for testing or direct construction):
// fn create_integer_type(val: i64) -> DataType {
//     DataType::Integer(val)
// }
//
// fn create_string_type(val: String) -> DataType {
//     DataType::String(val)
// }
