// src/core/types/mod.rs

use serde::{Deserialize, Serialize};
use serde_json; // For JsonBlob

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    Integer(i64),
    String(String),
    Boolean(bool),
    JsonBlob(serde_json::Value),
    // Potentially other types like Float, Timestamp, etc. could be added later
}

// Optional: Helper methods for DataType if needed, e.g., for type checking
impl DataType {
    pub fn type_name(&self) -> &'static str {
        match self {
            DataType::Integer(_) => "Integer",
            DataType::String(_) => "String",
            DataType::Boolean(_) => "Boolean",
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
