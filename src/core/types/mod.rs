// src/core/types/mod.rs

use serde::{Deserialize, Serialize};
use serde_json; // For JsonBlob
use serde_with::{base64::Base64, base64::Standard, formats::Padded, serde_as, Same};
use std::collections::HashMap; // Added for SimpleMap // Refined imports

// Define SimpleMap type alias - This will be replaced by JsonSafeMap structure
// pub type SimpleMap = HashMap<Vec<u8>, DataType>;

#[serde_as]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JsonSafeMap(
    // Ensures keys (Vec<u8>) are serialized/deserialized as Base64 strings with standard padding.
    // Values (DataType) use their existing Serialize/Deserialize impls via `Same`.
    #[serde_as(as = "HashMap<Base64<Standard, Padded>, Same>")] pub HashMap<Vec<u8>, DataType>, // Made field pub for direct construction/access if needed
);

// Re-export ID types from their actual location in common::types::ids
pub use crate::core::common::types::ids::{PageId, SlotId, TransactionId};

pub mod schema;
pub use schema::{ColumnDef, Schema};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    Integer(i64),
    String(String),
    Boolean(bool),
    Float(f64),       // Added Float variant
    Null,             // Added Null variant
    Map(JsonSafeMap), // Changed to use JsonSafeMap
    JsonBlob(serde_json::Value),
    RawBytes(Vec<u8>), // Added RawBytes variant
    Vector(VectorData), // Added Vector variant
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
            DataType::RawBytes(_) => "RawBytes",
            DataType::Vector(_) => "Vector",
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

// LSN type alias - this was from a previous session, ensure it's correct.
pub type Lsn = u64;

// Re-export Row and Value for convenience, assuming they are defined in their respective modules
// pub mod row; // Assuming row.rs exists
// pub use row::Row;
// pub mod value; // Assuming value.rs exists
// pub use value::Value;

/// Represents vector data, including its dimension and the actual vector.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VectorData {
    pub dimension: u32,
    pub data: Vec<f32>,
}

impl VectorData {
    /// Creates a new VectorData, ensuring the data matches the dimension.
    /// Returns None if the data length does not match the dimension.
    pub fn new(dimension: u32, data: Vec<f32>) -> Option<Self> {
        if data.len() as u32 == dimension {
            // This covers both (0, empty_vec) and (N, vec_of_N_elements)
            // Note: A dimension of 0 might be valid for an "empty" vector concept.
            Some(Self { dimension, data })
        } else {
            None
        }
    }

    /// Calculates the Euclidean distance between this vector and another.
    /// Returns None if dimensions do not match or if dimension is 0.
    pub fn euclidean_distance(&self, other: &VectorData) -> Option<f32> {
        if self.dimension != other.dimension || self.dimension == 0 {
            return None;
        }
        // VectorData::new should ensure self.data.len() == self.dimension,
        // but an explicit check here could be added for robustness if VectorData instances
        // could be created without ::new (e.g. direct struct instantiation).
        // Assuming ::new is the canonical way, this check is redundant:
        // if self.data.len() != self.dimension as usize || other.data.len() != other.dimension as usize {
        //     return None;
        // }

        let mut sum_sq_diff = 0.0;
        for i in 0..self.dimension as usize {
            // Accessing self.data[i] and other.data[i] is safe due to the dimension check
            // and the invariant that data.len() == dimension.
            let diff = self.data[i] - other.data[i];
            sum_sq_diff += diff * diff;
        }
        Some(sum_sq_diff.sqrt())
    }
}

// Tests module (if any specific to types/mod.rs, otherwise it's usually in individual type files)
// #[cfg(test)]
// mod tests; // Removed as src/core/types/tests.rs does not exist
