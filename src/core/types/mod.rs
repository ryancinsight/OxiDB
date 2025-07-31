// src/core/types/mod.rs

use serde::{Deserialize, Serialize};
use serde_json; // For JsonBlob
use serde_with::{base64::Base64, base64::Standard, formats::Padded, serde_as, Same};
use std::collections::HashMap; // Added for SimpleMap // Refined imports

// Re-export ID types from their actual location in common::types::ids
pub use crate::core::common::types::ids::{PageId, SlotId, TransactionId};

// Re-export common types from the common module
pub use crate::core::common::types::{
    ColumnDef, DataType as CommonDataType, Lsn, Row, Schema, Value,
};

pub mod schema;

// Re-export the modules for direct access if needed
pub mod data_type {
    pub use crate::core::common::types::data_type::*;
}

pub mod value {
    pub use crate::core::common::types::value::*;
}

pub mod ids {
    pub use crate::core::common::types::ids::*;
}

pub mod row {
    pub use crate::core::common::types::row::*;
}

// Define SimpleMap type alias - This will be replaced by JsonSafeMap structure
// pub type SimpleMap = HashMap<Vec<u8>, DataType>;

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsonSafeMap(
    // Ensures keys (Vec<u8>) are serialized/deserialized as Base64 strings with standard padding.
    // Values (DataType) use their existing Serialize/Deserialize impls via `Same`.
    #[serde_as(as = "HashMap<Base64<Standard, Padded>, Same>")] pub HashMap<Vec<u8>, DataType>, // Made field pub for direct construction/access if needed
);

impl std::hash::Hash for JsonSafeMap {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Hash the length of the map
        self.0.len().hash(state);
        // Sort keys for consistent hashing
        let mut sorted_keys: Vec<_> = self.0.keys().collect();
        sorted_keys.sort();
        for key in sorted_keys {
            key.hash(state);
            self.0[key].hash(state);
        }
    }
}

impl PartialOrd for JsonSafeMap {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for JsonSafeMap {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Compare by length first
        match self.0.len().cmp(&other.0.len()) {
            std::cmp::Ordering::Equal => {
                // If same length, compare sorted key-value pairs
                let mut self_pairs: Vec<_> = self.0.iter().collect();
                let mut other_pairs: Vec<_> = other.0.iter().collect();
                self_pairs.sort_by_key(|(k, _)| *k);
                other_pairs.sort_by_key(|(k, _)| *k);
                self_pairs.cmp(&other_pairs)
            }
            other => other,
        }
    }
}

// Legacy DataType for compatibility with existing code
// This will be gradually migrated to use CommonDataType and Value
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum DataType {
    Integer(i64),
    String(String),
    Boolean(bool),
    Float(OrderedFloat),       // Added Float variant with ordering
    Null,             // Added Null variant
    Map(JsonSafeMap), // Changed to use JsonSafeMap
    JsonBlob(JsonValue),
    RawBytes(Vec<u8>), // Added RawBytes variant
    Vector(HashableVectorData), // Added Vector variant
                       // Potentially other types like Timestamp, etc. could be added later
}

/// Wrapper for f64 that implements Eq and Hash for use in DataType
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct OrderedFloat(pub f64);

impl Eq for OrderedFloat {}

impl Ord for OrderedFloat {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap_or(std::cmp::Ordering::Equal)
    }
}

impl std::hash::Hash for OrderedFloat {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.to_bits().hash(state);
    }
}

/// Wrapper for serde_json::Value that implements Hash and Eq
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsonValue(pub serde_json::Value);

impl std::hash::Hash for JsonValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Hash based on the JSON string representation
        self.0.to_string().hash(state);
    }
}

impl PartialOrd for JsonValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for JsonValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Compare by string representation
        self.0.to_string().cmp(&other.0.to_string())
    }
}

/// Wrapper for VectorData that implements Hash and Eq
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HashableVectorData(pub VectorData);

impl Eq for HashableVectorData {}

impl std::hash::Hash for HashableVectorData {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.dimension.hash(state);
        for &val in &self.0.data {
            val.to_bits().hash(state);
        }
    }
}

impl PartialOrd for HashableVectorData {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HashableVectorData {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.0.dimension.cmp(&other.0.dimension) {
            std::cmp::Ordering::Equal => {
                // Compare data vectors element by element
                for (a, b) in self.0.data.iter().zip(other.0.data.iter()) {
                    match a.partial_cmp(b) {
                        Some(std::cmp::Ordering::Equal) => continue,
                        Some(ord) => return ord,
                        None => return std::cmp::Ordering::Equal, // Handle NaN
                    }
                }
                self.0.data.len().cmp(&other.0.data.len())
            }
            ord => ord,
        }
    }
}

// Optional: Helper methods for DataType if needed, e.g., for type checking
impl DataType {
    #[must_use]
    pub const fn type_name(&self) -> &'static str {
        match self {
            Self::Integer(_) => "Integer",
            Self::String(_) => "String",
            Self::Boolean(_) => "Boolean",
            Self::Float(_) => "Float",
            Self::Null => "Null",
            Self::Map(_) => "Map",
            Self::JsonBlob(_) => "JsonBlob",
            Self::RawBytes(_) => "RawBytes",
            Self::Vector(_) => "Vector",
        }
    }
}

/// Represents vector data, including its dimension and the actual vector.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VectorData {
    pub dimension: u32,
    pub data: Vec<f32>,
}

impl VectorData {
    /// Creates a new `VectorData`, ensuring the data matches the dimension.
    /// Returns None if the data length does not match the dimension.
    #[must_use]
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
    #[must_use]
    pub fn euclidean_distance(&self, other: &Self) -> Option<f32> {
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

    /// Calculates the magnitude (L2 norm) of the vector
    #[must_use]
    pub fn magnitude(&self) -> f64 {
        self.data.iter()
            .map(|&x| (x as f64) * (x as f64))
            .sum::<f64>()
            .sqrt()
    }

    /// Calculates the L2 norm of the vector (alias for magnitude)
    #[must_use]
    pub fn norm(&self) -> f64 {
        self.magnitude()
    }
}

// Tests module (if any specific to types/mod.rs, otherwise it's usually in individual type files)
// #[cfg(test)]
// mod tests; // Removed as src/core/types/tests.rs does not exist
