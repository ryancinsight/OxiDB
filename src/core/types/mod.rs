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

impl std::fmt::Display for OrderedFloat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Implements `Hash` for `OrderedFloat`.
/// 
/// This implementation uses the bit representation of the floating-point value
/// to compute the hash. As a result, `NaN` values are treated as equal if they
/// have the same bit representation. This behavior is consistent with the IEEE 754
/// standard but may differ from how `NaN` values are treated in other contexts.
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
        // Use a configurable depth limit to prevent stack overflow
        const MAX_RECURSION_DEPTH: usize = 1000;
        self.cmp_with_depth(other, 0, MAX_RECURSION_DEPTH)
    }
}

impl JsonValue {
    /// Compare JsonValues with recursion depth tracking
    fn cmp_with_depth(&self, other: &Self, current_depth: usize, max_depth: usize) -> std::cmp::Ordering {
        use serde_json::Value;
        
        // Prevent stack overflow by limiting recursion depth
        if current_depth >= max_depth {
            // Fall back to string comparison for deeply nested structures
            return self.0.to_string().cmp(&other.0.to_string());
        }
        
        match (&self.0, &other.0) {
            (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
            (Value::Null, _) => std::cmp::Ordering::Less,
            (_, Value::Null) => std::cmp::Ordering::Greater,
            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
            (Value::Bool(_), _) => std::cmp::Ordering::Less,
            (_, Value::Bool(_)) => std::cmp::Ordering::Greater,
            (Value::Number(a), Value::Number(b)) => a.as_f64().partial_cmp(&b.as_f64()).unwrap_or(std::cmp::Ordering::Equal),
            (Value::Number(_), _) => std::cmp::Ordering::Less,
            (_, Value::Number(_)) => std::cmp::Ordering::Greater,
            (Value::String(a), Value::String(b)) => a.cmp(b),
            (Value::String(_), _) => std::cmp::Ordering::Less,
            (_, Value::String(_)) => std::cmp::Ordering::Greater,
            (Value::Array(a), Value::Array(b)) => {
                // Use iterator combinators for efficient array comparison
                self.compare_arrays_with_iterators(a, b, current_depth, max_depth)
            },
            (Value::Array(_), _) => std::cmp::Ordering::Less,
            (_, Value::Array(_)) => std::cmp::Ordering::Greater,
            (Value::Object(a), Value::Object(b)) => {
                // Use iterator combinators for efficient object comparison
                self.compare_objects_with_iterators(a, b, current_depth, max_depth)
            }
        }
    }

    /// Compare arrays using advanced iterator patterns
    fn compare_arrays_with_iterators(
        &self, 
        a: &[serde_json::Value], 
        b: &[serde_json::Value], 
        current_depth: usize, 
        max_depth: usize
    ) -> std::cmp::Ordering {
        // First compare lengths for early termination
        match a.len().cmp(&b.len()) {
            std::cmp::Ordering::Equal => {
                // Use iterator combinators to find the first differing element
                a.iter()
                    .zip(b.iter())
                    .map(|(a_item, b_item)| {
                        JsonValue(a_item.clone()).cmp_with_depth(
                            &JsonValue(b_item.clone()), 
                            current_depth + 1, 
                            max_depth
                        )
                    })
                    .find(|&ord| ord != std::cmp::Ordering::Equal)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }
            other => other,
        }
    }

    /// Compare objects using advanced iterator patterns and sorting
    fn compare_objects_with_iterators(
        &self, 
        a: &serde_json::Map<String, serde_json::Value>, 
        b: &serde_json::Map<String, serde_json::Value>, 
        current_depth: usize, 
        max_depth: usize
    ) -> std::cmp::Ordering {
        // First compare lengths for early termination
        match a.len().cmp(&b.len()) {
            std::cmp::Ordering::Equal => {
                // Use iterator combinators to create sorted key-value pairs efficiently
                let a_sorted = self.create_sorted_pairs(a);
                let b_sorted = self.create_sorted_pairs(b);
                
                // Use iterator combinators to compare sorted pairs
                a_sorted
                    .iter()
                    .zip(b_sorted.iter())
                    .map(|((k1, v1), (k2, v2))| {
                        // First compare keys
                        match k1.cmp(k2) {
                            std::cmp::Ordering::Equal => {
                                // If keys are equal, compare values recursively
                                JsonValue((*v1).clone()).cmp_with_depth(
                                    &JsonValue((*v2).clone()), 
                                    current_depth + 1, 
                                    max_depth
                                )
                            }
                            key_ord => key_ord,
                        }
                    })
                    .find(|&ord| ord != std::cmp::Ordering::Equal)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }
            other => other,
        }
    }

    /// Create sorted key-value pairs using iterator combinators
    fn create_sorted_pairs<'a>(
        &self, 
        map: &'a serde_json::Map<String, serde_json::Value>
    ) -> Vec<(&'a String, &'a serde_json::Value)> {
        // Use iterator combinators to collect and sort in one efficient operation
        let mut pairs: Vec<_> = map.iter().collect();
        pairs.sort_unstable_by_key(|(k, _)| *k);
        pairs
    }

    /// Compare JsonValues with custom depth limit (for testing and specialized use cases)
    pub fn cmp_with_custom_depth(&self, other: &Self, max_depth: usize) -> std::cmp::Ordering {
        self.cmp_with_depth(other, 0, max_depth)
    }

    /// Get the nesting depth of a JsonValue using iterators
    pub fn nesting_depth(&self) -> usize {
        self.calculate_depth(&self.0, 0)
    }

    /// Calculate nesting depth using iterator-based approach
    fn calculate_depth(&self, value: &serde_json::Value, current_depth: usize) -> usize {
        use serde_json::Value;
        
        match value {
            Value::Array(arr) => {
                if arr.is_empty() {
                    current_depth + 1
                } else {
                    // Use iterator combinators to find maximum depth efficiently
                    arr.iter()
                        .map(|item| self.calculate_depth(item, current_depth + 1))
                        .max()
                        .unwrap_or(current_depth + 1)
                }
            }
            Value::Object(obj) => {
                if obj.is_empty() {
                    current_depth + 1
                } else {
                    // Use iterator combinators to find maximum depth efficiently
                    obj.values()
                        .map(|item| self.calculate_depth(item, current_depth + 1))
                        .max()
                        .unwrap_or(current_depth + 1)
                }
            }
            _ => current_depth + 1,
        }
    }

    /// Check if JsonValue exceeds a certain depth threshold
    pub fn exceeds_depth(&self, threshold: usize) -> bool {
        self.check_depth_threshold(&self.0, 0, threshold)
    }

    /// Early-terminating depth check using iterators
    fn check_depth_threshold(&self, value: &serde_json::Value, current_depth: usize, threshold: usize) -> bool {
        use serde_json::Value;
        
        if current_depth >= threshold {
            return true;
        }
        
        match value {
            Value::Array(arr) => {
                // Use iterator combinators with early termination
                arr.iter()
                    .any(|item| self.check_depth_threshold(item, current_depth + 1, threshold))
            }
            Value::Object(obj) => {
                // Use iterator combinators with early termination
                obj.values()
                    .any(|item| self.check_depth_threshold(item, current_depth + 1, threshold))
            }
            _ => false,
        }
    }

    /// Create a flattened iterator over all leaf values in the JSON structure
    pub fn leaf_values(&self) -> impl Iterator<Item = &serde_json::Value> + '_ {
        JsonLeafIterator::new(&self.0)
    }

    /// Create an iterator over all key paths in the JSON structure
    pub fn key_paths(&self) -> impl Iterator<Item = String> + '_ {
        JsonPathIterator::new(&self.0)
    }
}

/// Iterator for traversing leaf values in a JSON structure
pub struct JsonLeafIterator<'a> {
    stack: Vec<&'a serde_json::Value>,
}

impl<'a> JsonLeafIterator<'a> {
    fn new(value: &'a serde_json::Value) -> Self {
        Self {
            stack: vec![value],
        }
    }
}

impl<'a> Iterator for JsonLeafIterator<'a> {
    type Item = &'a serde_json::Value;

    fn next(&mut self) -> Option<Self::Item> {
        use serde_json::Value;
        
        while let Some(current) = self.stack.pop() {
            match current {
                Value::Array(arr) => {
                    // Add array elements to stack in reverse order for correct iteration order
                    self.stack.extend(arr.iter().rev());
                }
                Value::Object(obj) => {
                    // Add object values to stack
                    self.stack.extend(obj.values().collect::<Vec<_>>().into_iter().rev());
                }
                leaf => return Some(leaf),
            }
        }
        None
    }
}

/// Iterator for traversing key paths in a JSON structure
pub struct JsonPathIterator<'a> {
    stack: Vec<(&'a serde_json::Value, String)>,
}

impl<'a> JsonPathIterator<'a> {
    fn new(value: &'a serde_json::Value) -> Self {
        Self {
            stack: vec![(value, String::new())],
        }
    }
}

impl<'a> Iterator for JsonPathIterator<'a> {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        use serde_json::Value;
        
        while let Some((current, path)) = self.stack.pop() {
            match current {
                Value::Array(arr) => {
                    // Add array elements with indexed paths
                    for (i, item) in arr.iter().enumerate().rev() {
                        let new_path = if path.is_empty() {
                            format!("[{}]", i)
                        } else {
                            format!("{}[{}]", path, i)
                        };
                        self.stack.push((item, new_path));
                    }
                }
                Value::Object(obj) => {
                    // Add object values with key paths
                    for (key, value) in obj.iter().rev() {
                        let new_path = if path.is_empty() {
                            key.clone()
                        } else {
                            format!("{}.{}", path, key)
                        };
                        self.stack.push((value, new_path));
                    }
                }
                _ => {
                    if !path.is_empty() {
                        return Some(path);
                    }
                }
            }
        }
        None
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

#[cfg(test)]
mod json_value_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_basic_json_comparison() {
        let json1 = JsonValue(json!({"a": 1, "b": [1, 2, 3]}));
        let json2 = JsonValue(json!({"a": 1, "b": [1, 2, 3]}));
        let json3 = JsonValue(json!({"a": 2, "b": [1, 2, 3]}));

        assert_eq!(json1.cmp(&json2), std::cmp::Ordering::Equal);
        assert_eq!(json1.cmp(&json3), std::cmp::Ordering::Less);
    }

    #[test]
    fn test_array_comparison_with_iterators() {
        let json1 = JsonValue(json!([1, 2, 3, 4, 5]));
        let json2 = JsonValue(json!([1, 2, 3, 4, 5]));
        let json3 = JsonValue(json!([1, 2, 3, 4, 6]));
        let json4 = JsonValue(json!([1, 2, 3, 4]));

        assert_eq!(json1.cmp(&json2), std::cmp::Ordering::Equal);
        assert_eq!(json1.cmp(&json3), std::cmp::Ordering::Less);
        assert_eq!(json1.cmp(&json4), std::cmp::Ordering::Greater);
    }

    #[test]
    fn test_object_comparison_with_iterators() {
        let json1 = JsonValue(json!({"z": 1, "a": 2, "m": 3}));
        let json2 = JsonValue(json!({"a": 2, "m": 3, "z": 1}));
        let json3 = JsonValue(json!({"z": 1, "a": 3, "m": 3}));

        assert_eq!(json1.cmp(&json2), std::cmp::Ordering::Equal);
        assert_eq!(json1.cmp(&json3), std::cmp::Ordering::Less);
    }

    #[test]
    fn test_nested_json_comparison() {
        let json1 = JsonValue(json!({
            "users": [
                {"name": "Alice", "age": 30, "hobbies": ["reading", "swimming"]},
                {"name": "Bob", "age": 25, "hobbies": ["gaming", "cooking"]}
            ],
            "meta": {"version": 1, "created": "2023-01-01"}
        }));

        let json2 = JsonValue(json!({
            "meta": {"created": "2023-01-01", "version": 1},
            "users": [
                {"hobbies": ["reading", "swimming"], "name": "Alice", "age": 30},
                {"age": 25, "name": "Bob", "hobbies": ["gaming", "cooking"]}
            ]
        }));

        assert_eq!(json1.cmp(&json2), std::cmp::Ordering::Equal);
    }

    #[test]
    fn test_recursion_depth_limit() {
        // Create a deeply nested JSON structure (smaller depth for testing)
        let mut deep_json = json!({"level": 0});
        for i in 1..=500 {
            deep_json = json!({"level": i, "nested": deep_json});
        }

        let json1 = JsonValue(deep_json.clone());
        let json2 = JsonValue(deep_json);

        // This should not cause a stack overflow
        assert_eq!(json1.cmp(&json2), std::cmp::Ordering::Equal);
    }

    #[test]
    fn test_custom_depth_limit() {
        let json1 = JsonValue(json!({"a": {"b": {"c": {"d": 1}}}}));
        let json2 = JsonValue(json!({"a": {"b": {"c": {"d": 2}}}}));

        // With a low depth limit, should fall back to string comparison
        let result = json1.cmp_with_custom_depth(&json2, 2);
        // String comparison of JSON representations
        assert_ne!(result, std::cmp::Ordering::Equal);
    }

    #[test]
    fn test_nesting_depth_calculation() {
        let shallow = JsonValue(json!({"a": 1, "b": 2}));
        let deep = JsonValue(json!({"a": {"b": {"c": {"d": [1, 2, 3]}}}}));

        assert_eq!(shallow.nesting_depth(), 2); // {"a": 1} -> depth 2 (object + leaf)
        assert_eq!(deep.nesting_depth(), 6); // {"a": {"b": {"c": {"d": [1, 2, 3]}}}} = 6 levels
    }

    #[test]
    fn test_depth_threshold_check() {
        let json = JsonValue(json!({"a": {"b": {"c": {"d": 1}}}}));

        assert!(!json.exceeds_depth(5));
        assert!(json.exceeds_depth(3));
        assert!(json.exceeds_depth(2));
    }

    #[test]
    fn test_leaf_values_iterator() {
        let json = JsonValue(json!({
            "name": "Alice",
            "age": 30,
            "hobbies": ["reading", "swimming"],
            "address": {
                "street": "123 Main St",
                "city": "Anytown"
            }
        }));

        let leaves: Vec<_> = json.leaf_values().collect();
        
        // Should contain all leaf values (strings and numbers)
        assert_eq!(leaves.len(), 6); // "Alice", 30, "reading", "swimming", "123 Main St", "Anytown"
        
        // Check that we have the expected leaf values (order may vary)
        let leaf_strings: Vec<String> = leaves.iter()
            .map(|v| v.to_string().trim_matches('"').to_string())
            .collect();
        
        assert!(leaf_strings.contains(&"Alice".to_string()));
        assert!(leaf_strings.contains(&"30".to_string()));
        assert!(leaf_strings.contains(&"reading".to_string()));
        assert!(leaf_strings.contains(&"swimming".to_string()));
        assert!(leaf_strings.contains(&"123 Main St".to_string()));
        assert!(leaf_strings.contains(&"Anytown".to_string()));
    }

    #[test]
    fn test_key_paths_iterator() {
        let json = JsonValue(json!({
            "user": {
                "name": "Alice",
                "contacts": {
                    "email": "alice@example.com"
                }
            },
            "items": ["item1", "item2"]
        }));

        let paths: Vec<_> = json.key_paths().collect();
        
        // Should contain paths to all leaf values
        assert!(paths.contains(&"user.name".to_string()));
        assert!(paths.contains(&"user.contacts.email".to_string()));
        assert!(paths.contains(&"items[0]".to_string()));
        assert!(paths.contains(&"items[1]".to_string()));
    }

    #[test]
    fn test_empty_structures() {
        let empty_array = JsonValue(json!([]));
        let empty_object = JsonValue(json!({}));
        let non_empty_array = JsonValue(json!([1]));
        let non_empty_object = JsonValue(json!({"a": 1}));

        assert_eq!(empty_array.cmp(&empty_array), std::cmp::Ordering::Equal);
        assert_eq!(empty_object.cmp(&empty_object), std::cmp::Ordering::Equal);
        assert_eq!(empty_array.cmp(&non_empty_array), std::cmp::Ordering::Less);
        assert_eq!(empty_object.cmp(&non_empty_object), std::cmp::Ordering::Less);
    }

    #[test]
    fn test_type_ordering() {
        let null_val = JsonValue(json!(null));
        let bool_val = JsonValue(json!(true));
        let num_val = JsonValue(json!(42));
        let str_val = JsonValue(json!("hello"));
        let arr_val = JsonValue(json!([1, 2, 3]));
        let obj_val = JsonValue(json!({"key": "value"}));

        // Test the type ordering: null < bool < number < string < array < object
        assert_eq!(null_val.cmp(&bool_val), std::cmp::Ordering::Less);
        assert_eq!(bool_val.cmp(&num_val), std::cmp::Ordering::Less);
        assert_eq!(num_val.cmp(&str_val), std::cmp::Ordering::Less);
        assert_eq!(str_val.cmp(&arr_val), std::cmp::Ordering::Less);
        assert_eq!(arr_val.cmp(&obj_val), std::cmp::Ordering::Less);
    }

    #[test]
    fn test_large_json_performance() {
        // Create a large JSON structure with many elements
        let mut large_object = serde_json::Map::new();
        for i in 0..1000 {
            large_object.insert(
                format!("key_{}", i),
                json!({
                    "id": i,
                    "data": vec![i; 10],
                    "nested": {
                        "value": i * 2,
                        "description": format!("Item {}", i)
                    }
                })
            );
        }

        let json1 = JsonValue(serde_json::Value::Object(large_object.clone()));
        let json2 = JsonValue(serde_json::Value::Object(large_object));

        // This should complete efficiently without stack overflow
        assert_eq!(json1.cmp(&json2), std::cmp::Ordering::Equal);
    }

    #[test]
    fn test_iterator_early_termination() {
        // Test that comparison terminates early when differences are found
        let json1 = JsonValue(json!([1, 2, 3, 999999]));
        let json2 = JsonValue(json!([1, 2, 4, 0]));

        // Should terminate at the third element without comparing the fourth
        assert_eq!(json1.cmp(&json2), std::cmp::Ordering::Less);
    }

    #[test]
    fn test_mixed_nested_structures() {
        let json1 = JsonValue(json!({
            "arrays": [
                [1, 2, 3],
                [4, 5, 6]
            ],
            "objects": [
                {"a": 1, "b": 2},
                {"c": 3, "d": 4}
            ],
            "mixed": [
                {"array": [1, 2]},
                [{"object": "value"}]
            ]
        }));

        let json2 = JsonValue(json!({
            "mixed": [
                {"array": [1, 2]},
                [{"object": "value"}]
            ],
            "arrays": [
                [1, 2, 3],
                [4, 5, 6]
            ],
            "objects": [
                {"b": 2, "a": 1},
                {"d": 4, "c": 3}
            ]
        }));

        assert_eq!(json1.cmp(&json2), std::cmp::Ordering::Equal);
    }
}

#[cfg(test)]
mod json_value_benchmarks {
    use super::*;
    use serde_json::json;
    use std::time::Instant;

    /// Benchmark helper to create deeply nested JSON structures
    fn create_deep_json(depth: usize) -> serde_json::Value {
        let mut json = json!({"value": depth});
        for i in (0..depth).rev() {
            json = json!({"level": i, "nested": json});
        }
        json
    }

    /// Benchmark helper to create wide JSON structures
    fn create_wide_json(width: usize) -> serde_json::Value {
        let mut object = serde_json::Map::new();
        for i in 0..width {
            object.insert(
                format!("key_{}", i),
                json!({
                    "id": i,
                    "data": vec![i; 5],
                    "metadata": {
                        "created": format!("2023-{:02}-{:02}", (i % 12) + 1, (i % 28) + 1),
                        "updated": format!("2024-{:02}-{:02}", (i % 12) + 1, (i % 28) + 1)
                    }
                })
            );
        }
        serde_json::Value::Object(object)
    }

    #[test]
    #[ignore] // Ignore by default to avoid slow tests in CI
    fn benchmark_deep_json_comparison() {
        println!("\n=== Deep JSON Comparison Benchmark ===");
        
        for depth in [100, 500, 1000, 1500] {
            let json_data = create_deep_json(depth);
            let json1 = JsonValue(json_data.clone());
            let json2 = JsonValue(json_data);

            let start = Instant::now();
            let result = json1.cmp(&json2);
            let duration = start.elapsed();

            println!("Depth {}: {:?} in {:?}", depth, result, duration);
            assert_eq!(result, std::cmp::Ordering::Equal);
        }
    }

    #[test]
    #[ignore] // Ignore by default to avoid slow tests in CI
    fn benchmark_wide_json_comparison() {
        println!("\n=== Wide JSON Comparison Benchmark ===");
        
        for width in [100, 500, 1000, 2000] {
            let json_data = create_wide_json(width);
            let json1 = JsonValue(json_data.clone());
            let json2 = JsonValue(json_data);

            let start = Instant::now();
            let result = json1.cmp(&json2);
            let duration = start.elapsed();

            println!("Width {}: {:?} in {:?}", width, result, duration);
            assert_eq!(result, std::cmp::Ordering::Equal);
        }
    }

    #[test]
    #[ignore] // Ignore by default to avoid slow tests in CI
    fn benchmark_iterator_methods() {
        println!("\n=== Iterator Methods Benchmark ===");
        
        let complex_json = JsonValue(json!({
            "users": (0..100).map(|i| json!({
                "id": i,
                "name": format!("User {}", i),
                "profile": {
                    "age": 20 + (i % 50),
                    "interests": ["reading", "gaming", "cooking"],
                    "settings": {
                        "theme": "dark",
                        "notifications": true,
                        "privacy": {
                            "public": i % 2 == 0,
                            "searchable": true
                        }
                    }
                }
            })).collect::<Vec<_>>(),
            "metadata": {
                "version": "1.0",
                "created": "2023-01-01",
                "features": ["auth", "profiles", "notifications"]
            }
        }));

        // Benchmark nesting depth calculation
        let start = Instant::now();
        let depth = complex_json.nesting_depth();
        let depth_duration = start.elapsed();
        println!("Nesting depth calculation: {} in {:?}", depth, depth_duration);

        // Benchmark depth threshold check
        let start = Instant::now();
        let exceeds = complex_json.exceeds_depth(10);
        let threshold_duration = start.elapsed();
        println!("Depth threshold check: {} in {:?}", exceeds, threshold_duration);

        // Benchmark leaf values iteration
        let start = Instant::now();
        let leaf_count = complex_json.leaf_values().count();
        let leaf_duration = start.elapsed();
        println!("Leaf values iteration: {} leaves in {:?}", leaf_count, leaf_duration);

        // Benchmark key paths iteration
        let start = Instant::now();
        let path_count = complex_json.key_paths().count();
        let path_duration = start.elapsed();
        println!("Key paths iteration: {} paths in {:?}", path_count, path_duration);
    }

    #[test]
    #[ignore] // Ignore by default to avoid slow tests in CI
    fn benchmark_comparison_early_termination() {
        println!("\n=== Early Termination Benchmark ===");
        
        // Create two large arrays that differ only in the first element
        let arr1: Vec<_> = (0..10000).collect();
        let arr2: Vec<_> = std::iter::once(1).chain(1..10000).collect();
        
        let json1 = JsonValue(json!(arr1));
        let json2 = JsonValue(json!(arr2));

        let start = Instant::now();
        let result = json1.cmp(&json2);
        let duration = start.elapsed();

        println!("Large array early termination: {:?} in {:?}", result, duration);
        assert_eq!(result, std::cmp::Ordering::Less);
        
        // Should be very fast due to early termination
        assert!(duration.as_millis() < 10, "Early termination should be very fast");
    }

    #[test]
    fn demonstrate_iterator_patterns() {
        println!("\n=== Iterator Patterns Demonstration ===");
        
        let json = JsonValue(json!({
            "analytics": {
                "users": [
                    {"id": 1, "sessions": [{"duration": 300}, {"duration": 450}]},
                    {"id": 2, "sessions": [{"duration": 200}, {"duration": 600}]}
                ],
                "metrics": {
                    "daily": [100, 150, 200],
                    "weekly": [700, 1050, 1400]
                }
            }
        }));

        // Demonstrate leaf values iterator
        println!("Leaf values:");
        json.leaf_values()
            .take(5)
            .for_each(|leaf| println!("  {}", leaf));

        // Demonstrate key paths iterator
        println!("Key paths:");
        json.key_paths()
            .take(8)
            .for_each(|path| println!("  {}", path));

        // Demonstrate iterator combinators for analysis
        let numeric_leaves: Vec<_> = json.leaf_values()
            .filter(|v| v.is_number())
            .filter_map(|v| v.as_f64())
            .collect();
        
        println!("Numeric values: {:?}", numeric_leaves);

        // Use iterator combinators for statistics
        let sum: f64 = numeric_leaves.iter().sum();
        let count = numeric_leaves.len();
        let average = if count > 0 { sum / count as f64 } else { 0.0 };
        
        println!("Statistics - Sum: {}, Count: {}, Average: {:.2}", sum, count, average);
    }

    #[test]
    fn demonstrate_advanced_iterator_usage() {
        println!("\n=== Advanced Iterator Usage ===");
        
        let json = JsonValue(json!({
            "products": [
                {"name": "Laptop", "price": 999.99, "category": "Electronics"},
                {"name": "Book", "price": 29.99, "category": "Education"},
                {"name": "Headphones", "price": 199.99, "category": "Electronics"}
            ]
        }));

        // Use windows iterator pattern for comparing adjacent elements
        let prices: Vec<f64> = json.leaf_values()
            .filter_map(|v| v.as_f64())
            .collect();

        if prices.len() >= 2 {
            let price_differences: Vec<f64> = prices
                .windows(2)
                .map(|window| (window[1] - window[0]).abs())
                .collect();
            
            println!("Price differences: {:?}", price_differences);
        }

        // Use iterator combinators for grouping and analysis
        let paths: Vec<String> = json.key_paths().collect();
        let category_paths: Vec<_> = paths
            .iter()
            .filter(|path| path.contains("category"))
            .collect();
        
        println!("Category paths: {:?}", category_paths);

        // Demonstrate chunking patterns
        let all_leaves: Vec<_> = json.leaf_values().collect();
        let chunked: Vec<_> = all_leaves
            .chunks(3)
            .map(|chunk| chunk.len())
            .collect();
        
        println!("Chunk sizes: {:?}", chunked);
    }
}

// Tests module (if any specific to types/mod.rs, otherwise it's usually in individual type files)
// #[cfg(test)]
// mod tests; // Removed as src/core/types/tests.rs does not exist
