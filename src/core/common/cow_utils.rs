// src/core/common/cow_utils.rs
//! Utilities for efficient data handling using Copy-on-Write patterns
//!
//! This module provides utilities to reduce unnecessary cloning and improve performance
//! by leveraging Rust's `Cow` (Clone on Write) type and other zero-copy techniques.

use crate::core::types::DataType;
use std::borrow::Cow;
use std::collections::HashMap;

/// Efficient key-value pair that avoids cloning when possible
#[derive(Debug, Clone)]
pub struct CowKeyValue<'a> {
    pub key: Cow<'a, [u8]>,
    pub value: Cow<'a, DataType>,
}

impl<'a> CowKeyValue<'a> {
    /// Creates a new `CowKeyValue` with borrowed data
    #[must_use]
    pub const fn borrowed(key: &'a [u8], value: &'a DataType) -> Self {
        Self { key: Cow::Borrowed(key), value: Cow::Borrowed(value) }
    }

    /// Creates a new `CowKeyValue` with owned data
    #[must_use]
    pub const fn owned(key: Vec<u8>, value: DataType) -> Self {
        Self { key: Cow::Owned(key), value: Cow::Owned(value) }
    }

    /// Converts to owned data if not already owned
    #[must_use]
    pub fn into_owned(self) -> (Vec<u8>, DataType) {
        (self.key.into_owned(), self.value.into_owned())
    }

    /// Gets a reference to the key
    #[must_use]
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Gets a reference to the value
    #[must_use]
    pub fn value(&self) -> &DataType {
        &self.value
    }

    /// Checks if both key and value are borrowed (zero-copy)
    #[must_use]
    pub const fn is_borrowed(&self) -> bool {
        matches!(self.key, Cow::Borrowed(_)) && matches!(self.value, Cow::Borrowed(_))
    }
}

/// Efficient string wrapper that avoids unnecessary allocations
#[derive(Debug, Clone)]
pub struct CowString<'a> {
    data: Cow<'a, str>,
}

impl<'a> CowString<'a> {
    /// Creates a `CowString` from a borrowed string slice
    #[must_use]
    pub const fn borrowed(s: &'a str) -> Self {
        Self { data: Cow::Borrowed(s) }
    }

    /// Creates a `CowString` from an owned String
    #[must_use]
    pub const fn owned(s: String) -> Self {
        Self { data: Cow::Owned(s) }
    }

    /// Gets a string slice reference
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.data
    }

    /// Converts to owned String if not already owned
    #[must_use]
    pub fn into_owned(self) -> String {
        self.data.into_owned()
    }

    /// Checks if the string is borrowed (zero-copy)
    #[must_use]
    pub const fn is_borrowed(&self) -> bool {
        matches!(self.data, Cow::Borrowed(_))
    }

    /// Creates a `CowString` from a `DataType` if it's a string
    #[must_use]
    pub fn from_datatype(dt: &'a DataType) -> Option<Self> {
        match dt {
            DataType::String(s) => Some(Self::borrowed(s)),
            _ => None,
        }
    }
}

/// Efficient bytes wrapper for binary data
#[derive(Debug, Clone)]
pub struct CowBytes<'a> {
    data: Cow<'a, [u8]>,
}

impl<'a> CowBytes<'a> {
    /// Creates `CowBytes` from a borrowed byte slice
    #[must_use]
    pub const fn borrowed(bytes: &'a [u8]) -> Self {
        Self { data: Cow::Borrowed(bytes) }
    }

    /// Creates `CowBytes` from owned Vec<u8>
    #[must_use]
    pub const fn owned(bytes: Vec<u8>) -> Self {
        Self { data: Cow::Owned(bytes) }
    }

    /// Gets a byte slice reference
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Converts to owned Vec<u8> if not already owned
    #[must_use]
    pub fn into_owned(self) -> Vec<u8> {
        self.data.into_owned()
    }

    /// Checks if the bytes are borrowed (zero-copy)
    #[must_use]
    pub const fn is_borrowed(&self) -> bool {
        matches!(self.data, Cow::Borrowed(_))
    }

    /// Creates `CowBytes` from a `DataType` if it's raw bytes
    #[must_use]
    pub fn from_datatype(dt: &'a DataType) -> Option<Self> {
        match dt {
            DataType::RawBytes(bytes) => Some(Self::borrowed(bytes)),
            _ => None,
        }
    }
}

/// Efficient map operations that minimize cloning
pub struct CowMap<'a> {
    data: Cow<'a, HashMap<Vec<u8>, DataType>>,
}

impl<'a> CowMap<'a> {
    /// Creates a `CowMap` from a borrowed `HashMap`
    #[must_use]
    pub const fn borrowed(map: &'a HashMap<Vec<u8>, DataType>) -> Self {
        Self { data: Cow::Borrowed(map) }
    }

    /// Creates a `CowMap` from an owned `HashMap`
    #[must_use]
    pub const fn owned(map: HashMap<Vec<u8>, DataType>) -> Self {
        Self { data: Cow::Owned(map) }
    }

    /// Gets a value by key without cloning
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<&DataType> {
        self.data.get(key)
    }

    /// Checks if a key exists
    #[must_use]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.data.contains_key(key)
    }

    /// Gets the number of entries
    #[must_use]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Checks if the map is empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Iterates over key-value pairs without cloning
    pub fn iter(&self) -> impl Iterator<Item = (&Vec<u8>, &DataType)> {
        self.data.iter()
    }

    /// Converts to owned `HashMap` if not already owned
    #[must_use]
    pub fn into_owned(self) -> HashMap<Vec<u8>, DataType> {
        self.data.into_owned()
    }

    /// Checks if the map is borrowed (zero-copy)
    #[must_use]
    pub const fn is_borrowed(&self) -> bool {
        matches!(self.data, Cow::Borrowed(_))
    }
}

/// Utility functions for efficient data operations
pub struct CowUtils;

impl CowUtils {
    /// Efficiently converts a `DataType` to a string representation without unnecessary cloning
    #[must_use]
    pub fn datatype_to_string_cow(dt: &DataType) -> Cow<'_, str> {
        match dt {
            DataType::String(s) => Cow::Borrowed(s),
            DataType::Integer(i) => Cow::Owned(i.to_string()),
            DataType::Float(f) => Cow::Owned(f.to_string()),
            DataType::Boolean(b) => Cow::Borrowed(if *b { "true" } else { "false" }),
            DataType::Null => Cow::Borrowed("NULL"),
            DataType::RawBytes(bytes) => Cow::Owned(String::from_utf8_lossy(bytes).into_owned()),
            DataType::Vector(vec) => Cow::Owned(format!(
                "[{}]",
                vec.data
                    .iter()
                    .map(std::string::ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
            DataType::Map(_) => Cow::Borrowed("{Map}"),
            DataType::JsonBlob(_) => Cow::Borrowed("{JsonBlob}"),
        }
    }

    /// Efficiently compares two `DataTypes` without cloning
    #[must_use]
    pub fn datatype_equals_efficient(left: &DataType, right: &DataType) -> bool {
        match (left, right) {
            (DataType::String(a), DataType::String(b)) => a == b,
            (DataType::Integer(a), DataType::Integer(b)) => a == b,
            (DataType::Float(a), DataType::Float(b)) => (a - b).abs() < f64::EPSILON,
            (DataType::Boolean(a), DataType::Boolean(b)) => a == b,
            (DataType::Null, DataType::Null) => true,
            (DataType::RawBytes(a), DataType::RawBytes(b)) => a == b,
            (DataType::Vector(a), DataType::Vector(b)) => {
                a.data.len() == b.data.len()
                    && a.data.iter().zip(b.data.iter()).all(|(x, y)| (x - y).abs() < f32::EPSILON)
            }
            _ => false,
        }
    }

    /// Efficiently extracts string data from `DataType` without cloning when possible
    #[must_use]
    pub fn extract_string_cow(dt: &DataType) -> Option<Cow<'_, str>> {
        match dt {
            DataType::String(s) => Some(Cow::Borrowed(s)),
            DataType::Integer(i) => Some(Cow::Owned(i.to_string())),
            DataType::Float(f) => Some(Cow::Owned(f.to_string())),
            DataType::Boolean(b) => Some(Cow::Borrowed(if *b { "true" } else { "false" })),
            _ => None,
        }
    }

    /// Efficiently extracts numeric data from `DataType`
    #[must_use]
    pub fn extract_number(dt: &DataType) -> Option<f64> {
        match dt {
            DataType::Integer(i) => Some(*i as f64),
            DataType::Float(f) => Some(*f),
            DataType::String(s) => s.parse().ok(),
            _ => None,
        }
    }

    /// Creates a vector of `CowKeyValue` from a slice of tuples without cloning when possible
    #[must_use]
    pub fn create_cow_pairs(pairs: &[(Vec<u8>, DataType)]) -> Vec<CowKeyValue<'_>> {
        pairs.iter().map(|(k, v)| CowKeyValue::borrowed(k, v)).collect()
    }

    /// Efficiently filters key-value pairs based on a predicate
    pub fn filter_pairs<F>(pairs: &[(Vec<u8>, DataType)], predicate: F) -> Vec<CowKeyValue<'_>>
    where
        F: Fn(&[u8], &DataType) -> bool,
    {
        pairs
            .iter()
            .filter(|(k, v)| predicate(k, v))
            .map(|(k, v)| CowKeyValue::borrowed(k, v))
            .collect()
    }
}

/// Performance metrics for tracking COW efficiency
#[derive(Debug, Default, Clone)]
pub struct CowMetrics {
    pub borrowed_operations: u64,
    pub cloned_operations: u64,
    pub total_operations: u64,
}

impl CowMetrics {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_borrowed(&mut self) {
        self.borrowed_operations = self.borrowed_operations.saturating_add(1);
        self.total_operations = self.total_operations.saturating_add(1);
    }

    pub fn record_cloned(&mut self) {
        self.cloned_operations = self.cloned_operations.saturating_add(1);
        self.total_operations = self.total_operations.saturating_add(1);
    }

    #[must_use]
    pub fn efficiency_ratio(&self) -> f64 {
        if self.total_operations == 0 {
            0.0
        } else {
            // Use explicit conversion to handle potential precision loss
            #[allow(clippy::cast_precision_loss)]
            let borrowed = self.borrowed_operations as f64;
            #[allow(clippy::cast_precision_loss)]
            let total = self.total_operations as f64;
            borrowed / total
        }
    }

    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cow_key_value() {
        let key = b"test_key";
        let value = DataType::String("test_value".to_string());

        let cow_kv = CowKeyValue::borrowed(key, &value);
        assert!(cow_kv.is_borrowed());
        assert_eq!(cow_kv.key(), key);
        assert_eq!(cow_kv.value(), &value);
    }

    #[test]
    fn test_cow_string() {
        let original = "test string";
        let cow_str = CowString::borrowed(original);

        assert!(cow_str.is_borrowed());
        assert_eq!(cow_str.as_str(), original);

        let owned_str = cow_str.into_owned();
        assert_eq!(owned_str, original);
    }

    #[test]
    fn test_cow_bytes() {
        let original = b"test bytes";
        let cow_bytes = CowBytes::borrowed(original);

        assert!(cow_bytes.is_borrowed());
        assert_eq!(cow_bytes.as_bytes(), original);

        let owned_bytes = cow_bytes.into_owned();
        assert_eq!(owned_bytes, original);
    }

    #[test]
    fn test_cow_utils_datatype_to_string() {
        let dt_string = DataType::String("hello".to_string());
        let cow_result = CowUtils::datatype_to_string_cow(&dt_string);
        assert!(matches!(cow_result, Cow::Borrowed(_)));
        assert_eq!(cow_result, "hello");

        let dt_int = DataType::Integer(42);
        let cow_result = CowUtils::datatype_to_string_cow(&dt_int);
        assert!(matches!(cow_result, Cow::Owned(_)));
        assert_eq!(cow_result, "42");
    }

    #[test]
    fn test_cow_utils_datatype_equals() {
        let dt1 = DataType::String("hello".to_string());
        let dt2 = DataType::String("hello".to_string());
        let dt3 = DataType::String("world".to_string());

        assert!(CowUtils::datatype_equals_efficient(&dt1, &dt2));
        assert!(!CowUtils::datatype_equals_efficient(&dt1, &dt3));
    }

    #[test]
    fn test_cow_metrics() {
        let mut metrics = CowMetrics::new();

        metrics.record_borrowed();
        metrics.record_borrowed();
        metrics.record_cloned();

        assert_eq!(metrics.borrowed_operations, 2);
        assert_eq!(metrics.cloned_operations, 1);
        assert_eq!(metrics.total_operations, 3);
        assert!((metrics.efficiency_ratio() - 2.0 / 3.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_cow_map() {
        let mut map = HashMap::new();
        map.insert(b"key1".to_vec(), DataType::String("value1".to_string()));
        map.insert(b"key2".to_vec(), DataType::Integer(42));

        let cow_map = CowMap::borrowed(&map);
        assert!(cow_map.is_borrowed());
        assert_eq!(cow_map.len(), 2);
        assert!(cow_map.contains_key(b"key1"));

        if let Some(DataType::String(s)) = cow_map.get(b"key1") {
            assert_eq!(s, "value1");
        } else {
            panic!("Expected string value");
        }
    }

    #[test]
    fn test_cow_utils_create_pairs() {
        let pairs = vec![
            (b"key1".to_vec(), DataType::String("value1".to_string())),
            (b"key2".to_vec(), DataType::Integer(42)),
        ];

        let cow_pairs = CowUtils::create_cow_pairs(&pairs);
        assert_eq!(cow_pairs.len(), 2);
        assert!(cow_pairs[0].is_borrowed());
        assert!(cow_pairs[1].is_borrowed());
    }

    #[test]
    fn test_cow_utils_filter_pairs() {
        let pairs = vec![
            (b"key1".to_vec(), DataType::String("value1".to_string())),
            (b"key2".to_vec(), DataType::Integer(42)),
            (b"key3".to_vec(), DataType::String("value3".to_string())),
        ];

        let filtered = CowUtils::filter_pairs(&pairs, |_k, v| matches!(v, DataType::String(_)));

        assert_eq!(filtered.len(), 2);
        assert!(filtered.iter().all(|kv| kv.is_borrowed()));
    }
}
