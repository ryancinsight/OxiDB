// src/core/common/serialization.rs

use crate::core::common::traits::{DataDeserializer, DataSerializer}; // Added
use crate::core::common::OxidbError; // Changed
use crate::core::types::DataType;
use serde_json;
use std::io::{Read, Write}; // Added

/// Serializes a `DataType` into a Vec<u8> using JSON.
pub fn serialize_data_type(data_type: &DataType) -> Result<Vec<u8>, OxidbError> {
    serde_json::to_vec(data_type).map_err(OxidbError::Json)
}

/// Deserializes a Vec<u8> (expected to be JSON) into a `DataType`.
pub fn deserialize_data_type(bytes: &[u8]) -> Result<DataType, OxidbError> {
    match serde_json::from_slice(bytes) {
        Ok(dt) => Ok(dt),
        Err(e) => {
            let err_string = e.to_string();
            // Print it regardless to see what errors occur
            println!("[deserialize_data_type] Serde JSON error string: '{err_string}'");
            println!(
                "[deserialize_data_type] Bytes as lossy UTF-8 for this error: '{}'",
                String::from_utf8_lossy(bytes)
            );

            if err_string.contains("key must be a string") {
                // Replaced panic with an error return
                return Err(OxidbError::Deserialization(format!(
                    "Key must be a string for JsonSafeMap. Full Error: '{}'. Bytes as lossy UTF-8: '{}'. Bytes as Debug: {:?}",
                    err_string,
                    String::from_utf8_lossy(bytes),
                    bytes
                )));
            }
            Err(OxidbError::Json(e)) // Propagate original error kind
        }
    }
}

// Implementations for Vec<u8>
impl DataSerializer<Self> for Vec<u8> {
    fn serialize<W: Write>(value: &Self, writer: &mut W) -> Result<(), OxidbError> {
        let len = value.len() as u64;
        writer.write_all(&len.to_be_bytes())?; // Relies on From<std::io::Error> for OxidbError
        writer.write_all(value)?;
        Ok(())
    }
}

impl DataDeserializer<Self> for Vec<u8> {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, OxidbError> {
        let mut len_bytes = [0u8; 8];
        reader.read_exact(&mut len_bytes)?; // Relies on From<std::io::Error> for OxidbError
        let len_u64 = u64::from_be_bytes(len_bytes);
        let len = usize::try_from(len_u64).map_err(|_| {
            OxidbError::Deserialization(format!(
                "Vec<u8> length {len_u64} exceeds usize capabilities"
            ))
        })?;
        // Basic protection against extremely large allocations
        if len > 1_000_000_000 {
            // 1GB limit, adjust as needed
            return Err(OxidbError::Deserialization(format!(
                // Changed
                "Vec<u8> length {len} exceeds maximum allowed size"
            )));
        }
        let mut buffer = vec![0u8; len];
        reader.read_exact(&mut buffer)?;
        Ok(buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::DataType;
    use serde_json::json;

    #[test]
    fn test_serialize_deserialize_integer() {
        let original = DataType::Integer(12345);
        let serialized = serialize_data_type(&original).unwrap();
        let deserialized = deserialize_data_type(&serialized).unwrap();
        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_serialize_deserialize_string() {
        let original = DataType::String("hello world".to_string());
        let serialized = serialize_data_type(&original).unwrap();
        let deserialized = deserialize_data_type(&serialized).unwrap();
        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_serialize_deserialize_boolean() {
        let original = DataType::Boolean(true);
        let serialized = serialize_data_type(&original).unwrap();
        let deserialized = deserialize_data_type(&serialized).unwrap();
        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_serialize_deserialize_json_blob() {
        let original = DataType::JsonBlob(crate::core::types::JsonValue(json!({ "name": "oxidb", "version": 0.1 })));
        let serialized = serialize_data_type(&original).unwrap();
        let deserialized = deserialize_data_type(&serialized).unwrap();
        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_deserialize_invalid_data() {
        let bytes = b"this is not valid json";
        let result = deserialize_data_type(bytes);
        assert!(result.is_err());
        match result.unwrap_err() {
            OxidbError::Json(e) => {
                // Changed
                assert!(e.to_string().contains("expected value at line 1 column 1"));
            }
            _ => panic!("Expected OxidbError::Json"), // Changed
        }
    }

    #[test]
    fn test_deserialize_wrong_json_structure() {
        // This JSON is valid, but doesn't match the DataType enum structure
        let bytes = br#"{"type": "UnknownType", "value": "some_value"}"#;
        let result = deserialize_data_type(bytes);
        assert!(result.is_err());
        match result.unwrap_err() {
            OxidbError::Json(e) => {
                // Changed
                // The exact error message might vary based on serde's internal logic
                // It might complain about missing fields for any of the DataType variants
                // or an unknown variant.
                let msg = e.to_string();
                println!("Deserialization error for wrong structure: {}", msg); // For debugging
                assert!(msg.contains("missing field") || msg.contains("unknown variant"));
            }
            _ => panic!("Expected OxidbError::Json for wrong JSON structure"), // Changed
        }
    }
}
