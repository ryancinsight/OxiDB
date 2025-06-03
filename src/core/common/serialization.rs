// src/core/common/serialization.rs

use crate::core::types::DataType;
use crate::core::common::error::DbError; // Already present
use serde_json;
use crate::core::common::traits::{DataSerializer, DataDeserializer}; // Added
use std::io::{Read, Write}; // Added

/// Serializes a DataType into a Vec<u8> using JSON.
pub fn serialize_data_type(data_type: &DataType) -> Result<Vec<u8>, DbError> {
    serde_json::to_vec(data_type).map_err(|e| DbError::SerializationError(e.to_string()))
}

/// Deserializes a Vec<u8> (expected to be JSON) into a DataType.
pub fn deserialize_data_type(bytes: &[u8]) -> Result<DataType, DbError> {
    serde_json::from_slice(bytes).map_err(|e| DbError::DeserializationError(e.to_string()))
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
        let original = DataType::JsonBlob(json!({ "name": "oxidb", "version": 0.1 }));
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
            DbError::DeserializationError(msg) => {
                assert!(msg.contains("expected value at line 1 column 1"));
            }
            _ => panic!("Expected DeserializationError"),
        }
    }
    
    #[test]
    fn test_deserialize_wrong_json_structure() {
        // This JSON is valid, but doesn't match the DataType enum structure
        let bytes = br#"{"type": "UnknownType", "value": "some_value"}"#;
        let result = deserialize_data_type(bytes);
        assert!(result.is_err());
        match result.unwrap_err() {
            DbError::DeserializationError(msg) => {
                // The exact error message might vary based on serde's internal logic
                // It might complain about missing fields for any of the DataType variants
                // or an unknown variant.
                println!("Deserialization error for wrong structure: {}", msg); // For debugging
                assert!(msg.contains("missing field") || msg.contains("unknown variant"));
            }
            _ => panic!("Expected DeserializationError for wrong JSON structure"),
        }
    }
}

// Implementations for Vec<u8>
impl DataSerializer<Vec<u8>> for Vec<u8> {
    fn serialize<W: Write>(value: &Vec<u8>, writer: &mut W) -> Result<(), DbError> {
        let len = value.len() as u64;
        writer.write_all(&len.to_be_bytes())?; // Relies on From<std::io::Error> for DbError
        writer.write_all(value)?;
        Ok(())
    }
}

impl DataDeserializer<Vec<u8>> for Vec<u8> {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Vec<u8>, DbError> {
        let mut len_bytes = [0u8; 8];
        reader.read_exact(&mut len_bytes)?; // Relies on From<std::io::Error> for DbError
        let len = u64::from_be_bytes(len_bytes) as usize;
        // Basic protection against extremely large allocations
        if len > 1_000_000_000 { // 1GB limit, adjust as needed
            return Err(DbError::DeserializationError(format!("Vec<u8> length {} exceeds maximum allowed size", len)));
        }
        let mut buffer = vec![0u8; len];
        reader.read_exact(&mut buffer)?;
        Ok(buffer)
    }
}
