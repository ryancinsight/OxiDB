use std::io::{Read, Write};
use crate::core::common::traits::{DataSerializer, DataDeserializer};
use crate::core::common::error::DbError;

// --- u64 Implementation ---
impl DataSerializer<u64> for u64 {
    fn serialize<W: Write>(value: &u64, writer: &mut W) -> Result<(), DbError> {
        writer.write_all(&value.to_be_bytes()).map_err(DbError::IoError)
    }
}

impl DataDeserializer<u64> for u64 {
    fn deserialize<R: Read>(reader: &mut R) -> Result<u64, DbError> {
        let mut bytes = [0u8; 8];
        reader.read_exact(&mut bytes).map_err(DbError::IoError)?;
        Ok(u64::from_be_bytes(bytes))
    }
}

// --- String Implementation ---
impl DataSerializer<String> for String {
    fn serialize<W: Write>(value: &String, writer: &mut W) -> Result<(), DbError> {
        let bytes = value.as_bytes();
        let len = bytes.len() as u64;
        u64::serialize(&len, writer)?; // Serialize length
        writer.write_all(bytes).map_err(DbError::IoError) // Serialize bytes
    }
}

impl DataDeserializer<String> for String {
    fn deserialize<R: Read>(reader: &mut R) -> Result<String, DbError> {
        let len = u64::deserialize(reader)? as usize; // Deserialize length
        let mut buffer = vec![0u8; len];
        reader.read_exact(&mut buffer).map_err(DbError::IoError)?;
        String::from_utf8(buffer)
            .map_err(|e| DbError::DeserializationError(format!("UTF-8 conversion error: {}", e)))
    }
}

// --- Vec<u8> Implementation ---
impl DataSerializer<Vec<u8>> for Vec<u8> {
    fn serialize<W: Write>(value: &Vec<u8>, writer: &mut W) -> Result<(), DbError> {
        let len = value.len() as u64;
        u64::serialize(&len, writer)?; // Serialize length
        writer.write_all(value).map_err(DbError::IoError) // Serialize bytes
    }
}

impl DataDeserializer<Vec<u8>> for Vec<u8> {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Vec<u8>, DbError> {
        let len = u64::deserialize(reader)? as usize; // Deserialize length
        let mut buffer = vec![0u8; len];
        reader.read_exact(&mut buffer).map_err(DbError::IoError)?;
        Ok(buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor; // Ensure Cursor is imported for tests

    fn test_round_trip<T>(value: &T) -> Result<(), DbError>
    where
        T: DataSerializer<T> + DataDeserializer<T> + PartialEq + std::fmt::Debug,
    {
        let mut buffer = Vec::new();
        T::serialize(value, &mut buffer)?;
        let deserialized = T::deserialize(&mut Cursor::new(buffer))?;
        assert_eq!(*value, deserialized);
        Ok(())
    }

    #[test]
    fn u64_round_trip() {
        test_round_trip(&0u64).unwrap();
        test_round_trip(&1234567890123456789u64).unwrap();
        test_round_trip(&u64::MAX).unwrap();
    }

    #[test]
    fn string_round_trip() {
        test_round_trip(&String::from("hello world")).unwrap();
        test_round_trip(&String::from("")).unwrap();
        test_round_trip(&String::from("a").repeat(1000)).unwrap();
    }

    #[test]
    fn vec_u8_round_trip() {
        test_round_trip(&vec![1, 2, 3, 4, 5]).unwrap();
        test_round_trip(&Vec::<u8>::new()).unwrap();
        test_round_trip(&vec![0u8; 1000]).unwrap();
    }

    #[test]
    fn string_deserialize_insufficient_data_for_length() {
        let bytes = vec![0,0,0,0,0,0,0]; // 7 bytes, not enough for u64 length
        let result = String::deserialize(&mut Cursor::new(bytes));
        // Expect an IoError from read_exact within u64::deserialize
        match result {
            Err(DbError::IoError(e)) => {
                assert_eq!(e.kind(), std::io::ErrorKind::UnexpectedEof);
            }
            _ => panic!("Expected IoError for insufficient length data"),
        }
    }
    
    #[test]
    fn string_deserialize_insufficient_data_for_content() {
        let mut bytes = Vec::new();
        let len: u64 = 10;
        u64::serialize(&len, &mut bytes).unwrap(); // Length is 10 (8 bytes)
        bytes.extend_from_slice(b"short"); // Content is "short" (5 bytes)
        // Total bytes: 8 (for len) + 5 (for "short") = 13 bytes.
        // Reader will try to read 10 bytes for content but only 5 are available after length.
        let result = String::deserialize(&mut Cursor::new(bytes));
         match result {
            Err(DbError::IoError(e)) => {
                assert_eq!(e.kind(), std::io::ErrorKind::UnexpectedEof);
            }
            _ => panic!("Expected IoError for insufficient content data"),
        }
    }

    #[test]
    fn string_deserialize_invalid_utf8() {
        let mut bytes = Vec::new();
        let invalid_utf8: Vec<u8> = vec![0xC3, 0x28]; // Invalid UTF-8 sequence (an isolated start byte)
        let len = invalid_utf8.len() as u64;
        u64::serialize(&len, &mut bytes).unwrap();
        bytes.extend_from_slice(&invalid_utf8);
        let result = String::deserialize(&mut Cursor::new(bytes));
        assert!(matches!(result, Err(DbError::DeserializationError(_))));
    }
}
