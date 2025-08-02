//! Pure Rust binary serialization compatible with bincode format
//! 
//! This module provides binary serialization that replaces the external
//! bincode dependency, following the YAGNI principle by implementing
//! only what we need for WAL log records.

use crate::core::common::{OxidbError, io_utils::{IoResultExt, ReadExt, WriteExt}};
use std::io::{Read, Write};

/// Serialize a value to a writer in bincode-compatible format
pub fn serialize<W: Write, T: Serialize>(value: &T, writer: &mut W) -> Result<(), OxidbError> {
    value.serialize(writer)
}

/// Deserialize a value from a reader in bincode-compatible format
pub fn deserialize<R: Read, T: Deserialize>(reader: &mut R) -> Result<T, OxidbError> {
    T::deserialize(reader)
}

/// Serialize a value to a byte vector
pub fn serialize_to_vec<T: Serialize>(value: &T) -> Result<Vec<u8>, OxidbError> {
    let mut buffer = Vec::new();
    serialize(value, &mut buffer)?;
    Ok(buffer)
}

/// Trait for types that can be serialized
pub trait Serialize {
    /// Serialize self to a writer
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), OxidbError>;
}

/// Trait for types that can be deserialized
pub trait Deserialize: Sized {
    /// Deserialize from a reader
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, OxidbError>;
}

// Implement for primitive types
impl Serialize for u8 {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), OxidbError> {
        writer.write_all_oxidb(&[*self])
    }
}

impl Deserialize for u8 {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, OxidbError> {
        let mut buf = [0u8; 1];
        reader.read_exact_oxidb(&mut buf)?;
        Ok(buf[0])
    }
}

impl Serialize for u64 {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), OxidbError> {
        writer.write_all_oxidb(&self.to_le_bytes())
    }
}

impl Deserialize for u64 {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, OxidbError> {
        let mut buf = [0u8; 8];
        reader.read_exact_oxidb(&mut buf)?;
        Ok(u64::from_le_bytes(buf))
    }
}

impl Serialize for u32 {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), OxidbError> {
        writer.write_all_oxidb(&self.to_le_bytes())
    }
}

impl Deserialize for u32 {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, OxidbError> {
        let mut buf = [0u8; 4];
        reader.read_exact_oxidb(&mut buf)?;
        Ok(u32::from_le_bytes(buf))
    }
}

impl Serialize for i32 {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), OxidbError> {
        writer.write_all_oxidb(&self.to_le_bytes())
    }
}

impl Deserialize for i32 {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, OxidbError> {
        let mut buf = [0u8; 4];
        reader.read_exact_oxidb(&mut buf)?;
        Ok(i32::from_le_bytes(buf))
    }
}

impl Serialize for Vec<u8> {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), OxidbError> {
        // Write length as u64 (bincode default)
        (self.len() as u64).serialize(writer)?;
        // Write data
        writer.write_all_oxidb(self)
    }
}

impl Deserialize for Vec<u8> {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, OxidbError> {
        // Read length
        let len = u64::deserialize(reader)? as usize;
        // Read data
        let mut data = vec![0u8; len];
        reader.read_exact_oxidb(&mut data)?;
        Ok(data)
    }
}

impl Serialize for String {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), OxidbError> {
        self.as_bytes().to_vec().serialize(writer)
    }
}

impl Deserialize for String {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, OxidbError> {
        let bytes = Vec::<u8>::deserialize(reader)?;
        String::from_utf8(bytes)
            .map_err(|e| OxidbError::Deserialization(format!("Invalid UTF-8: {}", e)))
    }
}

// Implement for Option<T>
impl<T: Serialize> Serialize for Option<T> {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), OxidbError> {
        match self {
            None => 0u8.serialize(writer),
            Some(value) => {
                1u8.serialize(writer)?;
                value.serialize(writer)
            }
        }
    }
}

impl<T: Deserialize> Deserialize for Option<T> {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, OxidbError> {
        let tag = u8::deserialize(reader)?;
        match tag {
            0 => Ok(None),
            1 => Ok(Some(T::deserialize(reader)?)),
            _ => Err(OxidbError::Deserialization(format!("Invalid Option tag: {}", tag)))
        }
    }
}

// Generic Vec implementation
impl<T: Serialize> Serialize for Vec<T> {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), OxidbError> {
        // Use u64 for length to ensure portability
        (self.len() as u64).serialize(writer)?;
        for item in self {
            item.serialize(writer)?;
        }
        Ok(())
    }
}

impl<T: Deserialize> Deserialize for Vec<T> {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, OxidbError> {
        let len = u64::deserialize(reader)?;
        if len > usize::MAX as u64 {
            return Err(OxidbError::Deserialization("Vec length too large".into()));
        }
        let mut vec = Vec::with_capacity(len as usize);
        for _ in 0..len {
            vec.push(T::deserialize(reader)?);
        }
        Ok(vec)
    }
}

// Implementations for ID types
use crate::core::common::types::ids::{PageId, TransactionId, SlotId};

impl Serialize for PageId {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), OxidbError> {
        self.0.serialize(writer)
    }
}

impl Deserialize for PageId {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, OxidbError> {
        Ok(PageId(u64::deserialize(reader)?))
    }
}

impl Serialize for TransactionId {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), OxidbError> {
        self.0.serialize(writer)
    }
}

impl Deserialize for TransactionId {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, OxidbError> {
        Ok(TransactionId(u64::deserialize(reader)?))
    }
}

impl Serialize for SlotId {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), OxidbError> {
        self.0.serialize(writer)
    }
}

impl Deserialize for SlotId {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, OxidbError> {
        Ok(SlotId(u16::deserialize(reader)?))
    }
}

// Implementation for u16
impl Serialize for u16 {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), OxidbError> {
        writer.write_all_oxidb(&self.to_le_bytes())
    }
}

impl Deserialize for u16 {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, OxidbError> {
        let mut bytes = [0u8; 2];
        reader.read_exact_oxidb(&mut bytes)?;
        Ok(u16::from_le_bytes(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_u8_roundtrip() {
        let value = 42u8;
        let serialized = serialize_to_vec(&value).unwrap();
        let deserialized: u8 = deserialize(&mut &serialized[..]).unwrap();
        assert_eq!(value, deserialized);
    }
    
    #[test]
    fn test_u64_roundtrip() {
        let value = 0x1234_5678_9ABC_DEF0u64;
        let serialized = serialize_to_vec(&value).unwrap();
        let deserialized: u64 = deserialize(&mut &serialized[..]).unwrap();
        assert_eq!(value, deserialized);
    }
    
    #[test]
    fn test_vec_roundtrip() {
        let value = vec![1, 2, 3, 4, 5];
        let serialized = serialize_to_vec(&value).unwrap();
        let deserialized: Vec<u8> = deserialize(&mut &serialized[..]).unwrap();
        assert_eq!(value, deserialized);
    }
    
    #[test]
    fn test_string_roundtrip() {
        let value = "Hello, World!".to_string();
        let serialized = serialize_to_vec(&value).unwrap();
        let deserialized: String = deserialize(&mut &serialized[..]).unwrap();
        assert_eq!(value, deserialized);
    }
    
    #[test]
    fn test_option_roundtrip() {
        let value1: Option<u64> = Some(42);
        let serialized1 = serialize_to_vec(&value1).unwrap();
        let deserialized1: Option<u64> = deserialize(&mut &serialized1[..]).unwrap();
        assert_eq!(value1, deserialized1);
        
        let value2: Option<u64> = None;
        let serialized2 = serialize_to_vec(&value2).unwrap();
        let deserialized2: Option<u64> = deserialize(&mut &serialized2[..]).unwrap();
        assert_eq!(value2, deserialized2);
    }
}