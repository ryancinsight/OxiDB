//! Pure Rust binary serialization compatible with bincode format
//! 
//! This module provides binary serialization that replaces the external
//! bincode dependency, following the YAGNI principle by implementing
//! only what we need for WAL log records.

use crate::core::common::{OxidbError, io_utils::{ReadExt, WriteExt}};
use std::io::{Read, Write};

/// Serialize a value to a writer in bincode-compatible format
/// 
/// # Errors
/// 
/// Returns an error if the serialization fails or if writing to the writer fails
pub fn serialize<W: Write, T: Serialize>(value: &T, writer: &mut W) -> Result<(), OxidbError> {
    value.serialize(writer)
}

/// Deserialize a value from a reader in bincode-compatible format
/// 
/// # Errors
/// 
/// Returns an error if the deserialization fails or if reading from the reader fails
pub fn deserialize<R: Read, T: Deserialize>(reader: &mut R) -> Result<T, OxidbError> {
    T::deserialize(reader)
}

/// Serialize a value to a byte vector
/// 
/// # Errors
/// 
/// Returns an error if the serialization fails
pub fn serialize_to_vec<T: Serialize>(value: &T) -> Result<Vec<u8>, OxidbError> {
    let mut buffer = Vec::new();
    serialize(value, &mut buffer)?;
    Ok(buffer)
}

/// Trait for types that can be serialized
pub trait Serialize {
    /// Serialize self to a writer
    /// 
    /// # Errors
    /// 
    /// Returns an error if serialization fails or if writing to the writer fails
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), OxidbError>;
}

/// Trait for types that can be deserialized
pub trait Deserialize: Sized {
    /// Deserialize from a reader
    /// 
    /// # Errors
    /// 
    /// Returns an error if deserialization fails or if reading from the reader fails
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
        // Serialize length first
        (self.len() as u64).serialize(writer)?;
        // Then write bytes directly
        writer.write_all(self.as_bytes()).map_err(OxidbError::Io)
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

// Generic Vec implementation (excluding Vec<u8> which has its own implementation)
// Note: This would require negative trait bounds which Rust doesn't support well
// So we'll implement for specific types as needed

// Vec<ActiveTransactionInfo> implementation
use crate::core::wal::log_record::{ActiveTransactionInfo, DirtyPageInfo};

impl Serialize for Vec<ActiveTransactionInfo> {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), OxidbError> {
        (self.len() as u64).serialize(writer)?;
        for item in self {
            item.serialize(writer)?;
        }
        Ok(())
    }
}

impl Deserialize for Vec<ActiveTransactionInfo> {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, OxidbError> {
        let len = u64::deserialize(reader)?;
        if len > usize::MAX as u64 {
            return Err(OxidbError::Deserialization(
                "Vector length exceeds maximum size".to_string()
            ));
        }
        let len = len as usize;
        let mut vec = Vec::with_capacity(len);
        for _ in 0..len {
            vec.push(ActiveTransactionInfo::deserialize(reader)?);
        }
        Ok(vec)
    }
}

// Vec<DirtyPageInfo> implementation
impl Serialize for Vec<DirtyPageInfo> {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), OxidbError> {
        (self.len() as u64).serialize(writer)?;
        for item in self {
            item.serialize(writer)?;
        }
        Ok(())
    }
}

impl Deserialize for Vec<DirtyPageInfo> {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, OxidbError> {
        let len = u64::deserialize(reader)?;
        if len > usize::MAX as u64 {
            return Err(OxidbError::Deserialization(
                "Vector length exceeds maximum size".to_string()
            ));
        }
        let len = len as usize;
        let mut vec = Vec::with_capacity(len);
        for _ in 0..len {
            vec.push(DirtyPageInfo::deserialize(reader)?);
        }
        Ok(vec)
    }
}

// Vec<Vec<u8>> implementation for HashMap values
impl Serialize for Vec<Vec<u8>> {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), OxidbError> {
        (self.len() as u64).serialize(writer)?;
        for item in self {
            item.serialize(writer)?;
        }
        Ok(())
    }
}

impl Deserialize for Vec<Vec<u8>> {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, OxidbError> {
        let len = u64::deserialize(reader)? as usize;
        let mut vec = Vec::with_capacity(len);
        for _ in 0..len {
            vec.push(Vec::<u8>::deserialize(reader)?);
        }
        Ok(vec)
    }
}

// Vec<f32> implementation for Vector values
impl Serialize for Vec<f32> {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), OxidbError> {
        (self.len() as u64).serialize(writer)?;
        for item in self {
            item.serialize(writer)?;
        }
        Ok(())
    }
}

impl Deserialize for Vec<f32> {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, OxidbError> {
        let len = u64::deserialize(reader)?;
        if len > usize::MAX as u64 {
            return Err(OxidbError::Deserialization(
                "Vector length exceeds maximum size".to_string()
            ));
        }
        // Additional safety check: prevent excessive allocations (1GB limit for f32 vec)
        const MAX_ELEMENTS: u64 = 256 * 1024 * 1024; // 256M elements = 1GB for f32
        if len > MAX_ELEMENTS {
            return Err(OxidbError::Deserialization(
                format!("Vector length {} exceeds maximum allowed elements {}", len, MAX_ELEMENTS)
            ));
        }
        let len = len as usize;
        let mut vec = Vec::with_capacity(len);
        for _ in 0..len {
            vec.push(f32::deserialize(reader)?);
        }
        Ok(vec)
    }
}

// Implementations for ID types
use crate::core::common::types::ids::{PageId, TransactionId, SlotId};

// Implementations for Row and Value types
use crate::core::common::types::row::Row;
use crate::core::common::types::value::Value;

impl Serialize for Row {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), OxidbError> {
        self.values.serialize(writer)
    }
}

impl Deserialize for Row {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, OxidbError> {
        Ok(Row {
            values: Vec::<Value>::deserialize(reader)?,
        })
    }
}

// Vec<Value> implementation
impl Serialize for Vec<Value> {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), OxidbError> {
        (self.len() as u64).serialize(writer)?;
        for item in self {
            item.serialize(writer)?;
        }
        Ok(())
    }
}

impl Deserialize for Vec<Value> {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, OxidbError> {
        let len = u64::deserialize(reader)?;
        if len > usize::MAX as u64 {
            return Err(OxidbError::Deserialization(
                "Vector length exceeds maximum size".to_string()
            ));
        }
        let len = len as usize;
        let mut vec = Vec::with_capacity(len);
        for _ in 0..len {
            vec.push(Value::deserialize(reader)?);
        }
        Ok(vec)
    }
}

impl Serialize for Value {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), OxidbError> {
        match self {
            Value::Integer(i) => {
                0u8.serialize(writer)?;
                i.serialize(writer)?;
            }
            Value::Float(f) => {
                1u8.serialize(writer)?;
                f.serialize(writer)?;
            }
            Value::Text(s) => {
                2u8.serialize(writer)?;
                s.serialize(writer)?;
            }
            Value::Boolean(b) => {
                3u8.serialize(writer)?;
                b.serialize(writer)?;
            }
            Value::Blob(data) => {
                4u8.serialize(writer)?;
                data.serialize(writer)?;
            }
            Value::Vector(vec) => {
                5u8.serialize(writer)?;
                vec.serialize(writer)?;
            }
            Value::Null => {
                6u8.serialize(writer)?;
            }
        }
        Ok(())
    }
}

impl Deserialize for Value {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, OxidbError> {
        match u8::deserialize(reader)? {
            0 => Ok(Value::Integer(i64::deserialize(reader)?)),
            1 => Ok(Value::Float(f64::deserialize(reader)?)),
            2 => Ok(Value::Text(String::deserialize(reader)?)),
            3 => Ok(Value::Boolean(bool::deserialize(reader)?)),
            4 => Ok(Value::Blob(Vec::<u8>::deserialize(reader)?)),
            5 => Ok(Value::Vector(Vec::<f32>::deserialize(reader)?)),
            6 => Ok(Value::Null),
            n => Err(OxidbError::Deserialization(format!("Invalid Value variant: {}", n))),
        }
    }
}

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

// Implementation for i64
impl Serialize for i64 {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), OxidbError> {
        writer.write_all_oxidb(&self.to_le_bytes())
    }
}

impl Deserialize for i64 {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, OxidbError> {
        let mut bytes = [0u8; 8];
        reader.read_exact_oxidb(&mut bytes)?;
        Ok(i64::from_le_bytes(bytes))
    }
}

// Implementation for f64
impl Serialize for f64 {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), OxidbError> {
        writer.write_all_oxidb(&self.to_le_bytes())
    }
}

impl Deserialize for f64 {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, OxidbError> {
        let mut bytes = [0u8; 8];
        reader.read_exact_oxidb(&mut bytes)?;
        Ok(f64::from_le_bytes(bytes))
    }
}

// Implementation for bool
impl Serialize for bool {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), OxidbError> {
        (*self as u8).serialize(writer)
    }
}

impl Deserialize for bool {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, OxidbError> {
        Ok(u8::deserialize(reader)? != 0)
    }
}

// Implementation for f32
impl Serialize for f32 {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), OxidbError> {
        writer.write_all_oxidb(&self.to_le_bytes())
    }
}

impl Deserialize for f32 {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, OxidbError> {
        let mut bytes = [0u8; 4];
        reader.read_exact_oxidb(&mut bytes)?;
        Ok(f32::from_le_bytes(bytes))
    }
}

// Implementation for HashMap
use std::collections::HashMap;

impl<K, V> Serialize for HashMap<K, V> 
where 
    K: Serialize,
    V: Serialize,
{
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), OxidbError> {
        // Write the number of entries
        (self.len() as u64).serialize(writer)?;
        
        // Write each key-value pair
        for (key, value) in self {
            key.serialize(writer)?;
            value.serialize(writer)?;
        }
        
        Ok(())
    }
}

impl<K, V> Deserialize for HashMap<K, V> 
where 
    K: Deserialize + Eq + std::hash::Hash,
    V: Deserialize,
{
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, OxidbError> {
        // Read the number of entries
        let len = u64::deserialize(reader)? as usize;
        
        let mut map = HashMap::with_capacity(len);
        
        // Read each key-value pair
        for _ in 0..len {
            let key = K::deserialize(reader)?;
            let value = V::deserialize(reader)?;
            map.insert(key, value);
        }
        
        Ok(map)
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