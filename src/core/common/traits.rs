use std::io::{Read, Write};
// Assuming DbError will be defined in crate::core::common::error
// Adjust the path if error.rs is structured within a module e.g. crate::core::common::error::DbError
use crate::core::common::error::DbError; 

/// Trait for serializing data of type T into a byte stream.
pub trait DataSerializer<T> {
    fn serialize<W: Write>(value: &T, writer: &mut W) -> Result<(), DbError>;
}

/// Trait for deserializing data of type T from a byte stream.
pub trait DataDeserializer<T> {
    fn deserialize<R: Read>(reader: &mut R) -> Result<T, DbError>;
}
