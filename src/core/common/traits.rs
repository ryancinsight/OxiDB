use std::io::{Read, Write};
// Assuming OxidbError will be defined in crate::core::common::error
// Adjust the path if error.rs is structured within a module e.g. crate::core::common::error::OxidbError
use crate::core::common::OxidbError; // Changed

/// Trait for serializing data of type T into a byte stream.
pub trait DataSerializer<T> {
    fn serialize<W: Write>(value: &T, writer: &mut W) -> Result<(), OxidbError>;
}

/// Trait for deserializing data of type T from a byte stream.
pub trait DataDeserializer<T> {
    fn deserialize<R: Read>(reader: &mut R) -> Result<T, OxidbError>;
}
