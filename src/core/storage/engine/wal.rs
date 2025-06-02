use std::fs::OpenOptions; // Removed File
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use crc32fast::Hasher;
use crate::core::common::error::DbError; // Corrected path for DbError
use crate::core::common::traits::{DataDeserializer, DataSerializer}; // Corrected path for traits

const PUT_OPERATION: u8 = 0x01;
const DELETE_OPERATION: u8 = 0x02;

/// Represents an entry in the Write-Ahead Log (WAL).
/// Each entry corresponds to a database operation (Put or Delete).
pub enum WalEntry {
    /// Represents a 'Put' operation with a key and a value.
    Put { key: Vec<u8>, value: Vec<u8> },
    /// Represents a 'Delete' operation with a key.
    Delete { key: Vec<u8> },
}

impl DataSerializer<WalEntry> for WalEntry {
    /// Serializes a `WalEntry` into a byte stream.
    /// The format is:
    /// - Operation type (1 byte: 0x01 for Put, 0x02 for Delete)
    /// - Key (length-prefixed Vec<u8>)
    /// - Value (length-prefixed Vec<u8>, only for Put operation)
    /// - CRC32 checksum (4 bytes) of all preceding data in this entry.
    fn serialize<W: Write>(value: &WalEntry, writer: &mut W) -> Result<(), DbError> {
        let mut buffer = Vec::new(); // Buffer to hold data before checksum calculation
        match value {
            WalEntry::Put { key, value } => {
                buffer.push(PUT_OPERATION);
                // Assuming Vec<u8> implements DataSerializer<Vec<u8>>
                // and it's called like Trait::method(value, writer)
                // However, the previous code `key.serialize(&mut buffer)?` suggests
                // Vec<u8> might have an inherent serialize method or uses a different trait.
                // Let's check how Vec<u8>::serialize is defined/used.
                // For now, stick to the pattern from the original code if it compiled,
                // which implies Vec<u8> has its own serialize method.
                // If `key.serialize` refers to `DataSerializer::serialize`, it would need to be
                // `<Vec<u8> as DataSerializer<Vec<u8>>>::serialize(key, &mut buffer)?;`
                // Or, if there's a blanket `impl<T: SomeOtherTrait> DataSerializer<T> for T`.
                // The error was on `impl DataSerializer for WalEntry`, not on `key.serialize`.
                // So, `key.serialize` must be something else, perhaps an inherent method from
                // the `crate::core::common::serialization` which re-exports actual trait methods.

                // Let's assume `key.serialize()` comes from `crate::core::common::serialization::DataSerializer`
                // which was `use crate::core::common::serialization::{DataDeserializer, DataSerializer};`
                // This might be a module providing helper functions that internally call the trait methods.
                // Or, Vec<u8> itself implements a method `serialize`.
                // Given `Vec::<u8>::deserialize(reader)` and `key.serialize(&mut buffer)`, these look like inherent methods
                // or methods from a trait directly implemented by Vec<u8> and brought into scope.

                // The `crate::core::common::serialization` file has:
                // pub trait DataSerializer: Sized { fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), DbError>; }
                // pub trait DataDeserializer: Sized { type Item; fn deserialize<R: Read>(reader: &mut R) -> Result<Self::Item, DbError>; }
                // impl DataSerializer for Vec<u8> { ... }
                // impl DataDeserializer for Vec<u8> { type Item = Vec<u8>; ... }
                // This was the *old* definition. The new one is `DataSerializer<T>`.

                // The current `traits.rs` has `pub trait DataSerializer<T> { fn serialize<W: Write>(value: &T, writer: &mut W) -> Result<(), DbError>; }`
                // If `Vec<u8>` is to be serialized using this, then we need an `impl DataSerializer<Vec<u8>> for Something`.
                // It's likely `impl DataSerializer<Vec<u8>> for Vec<u8>`.
                // Let's assume that's the case:
                <Vec<u8> as DataSerializer<Vec<u8>>>::serialize(key, &mut buffer)?;
                <Vec<u8> as DataSerializer<Vec<u8>>>::serialize(value, &mut buffer)?;
            }
            WalEntry::Delete { key } => {
                buffer.push(DELETE_OPERATION);
                <Vec<u8> as DataSerializer<Vec<u8>>>::serialize(key, &mut buffer)?;
            }
        }

        let mut hasher = Hasher::new();
        hasher.update(&buffer);
        let checksum = hasher.finalize();

        writer.write_all(&buffer)?;
        writer.write_all(&checksum.to_le_bytes())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_wal_entry_serialize_deserialize() {
        let put_entry = WalEntry::Put {
            key: b"test_key".to_vec(),
            value: b"test_value".to_vec(),
        };
        let delete_entry = WalEntry::Delete {
            key: b"test_key_delete".to_vec(),
        };

        let entries = vec![put_entry, delete_entry];

        for original_entry in entries {
            let mut buffer = Vec::new();
            <WalEntry as DataSerializer<WalEntry>>::serialize(&original_entry, &mut buffer).unwrap();

            // Simulate reading the whole buffer as one entry for this specific old test
            // by creating a new cursor for each entry.
            // The new test `test_wal_entry_sequential_deserialization` will handle sequential reads.
            let mut reader = Cursor::new(&buffer);
            
            // Call deserialize to get the entry
            let deserialized_entry = <WalEntry as DataDeserializer<WalEntry>>::deserialize(&mut reader).unwrap();

            match (&original_entry, &deserialized_entry) {
                (WalEntry::Put { key: ok, value: ov }, WalEntry::Put { key: dk, value: dv }) => {
                    assert_eq!(ok, dk);
                    assert_eq!(ov, dv);
                }
                (WalEntry::Delete { key: ok }, WalEntry::Delete { key: dk }) => {
                    assert_eq!(ok, dk);
                }
                _ => panic!("Mismatched entry types after simulated deserialization"),
            }
        }
    }

    #[test]
    fn test_wal_entry_sequential_deserialization() {
        let entry1 = WalEntry::Put {
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
        };
        let entry2 = WalEntry::Delete {
            key: b"key1".to_vec(),
        };
        let entry3 = WalEntry::Put {
            key: b"key2".to_vec(),
            value: b"value2_longer".to_vec(),
        };

        let mut buffer = Vec::new();
        <WalEntry as DataSerializer<WalEntry>>::serialize(&entry1, &mut buffer).unwrap();
        <WalEntry as DataSerializer<WalEntry>>::serialize(&entry2, &mut buffer).unwrap();
        <WalEntry as DataSerializer<WalEntry>>::serialize(&entry3, &mut buffer).unwrap();

        let mut cursor = Cursor::new(&buffer);

        // Deserialize first entry
        match <WalEntry as DataDeserializer<WalEntry>>::deserialize(&mut cursor) {
            Ok(WalEntry::Put { key, value }) => {
                assert_eq!(key, b"key1".to_vec());
                assert_eq!(value, b"value1".to_vec());
            }
            _ => panic!("Deserialization of entry1 failed or wrong type"),
        }

        // Deserialize second entry
        match <WalEntry as DataDeserializer<WalEntry>>::deserialize(&mut cursor) {
            Ok(WalEntry::Delete { key }) => {
                assert_eq!(key, b"key1".to_vec());
            }
            _ => panic!("Deserialization of entry2 failed or wrong type"),
        }

        // Deserialize third entry
        match <WalEntry as DataDeserializer<WalEntry>>::deserialize(&mut cursor) {
            Ok(WalEntry::Put { key, value }) => {
                assert_eq!(key, b"key2".to_vec());
                assert_eq!(value, b"value2_longer".to_vec());
            }
            _ => panic!("Deserialization of entry3 failed or wrong type"),
        }

        // Try to deserialize again, expecting EOF
        match <WalEntry as DataDeserializer<WalEntry>>::deserialize(&mut cursor) {
            Err(DbError::IoError(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // This is the expected outcome
            }
            Ok(_) => panic!("Expected EOF error, but got an entry"),
            Err(e) => panic!("Expected EOF error, but got a different error: {:?}", e),
        }
    }
}

#[derive(Debug)]
pub struct WalWriter {
    wal_file_path: PathBuf,
}

impl WalWriter {
    /// Creates a new `WalWriter`.
    /// The WAL file path is derived from the main database file path
    /// by appending ".wal" to its extension (e.g., "data.db" -> "data.db.wal")
    /// or by setting the extension to "wal" if the DB file has no extension.
    pub fn new(db_file_path: &Path) -> Self {
        let wal_file_path = db_file_path.with_extension(
            db_file_path.extension()
                .map(|ext| ext.to_str().unwrap_or("").to_owned() + ".wal")
                .unwrap_or_else(|| "wal".to_string())
        );
        WalWriter { wal_file_path }
    }

    /// Logs a `WalEntry` to the WAL file.
    /// This involves serializing the entry and appending it to the file.
    /// The write is flushed and synced to disk to ensure durability.
    pub fn log_entry(&self, entry: &WalEntry) -> Result<(), DbError> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.wal_file_path)
            .map_err(|e| DbError::IoError(e))?;

        let mut writer = BufWriter::new(file);
        <WalEntry as DataSerializer<WalEntry>>::serialize(entry, &mut writer)?;
        writer.flush().map_err(|e| DbError::IoError(e))?;
        writer.get_ref().sync_all().map_err(|e| DbError::IoError(e))?;

        Ok(())
    }
}

impl WalEntry {
    // This is now the implementation of DataDeserializer<WalEntry>::deserialize
}

// Now, let's make deserialize_from_reader the official implementation for DataDeserializer<WalEntry>
impl DataDeserializer<WalEntry> for WalEntry {
    /// Deserializes a single `WalEntry` from a reader.
    ///
    /// This method reads bytes incrementally to parse one WAL entry. The process is:
    /// 1. Read the operation type (1 byte).
    ///    - If EOF occurs here, it's treated as an unexpected end of stream, returning
    ///      `DbError::IoError` with `ErrorKind::UnexpectedEof`. This indicates a potentially
    ///      truncated WAL file if an entry was expected.
    /// 2. Based on the operation type, read the key and (if applicable) value.
    ///    These are deserialized as length-prefixed `Vec<u8>`.
    ///    - EOF during these reads will also result in `DbError::IoError`.
    /// 3. Read the 4-byte CRC32 checksum.
    ///    - EOF here results in `DbError::IoError`.
    /// 4. Compute the checksum of the data read so far (operation type + key bytes + value bytes).
    /// 5. Compare the computed checksum with the checksum read from the stream.
    ///    - A mismatch results in `DbError::DeserializationError`.
    ///
    /// Returns:
    /// - `Ok(WalEntry)` if deserialization is successful.
    /// - `Err(DbError::IoError)` for any I/O issues, including unexpected EOF.
    /// - `Err(DbError::DeserializationError)` for checksum mismatches or unknown operation types.
    fn deserialize<R: Read>(reader: &mut R) -> Result<WalEntry, DbError> {
        let mut operation_type_buffer = [0u8; 1];
        match reader.read_exact(&mut operation_type_buffer) {
            Ok(_) => (),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // This typically means the WAL file ended cleanly or is empty.
                // If called when expecting an entry (e.g. mid-recovery), it's an error.
                return Err(DbError::IoError(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Reached end of WAL stream while expecting operation type")));
            }
            Err(e) => return Err(DbError::IoError(e)), // Other I/O error
        }
        let operation_type = operation_type_buffer[0];

        // Buffer to collect all parts of the entry that are part of the checksum
        let mut data_to_checksum = vec![operation_type];

        let entry = match operation_type {
            PUT_OPERATION => {
                // Assuming Vec<u8> implements DataDeserializer<Vec<u8>>
                let key = <Vec<u8> as DataDeserializer<Vec<u8>>>::deserialize(reader).map_err(|e| {
                    if let DbError::IoError(io_err) = e {
                        if io_err.kind() == std::io::ErrorKind::UnexpectedEof {
                            return DbError::IoError(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Reached end of WAL stream while expecting key for PUT operation"));
                        }
                        DbError::IoError(io_err)
                    } else {
                        e
                    }
                })?;
                let value = <Vec<u8> as DataDeserializer<Vec<u8>>>::deserialize(reader).map_err(|e| {
                     if let DbError::IoError(io_err) = e {
                        if io_err.kind() == std::io::ErrorKind::UnexpectedEof {
                            return DbError::IoError(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Reached end of WAL stream while expecting value for PUT operation"));
                        }
                        DbError::IoError(io_err)
                    } else {
                        e
                    }
                })?;
                // For checksumming, we need the raw bytes.
                // The previous `key.serialize` was adding length prefix + data.
                // This is problematic if `data_to_checksum` is expected to be just op_type + raw_key_bytes + raw_value_bytes.
                // The `serialize` method of `WalEntry` does:
                //   buffer.push(op_type);
                //   key.serialize(&mut buffer)?; // This writes len + data for key
                //   value.serialize(&mut buffer)?; // This writes len + data for value
                //   hasher.update(&buffer); // buffer here contains op_type + len_key + key_bytes + len_value + value_bytes
                // So, for checksum calculation in deserialize, we need to reconstruct this exact sequence.
                // The current `data_to_checksum` starts with `vec![operation_type]`.
                // We need to append the serialized form of key and value to it.

                // Re-serializing key and value to a temporary buffer to get their byte representation for checksum
                let mut temp_key_bytes = Vec::new();
                <Vec<u8> as DataSerializer<Vec<u8>>>::serialize(&key, &mut temp_key_bytes)?;
                data_to_checksum.extend_from_slice(&temp_key_bytes);

                let mut temp_value_bytes = Vec::new();
                <Vec<u8> as DataSerializer<Vec<u8>>>::serialize(&value, &mut temp_value_bytes)?;
                data_to_checksum.extend_from_slice(&temp_value_bytes);
                
                WalEntry::Put { key, value }
            }
            DELETE_OPERATION => {
                let key = <Vec<u8> as DataDeserializer<Vec<u8>>>::deserialize(reader).map_err(|e| {
                    if let DbError::IoError(io_err) = e {
                        if io_err.kind() == std::io::ErrorKind::UnexpectedEof {
                            return DbError::IoError(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Reached end of WAL stream while expecting key for DELETE operation"));
                        }
                        DbError::IoError(io_err)
                    } else {
                        e
                    }
                })?;
                let mut temp_key_bytes = Vec::new();
                <Vec<u8> as DataSerializer<Vec<u8>>>::serialize(&key, &mut temp_key_bytes)?;
                data_to_checksum.extend_from_slice(&temp_key_bytes);

                WalEntry::Delete { key }
            }
            _ => return Err(DbError::DeserializationError(format!("Unknown WAL operation type: {}", operation_type))),
        };

        let mut checksum_bytes = [0u8; 4];
        reader.read_exact(&mut checksum_bytes).map_err(|e| {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                DbError::IoError(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Reached end of WAL stream while expecting checksum"))
            } else {
                DbError::IoError(e)
            }
        })?;
        let expected_checksum = u32::from_le_bytes(checksum_bytes);

        let mut hasher = Hasher::new();
        hasher.update(&data_to_checksum);
        let calculated_checksum = hasher.finalize();

        if expected_checksum != calculated_checksum {
            return Err(DbError::DeserializationError("WAL entry checksum mismatch".to_string()));
        }

        Ok(entry)
    }
}

// Remove the standalone `deserialize_from_reader` as its logic is now part of the trait impl.
// The `impl WalEntry { ... }` block for `deserialize_from_reader` is now combined into `impl DataDeserializer<WalEntry> for WalEntry`.
// However, the original `deserialize_from_reader` was defined within `impl WalEntry { ... }`.
// To make it part of the trait, we move the body of `deserialize_from_reader` into the trait's `deserialize` method.
// The `impl WalEntry { fn deserialize_from_reader ... }` definition needs to be removed if we don't want duplication.
// For now, I'll modify the existing `deserialize_from_reader` to fit the trait and then make it the trait impl.
// The diff above already moved `deserialize_from_reader`'s body into `DataDeserializer::deserialize`.
// The original inherent method `WalEntry::deserialize_from_reader` is effectively replaced by the trait implementation.
