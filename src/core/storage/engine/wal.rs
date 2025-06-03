use std::fs::OpenOptions; // Removed File
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use crc32fast::Hasher;
use crate::core::common::error::DbError; // Corrected path for DbError
use crate::core::common::traits::{DataDeserializer, DataSerializer}; // Corrected path for traits

const PUT_OPERATION: u8 = 0x01;
const DELETE_OPERATION: u8 = 0x02;
const TRANSACTION_COMMIT_OPERATION: u8 = 0x03;
const TRANSACTION_ROLLBACK_OPERATION: u8 = 0x04;

/// Represents an entry in the Write-Ahead Log (WAL).
#[derive(Debug, PartialEq)] // Added PartialEq for easier testing
pub enum WalEntry {
    /// Represents a 'Put' operation with a key and a value.
    Put { transaction_id: u64, key: Vec<u8>, value: Vec<u8> },
    /// Represents a 'Delete' operation with a key.
    Delete { transaction_id: u64, key: Vec<u8> },
    /// Marks the commit of a transaction.
    TransactionCommit { transaction_id: u64 },
    /// Marks the rollback of a transaction.
    TransactionRollback { transaction_id: u64 },
}

impl DataSerializer<WalEntry> for WalEntry {
    /// Serializes a `WalEntry` into a byte stream.
    /// The format is:
    /// - Operation type (1 byte)
    /// - Transaction ID (8 bytes, for Put, Delete, Commit, Rollback)
    /// - Key (length-prefixed Vec<u8>, for Put, Delete)
    /// - Value (length-prefixed Vec<u8>, only for Put operation)
    /// - CRC32 checksum (4 bytes) of all preceding data in this entry.
    fn serialize<W: Write>(value: &WalEntry, writer: &mut W) -> Result<(), DbError> {
        let mut buffer = Vec::new(); // Buffer to hold data before checksum calculation
        match value {
            WalEntry::Put { transaction_id, key, value } => {
                buffer.push(PUT_OPERATION);
                buffer.extend_from_slice(&transaction_id.to_le_bytes());
                <Vec<u8> as DataSerializer<Vec<u8>>>::serialize(key, &mut buffer)?;
                <Vec<u8> as DataSerializer<Vec<u8>>>::serialize(value, &mut buffer)?;
            }
            WalEntry::Delete { transaction_id, key } => {
                buffer.push(DELETE_OPERATION);
                buffer.extend_from_slice(&transaction_id.to_le_bytes());
                <Vec<u8> as DataSerializer<Vec<u8>>>::serialize(key, &mut buffer)?;
            }
            WalEntry::TransactionCommit { transaction_id } => {
                buffer.push(TRANSACTION_COMMIT_OPERATION);
                buffer.extend_from_slice(&transaction_id.to_le_bytes());
            }
            WalEntry::TransactionRollback { transaction_id } => {
                buffer.push(TRANSACTION_ROLLBACK_OPERATION);
                buffer.extend_from_slice(&transaction_id.to_le_bytes());
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
            transaction_id: 1,
            key: b"test_key".to_vec(),
            value: b"test_value".to_vec(),
        };
        let delete_entry = WalEntry::Delete {
            transaction_id: 2,
            key: b"test_key_delete".to_vec(),
        };
        let commit_entry = WalEntry::TransactionCommit { transaction_id: 3 };
        let rollback_entry = WalEntry::TransactionRollback { transaction_id: 4 };

        let entries = vec![put_entry, delete_entry, commit_entry, rollback_entry];

        for original_entry in entries {
            let mut buffer = Vec::new();
            <WalEntry as DataSerializer<WalEntry>>::serialize(&original_entry, &mut buffer).unwrap();
            
            let mut reader = Cursor::new(&buffer);
            let deserialized_entry = <WalEntry as DataDeserializer<WalEntry>>::deserialize(&mut reader).unwrap();
            
            // Using the PartialEq derived for WalEntry
            assert_eq!(&original_entry, &deserialized_entry);
        }
    }

    #[test]
    fn test_wal_entry_sequential_deserialization() {
        let entry1 = WalEntry::Put {
            transaction_id: 10,
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
        };
        let entry2 = WalEntry::Delete {
            transaction_id: 11,
            key: b"key1".to_vec(),
        };
        let entry3 = WalEntry::TransactionCommit { transaction_id: 11 };
        let entry4 = WalEntry::Put {
            transaction_id: 12,
            key: b"key2".to_vec(),
            value: b"value2_longer".to_vec(),
        };
        let entry5 = WalEntry::TransactionRollback { transaction_id: 12 };


        let mut buffer = Vec::new();
        <WalEntry as DataSerializer<WalEntry>>::serialize(&entry1, &mut buffer).unwrap();
        <WalEntry as DataSerializer<WalEntry>>::serialize(&entry2, &mut buffer).unwrap();
        <WalEntry as DataSerializer<WalEntry>>::serialize(&entry3, &mut buffer).unwrap();
        <WalEntry as DataSerializer<WalEntry>>::serialize(&entry4, &mut buffer).unwrap();
        <WalEntry as DataSerializer<WalEntry>>::serialize(&entry5, &mut buffer).unwrap();

        let mut cursor = Cursor::new(&buffer);

        assert_eq!(<WalEntry as DataDeserializer<WalEntry>>::deserialize(&mut cursor).unwrap(), entry1);
        assert_eq!(<WalEntry as DataDeserializer<WalEntry>>::deserialize(&mut cursor).unwrap(), entry2);
        assert_eq!(<WalEntry as DataDeserializer<WalEntry>>::deserialize(&mut cursor).unwrap(), entry3);
        assert_eq!(<WalEntry as DataDeserializer<WalEntry>>::deserialize(&mut cursor).unwrap(), entry4);
        assert_eq!(<WalEntry as DataDeserializer<WalEntry>>::deserialize(&mut cursor).unwrap(), entry5);
        
        // Try to deserialize again, expecting EOF
        match <WalEntry as DataDeserializer<WalEntry>>::deserialize(&mut cursor) {
            Err(DbError::IoError(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // This is the expected outcome
            }
            Ok(entry) => panic!("Expected EOF error, but got an entry: {:?}", entry),
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

                // For Put: op_type + tx_id + serialized_key + serialized_value
                let mut tx_id_bytes = [0u8; 8];
                reader.read_exact(&mut tx_id_bytes).map_err(|e| map_eof_error(e, "transaction ID for PUT"))?;
                let transaction_id = u64::from_le_bytes(tx_id_bytes);
                data_to_checksum.extend_from_slice(&tx_id_bytes);
                
                let key = <Vec<u8> as DataDeserializer<Vec<u8>>>::deserialize(reader).map_err(|e| map_deserialization_eof(e, "key for PUT operation"))?;
                let value = <Vec<u8> as DataDeserializer<Vec<u8>>>::deserialize(reader).map_err(|e| map_deserialization_eof(e, "value for PUT operation"))?;
                
                let mut temp_key_bytes = Vec::new();
                <Vec<u8> as DataSerializer<Vec<u8>>>::serialize(&key, &mut temp_key_bytes)?;
                data_to_checksum.extend_from_slice(&temp_key_bytes);

                let mut temp_value_bytes = Vec::new();
                <Vec<u8> as DataSerializer<Vec<u8>>>::serialize(&value, &mut temp_value_bytes)?;
                data_to_checksum.extend_from_slice(&temp_value_bytes);
                
                WalEntry::Put { transaction_id, key, value }
            }
            DELETE_OPERATION => {
                // For Delete: op_type + tx_id + serialized_key
                let mut tx_id_bytes = [0u8; 8];
                reader.read_exact(&mut tx_id_bytes).map_err(|e| map_eof_error(e, "transaction ID for DELETE"))?;
                let transaction_id = u64::from_le_bytes(tx_id_bytes);
                data_to_checksum.extend_from_slice(&tx_id_bytes);

                let key = <Vec<u8> as DataDeserializer<Vec<u8>>>::deserialize(reader).map_err(|e| map_deserialization_eof(e, "key for DELETE operation"))?;
                
                let mut temp_key_bytes = Vec::new();
                <Vec<u8> as DataSerializer<Vec<u8>>>::serialize(&key, &mut temp_key_bytes)?;
                data_to_checksum.extend_from_slice(&temp_key_bytes);

                WalEntry::Delete { transaction_id, key }
            }
            TRANSACTION_COMMIT_OPERATION => {
                // For Commit: op_type + tx_id
                let mut tx_id_bytes = [0u8; 8];
                reader.read_exact(&mut tx_id_bytes).map_err(|e| map_eof_error(e, "transaction ID for COMMIT"))?;
                let transaction_id = u64::from_le_bytes(tx_id_bytes);
                data_to_checksum.extend_from_slice(&tx_id_bytes);
                WalEntry::TransactionCommit { transaction_id }
            }
            TRANSACTION_ROLLBACK_OPERATION => {
                // For Rollback: op_type + tx_id
                let mut tx_id_bytes = [0u8; 8];
                reader.read_exact(&mut tx_id_bytes).map_err(|e| map_eof_error(e, "transaction ID for ROLLBACK"))?;
                let transaction_id = u64::from_le_bytes(tx_id_bytes);
                data_to_checksum.extend_from_slice(&tx_id_bytes);
                WalEntry::TransactionRollback { transaction_id }
            }
            _ => return Err(DbError::DeserializationError(format!("Unknown WAL operation type: {}", operation_type))),
        };

        let mut checksum_bytes = [0u8; 4];
        reader.read_exact(&mut checksum_bytes).map_err(|e| map_eof_error(e, "checksum"))?;
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

// Helper function to map EOF errors for deserialization steps
fn map_eof_error(e: std::io::Error, context: &str) -> DbError {
    if e.kind() == std::io::ErrorKind::UnexpectedEof {
        DbError::IoError(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            format!("Reached end of WAL stream while expecting {}", context),
        ))
    } else {
        DbError::IoError(e)
    }
}

fn map_deserialization_eof(e: DbError, context: &str) -> DbError {
    if let DbError::IoError(io_err) = &e {
        if io_err.kind() == std::io::ErrorKind::UnexpectedEof {
            return DbError::IoError(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("Reached end of WAL stream while expecting {}", context),
            ));
        }
    }
    e // Return original error if not an EOF related to Vec<u8> deserialization
}
