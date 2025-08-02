use crate::core::common::traits::{DataDeserializer, DataSerializer};
use crate::core::common::types::Lsn; // Added Lsn
use crate::core::common::{crc32, OxidbError};
use std::fs::OpenOptions;
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf}; // Corrected path for traits

/// Operation type identifier for a 'Put' (insert/update) operation in the WAL.
const PUT_OPERATION: u8 = 0x01;
/// Operation type identifier for a 'Delete' operation in the WAL.
const DELETE_OPERATION: u8 = 0x02;
/// Operation type identifier for a transaction commit operation in the WAL.
const TRANSACTION_COMMIT_OPERATION: u8 = 0x03;
/// Operation type identifier for a transaction rollback operation in the WAL.
const TRANSACTION_ROLLBACK_OPERATION: u8 = 0x04;

/// Represents an entry in the Write-Ahead Log (WAL).
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum WalEntry {
    /// Represents a 'Put' operation with a key and a value.
    Put { lsn: Lsn, transaction_id: u64, key: Vec<u8>, value: Vec<u8> },
    /// Represents a 'Delete' operation with a key.
    Delete { lsn: Lsn, transaction_id: u64, key: Vec<u8> },
    /// Marks the commit of a transaction.
    TransactionCommit { lsn: Lsn, transaction_id: u64 },
    /// Marks the rollback of a transaction.
    TransactionRollback { lsn: Lsn, transaction_id: u64 },
}

impl DataSerializer<Self> for WalEntry {
    /// Serializes a `WalEntry` into a byte stream.
    /// The format is:
    /// - Operation type (1 byte)
    /// - LSN (8 bytes)
    /// - Transaction ID (8 bytes, for Put, Delete, Commit, Rollback)
    /// - Key (length-prefixed Vec<u8>, for Put, Delete)
    /// - Value (length-prefixed Vec<u8>, only for Put operation)
    /// - CRC32 checksum (4 bytes) of all preceding data in this entry.
    fn serialize<W: Write>(value: &Self, writer: &mut W) -> Result<(), OxidbError> {
        let mut buffer = Vec::new(); // Buffer to hold data before checksum calculation
        match value {
            Self::Put { lsn, transaction_id, key, value } => {
                buffer.push(PUT_OPERATION);
                buffer.extend_from_slice(&lsn.to_le_bytes());
                buffer.extend_from_slice(&transaction_id.to_le_bytes());
                <Vec<u8> as DataSerializer<Vec<u8>>>::serialize(key, &mut buffer)?;
                <Vec<u8> as DataSerializer<Vec<u8>>>::serialize(value, &mut buffer)?;
            }
            Self::Delete { lsn, transaction_id, key } => {
                buffer.push(DELETE_OPERATION);
                buffer.extend_from_slice(&lsn.to_le_bytes());
                buffer.extend_from_slice(&transaction_id.to_le_bytes());
                <Vec<u8> as DataSerializer<Vec<u8>>>::serialize(key, &mut buffer)?;
            }
            Self::TransactionCommit { lsn, transaction_id } => {
                buffer.push(TRANSACTION_COMMIT_OPERATION);
                buffer.extend_from_slice(&lsn.to_le_bytes());
                buffer.extend_from_slice(&transaction_id.to_le_bytes());
            }
            Self::TransactionRollback { lsn, transaction_id } => {
                buffer.push(TRANSACTION_ROLLBACK_OPERATION);
                buffer.extend_from_slice(&lsn.to_le_bytes());
                buffer.extend_from_slice(&transaction_id.to_le_bytes());
            }
        }

        let mut hasher = crc32::Hasher::new();
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
            lsn: 0,
            transaction_id: 1,
            key: b"test_key".to_vec(),
            value: b"test_value".to_vec(),
        };
        let delete_entry =
            WalEntry::Delete { lsn: 1, transaction_id: 2, key: b"test_key_delete".to_vec() };
        let commit_entry = WalEntry::TransactionCommit { lsn: 2, transaction_id: 3 };
        let rollback_entry = WalEntry::TransactionRollback { lsn: 3, transaction_id: 4 };

        let entries = vec![put_entry, delete_entry, commit_entry, rollback_entry];

        for original_entry in entries {
            let mut buffer = Vec::new();
            WalEntry::serialize(&original_entry, &mut buffer)
                .expect("Serialization of WAL entry failed in test");

            let mut reader = Cursor::new(&buffer);
            let deserialized_entry = WalEntry::deserialize(&mut reader)
                .expect("Deserialization of WAL entry failed in test");
            assert_eq!(&original_entry, &deserialized_entry);
        }
    }

    #[test]
    fn test_wal_entry_sequential_deserialization() {
        let entry1 = WalEntry::Put {
            lsn: 100,
            transaction_id: 10,
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
        };
        let entry2 = WalEntry::Delete { lsn: 101, transaction_id: 11, key: b"key1".to_vec() };
        let entry3 = WalEntry::TransactionCommit { lsn: 102, transaction_id: 11 };
        let entry4 = WalEntry::Put {
            lsn: 103,
            transaction_id: 12,
            key: b"key2".to_vec(),
            value: b"value2_longer".to_vec(),
        };
        let entry5 = WalEntry::TransactionRollback { lsn: 104, transaction_id: 12 };

        let mut buffer = Vec::new();
        WalEntry::serialize(&entry1, &mut buffer).expect("Serialize entry1 failed");
        WalEntry::serialize(&entry2, &mut buffer).expect("Serialize entry2 failed");
        WalEntry::serialize(&entry3, &mut buffer).expect("Serialize entry3 failed");
        WalEntry::serialize(&entry4, &mut buffer).expect("Serialize entry4 failed");
        WalEntry::serialize(&entry5, &mut buffer).expect("Serialize entry5 failed");

        let mut cursor = Cursor::new(&buffer);

        assert_eq!(WalEntry::deserialize(&mut cursor).expect("Deserialize entry1 failed"), entry1);
        assert_eq!(WalEntry::deserialize(&mut cursor).expect("Deserialize entry2 failed"), entry2);
        assert_eq!(WalEntry::deserialize(&mut cursor).expect("Deserialize entry3 failed"), entry3);
        assert_eq!(WalEntry::deserialize(&mut cursor).expect("Deserialize entry4 failed"), entry4);
        assert_eq!(WalEntry::deserialize(&mut cursor).expect("Deserialize entry5 failed"), entry5);

        // Try to deserialize again, expecting EOF
        match WalEntry::deserialize(&mut cursor) {
            Err(OxidbError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
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
        eprintln!("[engine::wal::WalWriter::new] Received db_file_path: {db_file_path:?}");
        let mut wal_file_path_buf = db_file_path.to_path_buf();
        let original_extension = wal_file_path_buf.extension().and_then(std::ffi::OsStr::to_str);

        if let Some(ext_str) = original_extension {
            wal_file_path_buf.set_extension(format!("{ext_str}.wal"));
        } else {
            wal_file_path_buf.set_extension("wal");
        }
        eprintln!("[engine::wal::WalWriter::new] Derived wal_file_path: {wal_file_path_buf:?}");
        Self { wal_file_path: wal_file_path_buf }
    }

    /// Logs a `WalEntry` to the WAL file.
    /// This involves serializing the entry and appending it to the file.
    /// The write is flushed and synced to disk to ensure durability.
    pub fn log_entry(&self, entry: &WalEntry) -> Result<(), OxidbError> {
        eprintln!("[engine::wal::WalWriter::log_entry] Method entered. Attempting to log to: {:?}, entry: {:?}", &self.wal_file_path, entry); // ADDED THIS LINE
                                                                                                                                              // Changed
                                                                                                                                              // eprintln!( // This line is redundant due to the one above.
                                                                                                                                              //     "[engine::wal::WalWriter::log_entry] Attempting to log to: {:?}, entry: {:?}",
                                                                                                                                              //     &self.wal_file_path, entry
                                                                                                                                              // );
        let file_result = OpenOptions::new().create(true).append(true).open(&self.wal_file_path);

        if let Err(e) = &file_result {
            eprintln!(
                "[engine::wal::WalWriter::log_entry] Error opening file {:?}: {}",
                &self.wal_file_path, e
            );
        } else {
            eprintln!(
                "[engine::wal::WalWriter::log_entry] Successfully opened/created file: {:?}",
                &self.wal_file_path
            );
        }
        let file = file_result.map_err(OxidbError::Io)?;

        let mut writer = BufWriter::new(file);
        <WalEntry as DataSerializer<WalEntry>>::serialize(entry, &mut writer)?;
        writer.flush().map_err(OxidbError::Io)?;
        writer.get_ref().sync_all().map_err(OxidbError::Io)?;

        Ok(())
    }
}

impl WalEntry {
    // This is now the implementation of DataDeserializer<WalEntry>::deserialize
}

// Now, let's make deserialize_from_reader the official implementation for DataDeserializer<WalEntry>
impl DataDeserializer<Self> for WalEntry {
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
    /// - `Err(OxidbError::Io)` for any I/O issues, including unexpected EOF.
    /// - `Err(OxidbError::Deserialization)` for checksum mismatches or unknown operation types.
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, OxidbError> {
        // Changed
        let mut operation_type_buffer = [0u8; 1];
        match reader.read_exact(&mut operation_type_buffer) {
            Ok(()) => (),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // Return the original EOF error so callers can handle it appropriately
                return Err(OxidbError::Io(e));
            }
            Err(e) => return Err(OxidbError::Io(e)),
        }
        let operation_type = operation_type_buffer[0];

        // Buffer to collect all parts of the entry that are part of the checksum
        let mut data_to_checksum = vec![operation_type];

        let entry = match operation_type {
            PUT_OPERATION => {
                let mut lsn_bytes = [0u8; 8];
                reader.read_exact(&mut lsn_bytes).map_err(|e| map_eof_error(e, "LSN for PUT"))?;
                let lsn = u64::from_le_bytes(lsn_bytes);
                data_to_checksum.extend_from_slice(&lsn_bytes);

                let mut tx_id_bytes = [0u8; 8];
                reader
                    .read_exact(&mut tx_id_bytes)
                    .map_err(|e| map_eof_error(e, "transaction ID for PUT"))?;
                let transaction_id = u64::from_le_bytes(tx_id_bytes);
                data_to_checksum.extend_from_slice(&tx_id_bytes);

                let key = <Vec<u8> as DataDeserializer<Vec<u8>>>::deserialize(reader)
                    .map_err(|e| map_deserialization_eof(e, "key for PUT operation"))?;
                let mut temp_key_bytes = Vec::new();
                <Vec<u8> as DataSerializer<Vec<u8>>>::serialize(&key, &mut temp_key_bytes)?;
                data_to_checksum.extend_from_slice(&temp_key_bytes);

                let value = <Vec<u8> as DataDeserializer<Vec<u8>>>::deserialize(reader)
                    .map_err(|e| map_deserialization_eof(e, "value for PUT operation"))?;
                let mut temp_value_bytes = Vec::new();
                <Vec<u8> as DataSerializer<Vec<u8>>>::serialize(&value, &mut temp_value_bytes)?;
                data_to_checksum.extend_from_slice(&temp_value_bytes);

                Self::Put { lsn, transaction_id, key, value }
            }
            DELETE_OPERATION => {
                let mut lsn_bytes = [0u8; 8];
                reader
                    .read_exact(&mut lsn_bytes)
                    .map_err(|e| map_eof_error(e, "LSN for DELETE"))?;
                let lsn = u64::from_le_bytes(lsn_bytes);
                data_to_checksum.extend_from_slice(&lsn_bytes);

                let mut tx_id_bytes = [0u8; 8];
                reader
                    .read_exact(&mut tx_id_bytes)
                    .map_err(|e| map_eof_error(e, "transaction ID for DELETE"))?;
                let transaction_id = u64::from_le_bytes(tx_id_bytes);
                data_to_checksum.extend_from_slice(&tx_id_bytes);

                let key = <Vec<u8> as DataDeserializer<Vec<u8>>>::deserialize(reader)
                    .map_err(|e| map_deserialization_eof(e, "key for DELETE operation"))?;
                let mut temp_key_bytes = Vec::new();
                <Vec<u8> as DataSerializer<Vec<u8>>>::serialize(&key, &mut temp_key_bytes)?;
                data_to_checksum.extend_from_slice(&temp_key_bytes);

                Self::Delete { lsn, transaction_id, key }
            }
            TRANSACTION_COMMIT_OPERATION => {
                let mut lsn_bytes = [0u8; 8];
                reader
                    .read_exact(&mut lsn_bytes)
                    .map_err(|e| map_eof_error(e, "LSN for COMMIT"))?;
                let lsn = u64::from_le_bytes(lsn_bytes);
                data_to_checksum.extend_from_slice(&lsn_bytes);

                let mut tx_id_bytes = [0u8; 8];
                reader
                    .read_exact(&mut tx_id_bytes)
                    .map_err(|e| map_eof_error(e, "transaction ID for COMMIT"))?;
                let transaction_id = u64::from_le_bytes(tx_id_bytes);
                data_to_checksum.extend_from_slice(&tx_id_bytes);
                Self::TransactionCommit { lsn, transaction_id }
            }
            TRANSACTION_ROLLBACK_OPERATION => {
                let mut lsn_bytes = [0u8; 8];
                reader
                    .read_exact(&mut lsn_bytes)
                    .map_err(|e| map_eof_error(e, "LSN for ROLLBACK"))?;
                let lsn = u64::from_le_bytes(lsn_bytes);
                data_to_checksum.extend_from_slice(&lsn_bytes);

                let mut tx_id_bytes = [0u8; 8];
                reader
                    .read_exact(&mut tx_id_bytes)
                    .map_err(|e| map_eof_error(e, "transaction ID for ROLLBACK"))?;
                let transaction_id = u64::from_le_bytes(tx_id_bytes);
                data_to_checksum.extend_from_slice(&tx_id_bytes);
                Self::TransactionRollback { lsn, transaction_id }
            }
            0x00 => {
                // Specifically check for 0x00
                return Err(OxidbError::Deserialization(
                    "Read a zero byte where WAL operation type was expected. Possible file corruption or premature EOF.".to_string()
                ));
            }
            _ => {
                return Err(OxidbError::Deserialization(format!(
                    "Unknown WAL operation type: {operation_type}"
                )));
            }
        };

        let mut checksum_bytes = [0u8; 4];
        reader.read_exact(&mut checksum_bytes).map_err(|e| map_eof_error(e, "checksum"))?;
        let expected_checksum = u32::from_le_bytes(checksum_bytes);

        let mut hasher = crc32::Hasher::new();
        hasher.update(&data_to_checksum);
        let calculated_checksum = hasher.finalize();

        if expected_checksum != calculated_checksum {
            return Err(OxidbError::Deserialization("WAL entry checksum mismatch".to_string()));
            // Changed
        }

        Ok(entry)
    }
}

/// Helper function to map EOF errors encountered during WAL deserialization
/// to a more context-specific error message.
const fn map_eof_error(e: std::io::Error, _context: &str) -> OxidbError {
    // Always return the original IO error to maintain consistent error handling
    OxidbError::Io(e)
}

/// Helper function to specifically handle EOF errors that might occur during
/// the deserialization of length-prefixed data (like keys or values) within a WAL entry.
/// It distinguishes general I/O errors from those indicating a truncated entry part.
const fn map_deserialization_eof(e: OxidbError, _context: &str) -> OxidbError {
    // Always return the original error to maintain consistent error handling
    e
}
