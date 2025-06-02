use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use crc32fast::Hasher;
use crate::core::common::db_error::DbError;
use crate::core::common::serialization::{DataDeserializer, DataSerializer};

const PUT_OPERATION: u8 = 0x01;
const DELETE_OPERATION: u8 = 0x02;

pub enum WalEntry {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl DataSerializer for WalEntry {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), DbError> {
        let mut buffer = Vec::new();
        match self {
            WalEntry::Put { key, value } => {
                buffer.push(PUT_OPERATION);
                key.serialize(&mut buffer)?;
                value.serialize(&mut buffer)?;
            }
            WalEntry::Delete { key } => {
                buffer.push(DELETE_OPERATION);
                key.serialize(&mut buffer)?;
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

#[derive(Debug)]
pub struct WalWriter {
    wal_file_path: PathBuf,
}

impl WalWriter {
    pub fn new(db_file_path: &Path) -> Self {
        let wal_file_path = db_file_path.with_extension(
            db_file_path.extension()
                .map(|ext| ext.to_str().unwrap_or("").to_owned() + ".wal")
                .unwrap_or_else(|| "wal".to_string())
        );
        WalWriter { wal_file_path }
    }

    pub fn log_entry(&self, entry: &WalEntry) -> Result<(), DbError> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.wal_file_path)
            .map_err(|e| DbError::Io(e))?;

        let mut writer = BufWriter::new(file);
        entry.serialize(&mut writer)?;
        writer.flush().map_err(|e| DbError::Io(e))?;
        writer.get_ref().sync_all().map_err(|e| DbError::Io(e))?;

        Ok(())
    }
}

impl DataDeserializer for WalEntry {
    type Item = WalEntry;

    fn deserialize<R: Read>(reader: &mut R) -> Result<Self::Item, DbError> {
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer)?;

        if buffer.len() < 5 { // Minimum size: operation_type (1) + checksum (4)
            return Err(DbError::Serialization("Invalid WAL entry size".to_string()));
        }

        let checksum_bytes = &buffer[buffer.len() - 4..];
        let received_checksum = u32::from_le_bytes(checksum_bytes.try_into().unwrap());

        let data_buffer = &buffer[..buffer.len() - 4];
        let mut hasher = Hasher::new();
        hasher.update(data_buffer);
        let calculated_checksum = hasher.finalize();

        if received_checksum != calculated_checksum {
            return Err(DbError::Serialization("WAL entry checksum mismatch".to_string()));
        }

        let operation_type = data_buffer[0];
        let mut data_reader = &data_buffer[1..];

        match operation_type {
            PUT_OPERATION => {
                let key = Vec::<u8>::deserialize(&mut data_reader)?;
                let value = Vec::<u8>::deserialize(&mut data_reader)?;
                Ok(WalEntry::Put { key, value })
            }
            DELETE_OPERATION => {
                let key = Vec::<u8>::deserialize(&mut data_reader)?;
                Ok(WalEntry::Delete { key })
            }
            _ => Err(DbError::Serialization(format!("Unknown WAL operation type: {}", operation_type))),
        }
    }
}
