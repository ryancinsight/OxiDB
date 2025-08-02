// src/core/wal/reader.rs
//
// WAL Reader Component - The Foundation of Recovery Operations
//
// This module implements the WAL Reader, which provides the ability to read and parse
// Write-Ahead Log files. This is the foundational component for implementing crash
// recovery mechanisms (ARIES algorithm: Analysis, Redo, Undo phases).
//
// The WAL file format consists of length-prefixed bincode-serialized LogRecord entries:
// [4-byte length (big-endian)][serialized LogRecord][4-byte length][serialized LogRecord]...

use crate::core::common::bincode_compat as bincode;
use std::fs::File;
use std::io::{BufReader, Error as IoError, ErrorKind as IoErrorKind, Read};
use std::path::{Path, PathBuf};

use crate::core::common::types::Lsn;
use crate::core::wal::log_record::LogRecord;

/// Configuration for WAL Reader operations
#[derive(Debug, Clone, Copy)]
pub struct WalReaderConfig {
    /// Buffer size for reading WAL file (in bytes)
    pub buffer_size: usize,
    /// Whether to validate LSN ordering during reading
    pub validate_lsn_ordering: bool,
}

impl Default for WalReaderConfig {
    fn default() -> Self {
        Self {
            buffer_size: 8192, // 8KB buffer
            validate_lsn_ordering: true,
        }
    }
}

/// Errors that can occur during WAL reading operations
#[derive(Debug, thiserror::Error)]
pub enum WalReaderError {
    #[error("IO error: {0}")]
    Io(#[from] IoError),

    #[error("Deserialization error: {0}")]
    Deserialization(String),

    #[error("Invalid record length: {length}")]
    InvalidRecordLength { length: u32 },

    #[error("LSN ordering violation: expected >= {expected}, found {actual}")]
    LsnOrderingViolation { expected: Lsn, actual: Lsn },

    #[error("Unexpected end of file while reading record")]
    UnexpectedEof,

    #[error("WAL file not found: {path}")]
    FileNotFound { path: String },
}

/// Iterator over WAL records from a WAL file
pub struct WalRecordIterator {
    reader: BufReader<File>,
    config: WalReaderConfig,
    last_lsn: Option<Lsn>,
    records_read: usize,
}

impl WalRecordIterator {
    /// Create a new WAL record iterator from a file path
    ///
    /// # Errors
    /// Returns `WalReaderError` if:
    /// - The WAL file does not exist at the specified path
    /// - File permissions prevent reading the WAL file
    /// - I/O errors occur during file opening or buffer initialization
    pub fn new<P: AsRef<Path>>(
        wal_file_path: P,
        config: WalReaderConfig,
    ) -> Result<Self, WalReaderError> {
        let path = wal_file_path.as_ref();

        if !path.exists() {
            return Err(WalReaderError::FileNotFound { path: path.to_string_lossy().to_string() });
        }

        let file = File::open(path).map_err(WalReaderError::Io)?;
        let reader = BufReader::with_capacity(config.buffer_size, file);

        Ok(Self { reader, config, last_lsn: None, records_read: 0 })
    }

    /// Get the number of records read so far
    #[must_use]
    pub const fn records_read(&self) -> usize {
        self.records_read
    }

    /// Read the next log record from the WAL file
    ///
    /// # Errors
    /// Returns `WalReaderError` if:
    /// - I/O errors occur during file reading
    /// - Record deserialization fails due to corrupted data
    /// - Record length prefix is invalid or corrupted
    /// - Unexpected end of file during record reading
    pub fn next_record(&mut self) -> Result<Option<LogRecord>, WalReaderError> {
        // Read the 4-byte length prefix
        let mut length_bytes = [0u8; 4];
        match self.reader.read_exact(&mut length_bytes) {
            Ok(()) => {}
            Err(e) if e.kind() == IoErrorKind::UnexpectedEof => {
                // End of file reached
                return Ok(None);
            }
            Err(e) => return Err(WalReaderError::Io(e)),
        }

        let record_length = u32::from_be_bytes(length_bytes);

        // Validate record length (reasonable bounds check)
        if record_length == 0 || record_length > 1_000_000 {
            // 1MB max record size
            return Err(WalReaderError::InvalidRecordLength { length: record_length });
        }

        // Read the serialized record data
        let mut record_data = vec![0u8; record_length as usize];
        self.reader.read_exact(&mut record_data).map_err(|e| {
            if e.kind() == IoErrorKind::UnexpectedEof {
                WalReaderError::UnexpectedEof
            } else {
                WalReaderError::Io(e)
            }
        })?;

        // Deserialize the log record
        let log_record: LogRecord = bincode::deserialize(&record_data)
            .map_err(|e| WalReaderError::Deserialization(e.to_string()))?;

        // Validate LSN ordering if enabled
        if self.config.validate_lsn_ordering {
            let current_lsn = Self::extract_lsn(&log_record);
            if let Some(last_lsn) = self.last_lsn {
                if current_lsn < last_lsn {
                    return Err(WalReaderError::LsnOrderingViolation {
                        expected: last_lsn,
                        actual: current_lsn,
                    });
                }
            }
            self.last_lsn = Some(current_lsn);
        }

        self.records_read = self.records_read.saturating_add(1);
        Ok(Some(log_record))
    }

    /// Extract LSN from a log record
    const fn extract_lsn(record: &LogRecord) -> Lsn {
        match record {
            LogRecord::BeginTransaction { lsn, .. }
            | LogRecord::CommitTransaction { lsn, .. }
            | LogRecord::AbortTransaction { lsn, .. }
            | LogRecord::InsertRecord { lsn, .. }
            | LogRecord::DeleteRecord { lsn, .. }
            | LogRecord::UpdateRecord { lsn, .. }
            | LogRecord::NewPage { lsn, .. }
            | LogRecord::CompensationLogRecord { lsn, .. }
            | LogRecord::CheckpointBegin { lsn, .. }
            | LogRecord::CheckpointEnd { lsn, .. } => *lsn,
        }
    }
}

impl Iterator for WalRecordIterator {
    type Item = Result<LogRecord, WalReaderError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_record() {
            Ok(Some(record)) => Some(Ok(record)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

/// High-level WAL Reader for convenient access to WAL operations
#[derive(Debug)]
pub struct WalReader {
    wal_file_path: PathBuf,
    config: WalReaderConfig,
}

impl WalReader {
    /// Create a new WAL Reader
    pub fn new<P: AsRef<Path>>(wal_file_path: P, config: WalReaderConfig) -> Self {
        Self { wal_file_path: wal_file_path.as_ref().to_path_buf(), config }
    }

    /// Create a WAL Reader with default configuration
    pub fn with_defaults<P: AsRef<Path>>(wal_file_path: P) -> Self {
        Self::new(wal_file_path, WalReaderConfig::default())
    }

    /// Create an iterator over all records in the WAL file
    pub fn iter_records(&self) -> Result<WalRecordIterator, WalReaderError> {
        WalRecordIterator::new(&self.wal_file_path, self.config)
    }

    /// Read all records from the WAL file into a vector
    pub fn read_all_records(&self) -> Result<Vec<LogRecord>, WalReaderError> {
        let mut records = Vec::new();
        let mut iterator = self.iter_records()?;

        while let Some(record) = iterator.next_record()? {
            records.push(record);
        }

        Ok(records)
    }

    /// Find all records for a specific transaction ID
    pub fn find_transaction_records(
        &self,
        tx_id: crate::core::common::types::TransactionId,
    ) -> Result<Vec<LogRecord>, WalReaderError> {
        let mut tx_records = Vec::new();
        let mut iterator = self.iter_records()?;

        while let Some(record) = iterator.next_record()? {
            let record_tx_id = match &record {
                LogRecord::BeginTransaction { tx_id: id, .. }
                | LogRecord::CommitTransaction { tx_id: id, .. }
                | LogRecord::AbortTransaction { tx_id: id, .. }
                | LogRecord::InsertRecord { tx_id: id, .. }
                | LogRecord::DeleteRecord { tx_id: id, .. }
                | LogRecord::UpdateRecord { tx_id: id, .. }
                | LogRecord::NewPage { tx_id: id, .. }
                | LogRecord::CompensationLogRecord { tx_id: id, .. } => Some(*id),
                LogRecord::CheckpointBegin { .. } | LogRecord::CheckpointEnd { .. } => None,
            };

            if record_tx_id == Some(tx_id) {
                tx_records.push(record);
            }
        }

        Ok(tx_records)
    }

    /// Find the last checkpoint record pair in the WAL.
    ///
    /// # Errors
    ///
    /// Returns `WalReaderError` if:
    /// - The WAL file cannot be read
    /// - Record parsing fails
    /// - I/O errors occur during file operations
    pub fn find_last_checkpoint(&self) -> Result<Option<(LogRecord, LogRecord)>, WalReaderError> {
        let mut last_checkpoint_begin: Option<LogRecord> = None;
        let mut last_checkpoint_end: Option<LogRecord> = None;
        let mut iterator = self.iter_records()?;

        while let Some(record) = iterator.next_record()? {
            match &record {
                LogRecord::CheckpointBegin { .. } => {
                    last_checkpoint_begin = Some(record);
                    last_checkpoint_end = None; // Reset end until we find the matching end
                }
                LogRecord::CheckpointEnd { .. } => {
                    if last_checkpoint_begin.is_some() {
                        last_checkpoint_end = Some(record);
                    }
                }
                _ => {}
            }
        }

        match (last_checkpoint_begin, last_checkpoint_end) {
            (Some(begin), Some(end)) => Ok(Some((begin, end))),
            _ => Ok(None),
        }
    }

    /// Get comprehensive statistics about the WAL file.
    ///
    /// # Errors
    ///
    /// Returns `WalReaderError` if:
    /// - The WAL file cannot be read
    /// - Record parsing fails during iteration
    /// - I/O errors occur during file operations
    pub fn get_statistics(&self) -> Result<WalStatistics, WalReaderError> {
        let mut stats = WalStatistics::default();

        for record in WalRecordIterator::new(&self.wal_file_path, self.config)? {
            let record = record?;
            stats.total_records = stats.total_records.saturating_add(1);

            match record {
                LogRecord::BeginTransaction { .. } => {
                    stats.begin_transaction_count = stats.begin_transaction_count.saturating_add(1);
                }
                LogRecord::CommitTransaction { .. } => {
                    stats.commit_transaction_count =
                        stats.commit_transaction_count.saturating_add(1);
                }
                LogRecord::AbortTransaction { .. } => {
                    stats.abort_transaction_count = stats.abort_transaction_count.saturating_add(1);
                }
                LogRecord::InsertRecord { .. } => {
                    stats.insert_record_count = stats.insert_record_count.saturating_add(1);
                }
                LogRecord::DeleteRecord { .. } => {
                    stats.delete_record_count = stats.delete_record_count.saturating_add(1);
                }
                LogRecord::UpdateRecord { .. } => {
                    stats.update_record_count = stats.update_record_count.saturating_add(1);
                }
                LogRecord::NewPage { .. } => {
                    stats.new_page_count = stats.new_page_count.saturating_add(1);
                }
                LogRecord::CompensationLogRecord { .. } => {
                    stats.compensation_log_record_count =
                        stats.compensation_log_record_count.saturating_add(1);
                }
                LogRecord::CheckpointBegin { .. } => {
                    stats.checkpoint_begin_count = stats.checkpoint_begin_count.saturating_add(1);
                }
                LogRecord::CheckpointEnd { .. } => {
                    stats.checkpoint_end_count = stats.checkpoint_end_count.saturating_add(1);
                }
            }
        }

        Ok(stats)
    }
}

/// Statistics about WAL file contents.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct WalStatistics {
    pub total_records: usize,
    pub begin_transaction_count: usize,
    pub commit_transaction_count: usize,
    pub abort_transaction_count: usize,
    pub insert_record_count: usize,
    pub delete_record_count: usize,
    pub update_record_count: usize,
    pub new_page_count: usize,
    pub compensation_log_record_count: usize,
    pub checkpoint_begin_count: usize,
    pub checkpoint_end_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::common::types::{
        ids::{PageId, SlotId},
        TransactionId,
    };
    use crate::core::wal::writer::{WalWriter, WalWriterConfig};
    use tempfile::NamedTempFile;

    fn create_test_wal_file() -> (NamedTempFile, Vec<LogRecord>) {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let wal_path = temp_file.path().to_path_buf();

        // Create test records
        let test_records = vec![
            LogRecord::BeginTransaction { lsn: 1, tx_id: TransactionId(100) },
            LogRecord::InsertRecord {
                lsn: 2,
                tx_id: TransactionId(100),
                page_id: PageId(1),
                slot_id: SlotId(0),
                record_data: vec![1, 2, 3, 4],
                prev_lsn: 1,
            },
            LogRecord::UpdateRecord {
                lsn: 3,
                tx_id: TransactionId(100),
                page_id: PageId(1),
                slot_id: SlotId(0),
                old_record_data: vec![1, 2, 3, 4],
                new_record_data: vec![5, 6, 7, 8],
                prev_lsn: 2,
            },
            LogRecord::CommitTransaction { lsn: 4, tx_id: TransactionId(100), prev_lsn: 3 },
        ];

        // Write test records using WalWriter
        let config = WalWriterConfig { max_buffer_size: 1000, flush_interval_ms: None };
        let mut writer = WalWriter::new(wal_path, config);

        for record in &test_records {
            writer.add_record(record).expect("Failed to add record");
        }
        writer.flush().expect("Failed to flush WAL");

        (temp_file, test_records)
    }

    #[test]
    fn test_wal_reader_new() {
        let (temp_file, _) = create_test_wal_file();
        let config = WalReaderConfig::default();

        let reader = WalReader::new(temp_file.path(), config);
        assert_eq!(reader.wal_file_path, temp_file.path());
    }

    #[test]
    fn test_wal_reader_with_defaults() {
        let (temp_file, _) = create_test_wal_file();

        let reader = WalReader::with_defaults(temp_file.path());
        assert_eq!(reader.wal_file_path, temp_file.path());
        assert!(reader.config.validate_lsn_ordering);
    }

    #[test]
    fn test_wal_reader_file_not_found() {
        let non_existent_path = "/non/existent/path.wal";
        let config = WalReaderConfig::default();

        let result = WalRecordIterator::new(non_existent_path, config);
        assert!(matches!(result, Err(WalReaderError::FileNotFound { .. })));
    }

    #[test]
    fn test_read_all_records() {
        let (temp_file, expected_records) = create_test_wal_file();
        let reader = WalReader::with_defaults(temp_file.path());

        let records = reader.read_all_records().expect("Failed to read records");
        assert_eq!(records.len(), expected_records.len());

        for (actual, expected) in records.iter().zip(expected_records.iter()) {
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn test_iterator_interface() {
        let (temp_file, expected_records) = create_test_wal_file();
        let reader = WalReader::with_defaults(temp_file.path());

        let mut iterator = reader.iter_records().expect("Failed to create iterator");
        let mut collected_records = Vec::new();

        while let Some(record_result) = iterator.next() {
            let record = record_result.expect("Failed to read record");
            collected_records.push(record);
        }

        assert_eq!(collected_records.len(), expected_records.len());
        for (actual, expected) in collected_records.iter().zip(expected_records.iter()) {
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn test_find_transaction_records() {
        let (temp_file, _) = create_test_wal_file();
        let reader = WalReader::with_defaults(temp_file.path());

        let tx_records = reader
            .find_transaction_records(TransactionId(100))
            .expect("Failed to find transaction records");

        assert_eq!(tx_records.len(), 4); // Begin, Insert, Update, Commit

        // Verify transaction IDs
        for record in &tx_records {
            match record {
                LogRecord::BeginTransaction { tx_id, .. } => assert_eq!(*tx_id, TransactionId(100)),
                LogRecord::InsertRecord { tx_id, .. } => assert_eq!(*tx_id, TransactionId(100)),
                LogRecord::UpdateRecord { tx_id, .. } => assert_eq!(*tx_id, TransactionId(100)),
                LogRecord::CommitTransaction { tx_id, .. } => {
                    assert_eq!(*tx_id, TransactionId(100))
                }
                _ => panic!("Unexpected record type"),
            }
        }
    }

    #[test]
    fn test_lsn_ordering_validation() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let wal_path = temp_file.path().to_path_buf();

        // Create records with invalid LSN ordering
        let invalid_records = vec![
            LogRecord::BeginTransaction {
                lsn: 5, // Higher LSN first
                tx_id: TransactionId(100),
            },
            LogRecord::CommitTransaction {
                lsn: 3, // Lower LSN second - should cause error
                tx_id: TransactionId(100),
                prev_lsn: 5,
            },
        ];

        // Write invalid records
        let config = WalWriterConfig { max_buffer_size: 1000, flush_interval_ms: None };
        let mut writer = WalWriter::new(wal_path, config);

        for record in &invalid_records {
            writer.add_record(&record.clone()).expect("Failed to add record");
        }
        writer.flush().expect("Failed to flush WAL");

        // Try to read with LSN validation enabled
        let reader_config = WalReaderConfig { buffer_size: 8192, validate_lsn_ordering: true };
        let reader = WalReader::new(temp_file.path(), reader_config);

        let result = reader.read_all_records();
        assert!(matches!(result, Err(WalReaderError::LsnOrderingViolation { .. })));
    }

    #[test]
    fn test_lsn_ordering_validation_disabled() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let wal_path = temp_file.path().to_path_buf();

        // Create records with invalid LSN ordering
        let invalid_records = vec![
            LogRecord::BeginTransaction { lsn: 5, tx_id: TransactionId(100) },
            LogRecord::CommitTransaction {
                lsn: 3, // Lower LSN - should be accepted when validation is disabled
                tx_id: TransactionId(100),
                prev_lsn: 5,
            },
        ];

        // Write invalid records
        let config = WalWriterConfig { max_buffer_size: 1000, flush_interval_ms: None };
        let mut writer = WalWriter::new(wal_path, config);

        for record in &invalid_records {
            writer.add_record(&record.clone()).expect("Failed to add record");
        }
        writer.flush().expect("Failed to flush WAL");

        // Read with LSN validation disabled
        let reader_config = WalReaderConfig { buffer_size: 8192, validate_lsn_ordering: false };
        let reader = WalReader::new(temp_file.path(), reader_config);

        let records = reader.read_all_records().expect("Should succeed with validation disabled");
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn test_get_statistics() {
        let (temp_file, _) = create_test_wal_file();
        let reader = WalReader::with_defaults(temp_file.path());

        let stats = reader.get_statistics().expect("Failed to get statistics");

        assert_eq!(stats.total_records, 4);
        assert_eq!(stats.begin_transaction_count, 1);
        assert_eq!(stats.commit_transaction_count, 1);
        assert_eq!(stats.insert_record_count, 1);
        assert_eq!(stats.update_record_count, 1);
        assert_eq!(stats.delete_record_count, 0);
    }

    #[test]
    fn test_records_read_counter() {
        let (temp_file, expected_records) = create_test_wal_file();
        let config = WalReaderConfig::default();
        let mut iterator =
            WalRecordIterator::new(temp_file.path(), config).expect("Failed to create iterator");

        assert_eq!(iterator.records_read(), 0);

        let _ = iterator.next_record().expect("Failed to read first record");
        assert_eq!(iterator.records_read(), 1);

        let _ = iterator.next_record().expect("Failed to read second record");
        assert_eq!(iterator.records_read(), 2);

        // Read remaining records
        while iterator.next_record().expect("Failed to read record").is_some() {
            // Continue reading
        }

        assert_eq!(iterator.records_read(), expected_records.len());
    }

    #[test]
    fn test_empty_wal_file() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let reader = WalReader::with_defaults(temp_file.path());

        let records = reader.read_all_records().expect("Failed to read empty file");
        assert_eq!(records.len(), 0);

        let stats = reader.get_statistics().expect("Failed to get statistics");
        assert_eq!(stats.total_records, 0);
    }

    #[test]
    fn test_find_last_checkpoint() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let wal_path = temp_file.path().to_path_buf();

        // Create records with checkpoints
        let records_with_checkpoint = vec![
            LogRecord::BeginTransaction { lsn: 1, tx_id: TransactionId(100) },
            LogRecord::CheckpointBegin { lsn: 2 },
            LogRecord::CheckpointEnd { lsn: 3, active_transactions: vec![], dirty_pages: vec![] },
            LogRecord::CommitTransaction { lsn: 4, tx_id: TransactionId(100), prev_lsn: 1 },
        ];

        // Write records
        let config = WalWriterConfig { max_buffer_size: 1000, flush_interval_ms: None };
        let mut writer = WalWriter::new(wal_path, config);

        for record in &records_with_checkpoint {
            writer.add_record(&record.clone()).expect("Failed to add record");
        }
        writer.flush().expect("Failed to flush WAL");

        let reader = WalReader::with_defaults(temp_file.path());
        let checkpoint = reader.find_last_checkpoint().expect("Failed to find checkpoint");

        assert!(checkpoint.is_some());
        let (begin, end) = checkpoint.unwrap();

        assert!(matches!(begin, LogRecord::CheckpointBegin { lsn: 2 }));
        assert!(matches!(end, LogRecord::CheckpointEnd { lsn: 3, .. }));
    }

    #[test]
    fn test_find_last_checkpoint_none() {
        let (temp_file, _) = create_test_wal_file(); // No checkpoints in this file
        let reader = WalReader::with_defaults(temp_file.path());

        let checkpoint = reader.find_last_checkpoint().expect("Failed to search for checkpoint");
        assert!(checkpoint.is_none());
    }
}
