use std::fs::OpenOptions;
use std::io::{BufWriter, Error as IoError, ErrorKind as IoErrorKind, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use crate::core::wal::log_record::LogRecord;

/// Configuration for WAL Writer with zero-cost abstractions
#[derive(Debug, Clone, Copy)]
pub struct WalWriterConfig {
    pub max_buffer_size: usize,
    pub flush_interval_ms: Option<u64>,
}

impl Default for WalWriterConfig {
    fn default() -> Self {
        WalWriterConfig {
            max_buffer_size: 1024,         // Default max buffer size (number of records)
            flush_interval_ms: Some(1000), // Default 1 second interval
        }
    }
}

/// Pure stdlib WAL writer with minimal dependencies
#[derive(Debug)]
pub struct WalWriter {
    buffer: Vec<LogRecord>,
    wal_file_path: PathBuf,
    config: WalWriterConfig,
    last_flush_time: Option<Instant>,
}

impl WalWriter {
    pub fn new(wal_file_path: PathBuf, config: WalWriterConfig) -> Self {
        eprintln!(
            "[core::wal::writer::WalWriter::new] Initialized with wal_file_path: {:?}, config: {:?}",
            &wal_file_path, &config
        );
        let last_flush_time =
            if config.flush_interval_ms.is_some() { Some(Instant::now()) } else { None };
        WalWriter { 
            buffer: Vec::new(), 
            wal_file_path, 
            config, 
            last_flush_time 
        }
    }

    pub fn add_record(&mut self, record: LogRecord) -> Result<(), IoError> {
        eprintln!(
            "[core::wal::writer::WalWriter::add_record] Adding record: {:?}, current buffer size: {}, max_buffer_size: {}",
            &record,
            self.buffer.len(),
            self.config.max_buffer_size
        );
        
        self.buffer.push(record.clone());

        // BUG FIX #3: Correct zero buffer size handling
        // Use iterator-based zero-cost abstraction for flush decision
        let should_flush = [
            // Policy 1: Commit record always triggers flush
            matches!(record, LogRecord::CommitTransaction { .. }),
            // Policy 2: Buffer size exceeded (fixed for zero case)
            self.config.max_buffer_size == 0 || self.buffer.len() >= self.config.max_buffer_size,
            // Policy 3: Periodic flush interval exceeded
            self.should_flush_by_interval(),
        ]
        .iter()
        .any(|&condition| condition);

        if should_flush {
            let flush_reason = self.determine_flush_reason(&record);
            eprintln!(
                "[core::wal::writer::WalWriter::add_record] Triggering flush. Reason: {}.",
                flush_reason
            );
            self.flush()
        } else {
            Ok(())
        }
    }

    /// Zero-cost abstraction for interval-based flush check
    #[inline]
    fn should_flush_by_interval(&self) -> bool {
        self.config.flush_interval_ms
            .zip(self.last_flush_time)
            .map(|(interval_ms, last_flush)| {
                last_flush.elapsed() >= Duration::from_millis(interval_ms)
            })
            .unwrap_or(false)
    }

    /// Zero-cost abstraction for determining flush reason
    #[inline]
    fn determine_flush_reason(&self, record: &LogRecord) -> String {
        if matches!(record, LogRecord::CommitTransaction { .. }) {
            "Commit record".to_string()
        } else if self.config.max_buffer_size == 0 {
            "Zero buffer size - immediate flush".to_string()
        } else if self.buffer.len() >= self.config.max_buffer_size {
            format!(
                "Max buffer size ({}) exceeded (current: {})",
                self.config.max_buffer_size,
                self.buffer.len()
            )
        } else if self.should_flush_by_interval() {
            format!("Flush interval ({}ms) exceeded", 
                   self.config.flush_interval_ms.unwrap_or(0))
        } else {
            "Unknown reason".to_string()
        }
    }

    pub fn flush(&mut self) -> Result<(), IoError> {
        if self.buffer.is_empty() {
            eprintln!("[core::wal::writer::WalWriter::flush] Buffer empty, nothing to flush.");
            return Ok(());
        }
        
        eprintln!(
            "[core::wal::writer::WalWriter::flush] Flushing {} records. Attempting to open/create file: {:?}",
            self.buffer.len(),
            &self.wal_file_path
        );
        
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.wal_file_path)
            .map_err(|e| {
                eprintln!(
                    "[core::wal::writer::WalWriter::flush] Error opening file {:?}: {}",
                    &self.wal_file_path, e
                );
                e
            })?;

        eprintln!(
            "[core::wal::writer::WalWriter::flush] Successfully opened/created file: {:?}",
            &self.wal_file_path
        );

        let mut writer = BufWriter::new(file);

        // Use iterator combinators for zero-cost serialization
        self.buffer
            .iter()
            .try_for_each(|record| -> Result<(), IoError> {
                // Pure stdlib serialization - no external dependencies
                let serialized_record = self.serialize_record(record)?;
                let len = serialized_record.len() as u32;
                writer.write_all(&len.to_be_bytes())?;
                writer.write_all(&serialized_record)
            })?;

        writer.flush()?;

        // Get the underlying file back from BufWriter to sync
        let file = writer.into_inner().map_err(|e| {
            IoError::new(
                IoErrorKind::Other,
                format!("Failed to get file from BufWriter: {}", e.into_error()),
            )
        })?;
        file.sync_all()?;

        self.buffer.clear();
        if self.config.flush_interval_ms.is_some() {
            self.last_flush_time = Some(Instant::now());
            eprintln!("[core::wal::writer::WalWriter::flush] Updated last_flush_time.");
        }
        eprintln!("[core::wal::writer::WalWriter::flush] Flush successful.");
        Ok(())
    }

    /// Pure stdlib serialization - no external dependencies
    fn serialize_record(&self, record: &LogRecord) -> Result<Vec<u8>, IoError> {
        // Simple binary serialization using only stdlib
        let mut buffer = Vec::new();
        
        match record {
            LogRecord::StartTransaction { lsn, transaction_id } => {
                buffer.push(1u8); // Type marker
                buffer.extend_from_slice(&lsn.to_be_bytes());
                buffer.extend_from_slice(&transaction_id.to_be_bytes());
            }
            LogRecord::CommitTransaction { lsn, transaction_id } => {
                buffer.push(2u8); // Type marker
                buffer.extend_from_slice(&lsn.to_be_bytes());
                buffer.extend_from_slice(&transaction_id.to_be_bytes());
            }
            LogRecord::AbortTransaction { lsn, transaction_id } => {
                buffer.push(3u8); // Type marker
                buffer.extend_from_slice(&lsn.to_be_bytes());
                buffer.extend_from_slice(&transaction_id.to_be_bytes());
            }
            LogRecord::Insert { lsn, transaction_id, key, value } => {
                buffer.push(4u8); // Type marker
                buffer.extend_from_slice(&lsn.to_be_bytes());
                buffer.extend_from_slice(&transaction_id.to_be_bytes());
                buffer.extend_from_slice(&(key.len() as u32).to_be_bytes());
                buffer.extend_from_slice(key);
                buffer.extend_from_slice(&(value.len() as u32).to_be_bytes());
                buffer.extend_from_slice(value);
            }
            LogRecord::Update { lsn, transaction_id, key, old_value, new_value } => {
                buffer.push(5u8); // Type marker
                buffer.extend_from_slice(&lsn.to_be_bytes());
                buffer.extend_from_slice(&transaction_id.to_be_bytes());
                buffer.extend_from_slice(&(key.len() as u32).to_be_bytes());
                buffer.extend_from_slice(key);
                buffer.extend_from_slice(&(old_value.len() as u32).to_be_bytes());
                buffer.extend_from_slice(old_value);
                buffer.extend_from_slice(&(new_value.len() as u32).to_be_bytes());
                buffer.extend_from_slice(new_value);
            }
            LogRecord::Delete { lsn, transaction_id, key, value } => {
                buffer.push(6u8); // Type marker
                buffer.extend_from_slice(&lsn.to_be_bytes());
                buffer.extend_from_slice(&transaction_id.to_be_bytes());
                buffer.extend_from_slice(&(key.len() as u32).to_be_bytes());
                buffer.extend_from_slice(key);
                buffer.extend_from_slice(&(value.len() as u32).to_be_bytes());
                buffer.extend_from_slice(value);
            }
        }
        
        Ok(buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::common::types::TransactionId;
    use crate::core::wal::log_record::LogRecord;
    use std::fs::{self, File};
    use std::io::{BufReader, Read};
    use std::path::Path;
    use std::thread; // For testing periodic flush
    use std::time::Duration as StdDuration; // For testing periodic flush

    // Helper function to clean up test files
    fn cleanup_file(path: &Path) {
        let _ = fs::remove_file(path); // Ignore error if file doesn't exist
    }

    use std::path::PathBuf;

    // Using unique names for test files to prevent interference
    const TEST_NEW_WAL_FILE: &str = "test_wal_writer_new_output.log";
    const TEST_ADD_RECORD_WAL_FILE: &str = "test_wal_writer_add_record_output.log";
    const TEST_FLUSH_EMPTY_WAL_FILE: &str = "test_flush_empty_buffer_output.log";
    const TEST_FLUSH_WRITES_WAL_FILE: &str = "test_flush_writes_records_output.log";
    const TEST_FLUSH_APPENDS_WAL_FILE: &str = "test_flush_appends_records_output.log";
    const TEST_ADD_NON_COMMIT_NO_FLUSH_FILE: &str = "test_add_non_commit_no_flush.log";
    const TEST_ADD_COMMIT_FLUSHES_FILE: &str = "test_add_commit_flushes.log";
    const TEST_ADD_COMMIT_FLUSH_FAILS_FILE: &str = "test_add_commit_flush_fails.logdir";
    const TEST_PERIODIC_FLUSH_FILE: &str = "test_periodic_flush.log";
    const TEST_PERIODIC_FLUSH_DISABLED_FILE: &str = "test_periodic_flush_disabled.log";
    const TEST_MAX_BUFFER_SIZE_ZERO_FILE: &str = "test_max_buffer_size_zero.log";
    const TEST_MAX_BUFFER_SIZE_ONE_FILE: &str = "test_max_buffer_size_one.log";
    const TEST_PERIODIC_FLUSH_INTERVAL_ZERO_FILE: &str = "test_periodic_flush_interval_zero.log";
    const TEST_PERIODIC_FREQUENT_RECORDS_FILE: &str = "test_periodic_frequent_records.log";
    const TEST_PERIODIC_INFREQUENT_RECORDS_FILE: &str = "test_periodic_infrequent_records.log";
    const TEST_PERIODIC_INTERACTION_COMMIT_BEFORE_PERIODIC_FILE: &str =
        "test_periodic_commit_before_periodic.log";
    const TEST_PERIODIC_INTERACTION_COMMIT_AFTER_PERIODIC_FILE: &str =
        "test_periodic_commit_after_periodic.log";
    const TEST_LAST_FLUSH_TIME_UPDATE_SIZE_BASED_FILE: &str =
        "test_last_flush_time_update_size_based.log";
    const TEST_LAST_FLUSH_TIME_UPDATE_COMMIT_BASED_FILE: &str =
        "test_last_flush_time_update_commit_based.log";

    #[test]
    fn test_wal_writer_new() {
        let test_file_path = PathBuf::from(TEST_NEW_WAL_FILE);
        cleanup_file(&test_file_path);
        let config = WalWriterConfig::default();

        let writer = WalWriter::new(test_file_path.clone(), config);
        assert!(writer.buffer.is_empty());
        assert_eq!(writer.wal_file_path, test_file_path);
        assert_eq!(writer.config.max_buffer_size, config.max_buffer_size);
        assert_eq!(writer.config.flush_interval_ms, config.flush_interval_ms);
        if config.flush_interval_ms.is_some() {
            assert!(writer.last_flush_time.is_some());
        } else {
            assert!(writer.last_flush_time.is_none());
        }

        cleanup_file(&test_file_path);
    }

    // Helper function to clean up a directory
    fn cleanup_dir(path: &Path) {
        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn test_add_commit_flush_fails_returns_error() {
        let test_dir_path = PathBuf::from(TEST_ADD_COMMIT_FLUSH_FAILS_FILE);
        cleanup_dir(&test_dir_path);
        fs::create_dir_all(&test_dir_path).expect("Should be able to create test directory");
        // Disable other flush mechanisms for this test
        let config = WalWriterConfig { max_buffer_size: usize::MAX, flush_interval_ms: None };

        let mut writer = WalWriter::new(test_dir_path.clone(), config);

        let record1 = LogRecord::BeginTransaction { lsn: 0, tx_id: TransactionId(404) };
        // This add_record should be Ok because max_buffer_size is usize::MAX and no periodic flush
        assert!(writer.add_record(record1.clone()).is_ok());

        let record2 =
            LogRecord::CommitTransaction { lsn: 1, tx_id: TransactionId(404), prev_lsn: 0 };
        // This add_record should trigger flush due to Commit, which will fail
        let result = writer.add_record(record2.clone());

        assert!(result.is_err(), "add_record with commit should return Err when flush fails");
        assert!(!writer.buffer.is_empty(), "Buffer should not be cleared if flush fails");
        assert_eq!(
            writer.buffer.len(),
            2,
            "Buffer should still contain all records because flush failed"
        );
        assert_eq!(writer.buffer[0], record1);
        assert_eq!(writer.buffer[1], record2);

        cleanup_dir(&test_dir_path);
    }

    #[test]
    fn test_add_commit_record_flushes_and_clears_buffer() {
        let test_file_path = PathBuf::from(TEST_ADD_COMMIT_FLUSHES_FILE);
        cleanup_file(&test_file_path);
        // Disable other flush mechanisms
        let config = WalWriterConfig { max_buffer_size: usize::MAX, flush_interval_ms: None };

        let mut writer = WalWriter::new(test_file_path.clone(), config);
        let mut lsn_counter: u64 = 0;
        let mut next_lsn = || {
            let current = lsn_counter;
            lsn_counter += 1;
            current
        };

        let record1 = LogRecord::BeginTransaction { lsn: next_lsn(), tx_id: TransactionId(789) };
        let record2 = LogRecord::InsertRecord {
            lsn: next_lsn(),
            tx_id: TransactionId(789),
            page_id: crate::core::common::types::ids::PageId(1),
            slot_id: crate::core::common::types::ids::SlotId(0),
            record_data: vec![1, 2, 3],
            prev_lsn: 0,
        };
        let record3 = LogRecord::CommitTransaction {
            lsn: next_lsn(),
            tx_id: TransactionId(789),
            prev_lsn: 1,
        };

        assert!(writer.add_record(record1.clone()).is_ok());
        assert!(writer.add_record(record2.clone()).is_ok());
        assert_eq!(writer.buffer.len(), 2, "Buffer should have two records before commit");
        assert!(!test_file_path.exists(), "WAL file should not exist before commit");

        // Add commit record - this should trigger flush
        let result = writer.add_record(record3.clone());
        assert!(result.is_ok(), "add_record for commit should return Ok on successful flush");
        assert!(writer.buffer.is_empty(), "Buffer should be empty after commit and flush");
        assert!(test_file_path.exists(), "WAL file should be created after commit and flush");

        // Verify contents
        let records_from_file =
            read_records_from_file(&test_file_path).expect("Failed to read records from WAL file");
        assert_eq!(records_from_file.len(), 3);
        assert_eq!(records_from_file[0], record1);
        assert_eq!(records_from_file[1], record2);
        assert_eq!(records_from_file[2], record3);

        cleanup_file(&test_file_path);
    }

    #[test]
    fn test_add_non_commit_record_does_not_flush() {
        let test_file_path = PathBuf::from(TEST_ADD_NON_COMMIT_NO_FLUSH_FILE);
        cleanup_file(&test_file_path);
        // Configure high max buffer and no periodic flush to isolate non-commit behavior
        let config = WalWriterConfig { max_buffer_size: 100, flush_interval_ms: None };

        let mut writer = WalWriter::new(test_file_path.clone(), config);
        let record = LogRecord::BeginTransaction { lsn: 0, tx_id: TransactionId(123) };

        let result = writer.add_record(record.clone());
        assert!(result.is_ok(), "add_record for non-commit should return Ok");
        assert_eq!(writer.buffer.len(), 1, "Buffer should contain the added record");
        assert_eq!(writer.buffer[0], record);
        assert!(!test_file_path.exists(), "WAL file should not be created by non-commit record");

        cleanup_file(&test_file_path);
    }

    #[test]
    fn test_wal_writer_add_record() {
        let test_file_path = PathBuf::from(TEST_ADD_RECORD_WAL_FILE);
        cleanup_file(&test_file_path);
        // For this specific test, we rely on commit-based flush.
        // Disable other flush mechanisms to keep test focused.
        let config = WalWriterConfig { max_buffer_size: usize::MAX, flush_interval_ms: None };

        let mut writer = WalWriter::new(test_file_path.clone(), config);
        let mut lsn_counter: u64 = 0;
        let mut next_lsn = || {
            let current = lsn_counter;
            lsn_counter += 1;
            current
        };

        let record1 = LogRecord::BeginTransaction { lsn: next_lsn(), tx_id: TransactionId(1) };
        assert!(writer.add_record(record1.clone()).is_ok());
        assert_eq!(writer.buffer.len(), 1);
        assert_eq!(writer.buffer[0], record1);
        assert!(
            !test_file_path.exists(),
            "File should not be created by non-commit record (config dependent)"
        );

        let record2 =
            LogRecord::CommitTransaction { lsn: next_lsn(), tx_id: TransactionId(1), prev_lsn: 0 };
        assert!(writer.add_record(record2.clone()).is_ok());
        assert!(writer.buffer.is_empty(), "Buffer should be empty after commit record (flush)");
        assert!(test_file_path.exists(), "File should be created by commit record");

        let records_from_file =
            read_records_from_file(&test_file_path).expect("Failed to read records");
        assert_eq!(records_from_file.len(), 2);
        assert_eq!(records_from_file[0], record1);
        assert_eq!(records_from_file[1], record2);

        cleanup_file(&test_file_path);
    }

    #[test]
    fn test_flush_empty_buffer() {
        let test_file_path = PathBuf::from(TEST_FLUSH_EMPTY_WAL_FILE);
        cleanup_file(&test_file_path);
        let config = WalWriterConfig::default(); // Use default config

        let mut writer = WalWriter::new(test_file_path.clone(), config);
        let result = writer.flush();
        assert!(result.is_ok());
        assert!(!test_file_path.exists(), "File should not be created for empty flush");

        cleanup_file(&test_file_path);
    }

    fn read_records_from_file(path: &Path) -> Result<Vec<LogRecord>, IoError> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut records = Vec::new();

        loop {
            let mut len_bytes = [0u8; 4]; // u32 for length
            match reader.read_exact(&mut len_bytes) {
                Ok(_) => (),
                Err(ref e) if e.kind() == IoErrorKind::UnexpectedEof => {
                    // Reached end of file, expected if no more records
                    break;
                }
                Err(e) => return Err(e),
            }

            let len = u32::from_be_bytes(len_bytes);
            let mut record_bytes = vec![0u8; len as usize];
            reader.read_exact(&mut record_bytes)?;

            let record: LogRecord = bincode::deserialize(&record_bytes).map_err(|e| {
                IoError::new(
                    IoErrorKind::InvalidData,
                    format!("Log record deserialization failed: {}", e),
                )
            })?;
            records.push(record);
        }
        Ok(records)
    }

    #[test]
    fn test_flush_writes_records() {
        let test_file_path = PathBuf::from(TEST_FLUSH_WRITES_WAL_FILE);
        cleanup_file(&test_file_path);
        // High max buffer, no periodic to isolate commit flush
        let config = WalWriterConfig { max_buffer_size: 100, flush_interval_ms: None };

        let mut writer = WalWriter::new(test_file_path.clone(), config);
        let mut lsn_counter: u64 = 0;
        let mut next_lsn = || {
            let current = lsn_counter;
            lsn_counter += 1;
            current
        };

        let record1 = LogRecord::BeginTransaction { lsn: next_lsn(), tx_id: TransactionId(10) };
        let record2 =
            LogRecord::CommitTransaction { lsn: next_lsn(), tx_id: TransactionId(10), prev_lsn: 1 };

        assert!(writer.add_record(record1.clone()).is_ok());
        assert_eq!(writer.buffer.len(), 1, "Buffer should contain one record before commit");
        assert!(
            !test_file_path.exists(),
            "WAL file should not be created before commit (config dependent)"
        );

        let add_commit_result = writer.add_record(record2.clone()); // auto-flushes on commit
        assert!(add_commit_result.is_ok());
        assert!(
            writer.buffer.is_empty(),
            "Buffer should be empty after commit record (auto-flush)"
        );
        assert!(test_file_path.exists(), "WAL file should be created after commit");

        let flush_result = writer.flush(); // Explicit flush (should do nothing)
        assert!(flush_result.is_ok());
        assert!(writer.buffer.is_empty(), "Buffer should remain empty after explicit flush");

        let records_from_file =
            read_records_from_file(&test_file_path).expect("Failed to read records from WAL file");
        assert_eq!(records_from_file.len(), 2);
        assert_eq!(records_from_file[0], record1);
        assert_eq!(records_from_file[1], record2);

        cleanup_file(&test_file_path);
    }

    #[test]
    fn test_flush_appends_records() {
        let test_file_path = PathBuf::from(TEST_FLUSH_APPENDS_WAL_FILE);
        cleanup_file(&test_file_path);
        // High max buffer, no periodic to isolate explicit and commit flushes
        let config = WalWriterConfig { max_buffer_size: 100, flush_interval_ms: None };

        let mut writer = WalWriter::new(test_file_path.clone(), config);
        let mut lsn_counter: u64 = 0;
        let mut next_lsn = || {
            let current = lsn_counter;
            lsn_counter += 1;
            current
        };

        // First batch - explicit flush
        let record1 = LogRecord::BeginTransaction { lsn: next_lsn(), tx_id: TransactionId(20) };
        assert!(writer.add_record(record1.clone()).is_ok());
        assert_eq!(writer.buffer.len(), 1);
        let flush_result1 = writer.flush(); // Explicit flush
        assert!(flush_result1.is_ok());
        assert!(writer.buffer.is_empty());
        assert!(test_file_path.exists());

        // Second batch - commit flush
        let record2 = LogRecord::InsertRecord {
            lsn: next_lsn(),
            tx_id: TransactionId(20),
            page_id: crate::core::common::types::ids::PageId(1),
            slot_id: crate::core::common::types::ids::SlotId(0),
            record_data: vec![1, 2, 3],
            prev_lsn: 0,
        };
        let record3 =
            LogRecord::CommitTransaction { lsn: next_lsn(), tx_id: TransactionId(20), prev_lsn: 1 };

        assert!(writer.add_record(record2.clone()).is_ok());
        assert_eq!(writer.buffer.len(), 1);
        let add_commit_result = writer.add_record(record3.clone()); // Auto-flush on commit
        assert!(add_commit_result.is_ok());
        assert!(writer.buffer.is_empty());

        let flush_result2 = writer.flush(); // Explicit flush (should do nothing)
        assert!(flush_result2.is_ok());
        assert!(writer.buffer.is_empty());

        let records_from_file =
            read_records_from_file(&test_file_path).expect("Failed to read records from WAL file");
        assert_eq!(records_from_file.len(), 3);
        assert_eq!(records_from_file[0], record1);
        assert_eq!(records_from_file[1], record2);
        assert_eq!(records_from_file[2], record3);

        cleanup_file(&test_file_path);
    }

    #[test]
    fn test_max_buffer_size_triggers_flush() {
        use tempfile::NamedTempFile;
        let temp_file = NamedTempFile::new().expect("Failed to create temp WAL file");
        let test_file_path = temp_file.path().to_path_buf();
        // No need to cleanup_file, tempfile handles it
        // Flush when 2 records are in buffer, disable periodic flush
        let config = WalWriterConfig { max_buffer_size: 2, flush_interval_ms: None };

        let mut writer = WalWriter::new(test_file_path.clone(), config);
        let mut lsn_counter: u64 = 0;
        let mut next_lsn = || {
            let current = lsn_counter;
            lsn_counter += 1;
            current
        };

        let record1 = LogRecord::BeginTransaction { lsn: next_lsn(), tx_id: TransactionId(300) };
        let record2 = LogRecord::InsertRecord {
            lsn: next_lsn(),
            tx_id: TransactionId(300),
            prev_lsn: 0,
            page_id: crate::core::common::types::ids::PageId(1),
            slot_id: crate::core::common::types::ids::SlotId(0),
            record_data: vec![1],
        };
        let record3 = LogRecord::BeginTransaction { lsn: next_lsn(), tx_id: TransactionId(301) };

        // Add first record, buffer size = 1, no flush
        assert!(writer.add_record(record1.clone()).is_ok());
        assert_eq!(writer.buffer.len(), 1);

        // Add second record, buffer size = 2, should trigger flush (as per config.max_buffer_size = 2)
        assert!(writer.add_record(record2.clone()).is_ok());
        assert!(writer.buffer.is_empty(), "Buffer should be empty after max_buffer_size flush");
        assert!(test_file_path.exists(), "WAL file should exist after max_buffer_size flush");

        let records_from_file1 =
            read_records_from_file(&test_file_path).expect("Read failed stage 1");
        assert_eq!(records_from_file1.len(), 2);
        assert_eq!(records_from_file1[0], record1);
        assert_eq!(records_from_file1[1], record2);

        // Add third record, buffer size = 1, no flush
        assert!(writer.add_record(record3.clone()).is_ok());
        assert_eq!(writer.buffer.len(), 1);

        // Explicitly flush the third record for cleanup and verification
        assert!(writer.flush().is_ok());
        assert!(writer.buffer.is_empty());

        let records_from_file2 =
            read_records_from_file(&test_file_path).expect("Read failed stage 2");
        assert_eq!(records_from_file2.len(), 3, "Should have all three records now");
        assert_eq!(records_from_file2[2], record3);
        // No explicit cleanup needed
    }

    #[test]
    fn test_periodic_flush_triggers_flush() {
        let test_file_path = PathBuf::from(TEST_PERIODIC_FLUSH_FILE);
        cleanup_file(&test_file_path);
        let flush_interval_ms = 50;
        // Set high max_buffer_size to ensure periodic flush is the trigger
        let config =
            WalWriterConfig { max_buffer_size: 100, flush_interval_ms: Some(flush_interval_ms) };

        let mut writer = WalWriter::new(test_file_path.clone(), config);
        let initial_last_flush_time = writer.last_flush_time;
        assert!(initial_last_flush_time.is_some(), "last_flush_time should be set at init");

        let mut lsn_counter: u64 = 0;
        let mut next_lsn = || {
            let current = lsn_counter;
            lsn_counter += 1;
            current
        };

        let record1 = LogRecord::BeginTransaction { lsn: next_lsn(), tx_id: TransactionId(400) };

        // Add record, buffer should not be empty yet, file should not exist
        assert!(writer.add_record(record1.clone()).is_ok());
        assert_eq!(writer.buffer.len(), 1);
        assert!(!test_file_path.exists());
        // last_flush_time should not change as no flush condition met yet (other than init)
        assert_eq!(writer.last_flush_time, initial_last_flush_time);

        // Wait for longer than the flush interval
        thread::sleep(StdDuration::from_millis(flush_interval_ms + 50)); // Wait a bit longer

        // Add another record. This add_record call should notice the interval has passed and trigger a flush.
        // The flush will include record1 (already in buffer) and record2 (being added).
        let record2 = LogRecord::InsertRecord {
            lsn: next_lsn(),
            tx_id: TransactionId(400),
            prev_lsn: 0,
            page_id: crate::core::common::types::ids::PageId(2),
            slot_id: crate::core::common::types::ids::SlotId(0),
            record_data: vec![2],
        };
        assert!(writer.add_record(record2.clone()).is_ok());

        assert!(
            writer.buffer.is_empty(),
            "Buffer should be empty after periodic flush processing during add_record"
        );
        assert!(test_file_path.exists(), "WAL file should exist after periodic flush");
        assert!(
            writer.last_flush_time.is_some() && writer.last_flush_time > initial_last_flush_time,
            "last_flush_time should be updated after periodic flush"
        );

        let records_from_file =
            read_records_from_file(&test_file_path).expect("Failed to read records");
        assert_eq!(records_from_file.len(), 2, "Should have flushed both records");
        assert_eq!(records_from_file[0], record1);
        assert_eq!(records_from_file[1], record2);

        cleanup_file(&test_file_path);
    }

    #[test]
    fn test_periodic_flush_disabled_no_auto_flush() {
        let test_file_path = PathBuf::from(TEST_PERIODIC_FLUSH_DISABLED_FILE);
        cleanup_file(&test_file_path);
        // Disable periodic flush, high max buffer size
        let config = WalWriterConfig { max_buffer_size: 100, flush_interval_ms: None };

        let mut writer = WalWriter::new(test_file_path.clone(), config);
        assert!(
            writer.last_flush_time.is_none(),
            "last_flush_time should be None when interval is None"
        );

        let mut lsn_counter: u64 = 0;
        let mut next_lsn = || {
            let current = lsn_counter;
            lsn_counter += 1;
            current
        };
        let record1 = LogRecord::BeginTransaction { lsn: next_lsn(), tx_id: TransactionId(500) };

        assert!(writer.add_record(record1.clone()).is_ok());
        assert_eq!(writer.buffer.len(), 1);

        thread::sleep(StdDuration::from_millis(100)); // Wait for some time

        // No flush should occur as periodic flush is disabled
        assert_eq!(writer.buffer.len(), 1, "Buffer should still contain the record");
        assert!(!test_file_path.exists(), "WAL file should not exist");

        // Explicit flush to write record and clean up
        assert!(writer.flush().is_ok());
        assert!(writer.buffer.is_empty());
        assert!(test_file_path.exists());
        let records_from_file =
            read_records_from_file(&test_file_path).expect("Failed to read records");
        assert_eq!(records_from_file.len(), 1);
        assert_eq!(records_from_file[0], record1);

        cleanup_file(&test_file_path);
    }

    #[test]
    fn test_max_buffer_size_zero_flushes_every_record() {
        let test_file_path = PathBuf::from(TEST_MAX_BUFFER_SIZE_ZERO_FILE);
        cleanup_file(&test_file_path);
        // max_buffer_size = 0 means it should flush after every non-commit record if buffer becomes >= 0.
        // However, our current logic is `self.buffer.len() >= self.config.max_buffer_size`.
        // If max_buffer_size is 0, and buffer is empty, len (0) is not >= 0. This is wrong.
        // It should be len > 0 and len >= max_buffer_size.
        // Let's assume current logic: len (1) >= 0 is true. So it flushes after 1 record.
        // If max_buffer_size is 0, it implies an intent to flush immediately.
        // The condition `self.buffer.len() >= self.config.max_buffer_size` with size 0 means
        // `self.buffer.len() >= 0`. After one push, len is 1, so `1 >= 0` is true.
        // This means it will flush *after* the first record is added.
        let config = WalWriterConfig { max_buffer_size: 0, flush_interval_ms: None };
        let mut writer = WalWriter::new(test_file_path.clone(), config);
        let mut lsn_counter: u64 = 0;
        let mut next_lsn = || {
            let current = lsn_counter;
            lsn_counter += 1;
            current
        };

        let record1 = LogRecord::BeginTransaction { lsn: next_lsn(), tx_id: TransactionId(600) };
        assert!(writer.add_record(record1.clone()).is_ok());
        assert!(
            writer.buffer.is_empty(),
            "Buffer should be empty after adding one record with max_buffer_size=0"
        );
        assert!(test_file_path.exists(), "WAL file should exist");
        let records_from_file1 =
            read_records_from_file(&test_file_path).expect("Read failed stage 1");
        assert_eq!(records_from_file1.len(), 1);
        assert_eq!(records_from_file1[0], record1);

        cleanup_file(&test_file_path); // Clean up before next part of test

        let record2 = LogRecord::BeginTransaction { lsn: next_lsn(), tx_id: TransactionId(601) };
        assert!(writer.add_record(record2.clone()).is_ok());
        assert!(
            writer.buffer.is_empty(),
            "Buffer should be empty after adding second record with max_buffer_size=0"
        );
        let records_from_file2 =
            read_records_from_file(&test_file_path).expect("Read failed stage 2");
        assert_eq!(
            records_from_file2.len(),
            1,
            "File should contain only the second record as it was flushed"
        );
        assert_eq!(records_from_file2[0], record2);

        cleanup_file(&test_file_path);
    }

    #[test]
    fn test_max_buffer_size_one_flushes_every_record() {
        let test_file_path = PathBuf::from(TEST_MAX_BUFFER_SIZE_ONE_FILE);
        cleanup_file(&test_file_path);
        let config = WalWriterConfig { max_buffer_size: 1, flush_interval_ms: None };
        let mut writer = WalWriter::new(test_file_path.clone(), config);
        let mut lsn_counter: u64 = 0;
        let mut next_lsn = || {
            let current = lsn_counter;
            lsn_counter += 1;
            current
        };

        let record1 = LogRecord::BeginTransaction { lsn: next_lsn(), tx_id: TransactionId(700) };
        // Add first record, buffer size becomes 1. Since 1 >= 1, it flushes.
        assert!(writer.add_record(record1.clone()).is_ok());
        assert!(
            writer.buffer.is_empty(),
            "Buffer should be empty after adding one record with max_buffer_size=1"
        );
        assert!(test_file_path.exists(), "WAL file should exist");
        let records_from_file1 =
            read_records_from_file(&test_file_path).expect("Read failed stage 1");
        assert_eq!(records_from_file1.len(), 1);
        assert_eq!(records_from_file1[0], record1);

        cleanup_file(&test_file_path); // Clean for next check

        let record2 = LogRecord::BeginTransaction { lsn: next_lsn(), tx_id: TransactionId(701) };
        // Add second record, buffer size becomes 1. Since 1 >= 1, it flushes.
        assert!(writer.add_record(record2.clone()).is_ok());
        assert!(
            writer.buffer.is_empty(),
            "Buffer should be empty after adding second record with max_buffer_size=1"
        );
        let records_from_file2 =
            read_records_from_file(&test_file_path).expect("Read failed stage 2");
        assert_eq!(records_from_file2.len(), 1, "File should contain only the second record");
        assert_eq!(records_from_file2[0], record2);

        cleanup_file(&test_file_path);
    }

    #[test]
    fn test_periodic_flush_interval_zero_flushes_every_record() {
        let test_file_path = PathBuf::from(TEST_PERIODIC_FLUSH_INTERVAL_ZERO_FILE);
        cleanup_file(&test_file_path);
        // interval_ms = 0 means elapsed time will always be >= 0.
        let config = WalWriterConfig { max_buffer_size: 100, flush_interval_ms: Some(0) };
        let mut writer = WalWriter::new(test_file_path.clone(), config);
        let mut lsn_counter: u64 = 0;
        let mut next_lsn = || {
            let current = lsn_counter;
            lsn_counter += 1;
            current
        };

        let record1 = LogRecord::BeginTransaction { lsn: next_lsn(), tx_id: TransactionId(800) };
        // Sleep a tiny bit to ensure elapsed time > 0, though Instant::now() should differ.
        thread::sleep(StdDuration::from_micros(10));
        assert!(writer.add_record(record1.clone()).is_ok());
        assert!(
            writer.buffer.is_empty(),
            "Buffer should be empty after adding one record with interval_ms=0"
        );
        assert!(test_file_path.exists(), "WAL file should exist");
        let records_from_file1 =
            read_records_from_file(&test_file_path).expect("Read failed stage 1");
        assert_eq!(records_from_file1.len(), 1);
        assert_eq!(records_from_file1[0], record1);

        cleanup_file(&test_file_path);

        let record2 = LogRecord::BeginTransaction { lsn: next_lsn(), tx_id: TransactionId(801) };
        thread::sleep(StdDuration::from_micros(10));
        assert!(writer.add_record(record2.clone()).is_ok());
        assert!(
            writer.buffer.is_empty(),
            "Buffer should be empty after adding second record with interval_ms=0"
        );
        let records_from_file2 =
            read_records_from_file(&test_file_path).expect("Read failed stage 2");
        assert_eq!(records_from_file2.len(), 1);
        assert_eq!(records_from_file2[0], record2);

        cleanup_file(&test_file_path);
    }

    #[test]
    fn test_periodic_flush_frequent_records() {
        let test_file_path = PathBuf::from(TEST_PERIODIC_FREQUENT_RECORDS_FILE);
        cleanup_file(&test_file_path);
        let flush_interval_ms = 200;
        let config =
            WalWriterConfig { max_buffer_size: 100, flush_interval_ms: Some(flush_interval_ms) };
        let mut writer = WalWriter::new(test_file_path.clone(), config);
        let mut lsn_counter: u64 = 0;
        let mut next_lsn = || {
            let current = lsn_counter;
            lsn_counter += 1;
            current
        };

        let record1 = LogRecord::BeginTransaction { lsn: next_lsn(), tx_id: TransactionId(900) };
        let record2 = LogRecord::BeginTransaction { lsn: next_lsn(), tx_id: TransactionId(901) };
        let record3 = LogRecord::BeginTransaction { lsn: next_lsn(), tx_id: TransactionId(902) };

        // Add records quickly
        assert!(writer.add_record(record1.clone()).is_ok()); // last_flush_time updated at init
        thread::sleep(StdDuration::from_millis(flush_interval_ms / 3));
        assert!(writer.add_record(record2.clone()).is_ok()); // Should not flush yet
        thread::sleep(StdDuration::from_millis(flush_interval_ms / 3));
        assert!(writer.add_record(record3.clone()).is_ok()); // Should not flush yet

        assert_eq!(writer.buffer.len(), 3, "Buffer should have 3 records before periodic flush");

        // Wait for periodic flush to trigger
        thread::sleep(StdDuration::from_millis(flush_interval_ms * 2)); // Ensure time passes for flush

        // Add another record to trigger the check
        let record4 = LogRecord::BeginTransaction { lsn: next_lsn(), tx_id: TransactionId(903) };
        let last_flush_time_before_add = writer.last_flush_time;
        assert!(writer.add_record(record4.clone()).is_ok());
        // This add_record should have triggered a flush of record1, record2, record3, and record4.

        assert!(
            writer.buffer.is_empty(),
            "Buffer should be empty after periodic flush including record4"
        );
        assert!(
            writer.last_flush_time > last_flush_time_before_add,
            "last_flush_time should update"
        );

        let records_from_file = read_records_from_file(&test_file_path).expect("Read failed");
        assert_eq!(records_from_file.len(), 4, "File should contain all 4 records");
        assert_eq!(records_from_file[0], record1);
        assert_eq!(records_from_file[1], record2);
        assert_eq!(records_from_file[2], record3);
        assert_eq!(records_from_file[3], record4);

        cleanup_file(&test_file_path);
    }

    #[test]
    fn test_periodic_flush_infrequent_records() {
        let test_file_path = PathBuf::from(TEST_PERIODIC_INFREQUENT_RECORDS_FILE);
        cleanup_file(&test_file_path);
        let flush_interval_ms = 100;
        let config =
            WalWriterConfig { max_buffer_size: 100, flush_interval_ms: Some(flush_interval_ms) };
        let mut writer = WalWriter::new(test_file_path.clone(), config);
        let mut lsn_counter: u64 = 0;
        let mut next_lsn = || {
            let current = lsn_counter;
            lsn_counter += 1;
            current
        };

        let initial_last_flush_time = writer.last_flush_time;

        let record1 = LogRecord::BeginTransaction { lsn: next_lsn(), tx_id: TransactionId(1000) };
        assert!(writer.add_record(record1.clone()).is_ok());
        assert_eq!(writer.buffer.len(), 1);
        // No flush yet, so last_flush_time should be from init
        assert_eq!(writer.last_flush_time, initial_last_flush_time);

        thread::sleep(StdDuration::from_millis(flush_interval_ms + 50));

        let record2 = LogRecord::BeginTransaction { lsn: next_lsn(), tx_id: TransactionId(1001) };
        // This add_record will add record2 to buffer, then trigger flush of [record1, record2].
        assert!(writer.add_record(record2.clone()).is_ok());
        assert!(
            writer.buffer.is_empty(),
            "Buffer should be empty after periodic flush of record1 and record2"
        );
        assert!(
            writer.last_flush_time > initial_last_flush_time,
            "last_flush_time should have updated after flushing record1 and record2"
        );

        let records_from_file1 =
            read_records_from_file(&test_file_path).expect("Read failed stage 1");
        assert_eq!(records_from_file1.len(), 2, "File should contain record1 and record2");
        assert_eq!(records_from_file1[0], record1);
        assert_eq!(records_from_file1[1], record2);

        cleanup_file(&test_file_path); // Clear file for next check

        let last_flush_time_after_r1_r2_flush = writer.last_flush_time;
        thread::sleep(StdDuration::from_millis(flush_interval_ms + 50));

        let record3 = LogRecord::BeginTransaction { lsn: next_lsn(), tx_id: TransactionId(1002) };
        // This add_record will add record3 to buffer, then trigger flush of [record3].
        assert!(writer.add_record(record3.clone()).is_ok());
        assert!(writer.buffer.is_empty(), "Buffer should be empty after periodic flush of record3");
        assert!(
            writer.last_flush_time > last_flush_time_after_r1_r2_flush,
            "last_flush_time should have updated after flushing record3"
        );

        let records_from_file2 =
            read_records_from_file(&test_file_path).expect("Read failed stage 2");
        assert_eq!(records_from_file2.len(), 1, "File should contain only record3");
        assert_eq!(records_from_file2[0], record3);

        cleanup_file(&test_file_path);
    }

    #[test]
    fn test_periodic_interaction_commit_before_periodic() {
        let test_file_path = PathBuf::from(TEST_PERIODIC_INTERACTION_COMMIT_BEFORE_PERIODIC_FILE);
        cleanup_file(&test_file_path);
        let flush_interval_ms = 200;
        // High max_buffer_size to ensure it's not the trigger
        let config =
            WalWriterConfig { max_buffer_size: 100, flush_interval_ms: Some(flush_interval_ms) };
        let mut writer = WalWriter::new(test_file_path.clone(), config);
        let mut lsn_counter: u64 = 0;
        let mut next_lsn = || {
            let current = lsn_counter;
            lsn_counter += 1;
            current
        };

        let record1 = LogRecord::BeginTransaction { lsn: next_lsn(), tx_id: TransactionId(1100) };
        assert!(writer.add_record(record1.clone()).is_ok());
        let time_after_record1 = Instant::now();
        writer.last_flush_time = Some(time_after_record1); // Simulate this was the last flush time

        // Wait for a duration less than the interval, then commit
        thread::sleep(StdDuration::from_millis(flush_interval_ms / 2));

        let prev_lsn_for_commit = match record1 {
            LogRecord::BeginTransaction { lsn, .. } => lsn,
            _ => panic!("Expected BeginTransaction record for record1"),
        };
        let record_commit = LogRecord::CommitTransaction {
            lsn: next_lsn(),
            tx_id: TransactionId(1100),
            prev_lsn: prev_lsn_for_commit,
        };
        assert!(writer.add_record(record_commit.clone()).is_ok(), "Commit flush failed");
        // Commit should have flushed record1 and record_commit
        assert!(writer.buffer.is_empty(), "Buffer should be empty after commit flush");
        assert!(test_file_path.exists(), "WAL file should exist after commit flush");
        assert!(
            writer.last_flush_time.is_some()
                && writer.last_flush_time.unwrap() > time_after_record1,
            "last_flush_time should update after commit flush"
        );

        let records = read_records_from_file(&test_file_path).expect("Read failed");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0], record1);
        assert_eq!(records[1], record_commit);

        let last_flush_after_commit = writer.last_flush_time.unwrap();

        // Wait past original periodic interval.
        thread::sleep(StdDuration::from_millis(flush_interval_ms * 2)); // Ensure enough time for periodic if it were to happen
        let record_after_wait =
            LogRecord::BeginTransaction { lsn: next_lsn(), tx_id: TransactionId(1101) };

        // Adding record_after_wait. Since last_flush_time was updated by the commit,
        // and we've waited longer than flush_interval_ms, a periodic flush *will* occur here for record_after_wait.
        assert!(writer.add_record(record_after_wait.clone()).is_ok());
        assert!(
            writer.buffer.is_empty(),
            "Buffer should be empty after periodic flush of record_after_wait"
        );
        assert!(
            writer.last_flush_time.unwrap() > last_flush_after_commit,
            "last_flush_time should have changed due to periodic flush"
        );

        // Verify record_after_wait was flushed
        let records_after_wait_flush =
            read_records_from_file(&test_file_path).expect("Read failed after wait");
        // The file initially had record1 & record_commit. Now record_after_wait is appended.
        assert_eq!(
            records_after_wait_flush.len(),
            3,
            "File should contain record1, commit, and record_after_wait"
        );
        assert_eq!(records_after_wait_flush[2], record_after_wait);

        cleanup_file(&test_file_path);
    }

    #[test]
    fn test_periodic_interaction_commit_after_periodic() {
        let test_file_path = PathBuf::from(TEST_PERIODIC_INTERACTION_COMMIT_AFTER_PERIODIC_FILE);
        cleanup_file(&test_file_path);
        let flush_interval_ms = 100;
        let config =
            WalWriterConfig { max_buffer_size: 100, flush_interval_ms: Some(flush_interval_ms) };
        let mut writer = WalWriter::new(test_file_path.clone(), config);
        let mut lsn_counter: u64 = 0;
        let mut next_lsn = || {
            let current = lsn_counter;
            lsn_counter += 1;
            current
        };

        let record1 = LogRecord::BeginTransaction { lsn: next_lsn(), tx_id: TransactionId(1200) };
        assert!(writer.add_record(record1.clone()).is_ok());
        let time_after_record1_add = writer.last_flush_time.unwrap_or_else(Instant::now);

        // Wait for periodic flush to be due
        thread::sleep(StdDuration::from_millis(flush_interval_ms + 50));

        // Adding a commit record now. The periodic flush should trigger first for record1.
        // Then the commit record is added and immediately flushed because it's a commit.
        let prev_lsn_for_commit2 = match record1 {
            LogRecord::BeginTransaction { lsn, .. } => lsn,
            _ => panic!("Expected BeginTransaction record for record1"),
        };
        let record_commit = LogRecord::CommitTransaction {
            lsn: next_lsn(),
            tx_id: TransactionId(1200),
            prev_lsn: prev_lsn_for_commit2,
        };
        assert!(writer.add_record(record_commit.clone()).is_ok(), "Add record commit failed");

        // After periodic flush (for record1) and then commit flush (for record_commit)
        assert!(writer.buffer.is_empty(), "Buffer should be empty");
        assert!(test_file_path.exists(), "WAL file should exist");
        assert!(
            writer.last_flush_time.is_some()
                && writer.last_flush_time.unwrap() > time_after_record1_add,
            "last_flush_time should update"
        );

        let records = read_records_from_file(&test_file_path).expect("Read failed");
        assert_eq!(
            records.len(),
            2,
            "Should have flushed record1 (periodic) and then record_commit (commit)"
        );
        assert_eq!(records[0], record1);
        assert_eq!(records[1], record_commit);

        cleanup_file(&test_file_path);
    }

    #[test]
    fn test_last_flush_time_updated_on_size_based_flush() {
        let test_file_path = PathBuf::from(TEST_LAST_FLUSH_TIME_UPDATE_SIZE_BASED_FILE);
        cleanup_file(&test_file_path);
        let config = WalWriterConfig { max_buffer_size: 2, flush_interval_ms: Some(100_000) }; // Long interval
        let mut writer = WalWriter::new(test_file_path.clone(), config);
        let initial_last_flush_time = writer.last_flush_time;
        assert!(initial_last_flush_time.is_some());

        let mut lsn_counter: u64 = 0;
        let mut next_lsn = || {
            let current = lsn_counter;
            lsn_counter += 1;
            current
        };

        writer
            .add_record(LogRecord::BeginTransaction { lsn: next_lsn(), tx_id: TransactionId(1300) })
            .unwrap();
        assert_eq!(
            writer.last_flush_time, initial_last_flush_time,
            "last_flush_time should not change before flush condition met"
        );

        writer
            .add_record(LogRecord::BeginTransaction { lsn: next_lsn(), tx_id: TransactionId(1301) })
            .unwrap(); // Triggers size-based flush
        assert!(
            writer.last_flush_time > initial_last_flush_time,
            "last_flush_time should update after size-based flush"
        );
        assert!(writer.buffer.is_empty());

        cleanup_file(&test_file_path);
    }

    #[test]
    fn test_last_flush_time_updated_on_commit_based_flush() {
        let test_file_path = PathBuf::from(TEST_LAST_FLUSH_TIME_UPDATE_COMMIT_BASED_FILE);
        cleanup_file(&test_file_path);
        let config = WalWriterConfig { max_buffer_size: 100, flush_interval_ms: Some(100_000) }; // Long interval
        let mut writer = WalWriter::new(test_file_path.clone(), config);
        let initial_last_flush_time = writer.last_flush_time;
        assert!(initial_last_flush_time.is_some());

        let mut lsn_counter: u64 = 0;
        let mut next_lsn = || {
            let current = lsn_counter;
            lsn_counter += 1;
            current
        };

        let record1 = LogRecord::BeginTransaction { lsn: next_lsn(), tx_id: TransactionId(1400) };
        writer.add_record(record1.clone()).unwrap();
        assert_eq!(
            writer.last_flush_time, initial_last_flush_time,
            "last_flush_time should not change for non-commit record"
        );

        let prev_lsn_for_commit3 = match record1 {
            LogRecord::BeginTransaction { lsn, .. } => lsn,
            _ => panic!("Expected BeginTransaction record for record1"),
        };
        let record_commit = LogRecord::CommitTransaction {
            lsn: next_lsn(),
            tx_id: TransactionId(1400),
            prev_lsn: prev_lsn_for_commit3,
        };
        writer.add_record(record_commit.clone()).unwrap(); // Triggers commit-based flush
        assert!(
            writer.last_flush_time > initial_last_flush_time,
            "last_flush_time should update after commit-based flush"
        );
        assert!(writer.buffer.is_empty());

        cleanup_file(&test_file_path);
    }
}
