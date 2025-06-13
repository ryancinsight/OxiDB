use std::fs::OpenOptions;
use std::io::{BufWriter, Write, Error as IoError, ErrorKind as IoErrorKind}; // Added Write back
use std::path::PathBuf;
use bincode;
// Removed unused TransactionId import
use crate::core::wal::log_record::LogRecord;

#[derive(Debug)]
pub struct WalWriter {
    buffer: Vec<LogRecord>,
    wal_file_path: PathBuf,
}

impl WalWriter {
    pub fn new(wal_file_path: PathBuf) -> Self {
        WalWriter {
            buffer: Vec::new(),
            wal_file_path,
        }
    }

    pub fn add_record(&mut self, record: LogRecord) -> Result<(), IoError> {
        self.buffer.push(record.clone()); // Clone record to store in buffer

        if let LogRecord::CommitTransaction { .. } = record {
            self.flush()
        } else {
            Ok(())
        }
    }

    pub fn flush(&mut self) -> Result<(), IoError> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.wal_file_path)?;

        let mut writer = BufWriter::new(file);

        for record in self.buffer.iter() {
            let serialized_record = bincode::serialize(record)
                .map_err(|e| IoError::new(IoErrorKind::InvalidData, format!("Log record serialization failed: {}", e)))?;

            let len = serialized_record.len() as u32; // Assuming length fits in u32
            writer.write_all(&len.to_be_bytes())?;
            writer.write_all(&serialized_record)?;
        }

        writer.flush()?; // Flush BufWriter contents to the OS buffer

        // Get the underlying file back from BufWriter to sync
        let file = writer.into_inner().map_err(|e| IoError::new(IoErrorKind::Other, format!("Failed to get file from BufWriter: {}", e.into_error())))?;
        file.sync_all()?; // Ensure OS flushes its buffers to disk

        self.buffer.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::common::types::TransactionId; // Use direct import in tests too
    use crate::core::wal::log_record::LogRecord;
    use std::fs::{self, File};
    use std::io::{BufReader, Read};
    use std::path::Path;

    // Helper function to clean up test files
    fn cleanup_file(path: &Path) {
        let _ = fs::remove_file(path); // Ignore error if file doesn't exist
    }
    // Removed duplicate import of TransactionId here
    use std::path::PathBuf;

    // Using unique names for test files to prevent interference
    const TEST_NEW_WAL_FILE: &str = "test_wal_writer_new_output.log";
    const TEST_ADD_RECORD_WAL_FILE: &str = "test_wal_writer_add_record_output.log"; // Though add_record itself doesn't write
    const TEST_FLUSH_EMPTY_WAL_FILE: &str = "test_flush_empty_buffer_output.log";
    const TEST_FLUSH_WRITES_WAL_FILE: &str = "test_flush_writes_records_output.log";
    const TEST_FLUSH_APPENDS_WAL_FILE: &str = "test_flush_appends_records_output.log";
    const TEST_ADD_NON_COMMIT_NO_FLUSH_FILE: &str = "test_add_non_commit_no_flush.log";
    const TEST_ADD_COMMIT_FLUSHES_FILE: &str = "test_add_commit_flushes.log";
    const TEST_ADD_COMMIT_FLUSH_FAILS_FILE: &str = "test_add_commit_flush_fails.logdir"; // Use .logdir to signify it's a directory

    #[test]
    fn test_wal_writer_new() {
        let test_file_path = PathBuf::from(TEST_NEW_WAL_FILE);
        cleanup_file(&test_file_path);

        let writer = WalWriter::new(test_file_path.clone());
        assert!(writer.buffer.is_empty());
        assert_eq!(writer.wal_file_path, test_file_path);

        cleanup_file(&test_file_path);
    }

    // Helper function to clean up a directory
    fn cleanup_dir(path: &Path) {
        let _ = fs::remove_dir_all(path); // Ignore error if dir doesn't exist or has contents
    }

    #[test]
    fn test_add_commit_flush_fails_returns_error() {
        let test_dir_path = PathBuf::from(TEST_ADD_COMMIT_FLUSH_FAILS_FILE);
        cleanup_dir(&test_dir_path); // Ensure no leftover from previous runs

        // Create a directory at the path where the WAL file is expected.
        // This will cause the file open operation in `flush()` to fail.
        fs::create_dir_all(&test_dir_path).expect("Should be able to create test directory");

        let mut writer = WalWriter::new(test_dir_path.clone()); // WalWriter itself doesn't fail on new() with bad path

        let record1 = LogRecord::BeginTransaction { lsn: 0, tx_id: TransactionId(404) }; // Added lsn, use TransactionId()
        assert!(writer.add_record(record1.clone()).is_ok(), "Adding non-commit record should still be Ok");

        let record2 = LogRecord::CommitTransaction { lsn: 1, tx_id: TransactionId(404), prev_lsn: 0 }; // Added lsn, use TransactionId()
        let result = writer.add_record(record2.clone());

        assert!(result.is_err(), "add_record with commit should return Err when flush fails");

        // Check error kind if possible and makes sense (OS-dependent, but often permission denied or is a directory)
        // For example:
        // assert_eq!(result.unwrap_err().kind(), IoErrorKind::PermissionDenied);
        // Or on Linux when trying to open a directory as a file for writing:
        // assert_eq!(result.unwrap_err().kind(), IoErrorKind::IsADirectory);
        // For now, just checking it's an error is sufficient for the logic.

        assert!(!writer.buffer.is_empty(), "Buffer should not be cleared if flush fails");
        assert_eq!(writer.buffer.len(), 2, "Buffer should still contain all records");
        assert_eq!(writer.buffer[0], record1);
        assert_eq!(writer.buffer[1], record2);

        cleanup_dir(&test_dir_path);
    }


    #[test]
    fn test_add_commit_record_flushes_and_clears_buffer() {
        let test_file_path = PathBuf::from(TEST_ADD_COMMIT_FLUSHES_FILE);
        cleanup_file(&test_file_path);

        let mut writer = WalWriter::new(test_file_path.clone());
        let mut lsn_counter: u64 = 0;
        let next_lsn = || { let current = lsn_counter; lsn_counter += 1; current };


        let record1 = LogRecord::BeginTransaction { lsn: next_lsn(), tx_id: TransactionId(789) }; // Added lsn, use TransactionId()
        let record2 = LogRecord::InsertRecord {
            lsn: next_lsn(), // Added lsn
            tx_id: TransactionId(789), // Use TransactionId()
            page_id: crate::core::common::types::ids::PageId(1),
            slot_id: crate::core::common::types::ids::SlotId(0),
            record_data: vec![1,2,3],
            prev_lsn: 0 // Assuming this prev_lsn is for the transaction chain
        };
        let record3 = LogRecord::CommitTransaction { lsn: next_lsn(), tx_id: TransactionId(789), prev_lsn: 1 }; // Added lsn, use TransactionId()

        // Add non-commit records
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
        let records_from_file = read_records_from_file(&test_file_path).expect("Failed to read records from WAL file");
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

        let mut writer = WalWriter::new(test_file_path.clone());
        let record = LogRecord::BeginTransaction { lsn: 0, tx_id: TransactionId(123) }; // Added lsn, use TransactionId()

        let result = writer.add_record(record.clone());
        assert!(result.is_ok(), "add_record for non-commit should return Ok");
        assert_eq!(writer.buffer.len(), 1, "Buffer should contain the added record");
        assert_eq!(writer.buffer[0], record);
        assert!(!test_file_path.exists(), "WAL file should not be created by non-commit record");

        cleanup_file(&test_file_path);
    }

    #[test]
    fn test_wal_writer_add_record() {
        // This test uses a path for consistency in WalWriter creation.
        // Since CommitTransaction will trigger a flush, we need a valid path and cleanup.
        let test_file_path = PathBuf::from(TEST_ADD_RECORD_WAL_FILE);
        cleanup_file(&test_file_path);

        let mut writer = WalWriter::new(test_file_path.clone());
        let mut lsn_counter: u64 = 0;
        let next_lsn = || { let current = lsn_counter; lsn_counter += 1; current };

        let record1 = LogRecord::BeginTransaction { lsn: next_lsn(), tx_id: TransactionId(1) }; // Added lsn, use TransactionId()
        // Adding a non-commit record should not flush and return Ok(())
        assert!(writer.add_record(record1.clone()).is_ok());
        assert_eq!(writer.buffer.len(), 1);
        assert_eq!(writer.buffer[0], record1);
        assert!(!test_file_path.exists(), "File should not be created by non-commit record");


        let record2 = LogRecord::CommitTransaction { lsn: next_lsn(), tx_id: TransactionId(1), prev_lsn: 0 }; // Added lsn, use TransactionId()
        // Adding a commit record should flush and return Ok(()) if flush is successful
        // This will also clear the buffer.
        assert!(writer.add_record(record2.clone()).is_ok());
        assert!(writer.buffer.is_empty(), "Buffer should be empty after commit record (flush)");
        assert!(test_file_path.exists(), "File should be created by commit record");

        // Verify content of the file
        let records_from_file = read_records_from_file(&test_file_path).expect("Failed to read records");
        assert_eq!(records_from_file.len(), 2);
        assert_eq!(records_from_file[0], record1);
        assert_eq!(records_from_file[1], record2);

        cleanup_file(&test_file_path);
    }

    #[test]
    fn test_flush_empty_buffer() {
        let test_file_path = PathBuf::from(TEST_FLUSH_EMPTY_WAL_FILE);
        cleanup_file(&test_file_path);

        let mut writer = WalWriter::new(test_file_path.clone());
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

            let record: LogRecord = bincode::deserialize(&record_bytes)
                .map_err(|e| IoError::new(IoErrorKind::InvalidData, format!("Log record deserialization failed: {}", e)))?;
            records.push(record);
        }
        Ok(records)
    }

    #[test]
    fn test_flush_writes_records() {
        let test_file_path = PathBuf::from(TEST_FLUSH_WRITES_WAL_FILE);
        cleanup_file(&test_file_path);

        let mut writer = WalWriter::new(test_file_path.clone());
        let mut lsn_counter: u64 = 0;
        let next_lsn = || { let current = lsn_counter; lsn_counter += 1; current };

        let record1 = LogRecord::BeginTransaction { lsn: next_lsn(), tx_id: TransactionId(10) }; // Added lsn
        let record2 = LogRecord::CommitTransaction { lsn: next_lsn(), tx_id: TransactionId(10), prev_lsn: 1 }; // Added lsn

        // Add a non-commit record, should not flush yet.
        assert!(writer.add_record(record1.clone()).is_ok());
        assert_eq!(writer.buffer.len(), 1, "Buffer should contain one record before commit");
        assert!(!test_file_path.exists(), "WAL file should not be created before commit");

        // Add a commit record, this should trigger a flush.
        let add_commit_result = writer.add_record(record2.clone());
        assert!(add_commit_result.is_ok());
        assert!(writer.buffer.is_empty(), "Buffer should be empty after commit record (auto-flush)");
        assert!(test_file_path.exists(), "WAL file should be created after commit");

        // Explicit flush call now should do nothing as buffer is empty.
        let flush_result = writer.flush();
        assert!(flush_result.is_ok());
        assert!(writer.buffer.is_empty(), "Buffer should remain empty after explicit flush");

        let records_from_file = read_records_from_file(&test_file_path).expect("Failed to read records from WAL file");
        assert_eq!(records_from_file.len(), 2);
        assert_eq!(records_from_file[0], record1);
        assert_eq!(records_from_file[1], record2);

        cleanup_file(&test_file_path);
    }

    #[test]
    fn test_flush_appends_records() {
        let test_file_path = PathBuf::from(TEST_FLUSH_APPENDS_WAL_FILE);
        cleanup_file(&test_file_path);

        let mut writer = WalWriter::new(test_file_path.clone());
        let mut lsn_counter: u64 = 0;
        let next_lsn = || { let current = lsn_counter; lsn_counter += 1; current };

        // First batch
        let record1 = LogRecord::BeginTransaction { lsn: next_lsn(), tx_id: TransactionId(20) }; // Added lsn
        assert!(writer.add_record(record1.clone()).is_ok(), "Add BeginTransaction should succeed");
        assert_eq!(writer.buffer.len(), 1, "Buffer should have 1 record before explicit flush");
        let flush_result1 = writer.flush(); // Explicit flush for the first batch
        assert!(flush_result1.is_ok(), "Flush 1 should succeed");
        assert!(writer.buffer.is_empty(), "Buffer should be empty after flush 1");
        assert!(test_file_path.exists(), "WAL file should exist after flush 1");

        // Second batch
        let record2 = LogRecord::InsertRecord {
            lsn: next_lsn(), // Added lsn
            tx_id: TransactionId(20), // Use TransactionId()
            page_id: crate::core::common::types::ids::PageId(1),
            slot_id: crate::core::common::types::ids::SlotId(0),
            record_data: vec![1,2,3],
            prev_lsn: 0 // Assuming this prev_lsn is for the transaction chain
        };
        let record3 = LogRecord::CommitTransaction { lsn: next_lsn(), tx_id: TransactionId(20), prev_lsn: 1 }; // Added lsn

        // Add InsertRecord, should not flush
        assert!(writer.add_record(record2.clone()).is_ok(), "Add InsertRecord should succeed");
        assert_eq!(writer.buffer.len(), 1, "Buffer should have 1 record before commit");

        // Add CommitTransaction, should auto-flush
        let add_commit_result = writer.add_record(record3.clone());
        assert!(add_commit_result.is_ok(), "Add CommitTransaction should succeed and flush");
        assert!(writer.buffer.is_empty(), "Buffer should be empty after commit (auto-flush)");

        // Explicit flush call now should do nothing as buffer is empty.
        let flush_result2 = writer.flush();
        assert!(flush_result2.is_ok(), "Flush 2 should succeed (and do nothing)");
        assert!(writer.buffer.is_empty(), "Buffer should remain empty after flush 2");

        let records_from_file = read_records_from_file(&test_file_path).expect("Failed to read records from WAL file");
        assert_eq!(records_from_file.len(), 3);
        assert_eq!(records_from_file[0], record1);
        assert_eq!(records_from_file[1], record2);
        assert_eq!(records_from_file[2], record3);

        cleanup_file(&test_file_path);
    }
}
