use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write, Error as IoError, ErrorKind as IoErrorKind};
use std::path::PathBuf;
use bincode;
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

    pub fn add_record(&mut self, record: LogRecord) {
        self.buffer.push(record);
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
    use crate::core::wal::log_record::{LogRecord, TransactionId};
    use std::fs;
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

    #[test]
    fn test_wal_writer_new() {
        let test_file_path = PathBuf::from(TEST_NEW_WAL_FILE);
        cleanup_file(&test_file_path);

        let writer = WalWriter::new(test_file_path.clone());
        assert!(writer.buffer.is_empty());
        assert_eq!(writer.wal_file_path, test_file_path);

        cleanup_file(&test_file_path);
    }

    #[test]
    fn test_wal_writer_add_record() {
        // This test doesn't interact with the file system via flush,
        // but uses a path for consistency in WalWriter creation.
        let test_file_path = PathBuf::from(TEST_ADD_RECORD_WAL_FILE);
        // No cleanup needed here as no file is created by this test's direct actions.

        let mut writer = WalWriter::new(test_file_path.clone());

        let record1 = LogRecord::BeginTransaction { tx_id: 1 as TransactionId };
        writer.add_record(record1.clone());

        assert_eq!(writer.buffer.len(), 1);
        assert_eq!(writer.buffer[0], record1);

        let record2 = LogRecord::CommitTransaction { tx_id: 1 as TransactionId, prev_lsn: 0 };
        writer.add_record(record2.clone());

        assert_eq!(writer.buffer.len(), 2);
        assert_eq!(writer.buffer[1], record2);

        // No cleanup needed here as no file is created by this test's direct actions.
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

        let record1 = LogRecord::BeginTransaction { tx_id: 10 as TransactionId };
        let record2 = LogRecord::CommitTransaction { tx_id: 10 as TransactionId, prev_lsn: 1 };

        writer.add_record(record1.clone());
        writer.add_record(record2.clone());

        let flush_result = writer.flush();
        assert!(flush_result.is_ok());
        assert!(writer.buffer.is_empty(), "Buffer should be empty after flush");
        assert!(test_file_path.exists(), "WAL file should be created");

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

        // First batch
        let record1 = LogRecord::BeginTransaction { tx_id: 20 as TransactionId };
        writer.add_record(record1.clone());
        let flush_result1 = writer.flush();
        assert!(flush_result1.is_ok());
        assert!(writer.buffer.is_empty());

        // Second batch
        let record2 = LogRecord::InsertRecord {
            tx_id: 20,
            page_id: crate::core::common::types::ids::PageId(1),
            slot_id: crate::core::common::types::ids::SlotId(0),
            record_data: vec![1,2,3],
            prev_lsn: 0
        };
        let record3 = LogRecord::CommitTransaction { tx_id: 20 as TransactionId, prev_lsn: 1 };
        writer.add_record(record2.clone());
        writer.add_record(record3.clone());
        let flush_result2 = writer.flush();
        assert!(flush_result2.is_ok());
        assert!(writer.buffer.is_empty());

        let records_from_file = read_records_from_file(&test_file_path).expect("Failed to read records from WAL file");
        assert_eq!(records_from_file.len(), 3);
        assert_eq!(records_from_file[0], record1);
        assert_eq!(records_from_file[1], record2);
        assert_eq!(records_from_file[2], record3);

        cleanup_file(&test_file_path);
    }
}
