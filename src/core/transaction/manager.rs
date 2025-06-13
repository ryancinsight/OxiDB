use crate::core::transaction::{Transaction, TransactionState};
use crate::core::wal::log_record::LogRecord; // Added for LogRecord
use crate::core::wal::writer::WalWriter;
use std::collections::HashMap;
use std::io::Error as IoError; // For explicit return type
use std::path::PathBuf; // For Default impl

#[derive(Debug)]
pub struct TransactionManager {
    active_transactions: HashMap<u64, Transaction>,
    next_transaction_id: u64,
    current_active_transaction_id: Option<u64>,
    committed_tx_ids: Vec<u64>,
    wal_writer: WalWriter,
}

impl Default for TransactionManager {
    fn default() -> Self {
        let default_wal_path = PathBuf::from("default_transaction_manager.wal");
        // Consider cleaning up existing file: std::fs::remove_file(&default_wal_path).ok();
        let wal_writer = WalWriter::new(default_wal_path);
        TransactionManager {
            active_transactions: HashMap::new(),
            next_transaction_id: 1,
            current_active_transaction_id: None,
            committed_tx_ids: Vec::new(),
            wal_writer,
        }
    }
}

impl TransactionManager {
    pub fn new(wal_writer: WalWriter) -> Self {
        TransactionManager {
            active_transactions: HashMap::new(),
            next_transaction_id: 1,
            current_active_transaction_id: None,
            committed_tx_ids: Vec::new(),
            wal_writer,
        }
    }

    pub fn generate_tx_id(&mut self) -> u64 {
        let id = self.next_transaction_id;
        self.next_transaction_id += 1;
        id
    }

    pub fn begin_transaction(&mut self) -> Transaction {
        let id = self.generate_tx_id();
        let transaction = Transaction::new(id);
        self.active_transactions.insert(id, transaction.clone());
        self.current_active_transaction_id = Some(id);
        transaction
    }

    pub fn get_active_transaction(&self) -> Option<&Transaction> {
        self.current_active_transaction_id.and_then(|id| self.active_transactions.get(&id))
    }

    pub fn get_active_transaction_mut(&mut self) -> Option<&mut Transaction> {
        self.current_active_transaction_id.and_then(move |id| self.active_transactions.get_mut(&id))
    }

    pub fn current_active_transaction_id(&self) -> Option<u64> {
        self.current_active_transaction_id
    }

    pub fn commit_transaction(&mut self) -> Result<(), IoError> {
        if let Some(id) = self.current_active_transaction_id.take() {
            if let Some(mut transaction) = self.active_transactions.remove(&id) {
                // Log before committing state
                let tx_id = transaction.id as u32; // Cast u64 to u32
                let prev_lsn = transaction.last_lsn; // Will be 0 for now
                let commit_log_record = LogRecord::CommitTransaction { tx_id, prev_lsn };

                // Attempt to write to WAL. If this fails, the transaction is not committed.
                // The transaction has already been removed from active_transactions.
                // current_active_transaction_id is already None.
                self.wal_writer.add_record(commit_log_record.clone())?;

                // If WAL write is successful, then proceed to commit
                transaction.set_state(TransactionState::Committed);
                self.committed_tx_ids.push(id);
            }
            // If transaction with 'id' was not in active_transactions, do nothing further.
            // current_active_transaction_id is already None.
        }
        // If there was no current_active_transaction_id, also do nothing and return Ok.
        // Or, should this be an error? Current OxidbError::NoActiveTransaction suggests it should.
        // For now, aligning with previous behavior of doing nothing if no active tx.
        // The caller (QueryExecutor) is responsible for checking if there's an active tx.
        Ok(())
    }

    pub fn is_committed(&self, tx_id: u64) -> bool {
        // Assumes committed_tx_ids is sorted because tx IDs are monotonic and pushed in order.
        self.committed_tx_ids.binary_search(&tx_id).is_ok()
    }

    pub fn get_committed_tx_ids_snapshot(&self) -> Vec<u64> {
        self.committed_tx_ids.clone()
    }

    // Method to explicitly add a transaction ID to the committed list.
    // This is useful for auto-commit scenarios handled outside of begin/commit commands.
    pub fn add_committed_tx_id(&mut self, tx_id: u64) {
        // Could add a check to ensure it's not already there or respect order,
        // but for auto-commit ID 0, it's likely fine.
        // For simplicity, just push. If order matters for binary_search in is_committed,
        // and auto-commit IDs can be arbitrary, this might need sorting or a Set.
        // Given current usage (ID 0), direct push and then sort/dedup if needed is an option.
        // Or, ensure `is_committed` handles potential disorder if non-monotonic IDs are added.
        if !self.committed_tx_ids.contains(&tx_id) {
            // Avoid duplicates for sanity
            self.committed_tx_ids.push(tx_id);
            // If order is strictly required for is_committed's binary_search, sort here.
            // self.committed_tx_ids.sort_unstable();
        }
    }

    pub fn get_oldest_active_tx_id(&self) -> Option<u64> {
        self.active_transactions.values().map(|tx| tx.id).min()
    }

    pub fn get_next_transaction_id_peek(&self) -> u64 {
        self.next_transaction_id
    }

    pub fn rollback_transaction(&mut self) {
        if let Some(id) = self.current_active_transaction_id.take() {
            // take() sets current_active_transaction_id to None
            if let Some(mut transaction) = self.active_transactions.remove(&id) {
                transaction.set_state(TransactionState::Aborted);
                // The transaction (and its undo_log) is removed.
                // Executor is responsible for using the undo_log before this.
            }
            // current_active_transaction_id is already None due to take()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::wal::log_record::{LogRecord, TransactionId}; // TransactionId is u32 here
    use crate::core::wal::writer::WalWriter;
    use std::path::{Path, PathBuf};
    use std::fs::{self, File};
    use std::io::{BufReader, Read, ErrorKind as IoErrorKind, Error as IoError};

    // Helper function to clean up test files
    fn cleanup_file(path: &Path) {
        let _ = fs::remove_file(path); // Ignore error if file doesn't exist
    }

    // Helper function to clean up a directory
    fn cleanup_dir(path: &Path) {
        let _ = fs::remove_dir_all(path); // Ignore error if dir doesn't exist or has contents
    }

    // Adapted from src/core/wal/writer.rs tests
    fn read_records_from_file(path: &Path) -> Result<Vec<LogRecord>, IoError> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut records = Vec::new();

        loop {
            let mut len_bytes = [0u8; 4]; // u32 for length
            match reader.read_exact(&mut len_bytes) {
                Ok(_) => (),
                Err(ref e) if e.kind() == IoErrorKind::UnexpectedEof => {
                    break; // Reached end of file, expected if no more records
                }
                Err(e) => return Err(e),
            }

            let len = u32::from_be_bytes(len_bytes);
            if len == 0 { // Should not happen with current WalWriter logic if buffer wasn't empty
                break;
            }
            let mut record_bytes = vec![0u8; len as usize];
            reader.read_exact(&mut record_bytes)?;

            let record: LogRecord = bincode::deserialize(&record_bytes)
                .map_err(|e| IoError::new(IoErrorKind::InvalidData, format!("Log record deserialization failed: {}", e)))?;
            records.push(record);
        }
        Ok(records)
    }

    const TEST_COMMIT_SUCCESS_WAL_FILE: &str = "test_tm_commit_success.wal";
    const TEST_COMMIT_FAIL_WAL_DIR: &str = "test_tm_commit_fail.waldir";

    #[test]
    fn test_commit_transaction_writes_log_record_and_flushes() {
        let wal_path = PathBuf::from(TEST_COMMIT_SUCCESS_WAL_FILE);
        cleanup_file(&wal_path);

        let wal_writer = WalWriter::new(wal_path.clone());
        let mut manager = TransactionManager::new(wal_writer);

        let transaction = manager.begin_transaction();
        let tx_id_val = transaction.id; // u64

        let commit_result = manager.commit_transaction();
        assert!(commit_result.is_ok(), "Commit should succeed. Error: {:?}", commit_result.err());

        let records = read_records_from_file(&wal_path).expect("Should be able to read WAL records");
        assert_eq!(records.len(), 1, "Should be one log record for commit");

        if let Some(LogRecord::CommitTransaction { tx_id, prev_lsn }) = records.get(0) {
            assert_eq!(*tx_id, tx_id_val as TransactionId, "Transaction ID in log record should match"); // tx_id_val as u32
            assert_eq!(*prev_lsn, 0, "prev_lsn should be 0 as it's not yet tracked");
        } else {
            panic!("Expected CommitTransaction log record, got {:?}", records.get(0));
        }

        assert!(manager.is_committed(tx_id_val), "Transaction should be marked as committed");
        assert_eq!(manager.current_active_transaction_id(), None, "No active transaction should remain");
        assert!(manager.active_transactions.get(&tx_id_val).is_none(), "Transaction should be removed from active map");

        cleanup_file(&wal_path);
    }

    #[test]
    fn test_commit_transaction_fails_if_wal_write_fails() {
        let wal_dir_path = PathBuf::from(TEST_COMMIT_FAIL_WAL_DIR);
        cleanup_dir(&wal_dir_path); // Clean up potential leftovers
        fs::create_dir_all(&wal_dir_path).expect("Should be able to create test directory for WAL");

        let wal_writer = WalWriter::new(wal_dir_path.clone()); // WalWriter will fail to open/append to a directory
        let mut manager = TransactionManager::new(wal_writer);

        let transaction = manager.begin_transaction();
        let tx_id_val = transaction.id;

        let commit_result = manager.commit_transaction();
        assert!(commit_result.is_err(), "Commit should fail due to WAL write error");

        // Verify error kind if possible (might be platform-specific for directory write attempt)
        // e.g. on Linux, it's often `IsADirectory` when trying to open a dir as a file for writing.
        // On Windows, it might be `PermissionDenied` or other.
        // For now, checking `is_err()` is the primary goal.
        // if let Some(err) = commit_result.err() {
        //     assert_eq!(err.kind(), IoErrorKind::IsADirectory); // Or PermissionDenied
        // }

        assert!(!manager.is_committed(tx_id_val), "Transaction should NOT be marked as committed");
        assert_eq!(manager.current_active_transaction_id(), None, "Active transaction ID should be None as take() was called");
        assert!(manager.active_transactions.get(&tx_id_val).is_none(), "Transaction should still be removed from active map even if WAL fails");

        cleanup_dir(&wal_dir_path);
    }
}
