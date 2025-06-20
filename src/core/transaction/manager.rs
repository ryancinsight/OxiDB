use crate::core::common::types::TransactionId as CommonTransactionId;
use crate::core::transaction::transaction::{Transaction, TransactionState}; // Removed INVALID_LSN
use crate::core::wal::log_manager::LogManager;
use crate::core::wal::log_record::LogRecord;
use crate::core::wal::writer::WalWriter;
use std::collections::HashMap;
use std::io::Error as IoError;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Debug)]
pub struct TransactionManager {
    active_transactions: HashMap<CommonTransactionId, Transaction>,
    next_transaction_id: CommonTransactionId,
    pub(crate) current_active_transaction_id: Option<CommonTransactionId>, // Made pub(crate)
    committed_tx_ids: Vec<CommonTransactionId>,
    wal_writer: WalWriter,
    log_manager: Arc<LogManager>,
}

impl Default for TransactionManager {
    fn default() -> Self {
        let default_wal_path = PathBuf::from("default_transaction_manager.wal");
        let wal_config = crate::core::wal::writer::WalWriterConfig::default();
        let wal_writer = WalWriter::new(default_wal_path, wal_config);
        let log_manager = Arc::new(LogManager::default());
        TransactionManager {
            active_transactions: HashMap::new(),
            next_transaction_id: CommonTransactionId(1), // Initialize with TransactionId struct
            current_active_transaction_id: None,
            committed_tx_ids: Vec::new(),
            wal_writer,
            log_manager,
        }
    }
}

impl TransactionManager {
    pub fn new(wal_writer: WalWriter, log_manager: Arc<LogManager>) -> Self {
        TransactionManager {
            active_transactions: HashMap::new(),
            next_transaction_id: CommonTransactionId(1), // Initialize with TransactionId struct
            current_active_transaction_id: None,
            committed_tx_ids: Vec::new(),
            wal_writer,
            log_manager,
        }
    }

    pub fn generate_tx_id(&mut self) -> CommonTransactionId {
        let id = self.next_transaction_id;
        self.next_transaction_id += 1_u64; // Explicitly use u64 for AddAssign
        id
    }

    // Changed to return Result<Transaction, IoError>
    pub fn begin_transaction(&mut self) -> Result<Transaction, IoError> {
        let id: CommonTransactionId = self.generate_tx_id(); // id is CommonTransactionId
        let mut transaction = Transaction::new(id); // Pass TransactionId struct

        let lsn = self.log_manager.next_lsn();
        let begin_log_record = LogRecord::BeginTransaction {
            lsn,
            tx_id: transaction.id, // Already CommonTransactionId
        };
        transaction.prev_lsn = lsn;

        self.wal_writer.add_record(begin_log_record)?; // This can fail
        self.wal_writer.flush()?; // Ensure WAL is written for tests

        self.active_transactions.insert(id, transaction.clone());
        self.current_active_transaction_id = Some(id);
        Ok(transaction)
    }

    // Method to begin a transaction with a specific ID, e.g., for Tx0 auto-commit
    pub fn begin_transaction_with_id(
        &mut self,
        tx_id: CommonTransactionId,
    ) -> Result<Transaction, IoError> {
        if self.active_transactions.contains_key(&tx_id)
            || self.current_active_transaction_id.is_some()
        {
            // Or handle more gracefully depending on desired behavior for nested/overlapping auto-commits
            return Err(IoError::new(
                std::io::ErrorKind::Other,
                "Cannot begin specific transaction; another is active or ID exists.",
            ));
        }

        let mut transaction = Transaction::new(tx_id);

        if tx_id != CommonTransactionId(0) {
            // Only log BeginTransaction for non-Tx0
            let lsn = self.log_manager.next_lsn();
            let begin_log_record = LogRecord::BeginTransaction { lsn, tx_id: transaction.id };
            transaction.prev_lsn = lsn;
            self.wal_writer.add_record(begin_log_record)?;
        } else {
            // For Tx0 (auto-commit), its conceptual "begin" doesn't need a log record in TM's WAL.
            // Its operations will be logged, then a Commit/Rollback for Tx0.
            // prev_lsn for Tx0 will be set by its first actual operation's LSN.
            // Or, more consistently, QueryExecutor::handle_commit_transaction for Tx0
            // should use the LSN of the *last data operation* as prev_lsn for the physical commit WAL entry.
            // For now, let's ensure prev_lsn for Tx0 is handled by its first data op,
            // or correctly set by QueryExecutor::handle_commit_transaction.
            // The current handle_commit_transaction uses active_tx.prev_lsn, which for Tx0
            // would be 0 if not set by a data op, or the LSN of the Begin Tx0 if it was logged.
            // Let's ensure prev_lsn is 0 for a fresh Tx0.
            transaction.prev_lsn = self.log_manager.current_lsn(); // Or a more specific initial LSN for Tx0
        }

        // For Tx0 auto-commit, immediate flush might be debated, but for safety/testing:
        // if tx_id == CommonTransactionId(0) { // Flushing moved to execute_command logic if needed
        //     self.wal_writer.flush()?;
        // }

        self.active_transactions.insert(tx_id, transaction.clone());
        self.current_active_transaction_id = Some(tx_id);
        Ok(transaction)
    }

    pub fn get_active_transaction(&self) -> Option<&Transaction> {
        self.current_active_transaction_id.and_then(|id| self.active_transactions.get(&id))
    }

    pub fn get_active_transaction_mut(&mut self) -> Option<&mut Transaction> {
        self.current_active_transaction_id.and_then(move |id| self.active_transactions.get_mut(&id))
    }

    pub fn current_active_transaction_id(&self) -> Option<CommonTransactionId> {
        // Use CommonTransactionId
        self.current_active_transaction_id
    }

    pub fn commit_transaction(&mut self) -> Result<(), IoError> {
        let current_tx_id = match self.current_active_transaction_id.take() {
            Some(id) => id,
            None => return Ok(()), // Or an error like NoActiveTransaction
        };

        let mut transaction = match self.active_transactions.remove(&current_tx_id) {
            Some(txn) => txn,
            None => {
                // This case should ideally not happen if current_active_transaction_id was Some.
                // Indicates an inconsistent state.
                return Err(IoError::new(
                    std::io::ErrorKind::NotFound,
                    format!(
                        "Transaction {} not found in active transactions during commit.",
                        current_tx_id
                    ),
                ));
            }
        };

        let lsn = self.log_manager.next_lsn();
        // Note: The LogRecord::CommitTransaction's prev_lsn is the LSN of the *previous*
        // log record for this transaction. This is already stored in transaction.prev_lsn.
        let commit_log_record = LogRecord::CommitTransaction {
            lsn,
            tx_id: transaction.id,
            prev_lsn: transaction.prev_lsn, // Use the LSN of the prior record for this txn
        };

        // Attempt to write to WAL.
        self.wal_writer.add_record(commit_log_record.clone())?;

        // If WAL write is successful, then proceed to update transaction state
        transaction.prev_lsn = lsn; // Update transaction's prev_lsn to this commit record's LSN
        transaction.set_state(TransactionState::Committed);
        self.committed_tx_ids.push(current_tx_id);

        Ok(())
    }

    // New method for aborting a transaction with logging
    pub fn abort_transaction(&mut self) -> Result<(), IoError> {
        let current_tx_id = match self.current_active_transaction_id.take() {
            Some(id) => id,
            None => return Ok(()), // Or an error
        };

        let mut transaction = match self.active_transactions.remove(&current_tx_id) {
            Some(txn) => txn,
            None => {
                return Err(IoError::new(
                    std::io::ErrorKind::NotFound,
                    format!(
                        "Transaction {} not found in active transactions during abort.",
                        current_tx_id
                    ),
                ));
            }
        };

        let lsn = self.log_manager.next_lsn();
        let abort_log_record = LogRecord::AbortTransaction {
            lsn,
            tx_id: transaction.id,
            prev_lsn: transaction.prev_lsn, // LSN of the prior record for this txn
        };

        // Attempt to write to WAL. If this fails, the transaction is still considered aborted locally.
        // The recovery process would handle inconsistencies if the abort record isn't durable.
        self.wal_writer.add_record(abort_log_record.clone())?;
        self.wal_writer.flush()?; // Ensure WAL is written for tests

        transaction.prev_lsn = lsn; // Update transaction's prev_lsn to this abort record's LSN
        transaction.set_state(TransactionState::Aborted);
        // Do not add to committed_tx_ids.
        // The transaction (and its undo_log) is removed.
        // Executor is responsible for using the undo_log *before* calling this.
        Ok(())
    }

    pub fn is_committed(&self, tx_id: CommonTransactionId) -> bool {
        // Use CommonTransactionId
        self.committed_tx_ids.binary_search(&tx_id).is_ok()
    }

    pub fn get_committed_tx_ids_snapshot(&self) -> Vec<CommonTransactionId> {
        // Use CommonTransactionId
        self.committed_tx_ids.clone()
    }

    pub fn add_committed_tx_id(&mut self, tx_id: CommonTransactionId) {
        // Use CommonTransactionId
        if !self.committed_tx_ids.contains(&tx_id) {
            self.committed_tx_ids.push(tx_id);
        }
    }

    pub fn get_oldest_active_tx_id(&self) -> Option<CommonTransactionId> {
        // Use CommonTransactionId
        self.active_transactions.values().map(|tx| tx.id).min()
    }

    pub fn get_next_transaction_id_peek(&self) -> CommonTransactionId {
        // Use CommonTransactionId
        self.next_transaction_id
    }

    // #[cfg(test)]
    // pub(crate) fn set_current_active_transaction_id_for_test(&mut self, tx_id: Option<CommonTransactionId>) {
    //     self.current_active_transaction_id = tx_id;
    // }

    // This is the old rollback, which doesn't log. Replaced by abort_transaction.
    // pub fn rollback_transaction(&mut self) {
    //     if let Some(id) = self.current_active_transaction_id.take() {
    //         if let Some(mut transaction) = self.active_transactions.remove(&id) {
    //             transaction.set_state(TransactionState::Aborted);
    //         }
    //     }
    // }
}

#[cfg(test)]
mod tests {
    use super::*;
    // Use the fully qualified path for TransactionId from common::types if that's what LogRecord uses
    // or ensure LogRecord's TransactionId is the one from common::types.
    // For LogRecord tests, its internal TransactionId (u32) was used.
    // Here, CommonTransactionId (u64) is used for manager's maps.
    // LogRecord itself uses crate::core::common::types::TransactionId which is u64.
    // TransactionId from common::types is TransactionId(u64).
    // LogRecord variants use this TransactionId.
    use crate::core::wal::log_manager::LogManager;
    use crate::core::wal::log_record::LogRecord;
    use crate::core::wal::writer::WalWriter;
    // Removed unused CommonTransactionId_Test alias
    use std::fs::{self, File};
    use std::io::{BufReader, Error as IoError, ErrorKind as IoErrorKind, Read};
    use std::path::{Path, PathBuf};
    use std::sync::Arc; // For Arc<LogManager>

    // Helper function to clean up a directory
    fn cleanup_dir(path: &Path) {
        // Now specifically cleans the test's own directory
        if path.exists() {
            let _ = fs::remove_dir_all(path);
        }
    }

    fn read_records_from_file(path: &Path) -> Result<Vec<LogRecord>, IoError> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut records = Vec::new();
        loop {
            let mut len_bytes = [0u8; 4];
            match reader.read_exact(&mut len_bytes) {
                Ok(_) => (),
                Err(ref e) if e.kind() == IoErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            let len = u32::from_be_bytes(len_bytes);
            if len == 0 {
                break;
            }
            let mut record_bytes = vec![0u8; len as usize];
            reader.read_exact(&mut record_bytes)?;
            let record: LogRecord = bincode::deserialize(&record_bytes).map_err(|e| {
                IoError::new(IoErrorKind::InvalidData, format!("Deserialization failed: {}", e))
            })?;
            records.push(record);
        }
        Ok(records)
    }

    const TEST_WAL_BASE_DIR: &str = "test_tm_wal_files_isolated/";

    fn setup_test_tm(test_name: &str) -> (TransactionManager, PathBuf, PathBuf) {
        let base_dir = PathBuf::from(TEST_WAL_BASE_DIR);
        let test_specific_dir = base_dir.join(test_name);

        if test_specific_dir.exists() {
            cleanup_dir(&test_specific_dir); // Clean up from previous run if necessary
        }
        fs::create_dir_all(&test_specific_dir)
            .expect("Failed to create test-specific WAL directory.");

        let wal_path = test_specific_dir.join("test.wal");
        // cleanup_file(&wal_path); // Not needed as whole dir is cleaned

        let wal_config = crate::core::wal::writer::WalWriterConfig::default();
        let wal_writer = WalWriter::new(wal_path.clone(), wal_config);
        let log_manager = Arc::new(LogManager::new());
        (TransactionManager::new(wal_writer, log_manager), wal_path, test_specific_dir)
    }

    #[test]
    fn test_begin_transaction_logs_record() {
        let (mut manager, wal_path, test_dir_path) = setup_test_tm("begin_tx_logs");

        let transaction_result = manager.begin_transaction();
        assert!(transaction_result.is_ok());
        let transaction = transaction_result.unwrap();
        let tx_id_val = transaction.id;

        assert_eq!(transaction.prev_lsn, 0, "Transaction's prev_lsn should be 0 (first LSN)");

        let records = read_records_from_file(&wal_path).expect("Should read WAL records");
        assert_eq!(records.len(), 1, "Should be one BeginTransaction log record");

        match records.get(0) {
            Some(LogRecord::BeginTransaction { lsn, tx_id }) => {
                assert_eq!(*lsn, 0, "LSN of BeginTransaction should be 0");
                assert_eq!(*tx_id, tx_id_val, "Transaction ID should match"); // tx_id_val is TransactionId
            }
            other => panic!("Expected BeginTransaction, got {:?}", other),
        }
        cleanup_dir(&test_dir_path);
    }

    #[test]
    fn test_commit_transaction_writes_correct_log_record() {
        let (mut manager, wal_path, test_dir_path) = setup_test_tm("commit_tx_correct_log");

        let begin_tx = manager.begin_transaction().unwrap();
        let tx_id_val = begin_tx.id; // This is TransactionId
        let begin_lsn = begin_tx.prev_lsn;

        let commit_result = manager.commit_transaction();
        assert!(commit_result.is_ok(), "Commit should succeed. Error: {:?}", commit_result.err());

        let records = read_records_from_file(&wal_path).expect("Should read WAL records");
        assert_eq!(records.len(), 2, "Should be two log records: Begin and Commit");

        match records.get(0) {
            Some(LogRecord::BeginTransaction { lsn, tx_id }) => {
                assert_eq!(*lsn, begin_lsn);
                assert_eq!(*tx_id, tx_id_val); // Compare TransactionId with TransactionId
            }
            other => panic!("Expected BeginTransaction at index 0, got {:?}", other),
        }

        match records.get(1) {
            Some(LogRecord::CommitTransaction { lsn, tx_id, prev_lsn }) => {
                assert_eq!(*lsn, begin_lsn + 1, "Commit LSN should be Begin LSN + 1");
                assert_eq!(*tx_id, tx_id_val); // Compare TransactionId
                assert_eq!(*prev_lsn, begin_lsn, "Commit's prev_lsn should be Begin's LSN");
            }
            other => panic!("Expected CommitTransaction at index 1, got {:?}", other),
        }

        assert!(manager.is_committed(tx_id_val)); // Pass TransactionId
        cleanup_dir(&test_dir_path);
    }

    #[test]
    fn test_abort_transaction_logs_record() {
        let (mut manager, wal_path, test_dir_path) = setup_test_tm("abort_tx_logs");

        let begin_tx = manager.begin_transaction().unwrap();
        let tx_id_val = begin_tx.id; // This is TransactionId
        let begin_lsn = begin_tx.prev_lsn;

        let abort_result = manager.abort_transaction();
        assert!(abort_result.is_ok(), "Abort should succeed. Error: {:?}", abort_result.err());

        let records = read_records_from_file(&wal_path).expect("Should read WAL records");
        assert_eq!(records.len(), 2, "Should be two log records: Begin and Abort");

        match records.get(1) {
            Some(LogRecord::AbortTransaction { lsn, tx_id, prev_lsn }) => {
                assert_eq!(*lsn, begin_lsn + 1, "Abort LSN should be Begin LSN + 1");
                assert_eq!(*tx_id, tx_id_val); // Compare TransactionId
                assert_eq!(*prev_lsn, begin_lsn, "Abort's prev_lsn should be Begin's LSN");
            }
            other => panic!("Expected AbortTransaction at index 1, got {:?}", other),
        }
        assert!(!manager.is_committed(tx_id_val)); // Pass TransactionId
        assert!(manager.active_transactions.get(&tx_id_val).is_none()); // Use TransactionId for get
        cleanup_dir(&test_dir_path);
    }

    #[test]
    fn test_commit_transaction_fails_if_wal_write_fails() {
        let base_dir = PathBuf::from(TEST_WAL_BASE_DIR);
        let test_specific_dir = base_dir.join("commit_fail_dir");
        if test_specific_dir.exists() {
            cleanup_dir(&test_specific_dir);
        }
        // Do not create the directory, make WalWriter::new use a path that will fail on open/create
        // To make it fail, we make the *parent* a file, or make the path itself a directory.
        // Let's make the path itself a directory for WalWriter to fail.
        fs::create_dir_all(&test_specific_dir).expect("Should create dir to cause WAL write fail");

        let wal_config = crate::core::wal::writer::WalWriterConfig {
            max_buffer_size: 1,      // Small buffer to force flush
            flush_interval_ms: None, // Disable periodic to isolate failure
        };
        let wal_writer = WalWriter::new(test_specific_dir.clone(), wal_config); // WalWriter will try to open this directory as a file
        let log_manager = Arc::new(LogManager::new());
        let mut manager = TransactionManager::new(wal_writer, log_manager);

        // Begin transaction might also fail if it tries to flush to a directory
        let begin_result = manager.begin_transaction();
        if begin_result.is_ok() {
            let tx_id_val = begin_result.unwrap().id;
            let commit_result = manager.commit_transaction();
            assert!(commit_result.is_err(), "Commit should fail due to WAL write error");
            assert!(!manager.is_committed(tx_id_val));
        } else {
            // This path is more likely if WalWriter::new doesn't fail but flush does.
            assert!(begin_result.is_err(), "Begin transaction should fail if WAL is broken.");
        }
        cleanup_dir(&test_specific_dir); // Clean up the created directory
    }

    #[test]
    fn test_prev_lsn_after_begin_transaction() {
        let (mut manager, _wal_path, test_dir_path) = setup_test_tm("prev_lsn_begin");

        // LSNs start from 0. First call to next_lsn() in begin_transaction will return 0.
        let expected_begin_lsn = 0;

        let transaction_result = manager.begin_transaction();
        assert!(transaction_result.is_ok(), "begin_transaction failed");
        let transaction = transaction_result.unwrap();

        assert_eq!(
            transaction.prev_lsn, expected_begin_lsn,
            "Transaction.prev_lsn should be updated to the LSN of the BeginTransaction record."
        );

        // Verify that the LogManager's counter has advanced.
        // current_lsn() returns the *next* LSN to be allocated if called after fetch_add.
        // So, if 0 was allocated, current_lsn (which loads the atomic) should be 1.
        assert_eq!(
            manager.log_manager.current_lsn(),
            expected_begin_lsn + 1,
            "LogManager current_lsn should have advanced past the allocated LSN."
        );

        // Begin another transaction to see LSN increment
        let expected_next_begin_lsn = 1;
        let next_transaction_result = manager.begin_transaction();
        assert!(next_transaction_result.is_ok(), "Second begin_transaction failed");
        let next_transaction = next_transaction_result.unwrap();
        assert_eq!(
            next_transaction.prev_lsn, expected_next_begin_lsn,
            "Second Transaction.prev_lsn should be updated to the next LSN."
        );
        assert_eq!(
            manager.log_manager.current_lsn(),
            expected_next_begin_lsn + 1,
            "LogManager current_lsn should have advanced again."
        );

        cleanup_dir(&test_dir_path);
    }
}
