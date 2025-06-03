// src/core/query/executor.rs

use crate::core::common::error::DbError;
use crate::core::storage::engine::simple_file_kv_store::SimpleFileKvStore; // Added import
use crate::core::query::commands::Command;
use crate::core::storage::engine::traits::KeyValueStore;
use crate::core::transaction::{lock_manager::{LockManager, LockType}}; // Added LockType
use crate::core::transaction::manager::TransactionManager;
use crate::core::transaction::transaction::{Transaction, TransactionState, UndoOperation};

#[derive(Debug, PartialEq)]
pub enum ExecutionResult {
    Value(Option<Vec<u8>>),
    Success,
    Deleted(bool),
}

pub struct QueryExecutor<S: KeyValueStore<Vec<u8>, Vec<u8>>> {
    store: S,
    transaction_manager: TransactionManager,
    lock_manager: LockManager,
}

impl<S: KeyValueStore<Vec<u8>, Vec<u8>>> QueryExecutor<S> {
    pub fn new(store: S) -> Self {
        QueryExecutor {
            store,
            transaction_manager: TransactionManager::new(),
            lock_manager: LockManager::new(),
        }
    }

    pub fn execute_command(&mut self, command: Command) -> Result<ExecutionResult, DbError> {
        match command {
            Command::Insert { key, value } => {
                if let Some(active_tx) = self.transaction_manager.get_active_transaction_mut() {
                    self.lock_manager.acquire_lock(active_tx.id, &key, LockType::Exclusive)?;
                    
                    let current_value = self.store.get(&key)?;
                    let undo_op = if let Some(old_val) = current_value {
                        UndoOperation::RevertUpdate { key: key.clone(), old_value: old_val }
                    } else {
                        UndoOperation::RevertInsert { key: key.clone() }
                    };
                    active_tx.undo_log.push(undo_op);
                    
                    let tx_for_store = active_tx.clone();
                    self.store.put(key, value, &tx_for_store)
                        .map(|_| ExecutionResult::Success)
                } else {
                    // Auto-commit for Insert
                    let auto_commit_tx_id = 0; 
                    match self.lock_manager.acquire_lock(auto_commit_tx_id, &key, LockType::Exclusive) {
                        Ok(()) => {
                            let mut tx_for_store = Transaction::new(auto_commit_tx_id);
                            let put_result = self.store.put(key, value, &tx_for_store);
                            self.lock_manager.release_locks(auto_commit_tx_id); // Release lock
                            match put_result {
                                Ok(_) => {
                                    tx_for_store.set_state(TransactionState::Committed);
                                    Ok(ExecutionResult::Success)
                                }
                                Err(e) => {
                                    tx_for_store.set_state(TransactionState::Aborted);
                                    Err(e)
                                }
                            }
                        }
                        Err(lock_err) => Err(lock_err), 
                    }
                }
            }
            Command::Get { key } => {
                if let Some(active_tx) = self.transaction_manager.get_active_transaction() { 
                    self.lock_manager.acquire_lock(active_tx.id, &key, LockType::Shared)?;
                    self.store.get(&key).map(ExecutionResult::Value)
                } else { // Auto-commit for Get
                    let auto_commit_tx_id = 0; 
                    match self.lock_manager.acquire_lock(auto_commit_tx_id, &key, LockType::Shared) {
                        Ok(()) => {
                            let get_result = self.store.get(&key);
                            self.lock_manager.release_locks(auto_commit_tx_id); // Release lock
                            get_result.map(ExecutionResult::Value)
                        }
                        Err(lock_err) => Err(lock_err), 
                    }
                }
            }
            Command::Delete { key } => {
                if let Some(active_tx) = self.transaction_manager.get_active_transaction_mut() {
                    self.lock_manager.acquire_lock(active_tx.id, &key, LockType::Exclusive)?;

                    if let Some(old_value) = self.store.get(&key)? {
                        active_tx.undo_log.push(UndoOperation::RevertDelete { key: key.clone(), old_value });
                    }
                    
                    let tx_for_store = active_tx.clone();
                    self.store.delete(&key, &tx_for_store)
                        .map(ExecutionResult::Deleted)
                } else {
                    // Auto-commit for Delete
                    let auto_commit_tx_id = 0; 
                    match self.lock_manager.acquire_lock(auto_commit_tx_id, &key, LockType::Exclusive) {
                        Ok(()) => {
                            let mut tx_for_store = Transaction::new(auto_commit_tx_id);
                            let delete_result = self.store.delete(&key, &tx_for_store);
                            self.lock_manager.release_locks(auto_commit_tx_id); // Release lock
                            match delete_result {
                                Ok(deleted) => {
                                    tx_for_store.set_state(TransactionState::Committed);
                                    Ok(ExecutionResult::Deleted(deleted))
                                }
                                Err(e) => {
                                    tx_for_store.set_state(TransactionState::Aborted);
                                    Err(e)
                                }
                            }
                        }
                        Err(lock_err) => Err(lock_err),
                    }
                }
            }
            Command::BeginTransaction => {
                self.transaction_manager.begin_transaction(); // Consider if a previous active tx should be auto-committed/rolled_back
                Ok(ExecutionResult::Success)
            }
            Command::CommitTransaction => {
                if let Some(active_tx) = self.transaction_manager.get_active_transaction_mut() {
                    let tx_id_to_release = active_tx.id;
                    // The undo_log is typically cleared as part of the commit process by the transaction manager
                    // or after successful commit. Here, it's cleared before, which is fine.
                    active_tx.undo_log.clear(); 

                    // Log commit to WAL before releasing locks or finalizing commit in manager
                    let commit_entry = crate::core::storage::engine::wal::WalEntry::TransactionCommit { transaction_id: tx_id_to_release };
                    self.store.log_wal_entry(&commit_entry)?;
                    
                    self.lock_manager.release_locks(tx_id_to_release);
                    self.transaction_manager.commit_transaction(); // This will remove the tx from active list
                    Ok(ExecutionResult::Success)
                } else {
                    Err(DbError::NoActiveTransaction)
                }
            }
            Command::RollbackTransaction => {
                if let Some(mut active_tx) = self.transaction_manager.get_active_transaction_mut() {
                    let tx_id_to_release = active_tx.id;
                    
                    // Perform undo operations first
                    let temp_transaction_for_undo = Transaction::new(tx_id_to_release); // State is Active

                    for undo_op in active_tx.undo_log.iter().rev() { // Iterate in reverse
                        match undo_op {
                            UndoOperation::RevertInsert { key } => {
                                self.store.delete(key, &temp_transaction_for_undo)?;
                            }
                            UndoOperation::RevertUpdate { key, old_value } => {
                                self.store.put(key.clone(), old_value.clone(), &temp_transaction_for_undo)?;
                            }
                            UndoOperation::RevertDelete { key, old_value } => {
                                self.store.put(key.clone(), old_value.clone(), &temp_transaction_for_undo)?;
                            }
                        }
                    }
                    active_tx.undo_log.clear(); // Clear after processing

                    // Log rollback to WAL before releasing locks or finalizing rollback in manager
                    let rollback_entry = crate::core::storage::engine::wal::WalEntry::TransactionRollback { transaction_id: tx_id_to_release };
                    self.store.log_wal_entry(&rollback_entry)?;

                    self.lock_manager.release_locks(tx_id_to_release);
                    self.transaction_manager.rollback_transaction(); // This will remove the tx from active list
                    Ok(ExecutionResult::Success)
                } else {
                    Err(DbError::NoActiveTransaction)
                }
            }
        }
    }
}

// Methods specific to QueryExecutor when the store is SimpleFileKvStore
impl QueryExecutor<SimpleFileKvStore> {
    pub fn persist(&mut self) -> Result<(), DbError> {
        self.store.save_to_disk()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::query::commands::{Key, Value};
    use crate::core::transaction::transaction::UndoOperation; // Corrected import path
    use crate::core::transaction::TransactionState; // Corrected import path
    use crate::core::transaction::lock_manager::LockManager; // Explicit import for LockManager if needed by tests directly
    use crate::core::storage::engine::simple_file_kv_store::SimpleFileKvStore;
    use crate::core::storage::engine::wal::WalEntry;
    use crate::core::common::traits::DataDeserializer;
    use tempfile::NamedTempFile;
    use std::fs::File as StdFile;
    use std::io::{BufReader, ErrorKind as IoErrorKind};
    use std::path::PathBuf;


    // Helper to derive WAL path from DB path, similar to SimpleFileKvStore's internal logic
    fn derive_wal_path_for_test(store: &SimpleFileKvStore) -> PathBuf {
        let mut wal_path = store.file_path().to_path_buf();
        let original_extension = wal_path.extension().map(|s| s.to_os_string());
        if let Some(ext) = original_extension {
            let mut new_ext = ext;
            new_ext.push(".wal");
            wal_path.set_extension(new_ext);
        } else {
            wal_path.set_extension("wal");
        }
        wal_path
    }

    // Helper to read all entries from a WAL file for test verification
    fn read_all_wal_entries_for_test(wal_path: &std::path::Path) -> Result<Vec<WalEntry>, DbError> {
        if !wal_path.exists() {
            return Ok(Vec::new()); // No WAL file, no entries
        }
        let file = StdFile::open(wal_path).map_err(DbError::IoError)?;
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();
        loop {
            match <WalEntry as DataDeserializer<WalEntry>>::deserialize(&mut reader) {
                Ok(entry) => entries.push(entry),
                Err(DbError::IoError(e)) if e.kind() == IoErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e), // Other errors
            }
        }
        Ok(entries)
    }
    
    // Helper to create DbError::NoActiveTransaction for comparison, needs PartialEq on DbError
    // If DbError does not have PartialEq, we'll match on the discriminant or use string representation.
    // For now, let's assume we can compare them or we'll adjust the tests.
    // To make DbError comparable for tests:
    // In src/core/common/error.rs, add `PartialEq` to `#[derive(Debug)]` for `DbError`.
    // e.g. `#[derive(Debug, PartialEq)] pub enum DbError { ... }`
    // And also for `std::io::Error` it's tricky. A common way is to match specific Io errors or convert to string.
    // For NoActiveTransaction, PartialEq is straightforward if other variants are also comparable or not involved.

    fn create_temp_store() -> SimpleFileKvStore {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        SimpleFileKvStore::new(temp_file.path()).expect("Failed to create SimpleFileKvStore")
    }

    fn create_executor() -> QueryExecutor<SimpleFileKvStore> {
        let temp_store = create_temp_store();
        // QueryExecutor::new now initializes LockManager internally.
        QueryExecutor::new(temp_store)
    }

    #[test]
    fn test_insert_and_get() {
        let mut executor = create_executor();
        let key: Key = b"test_key_1".to_vec();
        let value: Value = b"test_value_1".to_vec();

        // Insert
        let insert_command = Command::Insert { key: key.clone(), value: value.clone() };
        let result = executor.execute_command(insert_command);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ExecutionResult::Success);

        // Get
        let get_command = Command::Get { key: key.clone() };
        let result = executor.execute_command(get_command);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ExecutionResult::Value(Some(value)));
    }

    #[test]
    fn test_get_non_existent() {
        let mut executor = create_executor();
        let key: Key = b"non_existent_key".to_vec();

        let get_command = Command::Get { key: key.clone() };
        let result = executor.execute_command(get_command);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ExecutionResult::Value(None));
    }

    #[test]
    fn test_insert_delete_get() {
        let mut executor = create_executor();
        let key: Key = b"test_key_2".to_vec();
        let value: Value = b"test_value_2".to_vec();

        // Insert
        let insert_command = Command::Insert { key: key.clone(), value: value.clone() };
        executor.execute_command(insert_command).unwrap();

        // Delete
        let delete_command = Command::Delete { key: key.clone() };
        let result = executor.execute_command(delete_command);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ExecutionResult::Deleted(true));

        // Get (should be Value(None))
        let get_command = Command::Get { key: key.clone() };
        let result = executor.execute_command(get_command);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ExecutionResult::Value(None));
    }

    #[test]
    fn test_delete_non_existent() {
        let mut executor = create_executor();
        let key: Key = b"non_existent_delete_key".to_vec();

        let delete_command = Command::Delete { key: key.clone() };
        let result = executor.execute_command(delete_command);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ExecutionResult::Deleted(false));
    }

    #[test]
    fn test_insert_update_get() {
        let mut executor = create_executor();
        let key: Key = b"test_key_3".to_vec();
        let value1: Value = b"initial_value".to_vec();
        let value2: Value = b"updated_value".to_vec();

        // Insert initial value
        let insert_command1 = Command::Insert { key: key.clone(), value: value1.clone() };
        assert_eq!(executor.execute_command(insert_command1).unwrap(), ExecutionResult::Success);

        // Get initial value
        let get_command1 = Command::Get { key: key.clone() };
        assert_eq!(executor.execute_command(get_command1).unwrap(), ExecutionResult::Value(Some(value1)));

        // Insert new value (update)
        let insert_command2 = Command::Insert { key: key.clone(), value: value2.clone() };
        assert_eq!(executor.execute_command(insert_command2).unwrap(), ExecutionResult::Success);

        // Get updated value
        let get_command2 = Command::Get { key: key.clone() };
        assert_eq!(executor.execute_command(get_command2).unwrap(), ExecutionResult::Value(Some(value2)));
    }

    #[test]
    fn test_delete_results() {
        let mut executor = create_executor();
        let key: Key = b"delete_me".to_vec();
        let value: Value = b"some_data".to_vec();

        // Insert
        let insert_cmd = Command::Insert { key: key.clone(), value: value.clone() };
        executor.execute_command(insert_cmd).expect("Insert failed");

        // Delete (item exists)
        let delete_cmd_exists = Command::Delete { key: key.clone() };
        let result_exists = executor.execute_command(delete_cmd_exists);
        
        assert!(result_exists.is_ok(), "Delete operation (existing) failed: {:?}", result_exists.err());
        assert_eq!(result_exists.unwrap(), ExecutionResult::Deleted(true), "Delete operation (existing) should return Deleted(true)");

        // Verify it's actually gone
        let get_cmd = Command::Get { key: key.clone() };
        let get_result = executor.execute_command(get_cmd);
        assert_eq!(get_result.unwrap(), ExecutionResult::Value(None), "Key should be Value(None) after deletion");

        // Delete (item doesn't exist)
        let delete_cmd_not_exists = Command::Delete { key: b"does_not_exist".to_vec() };
        let result_not_exists = executor.execute_command(delete_cmd_not_exists);

        assert!(result_not_exists.is_ok(), "Delete operation (non-existing) failed: {:?}", result_not_exists.err());
        assert_eq!(result_not_exists.unwrap(), ExecutionResult::Deleted(false), "Delete operation (non-existing) should return Deleted(false)");
    }

    // New tests for transaction handling
    #[test]
    fn test_insert_with_active_transaction() {
        let mut executor = create_executor();
        let key = b"tx_key_1".to_vec();
        let value = b"tx_value_1".to_vec();

        // Begin transaction
        let tx = executor.transaction_manager.begin_transaction();
        assert!(executor.transaction_manager.get_active_transaction().is_some());
        assert_eq!(executor.transaction_manager.get_active_transaction().unwrap().id, tx.id);

        // Insert within transaction
        let insert_command = Command::Insert { key: key.clone(), value: value.clone() };
        let result = executor.execute_command(insert_command);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ExecutionResult::Success);

        // Key should not be visible yet if store implements transactional visibility
        // For current SimpleFileKvStore, it might be visible. This depends on store's MVCC.
        // Let's assume for now it might be visible, but the commit is what makes it permanent.

        // Commit transaction
        let commit_result = executor.execute_command(Command::CommitTransaction);
        assert!(commit_result.is_ok());
        assert_eq!(commit_result.unwrap(), ExecutionResult::Success);
        assert!(executor.transaction_manager.get_active_transaction().is_none());

        // Verify data after commit
        let get_command = Command::Get { key: key.clone() };
        // Need a new executor or re-use if store is shared and state is external
        // For this test, create_executor provides a fresh store/executor.
        // To test persistence, one would need to re-open the store.
        // Here, we are testing QueryExecutor logic, assuming store works.
        // So, we get from the same executor.
        assert_eq!(executor.execute_command(get_command).unwrap(), ExecutionResult::Value(Some(value)));
    }

    #[test]
    fn test_insert_rollback_transaction() {
        let mut executor = create_executor();
        let key = b"tx_key_rollback".to_vec();
        let value = b"tx_value_rollback".to_vec();

        // Begin transaction
        let tx = executor.transaction_manager.begin_transaction();
        assert!(executor.transaction_manager.get_active_transaction().is_some());
        assert_eq!(executor.transaction_manager.get_active_transaction().unwrap().id, tx.id);
        
        // Insert within transaction
        let insert_command = Command::Insert { key: key.clone(), value: value.clone() };
        executor.execute_command(insert_command).unwrap();

        // Rollback transaction
        let rollback_result = executor.execute_command(Command::RollbackTransaction);
        assert!(rollback_result.is_ok());
        assert_eq!(rollback_result.unwrap(), ExecutionResult::Success);
        assert!(executor.transaction_manager.get_active_transaction().is_none());

        // Verify data is not present after rollback (depends on store implementing rollback)
        // SimpleFileKvStore as is might not roll back writes, it would just commit them.
        // This test highlights the need for store-level transaction support for rollback.
        // For now, we test that the QueryExecutor correctly calls rollback.
        // The actual data state depends on the store's implementation of put/delete with transactions.
        let get_command = Command::Get { key: key.clone() };
        // Assuming the store does not roll back, the data would be there.
        // If store supported rollback, this would be Value(None).
        // Current SimpleFileKvStore always writes through.
        // So, this test as-is for SimpleFileKvStore will show the value is present. // This comment is outdated.
        // This is acceptable as we are testing QueryExecutor's interaction with TransactionManager.
        // The effect of rollback is up to the store. // QueryExecutor now attempts to use store to revert.
         assert_eq!(executor.execute_command(get_command).unwrap(), ExecutionResult::Value(None)); // Expect None because RevertInsert should delete.
        // To make this test pass with Value(None), SimpleFileKvStore would need modification. // QueryExecutor handles this.
        // For now, we are asserting that the value is present because SimpleFileKvStore doesn't rollback. // Outdated.
        // If store supported rollback, this would be:
        // assert_eq!(executor.execute_command(get_command).unwrap(), ExecutionResult::Value(None));
    }

     #[test]
    fn test_delete_with_active_transaction_commit() {
        let mut executor = create_executor();
        let key = b"tx_delete_commit_key".to_vec();
        let value = b"tx_delete_commit_value".to_vec();

        // Setup: Insert data without transaction (auto-commit)
        let insert_command = Command::Insert { key: key.clone(), value: value.clone() };
        executor.execute_command(insert_command).unwrap();
        
        // Verify insertion
        let get_command_before = Command::Get { key: key.clone() };
        assert_eq!(executor.execute_command(get_command_before).unwrap(), ExecutionResult::Value(Some(value)));

        // Begin transaction
        let tx = executor.transaction_manager.begin_transaction();
        assert!(executor.transaction_manager.get_active_transaction().is_some());

        // Delete within transaction
        let delete_command = Command::Delete { key: key.clone() };
        let result = executor.execute_command(delete_command);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ExecutionResult::Deleted(true));

        // Commit transaction
        let commit_result = executor.execute_command(Command::CommitTransaction);
        assert!(commit_result.is_ok());
        assert_eq!(commit_result.unwrap(), ExecutionResult::Success);
        assert!(executor.transaction_manager.get_active_transaction().is_none());

        // Verify data is deleted after commit
        let get_command_after = Command::Get { key: key.clone() };
        assert_eq!(executor.execute_command(get_command_after).unwrap(), ExecutionResult::Value(None));
    }

    #[test]
    fn test_delete_with_active_transaction_rollback() {
        let mut executor = create_executor();
        let key = b"tx_delete_rollback_key".to_vec();
        let value = b"tx_delete_rollback_value".to_vec();

        // Setup: Insert data without transaction (auto-commit)
        let insert_command = Command::Insert { key: key.clone(), value: value.clone() };
        executor.execute_command(insert_command).unwrap();
        
        // Verify insertion
        let get_command_before = Command::Get { key: key.clone() };
        assert_eq!(executor.execute_command(get_command_before.clone()).unwrap(), ExecutionResult::Value(Some(value.clone())));

        // Begin transaction
        let _tx = executor.transaction_manager.begin_transaction();
        assert!(executor.transaction_manager.get_active_transaction().is_some());

        // Delete within transaction
        let delete_command = Command::Delete { key: key.clone() };
        let result = executor.execute_command(delete_command);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ExecutionResult::Deleted(true));
        
        // Rollback transaction
        let rollback_result = executor.execute_command(Command::RollbackTransaction);
        assert!(rollback_result.is_ok());
        assert_eq!(rollback_result.unwrap(), ExecutionResult::Success);
        assert!(executor.transaction_manager.get_active_transaction().is_none());

        // Verify data is still present after rollback (assuming store doesn't roll back deletions)
        // Similar to insert rollback, this depends on store's behavior.
        // SimpleFileKvStore writes through deletions.
        assert_eq!(executor.execute_command(get_command_before).unwrap(), ExecutionResult::Value(Some(value)));
        // If store supported rollback for delete, this would be:
        // assert_eq!(executor.execute_command(get_command_before).unwrap(), ExecutionResult::Value(None));
    }

    // --- New tests for BEGIN, COMMIT, ROLLBACK ---

    #[test]
    fn test_begin_transaction_command() {
        let mut executor = create_executor();
        let begin_cmd = Command::BeginTransaction;
        
        let result = executor.execute_command(begin_cmd);
        assert_eq!(result, Ok(ExecutionResult::Success));
        
        let active_tx = executor.transaction_manager.get_active_transaction();
        assert!(active_tx.is_some());
        let tx = active_tx.unwrap();
        assert_eq!(tx.state, TransactionState::Active);
        assert!(tx.id > 0); // Assuming IDs start from 1 or are positive
    }

    #[test]
    fn test_commit_transaction_command_with_active_tx() {
        let mut executor = create_executor();
        
        // Begin
        executor.execute_command(Command::BeginTransaction).unwrap();
        let active_tx_before_commit = executor.transaction_manager.get_active_transaction().unwrap();
        let tx_id = active_tx_before_commit.id;

        // Insert something (optional, but good to simulate a real transaction)
        let insert_cmd = Command::Insert { key: b"key_commit".to_vec(), value: b"val_commit".to_vec() };
        executor.execute_command(insert_cmd).unwrap();

        // Commit
        let commit_cmd = Command::CommitTransaction;
        let result = executor.execute_command(commit_cmd);
        assert_eq!(result, Ok(ExecutionResult::Success));
        
        assert!(executor.transaction_manager.get_active_transaction().is_none());
        // The fact that commit_transaction (and rollback_transaction) removes the transaction
        // from the internal active_transactions map is a TransactionManager implementation detail.
        // QueryExecutor tests should primarily focus on the observable state change,
        // e.g., get_active_transaction() returning None.
        
        // Verify WAL entry for commit
        let wal_path = derive_wal_path_for_test(&executor.store);
        let wal_entries = read_all_wal_entries_for_test(&wal_path).unwrap();
        
        assert_eq!(wal_entries.len(), 2, "Should be 1 Put and 1 Commit WAL entry");
        match &wal_entries[0] {
            WalEntry::Put { transaction_id: put_tx_id, .. } => {
                 assert_eq!(*put_tx_id, tx_id, "Put entry should have the correct transaction ID");
            }
            _ => panic!("Expected Put entry first"),
        }
        match &wal_entries[1] {
            WalEntry::TransactionCommit { transaction_id: commit_tx_id } => {
                assert_eq!(*commit_tx_id, tx_id, "Commit entry should have the correct transaction ID");
            }
            _ => panic!("Expected TransactionCommit entry second"),
        }
    }

    #[test]
    fn test_rollback_transaction_command_with_active_tx_logs_wal_and_reverts_cache() {
        let mut executor = create_executor();
        let key_orig = b"key_orig".to_vec();
        let val_orig = b"val_orig".to_vec();
        let key_rb = b"key_rollback_wal".to_vec();
        let val_rb = b"val_rollback_wal".to_vec();

        // Setup initial state (key_orig) - auto-committed
        executor.execute_command(Command::Insert { key: key_orig.clone(), value: val_orig.clone() }).unwrap();
        assert_eq!(executor.execute_command(Command::Get { key: key_orig.clone() }).unwrap(), ExecutionResult::Value(Some(val_orig.clone())));

        // Begin transaction
        executor.execute_command(Command::BeginTransaction).unwrap();
        let active_tx = executor.transaction_manager.get_active_transaction().unwrap().clone();
        let tx_id = active_tx.id;
        
        // 1. Insert new key (key_rb)
        executor.execute_command(Command::Insert { key: key_rb.clone(), value: val_rb.clone() }).unwrap();
        assert_eq!(executor.execute_command(Command::Get { key: key_rb.clone() }).unwrap(), ExecutionResult::Value(Some(val_rb.clone())));
        
        // 2. Update original key (key_orig)
        let val_orig_updated = b"val_orig_updated".to_vec();
        executor.execute_command(Command::Insert { key: key_orig.clone(), value: val_orig_updated.clone() }).unwrap();
        assert_eq!(executor.execute_command(Command::Get { key: key_orig.clone() }).unwrap(), ExecutionResult::Value(Some(val_orig_updated.clone())));

        // 3. Delete another key (setup for RevertDelete)
        let key_del = b"key_to_delete".to_vec();
        let val_del = b"val_to_delete".to_vec();
        executor.execute_command(Command::Insert { key: key_del.clone(), value: val_del.clone() }).unwrap(); // Insert in this tx
        assert_eq!(executor.execute_command(Command::Get { key: key_del.clone() }).unwrap(), ExecutionResult::Value(Some(val_del.clone())));
        executor.execute_command(Command::Delete { key: key_del.clone() }).unwrap();
        assert_eq!(executor.execute_command(Command::Get { key: key_del.clone() }).unwrap(), ExecutionResult::Value(None));


        // Rollback
        let rollback_cmd = Command::RollbackTransaction;
        let result = executor.execute_command(rollback_cmd);
        assert_eq!(result, Ok(ExecutionResult::Success));
        
        assert!(executor.transaction_manager.get_active_transaction().is_none());

        // Verify cache state after rollback
        assert_eq!(executor.execute_command(Command::Get { key: key_rb.clone() }).unwrap(), ExecutionResult::Value(None), "key_rb (RevertInsert) should be gone");
        assert_eq!(executor.execute_command(Command::Get { key: key_orig.clone() }).unwrap(), ExecutionResult::Value(Some(val_orig.clone())), "key_orig (RevertUpdate) should be back to original value");
        assert_eq!(executor.execute_command(Command::Get { key: key_del.clone() }).unwrap(), ExecutionResult::Value(None), "key_del (inserted then deleted in tx) should not exist after rollback");


        // Verify WAL entry for rollback
        let wal_path = derive_wal_path_for_test(&executor.store);
        let wal_entries = read_all_wal_entries_for_test(&wal_path).unwrap();
        
        // Expected WAL: 1 initial insert (auto-commit), then 3 ops in tx, then 1 rollback marker
        // Initial insert: Put { tx_id:0, key_orig, val_orig } (1)
        // TX operations:
        //   Put {tx_id, key_rb, val_rb} (2)
        //   Put {tx_id, key_orig, val_orig_updated} (3)
        //   Put {tx_id, key_del, val_del} (4)
        //   Delete {tx_id, key_del} (5)
        // Rollback Undo operations (all use tx_id of the original transaction):
        //   RevertDelete for key_del -> store.put(key_del, val_del) (6)
        //   RevertInsert for key_del -> store.delete(key_del) (7)
        //   RevertUpdate for key_orig -> store.put(key_orig, val_orig) (8)
        //   RevertInsert for key_rb -> store.delete(key_rb) (9)
        // Rollback marker: TransactionRollback { tx_id } (10)
        // Total: 10
        assert_eq!(wal_entries.len(), 10, "WAL entries count mismatch");

        // Check the last entry is TransactionRollback with correct tx_id
        match wal_entries.last().unwrap() {
            WalEntry::TransactionRollback { transaction_id: rollback_tx_id } => {
                assert_eq!(*rollback_tx_id, tx_id, "Rollback entry should have the correct transaction ID");
            }
            _ => panic!("Expected TransactionRollback entry last. Got: {:?}", wal_entries.last().unwrap()),
        }
    }

    #[test]
    fn test_commit_transaction_command_no_active_tx() {
        let mut executor = create_executor();
        let commit_cmd = Command::CommitTransaction;
        
        let result = executor.execute_command(commit_cmd);
        // This requires DbError to implement PartialEq. If not, match the error.
        // Assuming DbError derives PartialEq for simplicity here.
        // match result {
        //     Err(DbError::NoActiveTransaction) => (), // Correct
        //     _ => panic!("Expected DbError::NoActiveTransaction, got {:?}", result),
        // }
        // If DbError cannot derive PartialEq due to std::io::Error or other non-PartialEq fields:
        assert!(matches!(result, Err(DbError::NoActiveTransaction)));
    }

    #[test]
    fn test_rollback_transaction_command_no_active_tx() {
        let mut executor = create_executor();
        let rollback_cmd = Command::RollbackTransaction;
        
        let result = executor.execute_command(rollback_cmd);
        // Assuming DbError derives PartialEq
        // assert_eq!(result, Err(DbError::NoActiveTransaction));
        assert!(matches!(result, Err(DbError::NoActiveTransaction)));
    }

    #[test]
    fn test_multiple_begin_commands() {
        let mut executor = create_executor();

        // First BEGIN
        executor.execute_command(Command::BeginTransaction).unwrap();
        let tx1 = executor.transaction_manager.get_active_transaction().unwrap().clone();
        assert_eq!(tx1.state, TransactionState::Active);

        // Insert, should use tx1
        let insert_cmd1 = Command::Insert { key: b"key1".to_vec(), value: b"val1".to_vec() };
        executor.execute_command(insert_cmd1).unwrap();

        // Second BEGIN
        executor.execute_command(Command::BeginTransaction).unwrap();
        let tx2 = executor.transaction_manager.get_active_transaction().unwrap().clone();
        assert_eq!(tx2.state, TransactionState::Active);
        assert_ne!(tx1.id, tx2.id, "Second BEGIN should start a new transaction with a new ID.");

        // The first transaction (tx1) is now "orphaned" in the sense that it's no longer the
        // current_active_transaction_id. TransactionManager.begin_transaction replaces the current ID.
        // The previous current transaction (tx1) would be orphaned if not explicitly committed/rolled back.
        // Testing the internal state of active_transactions map for tx1's presence is a TransactionManager
        // unit test concern rather than QueryExecutor.
        // For QueryExecutor, we care that a new transaction context (tx2) is now active.
        assert_eq!(executor.transaction_manager.current_active_transaction_id(), Some(tx2.id));

        // Commit the second transaction (current one)
        executor.execute_command(Command::CommitTransaction).unwrap();
        assert!(executor.transaction_manager.get_active_transaction().is_none());
        // After tx2 is committed, trying to commit again without a new BEGIN should fail.
        let commit_again_cmd = Command::CommitTransaction;
        assert!(matches!(executor.execute_command(commit_again_cmd), Err(DbError::NoActiveTransaction)));
    }
    
    #[test]
    fn test_operations_use_active_transaction_after_begin() {
        let mut executor = create_executor();

        // Begin transaction
        executor.execute_command(Command::BeginTransaction).unwrap();
        let active_tx_id = executor.transaction_manager.get_active_transaction().unwrap().id;

        // Insert operation
        let insert_cmd = Command::Insert { key: b"key_tx".to_vec(), value: b"value_tx".to_vec() };
        executor.execute_command(insert_cmd).unwrap();
        
        // Check that the store's put method was called with a transaction that has active_tx_id
        // This is an indirect check. A more direct check would require mocking the store
        // or having the store record the transaction ID it received.
        // For now, we assume if an active transaction exists, execute_command passes it.
        // The data's visibility before commit depends on the store's MVCC properties.
        // SimpleFileKvStore writes through, so it would be visible.

        // Let's verify the item is in the store (due to SimpleFileKvStore behavior)
        let get_cmd = Command::Get { key: b"key_tx".to_vec() };
        assert_eq!(executor.execute_command(get_cmd.clone()).unwrap(), ExecutionResult::Value(Some(b"value_tx".to_vec())));

        // Commit
        executor.execute_command(Command::CommitTransaction).unwrap();
        assert!(executor.transaction_manager.get_active_transaction().is_none());

        // Check data persistence after commit
        assert_eq!(executor.execute_command(get_cmd).unwrap(), ExecutionResult::Value(Some(b"value_tx".to_vec())));
    }

    #[test]
    fn test_shared_lock_concurrency() {
        let mut executor = create_executor();
        let key: Key = b"shared_lock_key".to_vec();
        let value: Value = b"value".to_vec();

        // Setup: Insert key K with value V (auto-commit)
        let insert_command = Command::Insert { key: key.clone(), value: value.clone() };
        assert_eq!(executor.execute_command(insert_command).unwrap(), ExecutionResult::Success);

        // Tx1: BEGIN
        assert_eq!(executor.execute_command(Command::BeginTransaction).unwrap(), ExecutionResult::Success);
        let tx1_id = executor.transaction_manager.get_active_transaction().unwrap().id;

        // Tx1: GET K
        let get_command_tx1 = Command::Get { key: key.clone() };
        assert_eq!(executor.execute_command(get_command_tx1).unwrap(), ExecutionResult::Value(Some(value.clone())));

        // Tx2: BEGIN
        // Note: Tx1 is still "active" in terms of holding locks in LockManager,
        // but TransactionManager now considers Tx2 the "current active".
        assert_eq!(executor.execute_command(Command::BeginTransaction).unwrap(), ExecutionResult::Success);
        let tx2_id = executor.transaction_manager.get_active_transaction().unwrap().id;
        assert_ne!(tx1_id, tx2_id, "Transaction IDs should be different");

        // Tx2: GET K (should succeed as S locks are compatible)
        let get_command_tx2 = Command::Get { key: key.clone() };
        assert_eq!(executor.execute_command(get_command_tx2).unwrap(), ExecutionResult::Value(Some(value.clone())));

        // Tx2: COMMIT (Tx2 is the currently active transaction)
        assert_eq!(executor.execute_command(Command::CommitTransaction).unwrap(), ExecutionResult::Success);
        
        // At this point, Tx1's shared lock is still held in the LockManager because Tx1 was not
        // the active transaction when a COMMIT was issued. This is an accepted artifact of
        // the current simplified transaction management for this specific test, which focuses
        // on the concurrent acquisition of shared locks.
        // A separate test would be needed to ensure Tx1's locks are released if Tx1 was made active and then committed.
    }

    #[test]
    fn test_exclusive_lock_prevents_shared_read() {
        let mut executor = create_executor();
        let key: Key = b"exclusive_prevents_shared_key".to_vec();
        let value: Value = b"value".to_vec();

        // Transaction 1
        assert_eq!(executor.execute_command(Command::BeginTransaction).unwrap(), ExecutionResult::Success);
        let tx1_id = executor.transaction_manager.current_active_transaction_id().unwrap();
        let insert_command = Command::Insert { key: key.clone(), value: value.clone() };
        assert_eq!(executor.execute_command(insert_command).unwrap(), ExecutionResult::Success);

        // Transaction 2
        assert_eq!(executor.execute_command(Command::BeginTransaction).unwrap(), ExecutionResult::Success);
        let tx2_id = executor.transaction_manager.current_active_transaction_id().unwrap();
        assert_ne!(tx1_id, tx2_id, "Transaction IDs should be different");

        let get_command_tx2 = Command::Get { key: key.clone() };
        let result_tx2 = executor.execute_command(get_command_tx2);

        match result_tx2 {
            Err(DbError::LockConflict { key: err_key, current_tx: err_current_tx, locked_by_tx: err_locked_by_tx }) => {
                assert_eq!(err_key, key);
                assert_eq!(err_current_tx, tx2_id);
                assert_eq!(err_locked_by_tx, Some(tx1_id));
            }
            _ => panic!("Expected DbError::LockConflict, got {:?}", result_tx2),
        }

        // Cleanup Tx2
        assert_eq!(executor.execute_command(Command::RollbackTransaction).unwrap(), ExecutionResult::Success);
    }
}
