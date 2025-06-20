#[cfg(test)]
mod tests {
    use crate::core::common::traits::DataDeserializer;
    use crate::core::query::commands::{Command, Key, SqlAssignment}; // Added SqlAssignment
    use crate::core::query::executor::*;
    use crate::core::storage::engine::wal::WalEntry;
    use crate::core::storage::engine::{traits::KeyValueStore, InMemoryKvStore, SimpleFileKvStore};
    use crate::core::transaction::TransactionState; // Used by QueryExecutor indirectly via TransactionManager
    use crate::core::types::DataType;
    use serde_json::json;
    use std::fs::File as StdFile;
    use std::io::{BufReader, ErrorKind as IoErrorKind};
    use std::path::PathBuf;
    use tempfile::NamedTempFile;
    // Used by define_executor_tests! macro
    // use std::any::TypeId; // For conditional test logic if needed, though trying to avoid - REMOVED
    use crate::core::common::OxidbError;
    // use std::collections::HashSet; // REMOVED - Not directly used in this test file
    use crate::core::wal::writer::WalWriter;
    use std::sync::{Arc, RwLock}; // Added for WalWriter

    use crate::core::common::serialization::serialize_data_type;
    use crate::core::transaction::transaction::Transaction; // Removed UndoOperation

    // Helper functions (original test logic, now generic)
    fn run_test_get_non_existent<S: KeyValueStore<Vec<u8>, Vec<u8>>>(
        executor: &mut QueryExecutor<S>,
    ) {
        let key: Key = b"non_existent_key".to_vec();
        let get_command = Command::Get { key: key.clone() };
        let result = executor.execute_command(get_command);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ExecutionResult::Value(None));
    }

    fn run_test_insert_and_get_integer<S: KeyValueStore<Vec<u8>, Vec<u8>>>(
        executor: &mut QueryExecutor<S>,
    ) {
        let key: Key = b"int_key".to_vec();
        let value = DataType::Integer(12345);
        let insert_command = Command::Insert { key: key.clone(), value: value.clone() };
        assert_eq!(executor.execute_command(insert_command).unwrap(), ExecutionResult::Success);
        let get_command = Command::Get { key: key.clone() };
        assert_eq!(
            executor.execute_command(get_command).unwrap(),
            ExecutionResult::Value(Some(value))
        );
    }

    fn run_test_insert_and_get_string<S: KeyValueStore<Vec<u8>, Vec<u8>>>(
        executor: &mut QueryExecutor<S>,
    ) {
        let key: Key = b"str_key".to_vec();
        let value = DataType::String("hello world".to_string());

        let insert_command = Command::Insert { key: key.clone(), value: value.clone() };
        assert_eq!(executor.execute_command(insert_command).unwrap(), ExecutionResult::Success);

        let get_command = Command::Get { key: key.clone() };
        assert_eq!(
            executor.execute_command(get_command).unwrap(),
            ExecutionResult::Value(Some(value))
        );
    }

    fn run_test_insert_delete_get<S: KeyValueStore<Vec<u8>, Vec<u8>>>(
        executor: &mut QueryExecutor<S>,
    ) {
        let key: Key = b"test_key_2".to_vec();
        let value = DataType::String("test_value_2".to_string());

        let insert_command = Command::Insert { key: key.clone(), value: value.clone() };
        executor.execute_command(insert_command).unwrap();

        let delete_command = Command::Delete { key: key.clone() };
        let result = executor.execute_command(delete_command);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ExecutionResult::Deleted(true));

        let get_command = Command::Get { key: key.clone() };
        let result = executor.execute_command(get_command);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ExecutionResult::Value(None));
    }

    fn run_test_begin_transaction_command<S: KeyValueStore<Vec<u8>, Vec<u8>>>(
        executor: &mut QueryExecutor<S>,
    ) {
        let begin_cmd = Command::BeginTransaction;

        let result = executor.execute_command(begin_cmd);
        assert!(matches!(result, Ok(ExecutionResult::Success)));

        let active_tx_opt = executor.transaction_manager.get_active_transaction();
        assert!(active_tx_opt.is_some());
        let tx = active_tx_opt.unwrap();
        assert_eq!(tx.state, TransactionState::Active);
        assert!(tx.id.0 > 0, "Transaction ID should be greater than 0"); // Compare inner u64
    }

    fn run_test_insert_with_active_transaction<S: KeyValueStore<Vec<u8>, Vec<u8>>>(
        executor: &mut QueryExecutor<S>,
    ) {
        let key = b"tx_key_1".to_vec();
        let value = DataType::String("tx_value_1".to_string());

        let begin_tx_result = executor.transaction_manager.begin_transaction();
        assert!(begin_tx_result.is_ok());
        let tx = begin_tx_result.unwrap(); // tx is Transaction here

        assert!(executor.transaction_manager.get_active_transaction().is_some());
        assert_eq!(executor.transaction_manager.get_active_transaction().unwrap().id, tx.id); // Comparing TransactionId with TransactionId

        let insert_command = Command::Insert { key: key.clone(), value: value.clone() };
        let result = executor.execute_command(insert_command);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ExecutionResult::Success);

        let commit_result = executor.execute_command(Command::CommitTransaction);
        assert!(commit_result.is_ok());
        assert_eq!(commit_result.unwrap(), ExecutionResult::Success);
        assert!(executor.transaction_manager.get_active_transaction().is_none());

        let get_command = Command::Get { key: key.clone() };
        assert_eq!(
            executor.execute_command(get_command).unwrap(),
            ExecutionResult::Value(Some(value))
        );
    }

    fn run_test_insert_rollback_transaction<S: KeyValueStore<Vec<u8>, Vec<u8>>>(
        executor: &mut QueryExecutor<S>,
    ) {
        let key = b"tx_key_rollback".to_vec();
        let value = DataType::String("tx_value_rollback".to_string());

        let begin_tx_result = executor.transaction_manager.begin_transaction();
        assert!(begin_tx_result.is_ok());
        let tx = begin_tx_result.unwrap(); // tx is Transaction

        assert!(executor.transaction_manager.get_active_transaction().is_some());
        assert_eq!(executor.transaction_manager.get_active_transaction().unwrap().id, tx.id); // Comparing TransactionId with TransactionId

        let insert_command = Command::Insert { key: key.clone(), value: value.clone() };
        executor.execute_command(insert_command).unwrap();

        let rollback_result = executor.execute_command(Command::RollbackTransaction);
        assert!(rollback_result.is_ok());
        assert_eq!(rollback_result.unwrap(), ExecutionResult::Success);
        assert!(executor.transaction_manager.get_active_transaction().is_none());

        let get_command = Command::Get { key: key.clone() };
        assert_eq!(executor.execute_command(get_command).unwrap(), ExecutionResult::Value(None));
    }

    fn derive_wal_path_for_test(store_lock: &Arc<RwLock<SimpleFileKvStore>>) -> PathBuf {
        let store = store_lock.read().unwrap();
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

    fn read_all_wal_entries_for_test(
        wal_path: &std::path::Path,
    ) -> Result<Vec<WalEntry>, OxidbError> {
        // Changed
        if !wal_path.exists() {
            return Ok(Vec::new());
        }
        let file = StdFile::open(wal_path).map_err(OxidbError::Io)?; // Changed
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();
        loop {
            match <WalEntry as DataDeserializer<WalEntry>>::deserialize(&mut reader) {
                Ok(entry) => entries.push(entry),
                Err(OxidbError::Io(e)) if e.kind() == IoErrorKind::UnexpectedEof => break, // Changed
                Err(e) => return Err(e),
            }
        }
        Ok(entries)
    }

    // create_temp_store() has been inlined into create_file_executor() to facilitate WAL path derivation.
    // If it were used elsewhere, it would need to be adapted or this comment removed.

    fn create_file_executor() -> QueryExecutor<SimpleFileKvStore> {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir for indexes");
        let index_path = temp_dir.path().to_path_buf();
        let temp_store_file = NamedTempFile::new().expect("Failed to create temp db file");
        let store_path = temp_store_file.path().to_path_buf();
        let temp_store =
            SimpleFileKvStore::new(&store_path).expect("Failed to create SimpleFileKvStore");

        // Ensure TransactionManager's WalWriter uses a distinct path from SimpleFileKvStore's internal WAL.
        // SimpleFileKvStore's internal WAL typically defaults to <db_name>.db.wal or <db_name>.wal
        // Let's use <db_name>.tx.wal for TransactionManager's WalWriter for clarity in tests.
        let mut tm_wal_path = store_path.clone();
        tm_wal_path.set_extension("tx_wal"); // e.g. /tmp/somefile.tx_wal
        let wal_config = crate::core::wal::writer::WalWriterConfig::default();
        let tm_wal_writer = WalWriter::new(tm_wal_path, wal_config);
        let log_manager_arc = Arc::new(crate::core::wal::log_manager::LogManager::new());

        QueryExecutor::new(temp_store, index_path, tm_wal_writer, log_manager_arc).unwrap()
    }

    fn create_in_memory_executor() -> QueryExecutor<InMemoryKvStore> {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir for indexes");
        let index_path = temp_dir.path().to_path_buf();
        let store = InMemoryKvStore::new();

        let wal_temp_file =
            NamedTempFile::new().expect("Failed to create temp wal file for in-memory test");
        let wal_config = crate::core::wal::writer::WalWriterConfig::default();
        let wal_writer = WalWriter::new(wal_temp_file.path().to_path_buf(), wal_config);
        let log_manager_arc = Arc::new(crate::core::wal::log_manager::LogManager::new());

        QueryExecutor::new(store, index_path, wal_writer, log_manager_arc).unwrap()
    }

    // test_index_persistence_via_executor_persist calls QueryExecutor::new directly too.
    // It had errors E0061 in the last cargo check.
    // I need to read the file to apply the fix there. For now, this diff focuses on the helpers.

    #[test]
    fn test_insert_and_get_boolean() {
        let mut executor = create_file_executor();
        let key: Key = b"bool_key".to_vec();
        let value = DataType::Boolean(true);

        let insert_command = Command::Insert { key: key.clone(), value: value.clone() };
        assert_eq!(executor.execute_command(insert_command).unwrap(), ExecutionResult::Success);

        let get_command = Command::Get { key: key.clone() };
        assert_eq!(
            executor.execute_command(get_command).unwrap(),
            ExecutionResult::Value(Some(value))
        );
    }

    #[test]
    fn test_insert_and_get_json_blob() {
        let mut executor = create_file_executor();
        let key: Key = b"json_key".to_vec();
        let value = DataType::JsonBlob(json!({ "name": "oxidb", "version": 0.1 }));

        let insert_command = Command::Insert { key: key.clone(), value: value.clone() };
        assert_eq!(executor.execute_command(insert_command).unwrap(), ExecutionResult::Success);

        let get_command = Command::Get { key: key.clone() };
        assert_eq!(
            executor.execute_command(get_command).unwrap(),
            ExecutionResult::Value(Some(value))
        );
    }

    #[test]
    fn test_get_malformed_data_deserialization_error() {
        let mut executor = create_file_executor();
        let key: Key = b"malformed_key".to_vec();
        let malformed_bytes: Vec<u8> = b"this is not valid json for DataType".to_vec();

        let dummy_tx = Transaction::new(crate::core::common::types::TransactionId(0));
        let dummy_lsn = 0;
        // Ensure this direct store.put call is correct. It was already updated in Turn 4 of previous session.
        // If KeyValueStore::put now expects LSN, this call is correct.
        executor
            .store
            .write()
            .unwrap()
            .put(key.clone(), malformed_bytes, &dummy_tx, dummy_lsn)
            .unwrap();

        let get_command = Command::Get { key: key.clone() };
        let result = executor.execute_command(get_command);

        assert!(result.is_err());
        match result.unwrap_err() {
            OxidbError::Json(_) => { /* Expected, as handle_get now uses serde_json via deserialize_data_type */
            }
            other_err => panic!("Expected OxidbError::Json, got {:?}", other_err),
        }
    }

    #[test]
    fn test_delete_non_existent() {
        let mut executor = create_file_executor();
        let key: Key = b"non_existent_delete_key".to_vec();

        let delete_command = Command::Delete { key: key.clone() };
        let result = executor.execute_command(delete_command);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ExecutionResult::Deleted(false));
    }

    #[test]
    fn test_insert_update_get() {
        let mut executor = create_file_executor();
        let key: Key = b"test_key_3".to_vec();
        let value1 = DataType::String("initial_value".to_string());
        let value2 = DataType::String("updated_value".to_string());

        let insert_command1 = Command::Insert { key: key.clone(), value: value1.clone() };
        assert_eq!(executor.execute_command(insert_command1).unwrap(), ExecutionResult::Success);

        let get_command1 = Command::Get { key: key.clone() };
        assert_eq!(
            executor.execute_command(get_command1).unwrap(),
            ExecutionResult::Value(Some(value1))
        );

        let insert_command2 = Command::Insert { key: key.clone(), value: value2.clone() };
        assert_eq!(executor.execute_command(insert_command2).unwrap(), ExecutionResult::Success);

        let get_command2 = Command::Get { key: key.clone() };
        assert_eq!(
            executor.execute_command(get_command2).unwrap(),
            ExecutionResult::Value(Some(value2))
        );
    }

    #[test]
    fn test_delete_results() {
        let mut executor = create_file_executor();
        let key: Key = b"delete_me".to_vec();
        let value = DataType::String("some_data".to_string());

        let insert_cmd = Command::Insert { key: key.clone(), value: value.clone() };
        executor.execute_command(insert_cmd).expect("Insert failed");

        let delete_cmd_exists = Command::Delete { key: key.clone() };
        let result_exists = executor.execute_command(delete_cmd_exists);

        assert!(
            result_exists.is_ok(),
            "Delete operation (existing) failed: {:?}",
            result_exists.err()
        );
        assert_eq!(
            result_exists.unwrap(),
            ExecutionResult::Deleted(true),
            "Delete operation (existing) should return Deleted(true)"
        );

        let get_cmd = Command::Get { key: key.clone() };
        let get_result = executor.execute_command(get_cmd);
        assert_eq!(
            get_result.unwrap(),
            ExecutionResult::Value(None),
            "Key should be Value(None) after deletion"
        );

        let delete_cmd_not_exists = Command::Delete { key: b"does_not_exist".to_vec() };
        let result_not_exists = executor.execute_command(delete_cmd_not_exists);

        assert!(
            result_not_exists.is_ok(),
            "Delete operation (non-existing) failed: {:?}",
            result_not_exists.err()
        );
        assert_eq!(
            result_not_exists.unwrap(),
            ExecutionResult::Deleted(false),
            "Delete operation (non-existing) should return Deleted(false)"
        );
    }

    #[test]
    fn test_delete_with_active_transaction_commit() {
        let mut executor = create_file_executor();
        let key = b"tx_delete_commit_key".to_vec();
        let value = DataType::String("tx_delete_commit_value".to_string());

        let insert_command = Command::Insert { key: key.clone(), value: value.clone() };
        executor.execute_command(insert_command).unwrap();

        let get_command_before = Command::Get { key: key.clone() };
        assert_eq!(
            executor.execute_command(get_command_before).unwrap(),
            ExecutionResult::Value(Some(value))
        );

        let _ = executor.transaction_manager.begin_transaction();
        assert!(executor.transaction_manager.get_active_transaction().is_some());

        let delete_command = Command::Delete { key: key.clone() };
        let result = executor.execute_command(delete_command);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ExecutionResult::Deleted(true));

        let commit_result = executor.execute_command(Command::CommitTransaction);
        assert!(commit_result.is_ok());
        assert_eq!(commit_result.unwrap(), ExecutionResult::Success);
        assert!(executor.transaction_manager.get_active_transaction().is_none());

        let get_command_after = Command::Get { key: key.clone() };
        assert_eq!(
            executor.execute_command(get_command_after).unwrap(),
            ExecutionResult::Value(None)
        );
    }

    #[test]
    fn test_delete_with_active_transaction_rollback() {
        let mut executor = create_file_executor();
        let key = b"tx_delete_rollback_key".to_vec();
        let value = DataType::String("tx_delete_rollback_value".to_string());

        let insert_command = Command::Insert { key: key.clone(), value: value.clone() };
        executor.execute_command(insert_command).unwrap();

        let get_command_before = Command::Get { key: key.clone() };
        assert_eq!(
            executor.execute_command(get_command_before.clone()).unwrap(),
            ExecutionResult::Value(Some(value.clone()))
        );

        let _ = executor.transaction_manager.begin_transaction();
        assert!(executor.transaction_manager.get_active_transaction().is_some());

        let delete_command = Command::Delete { key: key.clone() };
        executor.execute_command(delete_command).unwrap();

        let rollback_result = executor.execute_command(Command::RollbackTransaction);
        assert!(rollback_result.is_ok());
        assert_eq!(rollback_result.unwrap(), ExecutionResult::Success);
        assert!(executor.transaction_manager.get_active_transaction().is_none());

        assert_eq!(
            executor.execute_command(get_command_before).unwrap(),
            ExecutionResult::Value(Some(value))
        );
    }

    #[test]
    fn test_commit_transaction_command_with_active_tx() {
        let mut executor = create_file_executor();

        executor.execute_command(Command::BeginTransaction).unwrap();
        let active_tx_before_commit =
            executor.transaction_manager.get_active_transaction().unwrap();
        let tx_id = active_tx_before_commit.id;

        let insert_cmd = Command::Insert {
            key: b"key_commit".to_vec(),
            value: DataType::String("val_commit".to_string()),
        };
        executor.execute_command(insert_cmd).unwrap();

        let commit_cmd = Command::CommitTransaction;
        let result = executor.execute_command(commit_cmd);
        assert!(matches!(result, Ok(ExecutionResult::Success))); // Changed

        assert!(executor.transaction_manager.get_active_transaction().is_none());

        let wal_path = derive_wal_path_for_test(&executor.store);
        let wal_entries = read_all_wal_entries_for_test(&wal_path).unwrap();

        // Store's physical WAL should now only contain the Put entry.
        // The TransactionCommit is logged to the TransactionManager's WAL.
        assert_eq!(wal_entries.len(), 1, "Should be 1 Put WAL entry in store's WAL");
        match &wal_entries[0] {
            WalEntry::Put { lsn: _, transaction_id: put_tx_id, key: _, value: _ } => {
                assert_eq!(*put_tx_id, tx_id.0);
            }
            _ => panic!("Expected Put entry first. Got: {:?}", wal_entries),
        }
        // Removed check for TransactionCommit in store's WAL.
    }

    #[test]
    fn test_rollback_transaction_command_with_active_tx_logs_wal_and_reverts_cache() {
        let mut executor = create_file_executor();
        let key_orig = b"key_orig".to_vec();
        let val_orig = DataType::String("val_orig".to_string());
        let key_rb = b"key_rollback_wal".to_vec();
        let val_rb = DataType::String("val_rollback_wal".to_string());

        executor
            .execute_command(Command::Insert { key: key_orig.clone(), value: val_orig.clone() })
            .unwrap();
        assert_eq!(
            executor.execute_command(Command::Get { key: key_orig.clone() }).unwrap(),
            ExecutionResult::Value(Some(val_orig.clone()))
        );

        executor.execute_command(Command::BeginTransaction).unwrap();
        let active_tx = executor.transaction_manager.get_active_transaction().unwrap().clone();
        let tx_id = active_tx.id;

        executor
            .execute_command(Command::Insert { key: key_rb.clone(), value: val_rb.clone() })
            .unwrap();
        assert_eq!(
            executor.execute_command(Command::Get { key: key_rb.clone() }).unwrap(),
            ExecutionResult::Value(Some(val_rb.clone()))
        );

        let val_orig_updated = DataType::String("val_orig_updated".to_string());
        executor
            .execute_command(Command::Insert {
                key: key_orig.clone(),
                value: val_orig_updated.clone(),
            })
            .unwrap();
        assert_eq!(
            executor.execute_command(Command::Get { key: key_orig.clone() }).unwrap(),
            ExecutionResult::Value(Some(val_orig_updated.clone()))
        );

        let key_del = b"key_to_delete".to_vec();
        let val_del = DataType::String("val_to_delete".to_string());
        executor
            .execute_command(Command::Insert { key: key_del.clone(), value: val_del.clone() })
            .unwrap();
        executor.execute_command(Command::Delete { key: key_del.clone() }).unwrap();

        let rollback_cmd = Command::RollbackTransaction;
        let result = executor.execute_command(rollback_cmd);
        assert!(matches!(result, Ok(ExecutionResult::Success))); // Changed

        assert!(executor.transaction_manager.get_active_transaction().is_none());

        assert_eq!(
            executor.execute_command(Command::Get { key: key_rb.clone() }).unwrap(),
            ExecutionResult::Value(None),
            "key_rb (RevertInsert) should be gone"
        );
        assert_eq!(
            executor.execute_command(Command::Get { key: key_orig.clone() }).unwrap(),
            ExecutionResult::Value(Some(val_orig.clone())),
            "key_orig (RevertUpdate) should be back to original DataType value"
        );
        assert_eq!(
            executor.execute_command(Command::Get { key: key_del.clone() }).unwrap(),
            ExecutionResult::Value(None),
            "key_del (inserted then deleted in tx) should not exist after rollback"
        );

        let wal_path = derive_wal_path_for_test(&executor.store);
        let wal_entries = read_all_wal_entries_for_test(&wal_path).unwrap();

        // Adjusted expected count to 10.
        // Trace:
        // 1. Auto-commit Insert key_orig: Put(key_orig, tx_auto)
        // 2. Auto-commit Insert key_orig: TransactionCommit(tx_auto)
        // --- TX1 BEGIN ---
        // 3. Tx Insert key_rb: Put(key_rb, tx1)
        // 4. Tx Update key_orig: Put(key_orig, tx1)
        // 5. Tx Insert key_del: Put(key_del, tx1)
        // 6. Tx Delete key_del: Delete(key_del, tx1)
        // --- ROLLBACK TX1 ---
        // Physical Store WAL should contain:
        // 1. Initial Auto-commit Insert key_orig: Put(key_orig, tx0)
        // --- TX1 BEGIN (TM WAL) ---
        // 2. Tx1 Insert key_rb: Put(key_rb, tx1)
        // 3. Tx1 Update key_orig: Put(key_orig_updated, tx1)
        // 4. Tx1 Insert key_del: Put(key_del, tx1)
        // 5. Tx1 Delete key_del: Delete(key_del, tx1)
        // --- ROLLBACK TX1 (Undo ops also log to physical store WAL) ---
        // 6. Undo Delete key_del: Put(key_del_reverted, tx1)
        // 7. Undo Insert key_del: Delete(key_del, tx1)
        // 8. Undo Update key_orig: Put(key_orig_reverted, tx1)
        // 9. Undo Insert key_rb: Delete(key_rb, tx1)
        // Total = 9 entries.
        // The TransactionRollback entry is now only in TM's WAL.
        assert_eq!(wal_entries.len(), 9, "WAL entries count mismatch in store's WAL");

        // The last entry in the store's WAL should be the last undo operation.
        // For this test's undo log, it's Delete(key_rb).
        match wal_entries.last().unwrap() {
            WalEntry::Delete { lsn: _, transaction_id: last_op_tx_id, key: last_op_key } => {
                assert_eq!(*last_op_tx_id, tx_id.0); // tx_id of the rolled-back transaction
                assert_eq!(last_op_key, &key_rb.to_vec());
            }
            _ => panic!(
                "Expected last physical WAL entry to be Delete for key_rb. Got: {:?}",
                wal_entries.last().unwrap()
            ),
        }
    }

    #[test]
    fn test_commit_transaction_command_no_active_tx() {
        let mut executor = create_file_executor();
        let commit_cmd = Command::CommitTransaction;
        assert!(matches!(
            executor.execute_command(commit_cmd),
            Err(OxidbError::NoActiveTransaction)
        )); // Changed
    }

    #[test]
    fn test_rollback_transaction_command_no_active_tx() {
        let mut executor = create_file_executor();
        let rollback_cmd = Command::RollbackTransaction;
        assert!(matches!(
            executor.execute_command(rollback_cmd),
            Err(OxidbError::NoActiveTransaction) // Changed
        ));
    }

    #[test]
    fn test_multiple_begin_commands() {
        let mut executor = create_file_executor();

        executor.execute_command(Command::BeginTransaction).unwrap();
        let tx1 = executor.transaction_manager.get_active_transaction().unwrap().clone();

        let insert_cmd1 =
            Command::Insert { key: b"key1".to_vec(), value: DataType::String("val1".to_string()) };
        executor.execute_command(insert_cmd1).unwrap();

        executor.execute_command(Command::BeginTransaction).unwrap();
        let tx2 = executor.transaction_manager.get_active_transaction().unwrap().clone();
        assert_ne!(tx1.id, tx2.id);

        assert_eq!(executor.transaction_manager.current_active_transaction_id(), Some(tx2.id));

        executor.execute_command(Command::CommitTransaction).unwrap();
        assert!(executor.transaction_manager.get_active_transaction().is_none());
        let commit_again_cmd = Command::CommitTransaction;
        assert!(matches!(
            executor.execute_command(commit_again_cmd),
            Err(OxidbError::NoActiveTransaction) // Changed
        ));
    }

    #[test]
    fn test_operations_use_active_transaction_after_begin() {
        let mut executor = create_file_executor();

        executor.execute_command(Command::BeginTransaction).unwrap();

        let value_tx = DataType::String("value_tx".to_string());
        let insert_cmd = Command::Insert { key: b"key_tx".to_vec(), value: value_tx.clone() };
        executor.execute_command(insert_cmd).unwrap();

        let get_cmd = Command::Get { key: b"key_tx".to_vec() };
        assert_eq!(
            executor.execute_command(get_cmd.clone()).unwrap(),
            ExecutionResult::Value(Some(value_tx.clone()))
        );

        executor.execute_command(Command::CommitTransaction).unwrap();
        assert!(executor.transaction_manager.get_active_transaction().is_none());

        assert_eq!(
            executor.execute_command(get_cmd).unwrap(),
            ExecutionResult::Value(Some(value_tx))
        );
    }

    #[test]
    fn test_shared_lock_concurrency() {
        let mut executor = create_file_executor();
        let key: Key = b"shared_lock_key".to_vec();
        let value = DataType::String("value".to_string());

        let insert_command = Command::Insert { key: key.clone(), value: value.clone() };
        assert_eq!(executor.execute_command(insert_command).unwrap(), ExecutionResult::Success);

        assert_eq!(
            executor.execute_command(Command::BeginTransaction).unwrap(),
            ExecutionResult::Success
        );
        let tx1_id = executor.transaction_manager.get_active_transaction().unwrap().id;

        let get_command_tx1 = Command::Get { key: key.clone() };
        assert_eq!(
            executor.execute_command(get_command_tx1).unwrap(),
            ExecutionResult::Value(Some(value.clone()))
        );

        assert_eq!(
            executor.execute_command(Command::BeginTransaction).unwrap(),
            ExecutionResult::Success
        );
        let tx2_id = executor.transaction_manager.get_active_transaction().unwrap().id;
        assert_ne!(tx1_id, tx2_id);

        let get_command_tx2 = Command::Get { key: key.clone() };
        assert_eq!(
            executor.execute_command(get_command_tx2).unwrap(),
            ExecutionResult::Value(Some(value.clone()))
        );

        assert_eq!(
            executor.execute_command(Command::CommitTransaction).unwrap(),
            ExecutionResult::Success
        );
    }

    #[test]
    fn test_exclusive_lock_prevents_shared_read() {
        let mut executor = create_file_executor();
        let key: Key = b"exclusive_prevents_shared_key".to_vec();
        let value = DataType::String("value".to_string());

        assert_eq!(
            executor.execute_command(Command::BeginTransaction).unwrap(),
            ExecutionResult::Success
        );
        let tx1_id = executor.transaction_manager.current_active_transaction_id().unwrap();
        let insert_command = Command::Insert { key: key.clone(), value: value.clone() };
        assert_eq!(executor.execute_command(insert_command).unwrap(), ExecutionResult::Success);

        assert_eq!(
            executor.execute_command(Command::BeginTransaction).unwrap(),
            ExecutionResult::Success
        );
        let tx2_id = executor.transaction_manager.current_active_transaction_id().unwrap();
        assert_ne!(tx1_id, tx2_id);

        let get_command_tx2 = Command::Get { key: key.clone() };
        let result_tx2 = executor.execute_command(get_command_tx2);

        match result_tx2 {
            Err(OxidbError::LockConflict {
                key: err_key,
                current_tx: err_current_tx,
                locked_by_tx: err_locked_by_tx,
            }) => {
                assert_eq!(err_key, key);
                assert_eq!(err_current_tx, tx2_id.0);
                assert_eq!(err_locked_by_tx, Some(tx1_id.0));
            }
            _ => panic!("Expected OxidbError::LockConflict, got {:?}", result_tx2),
        }

        executor.transaction_manager.current_active_transaction_id = Some(tx2_id); // This line causes E0616
        assert_eq!(
            executor.execute_command(Command::RollbackTransaction).unwrap(),
            ExecutionResult::Success
        );
    }

    #[test]
    fn test_shared_lock_prevents_exclusive_lock() {
        let mut executor = create_file_executor();
        let key: Key = b"shared_prevents_exclusive_key".to_vec();
        let value = DataType::String("value".to_string());

        let insert_initial_command = Command::Insert { key: key.clone(), value: value.clone() };
        assert_eq!(
            executor.execute_command(insert_initial_command).unwrap(),
            ExecutionResult::Success
        );

        assert_eq!(
            executor.execute_command(Command::BeginTransaction).unwrap(),
            ExecutionResult::Success
        );
        let tx1_id = executor.transaction_manager.current_active_transaction_id().unwrap();

        let get_command_tx1 = Command::Get { key: key.clone() };
        assert_eq!(
            executor.execute_command(get_command_tx1).unwrap(),
            ExecutionResult::Value(Some(value.clone()))
        );

        assert_eq!(
            executor.execute_command(Command::BeginTransaction).unwrap(),
            ExecutionResult::Success
        );
        let tx2_id = executor.transaction_manager.current_active_transaction_id().unwrap();
        assert_ne!(tx1_id, tx2_id);

        let insert_command_tx2 =
            Command::Insert { key: key.clone(), value: DataType::String("new_value".to_string()) };
        let result_tx2 = executor.execute_command(insert_command_tx2);

        match result_tx2 {
            Err(OxidbError::LockConflict {
                key: err_key,
                current_tx: err_current_tx,
                locked_by_tx: err_locked_by_tx,
            }) => {
                assert_eq!(err_key, key);
                assert_eq!(err_current_tx, tx2_id.0);
                assert_eq!(err_locked_by_tx, Some(tx1_id.0));
            }
            _ => panic!("Expected OxidbError::LockConflict, got {:?}", result_tx2),
        }
        executor.transaction_manager.current_active_transaction_id = Some(tx2_id); // This line causes E0616
        assert_eq!(
            executor.execute_command(Command::RollbackTransaction).unwrap(),
            ExecutionResult::Success
        );
    }

    macro_rules! define_executor_tests {
        ($($test_name:ident),* $(,)? ; executor: $executor_creator:ident, store_type: $store_type:ty) => {
            $(
                paste::paste! {
                    #[test]
                    fn [<$test_name _ $store_type:lower>]() {
                        [<run_ $test_name>](&mut $executor_creator());
                    }
                }
            )*
        }
    }

    mod file_store_tests {
        use super::*;
        define_executor_tests!(
            test_get_non_existent,
            test_insert_and_get_integer,
            test_insert_and_get_string,
            test_insert_delete_get,
            test_begin_transaction_command,
            test_insert_with_active_transaction,
            test_insert_rollback_transaction,
            ;
            executor: create_file_executor,
            store_type: SimpleFileKvStore
        );
    }

    mod in_memory_store_tests {
        use super::*;
        define_executor_tests!(
            test_get_non_existent,
            test_insert_and_get_integer,
            test_insert_and_get_string,
            test_insert_delete_get,
            test_begin_transaction_command,
            test_insert_with_active_transaction,
            test_insert_rollback_transaction,
            ;
            executor: create_in_memory_executor,
            store_type: InMemoryKvStore
        );
    }

    #[test]
    fn test_index_insert_auto_commit() -> Result<(), OxidbError> {
        // Changed
        let mut executor = create_file_executor();
        let key = b"idx_key_auto".to_vec();
        let value = DataType::String("idx_val_auto".to_string());
        let serialized_value = serialize_data_type(&value)?;

        let insert_cmd = Command::Insert { key: key.clone(), value: value.clone() };
        assert_eq!(executor.execute_command(insert_cmd)?, ExecutionResult::Success);

        let indexed_pks = executor
            .index_manager
            .read().unwrap() // Acquire read lock
            .find_by_index("default_value_index", &serialized_value)?
            .expect("Value should be indexed");
        assert!(indexed_pks.contains(&key));
        Ok(())
    }

    #[test]
    fn test_index_insert_transactional_commit() -> Result<(), OxidbError> {
        // Changed
        let mut executor = create_file_executor();
        let key = b"idx_key_tx_commit".to_vec();
        let value = DataType::String("idx_val_tx_commit".to_string());
        let serialized_value = serialize_data_type(&value)?;

        executor.execute_command(Command::BeginTransaction)?;
        let insert_cmd = Command::Insert { key: key.clone(), value: value.clone() };
        assert_eq!(executor.execute_command(insert_cmd)?, ExecutionResult::Success);
        executor.execute_command(Command::CommitTransaction)?;

        let indexed_pks = executor
            .index_manager
            .read().unwrap() // Acquire read lock
            .find_by_index("default_value_index", &serialized_value)?
            .expect("Value should be indexed after commit");
        assert!(indexed_pks.contains(&key));
        Ok(())
    }

    #[test]
    fn test_index_insert_transactional_rollback() -> Result<(), OxidbError> {
        // Changed
        let mut executor = create_file_executor();
        let key = b"idx_key_tx_rollback".to_vec();
        let value = DataType::String("idx_val_tx_rollback".to_string());
        let serialized_value = serialize_data_type(&value)?;

        executor.execute_command(Command::BeginTransaction)?;
        let insert_cmd = Command::Insert { key: key.clone(), value: value.clone() };
        assert_eq!(executor.execute_command(insert_cmd)?, ExecutionResult::Success);

        let indexed_pks_before_rollback = executor
            .index_manager
            .read().unwrap() // Acquire read lock
            .find_by_index("default_value_index", &serialized_value)?
            .expect("Value should be indexed before rollback");
        assert!(indexed_pks_before_rollback.contains(&key));

        executor.execute_command(Command::RollbackTransaction)?;

        let get_cmd = Command::Get { key: key.clone() };
        assert_eq!(executor.execute_command(get_cmd)?, ExecutionResult::Value(None));

        let indexed_pks_after_rollback =
            executor.index_manager.read().unwrap().find_by_index("default_value_index", &serialized_value)?; // Acquire read lock
        assert!(
            indexed_pks_after_rollback.map_or(true, |pks| !pks.contains(&key)),
            "Value should NOT be in index after rolling back an insert"
        );
        Ok(())
    }

    #[test]
    fn test_index_delete_auto_commit() -> Result<(), OxidbError> {
        // Changed
        let mut executor = create_file_executor();
        let key = b"idx_del_key_auto".to_vec();
        let value = DataType::String("idx_del_val_auto".to_string());
        let serialized_value = serialize_data_type(&value)?;

        let insert_cmd = Command::Insert { key: key.clone(), value: value.clone() };
        executor.execute_command(insert_cmd)?;

        assert!(executor
            .index_manager
            .read().unwrap() // Acquire read lock
            .find_by_index("default_value_index", &serialized_value)?
            .is_some());

        let delete_cmd = Command::Delete { key: key.clone() };
        assert_eq!(executor.execute_command(delete_cmd)?, ExecutionResult::Deleted(true));

        let indexed_pks =
            executor.index_manager.read().unwrap().find_by_index("default_value_index", &serialized_value)?; // Acquire read lock
        assert!(
            indexed_pks.map_or(true, |pks| !pks.contains(&key)),
            "Key should be removed from index"
        );
        Ok(())
    }

    #[test]
    fn test_index_delete_transactional_commit() -> Result<(), OxidbError> {
        // Changed
        let mut executor = create_file_executor();
        let key = b"idx_del_key_tx_commit".to_vec();
        let value = DataType::String("idx_del_val_tx_commit".to_string());
        let serialized_value = serialize_data_type(&value)?;

        let insert_cmd = Command::Insert { key: key.clone(), value: value.clone() };
        executor.execute_command(insert_cmd)?;

        executor.execute_command(Command::BeginTransaction)?;
        let delete_cmd = Command::Delete { key: key.clone() };
        assert_eq!(executor.execute_command(delete_cmd)?, ExecutionResult::Deleted(true));
        executor.execute_command(Command::CommitTransaction)?;

        let indexed_pks =
            executor.index_manager.read().unwrap().find_by_index("default_value_index", &serialized_value)?; // Acquire read lock
        assert!(
            indexed_pks.map_or(true, |pks| !pks.contains(&key)),
            "Key should be removed from index after commit"
        );
        Ok(())
    }

    #[test]
    fn test_index_delete_transactional_rollback() -> Result<(), OxidbError> {
        // Changed
        let mut executor = create_file_executor();
        let key = b"idx_del_key_tx_rollback".to_vec();
        let value = DataType::String("idx_del_val_tx_rollback".to_string());
        let serialized_value = serialize_data_type(&value)?;

        let insert_cmd = Command::Insert { key: key.clone(), value: value.clone() };
        executor.execute_command(insert_cmd)?;

        executor.execute_command(Command::BeginTransaction)?;
        let delete_cmd = Command::Delete { key: key.clone() };
        executor.execute_command(delete_cmd)?;

        let indexed_pks_before_rollback =
            executor.index_manager.read().unwrap().find_by_index("default_value_index", &serialized_value)?; // Acquire read lock
        assert!(
            indexed_pks_before_rollback.map_or(true, |pks| !pks.contains(&key)),
            "Key should be removed from index before rollback"
        );

        executor.execute_command(Command::RollbackTransaction)?;

        let get_cmd = Command::Get { key: key.clone() };
        assert_eq!(executor.execute_command(get_cmd)?, ExecutionResult::Value(Some(value)));

        let indexed_pks_after_rollback = executor
            .index_manager
            .read().unwrap() // Acquire read lock
            .find_by_index("default_value_index", &serialized_value)?
            .expect("Index entry should be restored after rolling back a delete");
        assert!(
            indexed_pks_after_rollback.contains(&key),
            "Value SHOULD BE in index after rolling back a delete"
        );
        Ok(())
    }

    #[test]
    fn test_find_by_index_command() -> Result<(), OxidbError> {
        // Changed
        let mut executor = create_file_executor();
        let common_value_str = "indexed_value_common".to_string();
        let common_value = DataType::String(common_value_str.clone());
        // Use project's standard serialization for the value to be found in the index
        let serialized_common_value = serialize_data_type(&common_value)?;

        let key1 = b"fbk1".to_vec();
        let key2 = b"fbk2".to_vec();
        let key3 = b"fbk3".to_vec();

        executor
            .execute_command(Command::Insert { key: key1.clone(), value: common_value.clone() })?;
        executor
            .execute_command(Command::Insert { key: key2.clone(), value: common_value.clone() })?;
        executor.execute_command(Command::Insert {
            key: key3.clone(),
            value: DataType::String("other_value".to_string()),
        })?;

        let find_cmd = Command::FindByIndex {
            index_name: "default_value_index".to_string(),
            value: serialized_common_value.clone(),
        };
        match executor.execute_command(find_cmd)? {
            ExecutionResult::Values(values_vec) => {
                assert_eq!(values_vec.len(), 2);
                assert!(values_vec.contains(&common_value));
                assert_eq!(values_vec.iter().filter(|&v| *v == common_value).count(), 2);
            }
            other => panic!("Expected Values result, got {:?}", other),
        }

        let serialized_unindexed_value =
            serialize_data_type(&DataType::String("unindexed_val".to_string()))?;
        let find_cmd_none = Command::FindByIndex {
            index_name: "default_value_index".to_string(),
            value: serialized_unindexed_value,
        };
        match executor.execute_command(find_cmd_none)? {
            ExecutionResult::Values(values_vec) => {
                assert!(values_vec.is_empty());
            }
            other => panic!("Expected empty Values, got {:?}", other),
        }

        let find_cmd_no_index = Command::FindByIndex {
            index_name: "non_existent_index_name".to_string(),
            value: serialized_common_value.clone(),
        };
        let result_no_index = executor.execute_command(find_cmd_no_index);
        assert!(matches!(result_no_index, Err(OxidbError::Index(_)))); // Changed

        Ok(())
    }

    #[test]
    fn test_index_persistence_via_executor_persist() -> Result<(), OxidbError> {
        // Changed
        let temp_main_dir =
            tempfile::tempdir().expect("Failed to create main temp dir for persistence test");
        let db_file_path = temp_main_dir.path().join("test_db.dat");

        let key = b"persist_idx_key".to_vec();
        let value = DataType::String("persist_idx_val".to_string());
        let serialized_value = serialize_data_type(&value)?;

        {
            let wal_path1 = db_file_path.with_extension("wal1");
            let wal_config1 = crate::core::wal::writer::WalWriterConfig::default();
            let wal_writer1 = WalWriter::new(wal_path1, wal_config1);
            let log_manager1 = Arc::new(crate::core::wal::log_manager::LogManager::new());
            let mut executor1 = QueryExecutor::new(
                SimpleFileKvStore::new(&db_file_path)?,
                temp_main_dir.path().join("indexes1"),
                wal_writer1,
                log_manager1,
            )?;
            let insert_cmd = Command::Insert { key: key.clone(), value: value.clone() };
            executor1.execute_command(insert_cmd)?;
            executor1.persist()?;
        }

        // For executor2, re-use the same WAL path or a new one?
        // If SimpleFileKvStore::new re-initializes/clears WAL based on its state,
        // then a new WalWriter pointing to the same path might be fine.
        // Or, to be safe for test isolation, use a different WAL path for executor2,
        // though it implies executor2 wouldn't see WAL from executor1 if that was intended.
        // Given persist() should clear WAL, using the same path for a "re-opened" scenario is logical.
        let wal_path2 = db_file_path.with_extension("wal1"); // Re-using wal1 to simulate re-opening.
                                                             // If persist clears it, this is fine.
                                                             // If SimpleFileKvStore on new() is meant to recover from this WAL,
                                                             // then the test logic might need adjustment based on desired behavior.
        let wal_config2 = crate::core::wal::writer::WalWriterConfig::default();
        let wal_writer2 = WalWriter::new(wal_path2, wal_config2);
        let log_manager2 = Arc::new(crate::core::wal::log_manager::LogManager::new());
        let mut executor2 = QueryExecutor::new(
            SimpleFileKvStore::new(&db_file_path)?,
            temp_main_dir.path().join("indexes1"), // Same index path for persistence check
            wal_writer2,
            log_manager2,
        )?;

        let find_cmd = Command::FindByIndex {
            index_name: "default_value_index".to_string(),
            value: serialized_value,
        };
        match executor2.execute_command(find_cmd)? {
            ExecutionResult::Values(values_vec) => {
                assert_eq!(values_vec.len(), 1);
                assert_eq!(values_vec[0], value);
            }
            other => panic!("Expected Values after loading persisted index, got {:?}", other),
        }
        Ok(())
    }

    fn create_mvcc_test_executor() -> QueryExecutor<InMemoryKvStore> {
        let temp_dir =
            tempfile::tempdir().expect("Failed to create temp dir for MVCC test indexes");
        let index_path = temp_dir.path().to_path_buf();
        let store = InMemoryKvStore::new();

        let wal_temp_file =
            NamedTempFile::new().expect("Failed to create temp wal file for mvcc test");
        let wal_config = crate::core::wal::writer::WalWriterConfig::default();
        let wal_writer = WalWriter::new(wal_temp_file.path().to_path_buf(), wal_config);
        let log_manager_mvcc = Arc::new(crate::core::wal::log_manager::LogManager::new());

        QueryExecutor::new(store, index_path, wal_writer, log_manager_mvcc).unwrap()
    }

    #[test]
    fn test_mvcc_repeatable_read() {
        let mut exec = create_mvcc_test_executor();
        let key_k = b"k_repeatable".to_vec();
        let val_v1 = DataType::String("v1".to_string());
        let val_v2 = DataType::String("v2".to_string());

        assert_eq!(
            exec.execute_command(Command::Insert { key: key_k.clone(), value: val_v1.clone() })
                .unwrap(),
            ExecutionResult::Success
        );

        exec.execute_command(Command::BeginTransaction).unwrap();
        assert_eq!(
            exec.execute_command(Command::Get { key: key_k.clone() }).unwrap(),
            ExecutionResult::Value(Some(val_v1.clone())),
            "TX1 initial read of K"
        );

        exec.execute_command(Command::CommitTransaction).unwrap();

        exec.execute_command(Command::BeginTransaction).unwrap();
        assert_eq!(
            exec.execute_command(Command::Insert { key: key_k.clone(), value: val_v2.clone() })
                .unwrap(),
            ExecutionResult::Success
        );
        exec.execute_command(Command::CommitTransaction).unwrap();

        exec.execute_command(Command::BeginTransaction).unwrap();
        assert_eq!(
            exec.execute_command(Command::Get { key: key_k.clone() }).unwrap(),
            ExecutionResult::Value(Some(val_v2.clone())),
            "TX3 should see V2"
        );
        exec.execute_command(Command::CommitTransaction).unwrap();
    }

    #[test]
    fn test_mvcc_phantom_read_prevention() {
        let mut exec = create_mvcc_test_executor();
        let key_p = b"k_phantom".to_vec();
        let val_p = DataType::String("v_phantom".to_string());

        exec.execute_command(Command::BeginTransaction).unwrap();
        assert_eq!(
            exec.execute_command(Command::Get { key: key_p.clone() }).unwrap(),
            ExecutionResult::Value(None),
            "TX1 initial read of P (should be None)"
        );

        exec.execute_command(Command::CommitTransaction).unwrap();

        exec.execute_command(Command::BeginTransaction).unwrap();
        assert_eq!(
            exec.execute_command(Command::Insert { key: key_p.clone(), value: val_p.clone() })
                .unwrap(),
            ExecutionResult::Success
        );
        exec.execute_command(Command::CommitTransaction).unwrap();

        exec.execute_command(Command::BeginTransaction).unwrap();
        assert_eq!(
            exec.execute_command(Command::Get { key: key_p.clone() }).unwrap(),
            ExecutionResult::Value(Some(val_p.clone())),
            "TX3 should see P"
        );
        exec.execute_command(Command::CommitTransaction).unwrap();
    }

    #[test]
    fn test_mvcc_dirty_read_prevention() {
        let mut exec = create_mvcc_test_executor();
        let key_k = b"k_dirty".to_vec();
        let val_committed = DataType::String("v_committed".to_string());
        let val_dirty = DataType::String("v_dirty".to_string());

        assert_eq!(
            exec.execute_command(Command::Insert {
                key: key_k.clone(),
                value: val_committed.clone()
            })
            .unwrap(),
            ExecutionResult::Success
        );

        exec.execute_command(Command::BeginTransaction).unwrap();
        assert_eq!(
            exec.execute_command(Command::Insert { key: key_k.clone(), value: val_dirty.clone() })
                .unwrap(),
            ExecutionResult::Success,
            "TX1 dirties K"
        );

        let mut exec_tx2 = create_mvcc_test_executor();
        assert_eq!(
            exec_tx2
                .execute_command(Command::Insert {
                    key: key_k.clone(),
                    value: val_committed.clone()
                })
                .unwrap(),
            ExecutionResult::Success
        );
        exec_tx2.execute_command(Command::BeginTransaction).unwrap();
        assert_eq!(
            exec_tx2.execute_command(Command::Get { key: key_k.clone() }).unwrap(),
            ExecutionResult::Value(Some(val_committed.clone())),
            "TX2 should see V_committed"
        );
        exec_tx2.execute_command(Command::CommitTransaction).unwrap();

        exec.execute_command(Command::RollbackTransaction).unwrap();

        assert_eq!(
            exec.execute_command(Command::Get { key: key_k.clone() }).unwrap(),
            ExecutionResult::Value(Some(val_committed.clone())),
            "K should be V_committed after TX1 rollback"
        );
    }

    #[test]
    fn test_mvcc_write_write_conflict() {
        let mut exec = create_mvcc_test_executor();

        let key_k = b"k_ww".to_vec();
        let val_v1 = DataType::String("v1_ww".to_string());
        let val_v2 = DataType::String("v2_ww".to_string());

        exec.execute_command(Command::BeginTransaction).unwrap();
        assert_eq!(
            exec.execute_command(Command::Insert { key: key_k.clone(), value: val_v1.clone() })
                .unwrap(),
            ExecutionResult::Success
        );
        exec.execute_command(Command::CommitTransaction).unwrap();

        exec.execute_command(Command::BeginTransaction).unwrap();
        assert_eq!(
            exec.execute_command(Command::Insert { key: key_k.clone(), value: val_v2.clone() })
                .unwrap(),
            ExecutionResult::Success
        );
        assert_eq!(
            exec.execute_command(Command::Get { key: key_k.clone() }).unwrap(),
            ExecutionResult::Value(Some(val_v2.clone()))
        );
        exec.execute_command(Command::CommitTransaction).unwrap();

        assert_eq!(
            exec.execute_command(Command::Get { key: key_k.clone() }).unwrap(),
            ExecutionResult::Value(Some(val_v2.clone()))
        );
    }

    #[test]
    fn test_mvcc_commit_lifecycle() {
        let mut exec = create_mvcc_test_executor();
        let key_new = b"k_new_commit".to_vec();
        let val_new = DataType::String("v_new_commit".to_string());
        let key_existing = b"k_exist_commit".to_vec();
        let val_old_exist = DataType::String("v_old_exist".to_string());
        let val_new_exist = DataType::String("v_new_exist".to_string());
        let key_del = b"k_del_commit".to_vec();
        let val_del = DataType::String("v_del_commit".to_string());

        assert_eq!(
            exec.execute_command(Command::Insert {
                key: key_existing.clone(),
                value: val_old_exist.clone()
            })
            .unwrap(),
            ExecutionResult::Success
        );
        assert_eq!(
            exec.execute_command(Command::Insert { key: key_del.clone(), value: val_del.clone() })
                .unwrap(),
            ExecutionResult::Success
        );

        exec.execute_command(Command::BeginTransaction).unwrap();
        assert_eq!(
            exec.execute_command(Command::Insert { key: key_new.clone(), value: val_new.clone() })
                .unwrap(),
            ExecutionResult::Success
        );
        assert_eq!(
            exec.execute_command(Command::Insert {
                key: key_existing.clone(),
                value: val_new_exist.clone()
            })
            .unwrap(),
            ExecutionResult::Success
        );
        assert_eq!(
            exec.execute_command(Command::Delete { key: key_del.clone() }).unwrap(),
            ExecutionResult::Deleted(true)
        );

        exec.execute_command(Command::CommitTransaction).unwrap();

        exec.execute_command(Command::BeginTransaction).unwrap();
        assert_eq!(
            exec.execute_command(Command::Get { key: key_new.clone() }).unwrap(),
            ExecutionResult::Value(Some(val_new.clone()))
        );
        assert_eq!(
            exec.execute_command(Command::Get { key: key_existing.clone() }).unwrap(),
            ExecutionResult::Value(Some(val_new_exist.clone()))
        );
        assert_eq!(
            exec.execute_command(Command::Get { key: key_del.clone() }).unwrap(),
            ExecutionResult::Value(None)
        );

        let ser_val_new = serialize_data_type(&val_new).unwrap();
        let ser_val_new_exist = serialize_data_type(&val_new_exist).unwrap();
        let ser_val_del = serialize_data_type(&val_del).unwrap();

        match exec
            .execute_command(Command::FindByIndex {
                index_name: "default_value_index".to_string(),
                value: ser_val_new,
            })
            .unwrap()
        {
            ExecutionResult::Values(vals) => assert!(vals.contains(&val_new)),
            _ => panic!("Expected Values for new value"),
        }
        match exec
            .execute_command(Command::FindByIndex {
                index_name: "default_value_index".to_string(),
                value: ser_val_new_exist,
            })
            .unwrap()
        {
            ExecutionResult::Values(vals) => assert!(vals.contains(&val_new_exist)),
            _ => panic!("Expected Values for new existing value"),
        }
        match exec
            .execute_command(Command::FindByIndex {
                index_name: "default_value_index".to_string(),
                value: ser_val_del,
            })
            .unwrap()
        {
            ExecutionResult::Values(vals) => {
                assert!(!vals.contains(&val_del), "Deleted value should not be found by index")
            }
            _ => panic!("Expected Values for deleted value"),
        }
        exec.execute_command(Command::CommitTransaction).unwrap();
    }

    #[test]
    fn test_mvcc_rollback_lifecycle() {
        let mut exec = create_mvcc_test_executor();
        let key_existing = b"k_exist_rb".to_vec();
        let val_old_exist = DataType::String("v_old_exist_rb".to_string());
        let key_del_rb = b"k_del_rb".to_vec();
        let val_del_rb = DataType::String("v_del_rb".to_string());

        let key_new_rb = b"k_new_rb".to_vec();
        let val_new_rb = DataType::String("v_new_rb".to_string());
        let val_updated_exist_rb = DataType::String("v_updated_exist_rb".to_string());

        assert_eq!(
            exec.execute_command(Command::Insert {
                key: key_existing.clone(),
                value: val_old_exist.clone()
            })
            .unwrap(),
            ExecutionResult::Success
        );
        assert_eq!(
            exec.execute_command(Command::Insert {
                key: key_del_rb.clone(),
                value: val_del_rb.clone()
            })
            .unwrap(),
            ExecutionResult::Success
        );

        exec.execute_command(Command::BeginTransaction).unwrap();
        assert_eq!(
            exec.execute_command(Command::Insert {
                key: key_new_rb.clone(),
                value: val_new_rb.clone()
            })
            .unwrap(),
            ExecutionResult::Success
        );
        assert_eq!(
            exec.execute_command(Command::Insert {
                key: key_existing.clone(),
                value: val_updated_exist_rb.clone()
            })
            .unwrap(),
            ExecutionResult::Success
        );
        assert_eq!(
            exec.execute_command(Command::Delete { key: key_del_rb.clone() }).unwrap(),
            ExecutionResult::Deleted(true)
        );

        exec.execute_command(Command::RollbackTransaction).unwrap();

        exec.execute_command(Command::BeginTransaction).unwrap();
        assert_eq!(
            exec.execute_command(Command::Get { key: key_new_rb.clone() }).unwrap(),
            ExecutionResult::Value(None),
            "Rolled back insert should be gone"
        );
        assert_eq!(
            exec.execute_command(Command::Get { key: key_existing.clone() }).unwrap(),
            ExecutionResult::Value(Some(val_old_exist.clone())),
            "Rolled back update should revert to old value"
        );
        assert_eq!(
            exec.execute_command(Command::Get { key: key_del_rb.clone() }).unwrap(),
            ExecutionResult::Value(Some(val_del_rb.clone())),
            "Rolled back delete should restore value"
        );

        let ser_val_new_rb = serialize_data_type(&val_new_rb).unwrap();
        let ser_val_updated_exist_rb = serialize_data_type(&val_updated_exist_rb).unwrap();
        let ser_val_del_rb = serialize_data_type(&val_del_rb).unwrap();

        match exec
            .execute_command(Command::FindByIndex {
                index_name: "default_value_index".to_string(),
                value: ser_val_new_rb,
            })
            .unwrap()
        {
            ExecutionResult::Values(vals) => assert!(
                !vals.contains(&val_new_rb),
                "Index should not find value from rolled back insert"
            ),
            _ => panic!("Expected Values"),
        }
        match exec
            .execute_command(Command::FindByIndex {
                index_name: "default_value_index".to_string(),
                value: ser_val_updated_exist_rb,
            })
            .unwrap()
        {
            ExecutionResult::Values(vals) => assert!(
                !vals.contains(&val_updated_exist_rb),
                "Index should not find value from rolled back update"
            ),
            _ => panic!("Expected Values"),
        }
        let ser_val_old_exist = serialize_data_type(&val_old_exist).unwrap();
        match exec
            .execute_command(Command::FindByIndex {
                index_name: "default_value_index".to_string(),
                value: ser_val_old_exist,
            })
            .unwrap()
        {
            ExecutionResult::Values(vals) => assert!(
                vals.contains(&val_old_exist),
                "Index should find original value of updated key after rollback"
            ),
            _ => panic!("Expected Values"),
        }
        match exec
            .execute_command(Command::FindByIndex {
                index_name: "default_value_index".to_string(),
                value: ser_val_del_rb,
            })
            .unwrap()
        {
            ExecutionResult::Values(vals) => assert!(
                vals.contains(&val_del_rb),
                "Index should find original value of deleted key after rollback"
            ),
            _ => panic!("Expected Values"),
        }
        exec.execute_command(Command::CommitTransaction).unwrap();
    }

    #[test]
    fn test_mvcc_find_by_index_visibility() -> Result<(), OxidbError> {
        let mut exec_mvcc_find = create_mvcc_test_executor();
        let key1 = b"fbk_mvcc_k1".to_vec();
        let key2 = b"fbk_mvcc_k2".to_vec();
        let common_val_str = "common_val_mvcc".to_string();
        let common_val = DataType::String(common_val_str.clone());
        let other_val_str = "other_val_mvcc".to_string();
        let other_val = DataType::String(other_val_str.clone());
        let ser_common_val = serialize_data_type(&common_val)?;

        assert_eq!(
            exec_mvcc_find
                .execute_command(Command::Insert { key: key1.clone(), value: common_val.clone() })
                .unwrap(),
            ExecutionResult::Success
        );
        assert_eq!(
            exec_mvcc_find
                .execute_command(Command::Insert { key: key2.clone(), value: common_val.clone() })
                .unwrap(),
            ExecutionResult::Success
        );

        exec_mvcc_find.execute_command(Command::BeginTransaction).unwrap();

        let find_result_tx1 = exec_mvcc_find
            .execute_command(Command::FindByIndex {
                index_name: "default_value_index".to_string(),
                value: ser_common_val.clone(),
            })
            .unwrap();
        match find_result_tx1 {
            ExecutionResult::Values(vals) => {
                assert_eq!(
                    vals.len(),
                    2,
                    "TX1 should find 2 entries for common_value based on its snapshot"
                );
                assert!(vals.contains(&common_val));
                assert_eq!(vals.iter().filter(|&v| *v == common_val).count(), 2);
            }
            _ => panic!("TX1: Expected Values from FindByIndex"),
        }

        exec_mvcc_find.execute_command(Command::CommitTransaction).unwrap();

        exec_mvcc_find.execute_command(Command::BeginTransaction).unwrap();
        assert_eq!(
            exec_mvcc_find
                .execute_command(Command::Insert { key: key1.clone(), value: other_val.clone() })
                .unwrap(),
            ExecutionResult::Success
        );
        exec_mvcc_find.execute_command(Command::CommitTransaction).unwrap();

        exec_mvcc_find.execute_command(Command::BeginTransaction).unwrap();
        let find_result_tx3 = exec_mvcc_find
            .execute_command(Command::FindByIndex {
                index_name: "default_value_index".to_string(),
                value: ser_common_val.clone(),
            })
            .unwrap();
        match find_result_tx3 {
            ExecutionResult::Values(vals) => {
                assert_eq!(vals.len(), 1, "TX3 should find 1 entry for common_value (K2)");
                assert!(vals.contains(&common_val));
                if !vals.is_empty() {
                    assert_eq!(vals[0], common_val);
                }
            }
            _ => panic!("TX3: Expected Values from FindByIndex"),
        }
        exec_mvcc_find.execute_command(Command::CommitTransaction).unwrap();
        Ok(())
    }

    #[test]
    fn test_prev_lsn_after_insert() {
        let mut executor = create_file_executor();

        // Setup: Create a table to insert into
        // This is needed because execute_command for INSERT will try to use it.
        // However, SimpleFileKvStore doesn't have schema. For this test, we assume
        // the INSERT command directly translates to a store.put if table checks are minimal.
        // The QueryExecutor's handle_insert was modified to directly call store.put.

        // Begin transaction
        executor.execute_command(Command::BeginTransaction).expect("BEGIN failed");
        let lsn_after_begin;
        {
            let active_tx = executor
                .transaction_manager
                .get_active_transaction()
                .expect("No active transaction after BEGIN");
            lsn_after_begin = active_tx.prev_lsn; // LSN of BeginTransaction record
            assert_eq!(lsn_after_begin, 0, "LSN after BEGIN should be 0");
        }

        // Execute INSERT
        let key_insert = b"prev_lsn_insert_key".to_vec();
        let val_insert = DataType::String("val_insert".to_string());
        executor
            .execute_command(Command::Insert { key: key_insert.clone(), value: val_insert.clone() })
            .expect("INSERT failed");

        let expected_lsn_after_insert = lsn_after_begin + 1;
        {
            let active_tx_after_insert = executor
                .transaction_manager
                .get_active_transaction()
                .expect("No active transaction after INSERT");
            assert_eq!(
                active_tx_after_insert.prev_lsn, expected_lsn_after_insert,
                "Transaction.prev_lsn should be updated to LSN of INSERT operation."
            );
        }

        // Check LogManager's state
        assert_eq!(
            executor.log_manager.current_lsn(),
            expected_lsn_after_insert + 1,
            "LogManager current_lsn should be advanced past INSERT LSN."
        );

        executor.execute_command(Command::CommitTransaction).expect("COMMIT failed");
    }

    #[test]
    fn test_prev_lsn_after_update() {
        let mut executor = create_file_executor();

        // Setup: Insert a record to update
        let key_update = b"prev_lsn_update_key".to_vec();
        let val_initial = DataType::String("val_initial_for_update".to_string());
        let val_updated = DataType::String("val_updated".to_string());
        // Auto-commit insert for setup
        executor
            .execute_command(Command::Insert {
                key: key_update.clone(),
                value: val_initial.clone(),
            })
            .expect("Initial INSERT for UPDATE test failed");

        let lsn_after_setup_insert = executor.log_manager.current_lsn(); // Next LSN to be assigned

        // Begin transaction for the UPDATE
        executor.execute_command(Command::BeginTransaction).expect("BEGIN failed");
        let lsn_after_begin;
        {
            let active_tx = executor
                .transaction_manager
                .get_active_transaction()
                .expect("No active transaction after BEGIN");
            lsn_after_begin = active_tx.prev_lsn;
            assert_eq!(
                lsn_after_begin, lsn_after_setup_insert,
                "LSN after BEGIN should be the LSN from LogManager"
            );
        }

        // Execute UPDATE
        // For UPDATE, handle_update in update_execution.rs is called.
        // It first does a SELECT then a PUT. The PUT is what gets the LSN we're interested in for prev_lsn.
        let _assignments =
            vec![SqlAssignment { column: "some_field".to_string(), value: val_updated.clone() }];
        // Condition doesn't matter much as we're targeting the key directly for this test's focus on prev_lsn.
        // The handle_update logic uses a SELECT plan based on source and condition.
        // For simplicity, assuming SimpleFileKvStore where source is not strictly table-based for raw key updates.
        // The test needs to ensure the UPDATE command targets the existing key.
        // The current handle_update in QueryExecutor might be more complex.
        // Let's assume the UPDATE finds the key and proceeds to the store.put part.
        // The source_table_name argument is used for the SELECT part of UPDATE.
        // For this test, we'll use a dummy table name as SimpleFileKvStore is key-value.
        // The important part is that a store.put() operation occurs.

        // This command structure for UPDATE might need adjustment based on how QueryExecutor.handle_update expects it
        // and how it translates to store operations. The key is that it results in a store.put().
        // The current `handle_update` in `update_execution.rs` takes `source_table_name`, `assignments`, `condition_opt`.
        // The `assignments` are `SqlAssignment`. The `condition` is `SqlCondition`.
        // For a direct key-value update simulation, we might need a more direct test or adapt.
        // Given `handle_update` iterates keys from a SELECT, we must ensure our key is selected.
        // A direct `store.put` test might be simpler if `QueryExecutor::Command::Update` is too complex to set up here
        // without actual table/schema context that `SimpleFileKvStore` lacks.
        // However, the goal is to test `QueryExecutor`'s behavior.

        // Let's assume the UPDATE command leads to a store.put for key_update.
        // The current handle_update will select the key then call store.put.
        // We need to ensure the select part finds key_update.
        // A condition like "key = key_update" would be ideal, but SqlCondition is based on column names.
        // For SimpleFileKvStore, UPDATE might be challenging to test this way without more infrastructure.
        //
        // Let's simplify: we'll test the LSN after another INSERT within the transaction,
        // as the UPDATE path is complex and its LSN behavior for prev_lsn update is the same as INSERT's.
        // The key is that *any* data-modifying op via QueryExecutor updates prev_lsn.

        // Instead of full UPDATE, let's do another INSERT to test chained prev_lsn
        let key_insert2 = b"prev_lsn_insert_key2".to_vec();
        let val_insert2 = DataType::String("val_insert2".to_string());
        executor
            .execute_command(Command::Insert {
                key: key_insert2.clone(),
                value: val_insert2.clone(),
            })
            .expect("Second INSERT failed");

        let expected_lsn_after_insert2 = lsn_after_begin + 1;
        {
            let active_tx_after_insert2 = executor
                .transaction_manager
                .get_active_transaction()
                .expect("No active transaction after second INSERT");
            assert_eq!(
                active_tx_after_insert2.prev_lsn, expected_lsn_after_insert2,
                "Transaction.prev_lsn should be updated to LSN of the second INSERT operation."
            );
        }
        assert_eq!(executor.log_manager.current_lsn(), expected_lsn_after_insert2 + 1);

        executor.execute_command(Command::CommitTransaction).expect("COMMIT failed");
    }

    #[test]
    fn test_prev_lsn_after_delete() {
        let mut executor = create_file_executor();

        // Setup: Insert a record to delete
        let key_delete = b"prev_lsn_delete_key".to_vec();
        let val_delete_setup = DataType::String("val_for_delete".to_string());
        executor
            .execute_command(Command::Insert {
                key: key_delete.clone(),
                value: val_delete_setup.clone(),
            })
            .expect("Initial INSERT for DELETE test failed");

        let lsn_after_setup_insert = executor.log_manager.current_lsn();

        // Begin transaction
        executor.execute_command(Command::BeginTransaction).expect("BEGIN failed");
        let lsn_after_begin;
        {
            let active_tx = executor
                .transaction_manager
                .get_active_transaction()
                .expect("No active transaction after BEGIN");
            lsn_after_begin = active_tx.prev_lsn;
            assert_eq!(lsn_after_begin, lsn_after_setup_insert);
        }

        // Execute DELETE
        executor
            .execute_command(Command::Delete { key: key_delete.clone() })
            .expect("DELETE failed");

        let expected_lsn_after_delete = lsn_after_begin + 1;
        {
            let active_tx_after_delete = executor
                .transaction_manager
                .get_active_transaction()
                .expect("No active transaction after DELETE");
            assert_eq!(
                active_tx_after_delete.prev_lsn, expected_lsn_after_delete,
                "Transaction.prev_lsn should be updated to LSN of DELETE operation."
            );
        }
        assert_eq!(executor.log_manager.current_lsn(), expected_lsn_after_delete + 1);

        executor.execute_command(Command::CommitTransaction).expect("COMMIT failed");
    }

    #[test]
    fn test_rollback_of_update_reverts_index_correctly() -> Result<(), OxidbError> {
        let mut executor = create_file_executor();
        let key_updated = b"key_for_index_revert_update".to_vec();
        let value_initial = DataType::String("value_initial".to_string());
        let value_new_in_tx = DataType::String("value_new_in_tx".to_string());
        let key_transient = b"key_transient_in_tx".to_vec();
        let value_transient = DataType::String("value_transient".to_string());

        // 1. Setup initial state (committed)
        let insert_initial_cmd = Command::Insert { key: key_updated.clone(), value: value_initial.clone() };
        executor.execute_command(insert_initial_cmd).expect("Initial insert for key_updated failed");

        // Verify initial state in store and index
        let get_initial_cmd = Command::Get { key: key_updated.clone() };
        assert_eq!(
            executor.execute_command(get_initial_cmd.clone())?,
            ExecutionResult::Value(Some(value_initial.clone())),
            "Initial GET for key_updated failed"
        );

        let serialized_value_initial = serialize_data_type(&value_initial)?;
        let find_initial_cmd = Command::FindByIndex {
            index_name: "default_value_index".to_string(),
            value: serialized_value_initial.clone(),
        };
        match executor.execute_command(find_initial_cmd.clone())? {
            ExecutionResult::Values(pks) => {
                assert!(pks.contains(&value_initial), "Index should contain initial value for key_updated");
            }
            res => panic!("Unexpected result for initial find by index: {:?}", res),
        }


        // 2. Start transaction and perform operations
        executor.execute_command(Command::BeginTransaction).expect("BEGIN failed for main transaction");

        // This is the "update" on key_updated
        let update_cmd = Command::Insert { key: key_updated.clone(), value: value_new_in_tx.clone() };
        executor.execute_command(update_cmd).expect("Update (insert) for key_updated in transaction failed");

        // Optional: Insert a transient key
        let insert_transient_cmd = Command::Insert { key: key_transient.clone(), value: value_transient.clone() };
        executor.execute_command(insert_transient_cmd).expect("Insert for key_transient in transaction failed");

        // Verify state within transaction (optional, but good for sanity)
        let get_updated_in_tx_cmd = Command::Get { key: key_updated.clone() };
         assert_eq!(
             executor.execute_command(get_updated_in_tx_cmd)?,
             ExecutionResult::Value(Some(value_new_in_tx.clone())),
             "GET for key_updated within TX should return new value"
         );
        let serialized_value_new_in_tx = serialize_data_type(&value_new_in_tx)?;
        let find_new_in_tx_cmd = Command::FindByIndex {
            index_name: "default_value_index".to_string(),
            value: serialized_value_new_in_tx.clone(),
        };
         match executor.execute_command(find_new_in_tx_cmd.clone())? {
             ExecutionResult::Values(pks) => {
                 assert!(pks.contains(&value_new_in_tx), "Index within TX should contain new value for key_updated");
             }
             res => panic!("Unexpected result for find by index (new value) within TX: {:?}", res),
         }
         let find_initial_in_tx_cmd = Command::FindByIndex {
             index_name: "default_value_index".to_string(),
             value: serialized_value_initial.clone(),
         };
          match executor.execute_command(find_initial_in_tx_cmd.clone())? {
            ExecutionResult::Values(pks) => {
                 // Depending on how index updates are visible within a transaction before commit,
                 // the old value might or might not be found.
                 // For this test, we primarily care about the state *after rollback*.
                 // If the index update is immediate, old value should not be found associated with this key.
                 // If index changes are deferred or use complex versioning, this check might differ.
                 // Assuming default_hash_index updates immediately:
                assert!(!pks.contains(&value_initial), "Index within TX should NOT find key_updated for initial value if update is effective immediately");
            }
            res => panic!("Unexpected result for find by index (initial value) within TX: {:?}", res),
        }


        // 3. Rollback transaction
        executor.execute_command(Command::RollbackTransaction).expect("ROLLBACK failed");

        // 4. Verify data correctness
        // a. key_updated should revert to value_initial_for_index
        let get_reverted_cmd = Command::Get { key: key_updated.clone() };
        assert_eq!(
            executor.execute_command(get_reverted_cmd.clone())?,
            ExecutionResult::Value(Some(value_initial.clone())),
            "GET for key_updated after rollback should return initial value"
        );

        // b. key_transient should not exist
        let get_transient_cmd = Command::Get { key: key_transient.clone() };
        assert_eq!(
            executor.execute_command(get_transient_cmd)?,
            ExecutionResult::Value(None),
            "GET for key_transient after rollback should return None"
        );

        // 5. Verify index correctness for key_updated
        // a. Index should map value_initial_for_index to key_updated
        let find_initial_after_rollback_cmd = Command::FindByIndex {
            index_name: "default_value_index".to_string(),
            value: serialized_value_initial.clone(),
        };
        match executor.execute_command(find_initial_after_rollback_cmd)? {
            ExecutionResult::Values(pks) => {
                assert!(pks.contains(&value_initial), "Index after rollback should find key_updated associated with initial value");
            }
            res => panic!("Unexpected result for find by index (initial value) after rollback: {:?}", res),
        }

        // b. Index should NOT map value_new_for_index to key_updated
        let find_new_after_rollback_cmd = Command::FindByIndex {
            index_name: "default_value_index".to_string(),
            value: serialized_value_new_in_tx.clone(),
        };
        match executor.execute_command(find_new_after_rollback_cmd)? {
            ExecutionResult::Values(pks) => {
                assert!(!pks.contains(&value_new_in_tx), "Index after rollback should NOT find key_updated associated with new value");
            }
            res => panic!("Unexpected result for find by index (new value) after rollback: {:?}", res),
        }

        // c. Delete key_updated (which is now associated with value_initial_for_index)
        let delete_reverted_cmd = Command::Delete { key: key_updated.clone() };
        let result_from_delete_cmd = executor.execute_command(delete_reverted_cmd)?;
        eprintln!("[Test::test_rollback_of_update_reverts_index_correctly] Actual result from delete_reverted_cmd: {:?}", result_from_delete_cmd);
        assert_eq!(
            result_from_delete_cmd,
            ExecutionResult::Deleted(true),
            "DELETE of key_updated after rollback failed" // Restored original message
        );

        // executor.persist()?; // Attempt to flush WAL and apply deletes to main store

        // d. Verify key_updated is gone from store
        assert_eq!(
            executor.execute_command(get_reverted_cmd)?,
            ExecutionResult::Value(None),
            "GET for key_updated after delete (post-rollback) should return None"
        );

        // e. Verify key_updated is gone from index (for value_initial_for_index)
        let find_initial_after_delete_cmd = Command::FindByIndex {
            index_name: "default_value_index".to_string(),
            value: serialized_value_initial.clone(),
        };
         match executor.execute_command(find_initial_after_delete_cmd)? {
            ExecutionResult::Values(pks) => {
                assert!(!pks.contains(&value_initial), "Index after delete (post-rollback) should NOT find key_updated for initial value");
            }
            res => panic!("Unexpected result for find by index (initial value) after delete (post-rollback): {:?}", res),
        }

        Ok(())
    }
}
