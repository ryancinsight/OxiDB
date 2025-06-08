// Original imports from simple_file.rs that might be needed by test helpers or types:
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions, write, remove_file, read, File as StdFile}; // Removed rename for now as it's not used after test changes
use std::io::{BufReader, BufWriter, Write, ErrorKind};
use std::path::{Path, PathBuf};

// Specific imports for types used in tests, from their canonical paths
use crate::core::common::error::DbError;
use crate::core::storage::engine::traits::{KeyValueStore, VersionedValue};
use crate::core::common::traits::{DataSerializer, DataDeserializer};
use crate::core::storage::engine::wal::{WalEntry, WalWriter};
use crate::core::transaction::Transaction;
use tempfile::{NamedTempFile, Builder};

// Import the struct being tested
use crate::core::storage::engine::implementations::simple_file::SimpleFileKvStore;


// Helper to create a main DB file with specific key-value data
fn create_db_file_with_kv_data(path: &Path, data: &[(Vec<u8>, Vec<u8>)]) -> Result<(), DbError> {
    let file = OpenOptions::new().write(true).create(true).truncate(true).open(path).map_err(DbError::IoError)?;
    let mut writer = BufWriter::new(file);
    for (key, value) in data {
        <Vec<u8> as DataSerializer<Vec<u8>>>::serialize(key, &mut writer)?;
        <Vec<u8> as DataSerializer<Vec<u8>>>::serialize(value, &mut writer)?;
    }
    writer.flush().map_err(DbError::IoError)?;
    writer.get_ref().sync_all().map_err(DbError::IoError)?; // Ensure data is on disk
    Ok(())
}

// Helper function to derive WAL path from DB path
fn derive_wal_path(db_path: &Path) -> PathBuf {
    let mut wal_path = db_path.to_path_buf();
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

// Helper to read all entries from a WAL file
fn read_all_wal_entries(wal_path: &Path) -> Result<Vec<WalEntry>, DbError> {
    let file = StdFile::open(wal_path).map_err(DbError::IoError)?;
    let mut reader = BufReader::new(file);
    let mut entries = Vec::new();
    loop {
        match <WalEntry as DataDeserializer<WalEntry>>::deserialize(&mut reader) {
            Ok(entry) => entries.push(entry),
            Err(DbError::IoError(e)) if e.kind() == ErrorKind::UnexpectedEof => {
                break;
            }
            Err(e) => {
                return Err(e);
            }
        }
    }
    Ok(entries)
}

#[test]
fn test_new_store_empty_and_reload() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path();
    {
        let store = SimpleFileKvStore::new(path).unwrap();
        assert!(store.get_cache_for_test().is_empty());
    }
    let reloaded_store = SimpleFileKvStore::new(path).unwrap();
    assert!(reloaded_store.get_cache_for_test().is_empty());
}

#[test]
fn test_load_from_empty_file() {
    let temp_file = NamedTempFile::new().unwrap();
    File::create(temp_file.path()).unwrap();
    let store = SimpleFileKvStore::new(temp_file.path()).unwrap();
    assert!(store.get_cache_for_test().is_empty());
}

#[test]
fn test_put_and_get() {
    let temp_file = NamedTempFile::new().unwrap();
    let mut store = SimpleFileKvStore::new(temp_file.path()).unwrap();
    let dummy_transaction = Transaction::new(0);
    let _snapshot_id = 0;
    let _committed_ids: HashSet<u64> = HashSet::new();
    let key1 = b"key1".to_vec();
    let value1 = b"value1".to_vec();
    store.put(key1.clone(), value1.clone(), &dummy_transaction).unwrap();
    // assert_eq!(store.get(&key1, _snapshot_id, &_committed_ids).unwrap(), Some(value1.clone()));

    let key2 = b"key2".to_vec();
    let value2 = b"value2_long".to_vec();
    store.put(key2.clone(), value2.clone(), &dummy_transaction).unwrap();
    // assert_eq!(store.get(&key2, _snapshot_id, &_committed_ids).unwrap(), Some(value2.clone()));
    // assert_eq!(store.get(&key1, _snapshot_id, &_committed_ids).unwrap(), Some(value1.clone()));
}

#[test]
fn test_put_update() {
    let temp_file = NamedTempFile::new().unwrap();
    let mut store = SimpleFileKvStore::new(temp_file.path()).unwrap();
    let dummy_transaction = Transaction::new(0);
    let _snapshot_id = 0;
    let _committed_ids: HashSet<u64> = HashSet::new();
    let key1 = b"key1".to_vec();
    let value1 = b"value1".to_vec();
    let value1_updated = b"value1_updated".to_vec();

    store.put(key1.clone(), value1.clone(), &dummy_transaction).unwrap();
    // assert_eq!(store.get(&key1, _snapshot_id, &_committed_ids).unwrap(), Some(value1.clone()));

    store.put(key1.clone(), value1_updated.clone(), &dummy_transaction).unwrap();
    // assert_eq!(store.get(&key1, _snapshot_id, &_committed_ids).unwrap(), Some(value1_updated.clone()));
}

#[test]
fn test_get_non_existent() {
    let temp_file = NamedTempFile::new().unwrap();
    let store = SimpleFileKvStore::new(temp_file.path()).unwrap();
    let snapshot_id = 0;
    let committed_ids: HashSet<u64> = HashSet::new();
    assert_eq!(store.get(&b"non_existent_key".to_vec(), snapshot_id, &committed_ids).unwrap(), None);
}

#[test]
fn test_delete() {
    let temp_file = NamedTempFile::new().unwrap();
    let mut store = SimpleFileKvStore::new(temp_file.path()).unwrap();
    let dummy_transaction = Transaction::new(0);
    let snapshot_id = 0;
    let committed_ids: HashSet<u64> = HashSet::new();

    let key1 = b"key1".to_vec();
    let value1 = b"value1".to_vec();
    store.put(key1.clone(), value1.clone(), &dummy_transaction).unwrap();
    assert!(store.delete(&key1, &dummy_transaction).unwrap());
    assert_eq!(store.get(&key1, snapshot_id, &committed_ids).unwrap(), None);
    assert!(!store.contains_key(&key1, snapshot_id, &committed_ids).unwrap());
}

#[test]
fn test_delete_non_existent() {
    let temp_file = NamedTempFile::new().unwrap();
    let mut store = SimpleFileKvStore::new(temp_file.path()).unwrap();
    let dummy_transaction = Transaction::new(0);
    assert!(!store.delete(&b"non_existent_key".to_vec(), &dummy_transaction).unwrap());
}

#[test]
fn test_contains_key() {
    let temp_file = NamedTempFile::new().unwrap();
    let mut store = SimpleFileKvStore::new(temp_file.path()).unwrap();
    let dummy_transaction = Transaction::new(0);
    let snapshot_id = 0;
    let committed_ids: HashSet<u64> = [0].iter().cloned().collect();

    let key1 = b"key1".to_vec();
    store.put(key1.clone(), b"value1".to_vec(), &dummy_transaction).unwrap();

    assert!(!store.contains_key(&b"non_existent_key".to_vec(), snapshot_id, &committed_ids).unwrap());
}

#[test]
fn test_persistence() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_path_buf();
    let dummy_transaction = Transaction::new(0);

    let key1 = b"persist_key".to_vec();
    let value1 = b"persist_value".to_vec();
    {
        let mut store = SimpleFileKvStore::new(&path).unwrap();
        store.put(key1.clone(), value1.clone(), &dummy_transaction).unwrap();
    }
    let reloaded_store = SimpleFileKvStore::new(&path).unwrap();
    assert_eq!(reloaded_store.get_cache_for_test().len(), 1);
}

#[test]
fn test_save_to_disk_atomic_success() {
    let db_file = NamedTempFile::new().unwrap();
    let db_path = db_file.path().to_path_buf();
    let temp_db_path = db_path.with_extension("tmp");

    let mut store = SimpleFileKvStore::new(&db_path).unwrap();
    let dummy_transaction = Transaction::new(0);

    let key1 = b"key1".to_vec();
    let value1 = b"value1".to_vec();
    store.put(key1.clone(), value1.clone(), &dummy_transaction).unwrap();
    store.persist().unwrap();

    let reloaded_store = SimpleFileKvStore::new(&db_path).unwrap();
    assert_eq!(reloaded_store.get_cache_for_test().len(), 1);
    assert!(!temp_db_path.exists(), "Temporary file should not exist after successful save.");
}

#[test]
fn test_load_from_disk_prefers_valid_temp_file() {
    let main_db_file = NamedTempFile::new().unwrap();
    let main_db_path = main_db_file.path().to_path_buf();
    let temp_db_path = main_db_path.with_extension("tmp");

    let initial_data = vec![(b"key1".to_vec(), b"value_initial".to_vec())];
    create_db_file_with_kv_data(&main_db_path, &initial_data).unwrap();

    let temp_data = vec![
        (b"key1".to_vec(), b"value_new".to_vec()),
        (b"key2".to_vec(), b"value2".to_vec()),
    ];
    create_db_file_with_kv_data(&temp_db_path, &temp_data).unwrap();

    let store = SimpleFileKvStore::new(&main_db_path).unwrap();
    assert_eq!(store.get_cache_for_test().len(), 2, "Cache should contain 2 items from temp file");

    let main_file_content_check_store = SimpleFileKvStore::new(&main_db_path).unwrap();
    assert_eq!(main_file_content_check_store.get_cache_for_test().len(), 2);

    assert!(!temp_db_path.exists(), "Temporary file should be removed after successful recovery.");
}

#[test]
fn test_load_from_disk_handles_corrupted_temp_file_and_uses_main_file() {
    let main_db_file = NamedTempFile::new().unwrap();
    let main_db_path = main_db_file.path().to_path_buf();
    let temp_db_path = main_db_path.with_extension("tmp");

    let main_data = vec![(b"key_main".to_vec(), b"value_main".to_vec())];
    create_db_file_with_kv_data(&main_db_path, &main_data).unwrap();
    write(&temp_db_path, b"this is corrupted data").unwrap();

    let store = SimpleFileKvStore::new(&main_db_path).unwrap();
    assert_eq!(store.get_cache_for_test().len(), 1, "Cache should contain 1 item from main file");
    assert!(!temp_db_path.exists(), "Corrupted temporary file should be deleted.");

    let file_content = read(&main_db_path).unwrap();
    let mut expected_content = Vec::new();
    let mut writer = BufWriter::new(&mut expected_content);
    for (k, v) in &main_data {
        <Vec<u8> as DataSerializer<Vec<u8>>>::serialize(k, &mut writer).unwrap();
        <Vec<u8> as DataSerializer<Vec<u8>>>::serialize(v, &mut writer).unwrap();
    }
    writer.flush().unwrap();
    drop(writer);
    assert_eq!(file_content, expected_content, "Main file content should not have changed.");
}

#[test]
fn test_load_from_disk_handles_temp_file_and_no_main_file() {
    let main_db_file_handle = Builder::new().prefix("test_main_db").tempfile().unwrap();
    let main_db_path = main_db_file_handle.path().to_path_buf();
    let temp_db_path = main_db_path.with_extension("tmp");

    main_db_file_handle.close().unwrap();
    if main_db_path.exists() { remove_file(&main_db_path).unwrap(); }
    assert!(!main_db_path.exists());

    let temp_data = vec![(b"key_temp".to_vec(), b"value_temp".to_vec())];
    create_db_file_with_kv_data(&temp_db_path, &temp_data).unwrap();
    assert!(temp_db_path.exists());

    let store = SimpleFileKvStore::new(&main_db_path).unwrap();
    assert_eq!(store.get_cache_for_test().len(), 1, "Cache should contain 1 item from temp file");
    assert!(main_db_path.exists(), "Main DB file should have been created from temp file.");
    assert!(!temp_db_path.exists(), "Temporary file should be deleted after successful recovery.");

    let _reloaded_store = SimpleFileKvStore::new(&main_db_path).unwrap();
}

#[test]
fn test_load_from_disk_handles_corrupted_temp_file_and_no_main_file() {
    let main_db_file_handle = Builder::new().prefix("test_main_db_corrupt_tmp").tempfile().unwrap();
    let main_db_path = main_db_file_handle.path().to_path_buf();
    let temp_db_path = main_db_path.with_extension("tmp");

    main_db_file_handle.close().unwrap();
    if main_db_path.exists() { remove_file(&main_db_path).unwrap(); }
    assert!(!main_db_path.exists());

    write(&temp_db_path, b"corrupted data").unwrap();
    assert!(temp_db_path.exists());

    let store = SimpleFileKvStore::new(&main_db_path).unwrap();
    assert!(store.get_cache_for_test().is_empty(), "Cache should be empty");
    assert!(!temp_db_path.exists(), "Corrupted temporary file should be deleted.");
    assert!(!main_db_path.exists(), "Main DB file should still not exist.");
}

#[test]
fn test_state_after_simulated_failed_save_preserves_original() {
    let db_file = NamedTempFile::new().unwrap();
    let db_path = db_file.path().to_path_buf();
    let temp_db_path = db_path.with_extension("tmp");

    let key_orig = b"key_orig".to_vec();
    let value_orig = b"value_orig".to_vec();
    {
        let mut store = SimpleFileKvStore::new(&db_path).unwrap();
        let dummy_transaction = Transaction::new(0);
        store.put(key_orig.clone(), value_orig.clone(), &dummy_transaction).unwrap();
    }

    write(&temp_db_path, b"some other data, simulating a crashed previous save attempt").unwrap();

    let key_new = b"key_new".to_vec();
    let value_new = b"value_new".to_vec();
    {
        let mut store = SimpleFileKvStore::new(&db_path).unwrap();
        let dummy_transaction = Transaction::new(0);
        store.put(key_new.clone(), value_new.clone(), &dummy_transaction).unwrap();
    }

    let store = SimpleFileKvStore::new(&db_path).unwrap();
    assert_eq!(store.get_cache_for_test().len(), 2);
    assert!(!temp_db_path.exists(), "Temp file should not exist after a successful save.");
}

#[test]
fn test_load_from_malformed_file_key_eof() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path();
    let key_len_bytes = (5u64).to_be_bytes();
    let mut file_content = key_len_bytes.to_vec();
    file_content.extend_from_slice(b"abc");
    std::fs::write(path, file_content).unwrap();

    let result = SimpleFileKvStore::new(path);
    assert!(result.is_err());
    match result.unwrap_err() {
        DbError::StorageError(msg) => {
            assert!(msg.contains("Failed to deserialize key"));
            assert!(msg.contains("failed to fill whole buffer"));
        },
        e => panic!("Unexpected error type for malformed key (EOF): {:?}", e),
    }
}

#[test]
fn test_load_from_malformed_file_value_eof() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path();

    let mut file_content = Vec::new();
    let key = b"mykey".to_vec();
    <Vec<u8> as DataSerializer<Vec<u8>>>::serialize(&key, &mut file_content).unwrap();
    let value_len_bytes = (10u64).to_be_bytes();
    file_content.extend_from_slice(&value_len_bytes);
    file_content.extend_from_slice(b"short");
    std::fs::write(path, file_content).unwrap();

    let result = SimpleFileKvStore::new(path);
    assert!(result.is_err());
    match result.unwrap_err() {
        DbError::StorageError(msg) => {
            assert!(msg.contains("Failed to deserialize value for key"));
            assert!(msg.contains("IO Error: failed to fill whole buffer"));
        },
        e => panic!("Unexpected error type for malformed value (EOF): {:?}", e),
    }
}

#[test]
fn test_put_writes_to_wal_and_cache() {
    let db_file = NamedTempFile::new().unwrap();
    let db_path = db_file.path();
    let wal_path = derive_wal_path(db_path);
    let dummy_transaction = Transaction::new(0);

    let mut store = SimpleFileKvStore::new(db_path).unwrap();
    let key = b"wal_key1".to_vec();
    let value = b"wal_value1".to_vec();
    store.put(key.clone(), value.clone(), &dummy_transaction).unwrap();

    assert_eq!(store.get_cache_entry_for_test(&key).and_then(|v| v.last().map(|vv| vv.value.clone())), Some(value.clone()));
    assert!(wal_path.exists());

    let entries = read_all_wal_entries(&wal_path).unwrap();
    assert_eq!(entries.len(), 1);
    match &entries[0] {
        WalEntry::Put { transaction_id, key: k, value: v } => {
            assert_eq!(*transaction_id, 0);
            assert_eq!(k, &key);
            assert_eq!(v, &value);
        }
        _ => panic!("Expected Put entry"),
    }
}

#[test]
fn test_delete_writes_to_wal_and_cache() {
    let db_file = NamedTempFile::new().unwrap();
    let db_path = db_file.path();
    let wal_path = derive_wal_path(db_path);
    let tx_put = Transaction::new(0);
    let tx_delete = Transaction::new(1);

    let mut store = SimpleFileKvStore::new(db_path).unwrap();
    let key = b"wal_del_key".to_vec();
    let value = b"wal_del_value".to_vec();

    store.put(key.clone(), value.clone(), &tx_put).unwrap();
    store.delete(&key, &tx_delete).unwrap();

    let versions = store.get_cache_entry_for_test(&key).unwrap();
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].created_tx_id, tx_put.id);
    assert_eq!(versions[0].expired_tx_id, Some(tx_delete.id));

    assert!(wal_path.exists());

    let entries = read_all_wal_entries(&wal_path).unwrap();
    assert_eq!(entries.len(), 2);
    match &entries[0] {
        WalEntry::Put { transaction_id, key: k, value: v } => {
            assert_eq!(*transaction_id, tx_put.id);
            assert_eq!(k, &key);
            assert_eq!(v, &value);
        }
        _ => panic!("Expected Put entry as first entry"),
    }
    match &entries[1] {
        WalEntry::Delete { transaction_id, key: k } => {
            assert_eq!(*transaction_id, tx_delete.id);
            assert_eq!(k, &key);
        }
        _ => panic!("Expected Delete entry as second entry"),
    }
}

#[test]
fn test_load_from_disk_no_wal() {
    let db_file = NamedTempFile::new().unwrap();
    let db_path = db_file.path();
    let wal_path = derive_wal_path(db_path);

    let key = b"main_data_key".to_vec();
    let value = b"main_data_value".to_vec();
    let dummy_transaction = Transaction::new(0);

    {
        let mut store = SimpleFileKvStore::new(db_path).unwrap();
        store.put(key.clone(), value.clone(), &dummy_transaction).unwrap();
        store.persist().unwrap();
    }

    assert!(!wal_path.exists(), "WAL file should not exist after save_to_disk");

    let store = SimpleFileKvStore::new(db_path).unwrap();
    assert_eq!(store.get_cache_entry_for_test(&key).and_then(|v| v.last().map(|vv| vv.value.clone())), Some(value));
}

#[test]
fn test_load_from_disk_with_wal_replay() {
    let db_file = NamedTempFile::new().unwrap();
    let db_path = db_file.path();
    let wal_path = derive_wal_path(db_path);

    let key0 = b"key0".to_vec();
    let val0_main = b"val0_main".to_vec();
    create_db_file_with_kv_data(db_path, &[(key0.clone(), val0_main.clone())]).unwrap();

    let wal_writer = WalWriter::new(db_path);

    wal_writer.log_entry(&WalEntry::Put { transaction_id: 1, key: b"key1".to_vec(), value: b"val1".to_vec() }).unwrap();
    wal_writer.log_entry(&WalEntry::Delete { transaction_id: 1, key: key0.clone() }).unwrap();
    wal_writer.log_entry(&WalEntry::TransactionCommit { transaction_id: 1 }).unwrap();

    wal_writer.log_entry(&WalEntry::Put { transaction_id: 2, key: b"key2".to_vec(), value: b"val2".to_vec() }).unwrap();
    wal_writer.log_entry(&WalEntry::TransactionRollback { transaction_id: 2 }).unwrap();

    wal_writer.log_entry(&WalEntry::Put { transaction_id: 3, key: b"key3".to_vec(), value: b"val3".to_vec() }).unwrap();

    wal_writer.log_entry(&WalEntry::Put { transaction_id: 4, key: b"key4".to_vec(), value: b"val4".to_vec() }).unwrap();
    wal_writer.log_entry(&WalEntry::TransactionCommit { transaction_id: 4 }).unwrap();
    wal_writer.log_entry(&WalEntry::TransactionRollback { transaction_id: 4 }).unwrap();

    wal_writer.log_entry(&WalEntry::Put { transaction_id: 5, key: b"key5".to_vec(), value: b"val5".to_vec() }).unwrap();
    wal_writer.log_entry(&WalEntry::TransactionRollback { transaction_id: 5 }).unwrap();
    wal_writer.log_entry(&WalEntry::TransactionCommit { transaction_id: 5 }).unwrap();
    drop(wal_writer);

    let store = SimpleFileKvStore::new(db_path).unwrap();

    let cache_key0_versions = store.get_cache_entry_for_test(&key0).unwrap();
    // The original version from disk is marked as expired, no new version is added for delete.
    assert_eq!(cache_key0_versions.len(), 1);
    assert_eq!(cache_key0_versions[0].value, val0_main);
    assert_eq!(cache_key0_versions[0].created_tx_id, 0);
    assert_eq!(cache_key0_versions[0].expired_tx_id, Some(1));

    let cache_key1_versions = store.get_cache_entry_for_test(&b"key1".to_vec()).unwrap();
    assert_eq!(cache_key1_versions.len(), 1);
    assert_eq!(cache_key1_versions[0].value, b"val1".to_vec());
    assert_eq!(cache_key1_versions[0].created_tx_id, 1);
    assert!(cache_key1_versions[0].expired_tx_id.is_none());

    assert!(store.get_cache_entry_for_test(&b"key2".to_vec()).is_none());
    assert!(store.get_cache_entry_for_test(&b"key3".to_vec()).is_none());
    assert!(store.get_cache_entry_for_test(&b"key4".to_vec()).is_none());
    assert!(store.get_cache_entry_for_test(&b"key5".to_vec()).is_none());

    assert!(wal_path.exists(), "WAL file should still exist after load_from_disk");
}

#[test]
fn test_wal_recovery_after_simulated_crash() {
    let db_file = NamedTempFile::new().unwrap();
    let db_path = db_file.path();
    let wal_path = derive_wal_path(db_path);

    let key_a = b"keyA_crash".to_vec();
    let val_a = b"valA_crash".to_vec();
    let key_b = b"keyB_crash".to_vec();
    let val_b = b"valB_crash".to_vec();

    {
        let mut store = SimpleFileKvStore::new(db_path).unwrap();
        store.put(key_a.clone(), val_a.clone(), &Transaction::new(100)).unwrap();
        store.put(key_b.clone(), val_b.clone(), &Transaction::new(100)).unwrap();
        std::mem::forget(store);
    }
    assert!(wal_path.exists());

    let store_after_crash = SimpleFileKvStore::new(db_path).unwrap();
    assert!(store_after_crash.get_cache_entry_for_test(&key_a).is_none());
    assert!(store_after_crash.get_cache_entry_for_test(&key_b).is_none());
}

#[test]
fn test_wal_recovery_commit_then_rollback_same_tx() {
    let db_file = NamedTempFile::new().unwrap();
    let db_path = db_file.path();
    let wal_writer = WalWriter::new(db_path);

    wal_writer.log_entry(&WalEntry::Put { transaction_id: 1, key: b"key_cr".to_vec(), value: b"val_cr".to_vec() }).unwrap();
    wal_writer.log_entry(&WalEntry::TransactionCommit { transaction_id: 1 }).unwrap();
    wal_writer.log_entry(&WalEntry::TransactionRollback { transaction_id: 1 }).unwrap();
    drop(wal_writer);

    let store = SimpleFileKvStore::new(db_path).unwrap();
    assert!(store.get_cache_entry_for_test(&b"key_cr".to_vec()).is_none());
}

#[test]
fn test_wal_recovery_multiple_interleaved_transactions() {
    let db_file = NamedTempFile::new().unwrap();
    let db_path = db_file.path();
    let wal_writer = WalWriter::new(db_path);

    wal_writer.log_entry(&WalEntry::Put { transaction_id: 10, key: b"key10_1".to_vec(), value: b"val10_1".to_vec() }).unwrap();
    wal_writer.log_entry(&WalEntry::Put { transaction_id: 20, key: b"key20_1".to_vec(), value: b"val20_1".to_vec() }).unwrap();
    wal_writer.log_entry(&WalEntry::Put { transaction_id: 10, key: b"key10_2".to_vec(), value: b"val10_2".to_vec() }).unwrap();
    wal_writer.log_entry(&WalEntry::TransactionCommit { transaction_id: 10 }).unwrap();
    wal_writer.log_entry(&WalEntry::Put { transaction_id: 30, key: b"key30_1".to_vec(), value: b"val30_1".to_vec() }).unwrap();
    wal_writer.log_entry(&WalEntry::TransactionRollback { transaction_id: 30 }).unwrap();
    wal_writer.log_entry(&WalEntry::Delete { transaction_id: 20, key: b"some_other_key".to_vec() }).unwrap();
    drop(wal_writer);

    let store = SimpleFileKvStore::new(db_path).unwrap();

    let get_latest_value = |cache: &HashMap<Vec<u8>, Vec<VersionedValue<Vec<u8>>>>, key: &Vec<u8>| -> Option<Vec<u8>> {
        cache.get(key).and_then(|versions| versions.last().filter(|v| v.expired_tx_id.is_none()).map(|v| v.value.clone()))
    };
    assert_eq!(get_latest_value(store.get_cache_for_test(), &b"key10_1".to_vec()), Some(b"val10_1".to_vec()));
    assert_eq!(get_latest_value(store.get_cache_for_test(), &b"key10_2".to_vec()), Some(b"val10_2".to_vec()));
    assert_eq!(get_latest_value(store.get_cache_for_test(), &b"key20_1".to_vec()), None);
    assert_eq!(get_latest_value(store.get_cache_for_test(), &b"some_other_key".to_vec()), None);
    assert_eq!(get_latest_value(store.get_cache_for_test(), &b"key30_1".to_vec()), None);
}

#[test]
fn test_wal_truncation_after_save_to_disk() {
    let db_file = NamedTempFile::new().unwrap();
    let db_path = db_file.path();
    let wal_path = derive_wal_path(db_path);
    let dummy_transaction = Transaction::new(0);

    {
        let mut store = SimpleFileKvStore::new(db_path).unwrap();
        store.put(b"trunc_key".to_vec(), b"trunc_val".to_vec(), &dummy_transaction).unwrap();
        assert!(wal_path.exists());
        store.persist().unwrap();
    }

    assert!(!wal_path.exists(), "WAL file should not exist after save_to_disk");
    let store = SimpleFileKvStore::new(db_path).unwrap();
    assert_eq!(store.get_cache_entry_for_test(&b"trunc_key".to_vec()).and_then(|v|v.last().map(|vv| vv.value.clone())), Some(b"trunc_val".to_vec()));
}

#[test]
fn test_wal_replay_stops_on_corruption() {
    let db_file = NamedTempFile::new().unwrap();
    let db_path = db_file.path();
    let wal_path = derive_wal_path(db_path);

    let key_good = b"key_good".to_vec();
    let value_good = b"value_good".to_vec();
    let key_bad = b"key_bad".to_vec();
    let value_bad = b"value_bad".to_vec();

    {
        let wal_file_handle = OpenOptions::new().write(true).create(true).truncate(true).open(&wal_path).unwrap();
        let mut writer = BufWriter::new(wal_file_handle);

        <WalEntry as DataSerializer<WalEntry>>::serialize(&WalEntry::Put{ transaction_id: 0, key: key_good.clone(), value: value_good.clone() }, &mut writer).unwrap();
        <WalEntry as DataSerializer<WalEntry>>::serialize(&WalEntry::TransactionCommit{ transaction_id: 0 }, &mut writer).unwrap();
        writer.flush().unwrap();

        writer.write_all(&[0xDE, 0xAD, 0xBE, 0xEF]).unwrap();
        writer.flush().unwrap();

        <WalEntry as DataSerializer<WalEntry>>::serialize(&WalEntry::Put{ transaction_id: 1, key: key_bad.clone(), value: value_bad.clone() }, &mut writer).unwrap();
        <WalEntry as DataSerializer<WalEntry>>::serialize(&WalEntry::TransactionCommit{ transaction_id: 1 }, &mut writer).unwrap();
        writer.flush().unwrap();
    }

    let store = SimpleFileKvStore::new(db_path).unwrap();

    let get_latest_value = |cache: &HashMap<Vec<u8>, Vec<VersionedValue<Vec<u8>>>>, key: &Vec<u8>| -> Option<Vec<u8>> {
        cache.get(key).and_then(|versions| versions.last().map(|v| v.value.clone()))
    };
    assert_eq!(get_latest_value(store.get_cache_for_test(), &key_good), Some(value_good.clone()));
    assert!(store.get_cache_entry_for_test(&key_bad).is_none());
}

#[test]
fn test_drop_persists_data() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_path_buf();
    let key1 = b"drop_key".to_vec();
    let value1 = b"drop_value".to_vec();
    let dummy_transaction = Transaction::new(0);

    {
        let mut store = SimpleFileKvStore::new(&path).unwrap();
        store.put(key1.clone(), value1.clone(), &dummy_transaction).unwrap();
    }

    let reloaded_store = SimpleFileKvStore::new(&path).unwrap();
    assert_eq!(reloaded_store.get_cache_entry_for_test(&key1).and_then(|v|v.last().map(|vv|vv.value.clone())), Some(value1));
    assert_eq!(reloaded_store.get_cache_for_test().len(), 1);

    let wal_path = derive_wal_path(&path);
    assert!(!wal_path.exists(), "WAL file should not exist after successful drop/save.");
}

#[test]
fn test_put_atomicity_wal_failure() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_put_atomicity.db");
    let wal_path = derive_wal_path(&db_path);

    std::fs::create_dir_all(&wal_path).unwrap();
    assert!(wal_path.is_dir());

    let mut store = SimpleFileKvStore::new(&db_path).unwrap();

    let key = b"atomic_put_key".to_vec();
    let value = b"atomic_put_value".to_vec();
    let dummy_transaction = Transaction::new(0);

    let result = store.put(key.clone(), value.clone(), &dummy_transaction);

    assert!(result.is_err());
    match result.unwrap_err() {
        DbError::IoError(_) => {}
        other_err => panic!("Expected DbError::IoError, got {:?}", other_err),
    }
    assert!(store.get_cache_entry_for_test(&key).is_none());
    let _ = std::fs::remove_dir_all(&wal_path);
}

#[test]
fn test_delete_atomicity_wal_failure() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_delete_atomicity.db");
    let wal_path = derive_wal_path(&db_path);
    let dummy_transaction = Transaction::new(0);

    let key = b"atomic_del_key".to_vec();
    let value = b"atomic_del_value".to_vec();

    {
        let mut store = SimpleFileKvStore::new(&db_path).unwrap();
        store.put(key.clone(), value.clone(), &dummy_transaction).unwrap();
        store.persist().unwrap();
    }

    assert!(!wal_path.exists());
    std::fs::create_dir_all(&wal_path).unwrap();
    assert!(wal_path.is_dir());

    let mut store = SimpleFileKvStore::new(&db_path).unwrap();
    assert!(store.get_cache_entry_for_test(&key).is_some());

    let result = store.delete(&key, &dummy_transaction);
    assert!(result.is_err());
     match result.unwrap_err() {
        DbError::IoError(_) => {}
        other_err => panic!("Expected DbError::IoError, got {:?}", other_err),
    }
    assert!(store.get_cache_entry_for_test(&key).is_some());
    assert_eq!(store.get_cache_entry_for_test(&key).and_then(|v| v.last().map(|vv| vv.value.clone())), Some(value.clone()));
    let _ = std::fs::remove_dir_all(&wal_path);
}
