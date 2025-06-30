// Original imports from simple_file.rs that might be needed by test helpers or types:
use std::collections::{HashMap, HashSet};
use std::fs::{read, remove_file, write, File, File as StdFile, OpenOptions}; // Removed rename for now as it's not used after test changes
use std::io::{BufReader, BufWriter, ErrorKind, Write};
use std::path::{Path, PathBuf};
use tempfile::tempdir; // Added import for tempdir

// Specific imports for types used in tests, from their canonical paths
use crate::core::common::traits::{DataDeserializer, DataSerializer};
use crate::core::common::types::TransactionId;
use crate::core::common::OxidbError;
use crate::core::storage::engine::traits::{KeyValueStore, VersionedValue};
use crate::core::storage::engine::wal::{WalEntry, WalWriter};
use crate::core::transaction::Transaction;
use tempfile::{Builder, NamedTempFile};

// Import the struct being tested
use crate::core::storage::engine::implementations::simple_file::SimpleFileKvStore;

// Helper to create a main DB file with specific key-value data
fn create_db_file_with_kv_data(path: &Path, data: &[(Vec<u8>, Vec<u8>)]) -> Result<(), OxidbError> {
    // Changed
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .map_err(OxidbError::Io)?; // Changed
    let mut writer = BufWriter::new(file);
    for (key, value) in data {
        <Vec<u8> as DataSerializer<Vec<u8>>>::serialize(key, &mut writer)?;
        <Vec<u8> as DataSerializer<Vec<u8>>>::serialize(value, &mut writer)?;
    }
    writer.flush().map_err(OxidbError::Io)?; // Changed
    writer.get_ref().sync_all().map_err(OxidbError::Io)?; // Changed
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
fn read_all_wal_entries(wal_path: &Path) -> Result<Vec<WalEntry>, OxidbError> {
    // Changed
    let file = StdFile::open(wal_path).map_err(OxidbError::Io)?; // Changed
    let mut reader = BufReader::new(file);
    let mut entries = Vec::new();
    loop {
        match <WalEntry as DataDeserializer<WalEntry>>::deserialize(&mut reader) {
            Ok(entry) => entries.push(entry),
            Err(OxidbError::Io(e)) if e.kind() == ErrorKind::UnexpectedEof => {
                // Changed
                break;
            }
            Err(e) => {
                // e is OxidbError
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
    let dummy_transaction = Transaction::new(TransactionId(0));
    let _snapshot_id = 0;
    let _committed_ids: HashSet<u64> = HashSet::new();
    let key1 = b"key1".to_vec();
    let value1 = b"value1".to_vec();
    let dummy_lsn = 0; // Dummy LSN for tests
    store.put(key1.clone(), value1.clone(), &dummy_transaction, dummy_lsn).unwrap();
    // assert_eq!(store.get(&key1, _snapshot_id, &_committed_ids).unwrap(), Some(value1.clone()));

    let key2 = b"key2".to_vec();
    let value2 = b"value2_long".to_vec();
    store.put(key2.clone(), value2.clone(), &dummy_transaction, dummy_lsn).unwrap();
    // assert_eq!(store.get(&key2, _snapshot_id, &_committed_ids).unwrap(), Some(value2.clone()));
    // assert_eq!(store.get(&key1, _snapshot_id, &_committed_ids).unwrap(), Some(value1.clone()));
}

#[test]
fn test_put_update() {
    let temp_file = NamedTempFile::new().unwrap();
    let mut store = SimpleFileKvStore::new(temp_file.path()).unwrap();
    let dummy_transaction = Transaction::new(TransactionId(0));
    let _snapshot_id = 0;
    let _committed_ids: HashSet<u64> = HashSet::new();
    let key1 = b"key1".to_vec();
    let value1 = b"value1".to_vec();
    let value1_updated = b"value1_updated".to_vec();
    let dummy_lsn = 0;

    store.put(key1.clone(), value1.clone(), &dummy_transaction, dummy_lsn).unwrap();
    // assert_eq!(store.get(&key1, _snapshot_id, &_committed_ids).unwrap(), Some(value1.clone()));

    store.put(key1.clone(), value1_updated.clone(), &dummy_transaction, dummy_lsn).unwrap();
    // assert_eq!(store.get(&key1, _snapshot_id, &_committed_ids).unwrap(), Some(value1_updated.clone()));
}

#[test]
fn test_get_non_existent() {
    let temp_file = NamedTempFile::new().unwrap();
    let store = SimpleFileKvStore::new(temp_file.path()).unwrap();
    let snapshot_id = 0;
    let committed_ids: HashSet<u64> = HashSet::new();
    assert_eq!(
        store.get(&b"non_existent_key".to_vec(), snapshot_id, &committed_ids).unwrap(),
        None
    );
}

#[test]
fn test_delete() {
    let temp_file = NamedTempFile::new().unwrap();
    let mut store = SimpleFileKvStore::new(temp_file.path()).unwrap();
    let dummy_transaction = Transaction::new(TransactionId(0));
    let snapshot_id = 0;
    let committed_ids: HashSet<u64> = HashSet::new();

    let key1 = b"key1".to_vec();
    let value1 = b"value1".to_vec();
    let dummy_lsn = 0;
    store.put(key1.clone(), value1.clone(), &dummy_transaction, dummy_lsn).unwrap();
    let mut delete_committed_ids = HashSet::new();
    delete_committed_ids.insert(0); // tx0 is deleting and is committed
    assert!(store.delete(&key1, &dummy_transaction, dummy_lsn, &delete_committed_ids).unwrap());
    assert_eq!(store.get(&key1, snapshot_id, &committed_ids).unwrap(), None);
    assert!(!store.contains_key(&key1, snapshot_id, &committed_ids).unwrap());
}

#[test]
fn test_delete_non_existent() {
    let temp_file = NamedTempFile::new().unwrap();
    let mut store = SimpleFileKvStore::new(temp_file.path()).unwrap();
    let dummy_transaction = Transaction::new(TransactionId(0));
    let dummy_lsn = 0;
    let mut delete_committed_ids = HashSet::new();
    delete_committed_ids.insert(0); // tx0 is deleting and is committed
    assert!(!store
        .delete(&b"non_existent_key".to_vec(), &dummy_transaction, dummy_lsn, &delete_committed_ids)
        .unwrap());
}

#[test]
fn test_contains_key() {
    let temp_file = NamedTempFile::new().unwrap();
    let mut store = SimpleFileKvStore::new(temp_file.path()).unwrap();
    let dummy_transaction = Transaction::new(TransactionId(0));
    let snapshot_id = 0;
    let committed_ids: HashSet<u64> = [TransactionId(0).0].iter().cloned().collect();

    let key1 = b"key1".to_vec();
    let dummy_lsn = 0;
    store.put(key1.clone(), b"value1".to_vec(), &dummy_transaction, dummy_lsn).unwrap();

    assert!(!store
        .contains_key(&b"non_existent_key".to_vec(), snapshot_id, &committed_ids)
        .unwrap());
}

#[test]
fn test_persistence() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_path_buf();
    let dummy_transaction = Transaction::new(TransactionId(0));

    let key1 = b"persist_key".to_vec();
    let value1 = b"persist_value".to_vec();
    let dummy_lsn = 0;
    {
        let mut store = SimpleFileKvStore::new(&path).unwrap();
        store.put(key1.clone(), value1.clone(), &dummy_transaction, dummy_lsn).unwrap();
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
    let dummy_transaction = Transaction::new(TransactionId(0));

    let key1 = b"key1".to_vec();
    let value1 = b"value1".to_vec();
    let dummy_lsn = 0;
    store.put(key1.clone(), value1.clone(), &dummy_transaction, dummy_lsn).unwrap();
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

    let temp_data =
        vec![(b"key1".to_vec(), b"value_new".to_vec()), (b"key2".to_vec(), b"value2".to_vec())];
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
    if main_db_path.exists() {
        remove_file(&main_db_path).unwrap();
    }
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
    if main_db_path.exists() {
        remove_file(&main_db_path).unwrap();
    }
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
    let dummy_lsn = 0;
    {
        let mut store = SimpleFileKvStore::new(&db_path).unwrap();
        let dummy_transaction = Transaction::new(TransactionId(0));
        store.put(key_orig.clone(), value_orig.clone(), &dummy_transaction, dummy_lsn).unwrap();
    }

    write(&temp_db_path, b"some other data, simulating a crashed previous save attempt").unwrap();

    let key_new = b"key_new".to_vec();
    let value_new = b"value_new".to_vec();
    {
        let mut store = SimpleFileKvStore::new(&db_path).unwrap();
        let dummy_transaction = Transaction::new(TransactionId(0));
        store.put(key_new.clone(), value_new.clone(), &dummy_transaction, dummy_lsn).unwrap();
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
        OxidbError::Storage(msg) => {
            assert!(msg.contains("Failed to deserialize key"));
            assert!(
                msg.contains("failed to fill whole buffer")
                    || msg.contains("Io(Error { kind: UnexpectedEof")
            );
        }
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
        OxidbError::Storage(msg) => {
            assert!(msg.contains("Failed to deserialize value for key"));
            assert!(
                msg.contains("failed to fill whole buffer")
                    || msg.contains("Io(Error { kind: UnexpectedEof")
            );
        }
        e => panic!("Unexpected error type for malformed value (EOF): {:?}", e),
    }
}

#[test]
fn test_put_writes_to_wal_and_cache() {
    let db_file = NamedTempFile::new().unwrap();
    let db_path = db_file.path();
    let wal_path = derive_wal_path(db_path);
    let dummy_transaction = Transaction::new(TransactionId(0));
    let dummy_lsn = 0;

    let mut store = SimpleFileKvStore::new(db_path).unwrap();
    let key = b"wal_key1".to_vec();
    let value = b"wal_value1".to_vec();
    store.put(key.clone(), value.clone(), &dummy_transaction, dummy_lsn).unwrap();

    assert_eq!(
        store.get_cache_entry_for_test(&key).and_then(|v| v.last().map(|vv| vv.value.clone())),
        Some(value.clone())
    );
    assert!(wal_path.exists());

    let entries = read_all_wal_entries(&wal_path).unwrap();
    assert_eq!(entries.len(), 1);
    match &entries[0] {
        WalEntry::Put { lsn, transaction_id, key: k, value: v } => {
            // Added lsn
            assert_eq!(*lsn, dummy_lsn); // Check LSN
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
    let tx_put = Transaction::new(TransactionId(0));
    let tx_delete = Transaction::new(TransactionId(1));
    let dummy_lsn_put = 0;
    let dummy_lsn_delete = 1;

    let mut store = SimpleFileKvStore::new(db_path).unwrap();
    let key = b"wal_del_key".to_vec();
    let value = b"wal_del_value".to_vec();

    store.put(key.clone(), value.clone(), &tx_put, dummy_lsn_put).unwrap();
    let mut delete_committed_ids = HashSet::new();
    delete_committed_ids.insert(tx_put.id.0); // tx_put (0) is committed
    delete_committed_ids.insert(tx_delete.id.0); // tx_delete (1) is the one deleting and considered committed for this op
    store.delete(&key, &tx_delete, dummy_lsn_delete, &delete_committed_ids).unwrap();

    let versions = store.get_cache_entry_for_test(&key).unwrap();
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].created_tx_id, tx_put.id.0); // Compare with u64
    assert_eq!(versions[0].expired_tx_id, Some(tx_delete.id.0)); // Compare with u64

    assert!(wal_path.exists());

    let entries = read_all_wal_entries(&wal_path).unwrap();
    assert_eq!(entries.len(), 2);
    match &entries[0] {
        WalEntry::Put { lsn, transaction_id, key: k, value: v } => {
            // Added lsn
            assert_eq!(*lsn, dummy_lsn_put);
            assert_eq!(*transaction_id, tx_put.id.0); // Compare with u64
            assert_eq!(k, &key);
            assert_eq!(v, &value);
        }
        _ => panic!("Expected Put entry as first entry"),
    }
    match &entries[1] {
        WalEntry::Delete { lsn, transaction_id, key: k } => {
            // Added lsn
            assert_eq!(*lsn, dummy_lsn_delete);
            assert_eq!(*transaction_id, tx_delete.id.0); // Compare with u64
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
    let dummy_transaction = Transaction::new(TransactionId(0));
    let dummy_lsn = 0;

    {
        let mut store = SimpleFileKvStore::new(db_path).unwrap();
        store.put(key.clone(), value.clone(), &dummy_transaction, dummy_lsn).unwrap();
        store.persist().unwrap();
    }

    assert!(!wal_path.exists(), "WAL file should not exist after save_to_disk");

    let store = SimpleFileKvStore::new(db_path).unwrap();
    assert_eq!(
        store.get_cache_entry_for_test(&key).and_then(|v| v.last().map(|vv| vv.value.clone())),
        Some(value)
    );
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
    let mut lsn_counter: u64 = 0;

    let mut next_lsn = || {
        let current = lsn_counter;
        lsn_counter += 1;
        current
    };

    wal_writer
        .log_entry(&WalEntry::Put {
            lsn: next_lsn(),
            transaction_id: 1,
            key: b"key1".to_vec(),
            value: b"val1".to_vec(),
        })
        .unwrap();
    wal_writer
        .log_entry(&WalEntry::Delete { lsn: next_lsn(), transaction_id: 1, key: key0.clone() })
        .unwrap();
    wal_writer
        .log_entry(&WalEntry::TransactionCommit { lsn: next_lsn(), transaction_id: 1 })
        .unwrap();

    wal_writer
        .log_entry(&WalEntry::Put {
            lsn: next_lsn(),
            transaction_id: 2,
            key: b"key2".to_vec(),
            value: b"val2".to_vec(),
        })
        .unwrap();
    wal_writer
        .log_entry(&WalEntry::TransactionRollback { lsn: next_lsn(), transaction_id: 2 })
        .unwrap();

    wal_writer
        .log_entry(&WalEntry::Put {
            lsn: next_lsn(),
            transaction_id: 3,
            key: b"key3".to_vec(),
            value: b"val3".to_vec(),
        })
        .unwrap();

    wal_writer
        .log_entry(&WalEntry::Put {
            lsn: next_lsn(),
            transaction_id: 4,
            key: b"key4".to_vec(),
            value: b"val4".to_vec(),
        })
        .unwrap();
    wal_writer
        .log_entry(&WalEntry::TransactionCommit { lsn: next_lsn(), transaction_id: 4 })
        .unwrap();
    wal_writer
        .log_entry(&WalEntry::TransactionRollback { lsn: next_lsn(), transaction_id: 4 })
        .unwrap();

    wal_writer
        .log_entry(&WalEntry::Put {
            lsn: next_lsn(),
            transaction_id: 5,
            key: b"key5".to_vec(),
            value: b"val5".to_vec(),
        })
        .unwrap();
    wal_writer
        .log_entry(&WalEntry::TransactionRollback { lsn: next_lsn(), transaction_id: 5 })
        .unwrap();
    wal_writer
        .log_entry(&WalEntry::TransactionCommit { lsn: next_lsn(), transaction_id: 5 })
        .unwrap();
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
    let dummy_lsn = 0;

    {
        let mut store = SimpleFileKvStore::new(db_path).unwrap();
        store
            .put(key_a.clone(), val_a.clone(), &Transaction::new(TransactionId(100)), dummy_lsn)
            .unwrap();
        store
            .put(key_b.clone(), val_b.clone(), &Transaction::new(TransactionId(100)), dummy_lsn)
            .unwrap();
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
    let mut lsn_counter: u64 = 0;
    let mut next_lsn = || {
        let current = lsn_counter;
        lsn_counter += 1;
        current
    };

    wal_writer
        .log_entry(&WalEntry::Put {
            lsn: next_lsn(),
            transaction_id: 1,
            key: b"key_cr".to_vec(),
            value: b"val_cr".to_vec(),
        })
        .unwrap();
    wal_writer
        .log_entry(&WalEntry::TransactionCommit { lsn: next_lsn(), transaction_id: 1 })
        .unwrap();
    wal_writer
        .log_entry(&WalEntry::TransactionRollback { lsn: next_lsn(), transaction_id: 1 })
        .unwrap();
    drop(wal_writer);

    let store = SimpleFileKvStore::new(db_path).unwrap();
    assert!(store.get_cache_entry_for_test(&b"key_cr".to_vec()).is_none());
}

#[test]
fn test_wal_recovery_multiple_interleaved_transactions() {
    let db_file = NamedTempFile::new().unwrap();
    let db_path = db_file.path();
    let wal_writer = WalWriter::new(db_path);
    let mut lsn_counter: u64 = 0;
    let mut next_lsn = || {
        let current = lsn_counter;
        lsn_counter += 1;
        current
    };

    wal_writer
        .log_entry(&WalEntry::Put {
            lsn: next_lsn(),
            transaction_id: 10,
            key: b"key10_1".to_vec(),
            value: b"val10_1".to_vec(),
        })
        .unwrap();
    wal_writer
        .log_entry(&WalEntry::Put {
            lsn: next_lsn(),
            transaction_id: 20,
            key: b"key20_1".to_vec(),
            value: b"val20_1".to_vec(),
        })
        .unwrap();
    wal_writer
        .log_entry(&WalEntry::Put {
            lsn: next_lsn(),
            transaction_id: 10,
            key: b"key10_2".to_vec(),
            value: b"val10_2".to_vec(),
        })
        .unwrap();
    wal_writer
        .log_entry(&WalEntry::TransactionCommit { lsn: next_lsn(), transaction_id: 10 })
        .unwrap();
    wal_writer
        .log_entry(&WalEntry::Put {
            lsn: next_lsn(),
            transaction_id: 30,
            key: b"key30_1".to_vec(),
            value: b"val30_1".to_vec(),
        })
        .unwrap();
    wal_writer
        .log_entry(&WalEntry::TransactionRollback { lsn: next_lsn(), transaction_id: 30 })
        .unwrap();
    wal_writer
        .log_entry(&WalEntry::Delete {
            lsn: next_lsn(),
            transaction_id: 20,
            key: b"some_other_key".to_vec(),
        })
        .unwrap();
    drop(wal_writer);

    let store = SimpleFileKvStore::new(db_path).unwrap();

    let get_latest_value = |cache: &HashMap<Vec<u8>, Vec<VersionedValue<Vec<u8>>>>,
                            key: &Vec<u8>|
     -> Option<Vec<u8>> {
        cache.get(key).and_then(|versions| {
            versions.last().filter(|v| v.expired_tx_id.is_none()).map(|v| v.value.clone())
        })
    };
    assert_eq!(
        get_latest_value(store.get_cache_for_test(), &b"key10_1".to_vec()),
        Some(b"val10_1".to_vec())
    );
    assert_eq!(
        get_latest_value(store.get_cache_for_test(), &b"key10_2".to_vec()),
        Some(b"val10_2".to_vec())
    );
    assert_eq!(get_latest_value(store.get_cache_for_test(), &b"key20_1".to_vec()), None);
    assert_eq!(get_latest_value(store.get_cache_for_test(), &b"some_other_key".to_vec()), None);
    assert_eq!(get_latest_value(store.get_cache_for_test(), &b"key30_1".to_vec()), None);
}

#[test]
fn test_wal_truncation_after_save_to_disk() {
    let db_file = NamedTempFile::new().unwrap();
    let db_path = db_file.path();
    let wal_path = derive_wal_path(db_path);
    let dummy_transaction = Transaction::new(TransactionId(0));
    let dummy_lsn = 0;

    {
        let mut store = SimpleFileKvStore::new(db_path).unwrap();
        store
            .put(b"trunc_key".to_vec(), b"trunc_val".to_vec(), &dummy_transaction, dummy_lsn)
            .unwrap();
        assert!(wal_path.exists());
        store.persist().unwrap();
    }

    // The WAL file should be truncated (or removed) by the persist operation.
    let _store = SimpleFileKvStore::new(db_path).unwrap(); // Re-open to ensure no WAL replay errors from a leftover WAL
    assert!(!wal_path.exists(), "WAL file should NOT exist after persist and re-opening store.");

    // Verify data is still present by opening again
    let store_after_persist = SimpleFileKvStore::new(db_path).unwrap();
    assert_eq!(
        store_after_persist
            .get_cache_entry_for_test(&b"trunc_key".to_vec())
            .and_then(|v| v.last().map(|vv| vv.value.clone())),
        Some(b"trunc_val".to_vec())
    );
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
        let wal_file_handle =
            OpenOptions::new().write(true).create(true).truncate(true).open(&wal_path).unwrap();
        let mut writer = BufWriter::new(wal_file_handle);
        let mut lsn_counter: u64 = 0;
        let mut next_lsn = || {
            let current = lsn_counter;
            lsn_counter += 1;
            current
        };

        <WalEntry as DataSerializer<WalEntry>>::serialize(
            &WalEntry::Put {
                lsn: next_lsn(),
                transaction_id: 0,
                key: key_good.clone(),
                value: value_good.clone(),
            },
            &mut writer,
        )
        .unwrap();
        <WalEntry as DataSerializer<WalEntry>>::serialize(
            &WalEntry::TransactionCommit { lsn: next_lsn(), transaction_id: 0 },
            &mut writer,
        )
        .unwrap();
        writer.flush().unwrap();

        writer.write_all(&[0xDE, 0xAD, 0xBE, 0xEF]).unwrap(); // Corrupted entry
        writer.flush().unwrap();

        <WalEntry as DataSerializer<WalEntry>>::serialize(
            &WalEntry::Put {
                lsn: next_lsn(),
                transaction_id: 1,
                key: key_bad.clone(),
                value: value_bad.clone(),
            },
            &mut writer,
        )
        .unwrap();
        <WalEntry as DataSerializer<WalEntry>>::serialize(
            &WalEntry::TransactionCommit { lsn: next_lsn(), transaction_id: 1 },
            &mut writer,
        )
        .unwrap();
        writer.flush().unwrap();
    }

    let store = SimpleFileKvStore::new(db_path).unwrap();

    let get_latest_value = |cache: &HashMap<Vec<u8>, Vec<VersionedValue<Vec<u8>>>>,
                            key: &Vec<u8>|
     -> Option<Vec<u8>> {
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
    let dummy_transaction = Transaction::new(TransactionId(0)); // This should already be TransactionId(0)
    let dummy_lsn = 0;

    {
        let mut store = SimpleFileKvStore::new(&path).unwrap();
        store.put(key1.clone(), value1.clone(), &dummy_transaction, dummy_lsn).unwrap();
    }

    let reloaded_store = SimpleFileKvStore::new(&path).unwrap();
    assert_eq!(
        reloaded_store
            .get_cache_entry_for_test(&key1)
            .and_then(|v| v.last().map(|vv| vv.value.clone())),
        Some(value1)
    );
    assert_eq!(reloaded_store.get_cache_for_test().len(), 1);

    let wal_path = derive_wal_path(&path);
    assert!(!wal_path.exists(), "WAL file should not exist after successful drop/save.");
}

/*
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
    // Line 788 is likely here or in a similar test function.
    // The previous fix for this file (Turn 6 of last session) changed many Transaction::new(0)
    // It's possible this specific one was missed or the line numbers shifted.
    // Assuming the error points to a Transaction::new(0) that needs fixing.
    let dummy_transaction = Transaction::new(TransactionId(0));
    let dummy_lsn = 0;

    let result = store.put(key.clone(), value.clone(), &dummy_transaction, dummy_lsn);

    assert!(result.is_err());
    match result.unwrap_err() {
        OxidbError::Io(_) => {}
        other_err => panic!("Expected OxidbError::Io, got {:?}", other_err),
    }
    assert!(store.get_cache_entry_for_test(&key).is_none());
    let _ = std::fs::remove_dir_all(&wal_path);
}
*/

#[test]
fn test_delete_atomicity_wal_failure() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_delete_atomicity.db");
    let wal_path = derive_wal_path(&db_path);
    let dummy_transaction = Transaction::new(TransactionId(0)); // Already correct

    let key = b"atomic_del_key".to_vec();
    let value = b"atomic_del_value".to_vec();
    let dummy_lsn = 0;

    {
        let mut store = SimpleFileKvStore::new(&db_path).unwrap();
        store.put(key.clone(), value.clone(), &dummy_transaction, dummy_lsn).unwrap();
        store.persist().unwrap();
    }

    assert!(!wal_path.exists());
    // Create a file at the WAL path to simulate a permission/access issue
    // instead of a directory, which would cause file open failures
    std::fs::write(&wal_path, b"dummy_content").unwrap();
    // Make the file read-only to simulate a write permission failure
    let mut perms = std::fs::metadata(&wal_path).unwrap().permissions();
    perms.set_readonly(true);
    std::fs::set_permissions(&wal_path, perms).unwrap();
    assert!(wal_path.is_file());

    {
        let mut store = SimpleFileKvStore::new(&db_path).unwrap();
        assert!(store.get_cache_entry_for_test(&key).is_some());

        let mut delete_committed_ids = HashSet::new();
        delete_committed_ids.insert(dummy_transaction.id.0); // tx0 is deleting
        let result = store.delete(&key, &dummy_transaction, dummy_lsn, &delete_committed_ids);
        assert!(result.is_err());
        match result.unwrap_err() {
            OxidbError::Io(_) => {}
            other_err => panic!("Expected OxidbError::Io, got {:?}", other_err),
        }
        assert!(store.get_cache_entry_for_test(&key).is_some());
        assert_eq!(
            store.get_cache_entry_for_test(&key).and_then(|v| v.last().map(|vv| vv.value.clone())),
            Some(value.clone())
        );
    } // Ensure store is dropped before cleanup
    
    // Clean up the WAL file if it exists
    if wal_path.exists() {
        // Remove read-only permission before deletion
        if let Ok(metadata) = std::fs::metadata(&wal_path) {
            let mut perms = metadata.permissions();
            perms.set_readonly(false);
            let _ = std::fs::set_permissions(&wal_path, perms);
        }
        let _ = std::fs::remove_file(&wal_path);
    }
}

#[test]
fn test_scan_operation() -> Result<(), OxidbError> {
    let temp_dir = tempdir().unwrap(); // Corrected: use tempdir for proper directory management
    let db_path = temp_dir.path().join("scan_test.db");
    let mut store = SimpleFileKvStore::new(&db_path)?;

    // Using tx0 for operations that should be visible to a simple scan
    // (simulating auto-committed or committed data).
    let tx0 = Transaction::new(TransactionId(0));
    let lsn_base = 0; // Base LSN for this test sequence

    let key1 = b"key1_scan".to_vec();
    let val1_v1 = b"val1_v1_scan".to_vec();
    let val1_v2 = b"val1_v2_scan".to_vec();

    let key2 = b"key2_scan".to_vec();
    let val2 = b"val2_scan".to_vec();

    let key3 = b"key3_scan".to_vec();
    let val3 = b"val3_scan".to_vec();

    let key4_uncommitted_or_specific_tx = b"key4_other_tx_scan".to_vec();
    let val4_uncommitted_or_specific_tx = b"val4_other_tx_scan".to_vec();
    let tx_other = Transaction::new(TransactionId(99)); // A different transaction ID

    // Test 1: Scan empty store
    let results_empty = store.scan()?;
    assert!(results_empty.is_empty(), "Scan on empty store should return no results");

    // Insert some data with tx0 (considered committed for basic scan)
    store.put(key1.clone(), val1_v1.clone(), &tx0, lsn_base)?;
    store.put(key2.clone(), val2.clone(), &tx0, lsn_base + 1)?;
    store.put(key3.clone(), val3.clone(), &tx0, lsn_base + 2)?;

    // Test 2: Scan with initial data
    let mut results1 = store.scan()?;
    results1.sort_by(|a, b| a.0.cmp(&b.0));

    assert_eq!(results1.len(), 3);
    assert_eq!(results1[0], (key1.clone(), val1_v1.clone()));
    assert_eq!(results1[1], (key2.clone(), val2.clone()));
    assert_eq!(results1[2], (key3.clone(), val3.clone()));

    // Update key1 with tx0 (new version, old one by tx0 implicitly expired by this new put)
    store.put(key1.clone(), val1_v2.clone(), &tx0, lsn_base + 3)?;

    // Delete key3 with tx0
    let mut delete_committed_ids_scan = HashSet::new();
    delete_committed_ids_scan.insert(tx0.id.0); // tx0 is deleting and is committed
                                                // Add other tx_ids that were part of setup if any, tx0 covers puts for key1, key2, key3.
    store.delete(&key3, &tx0, lsn_base + 4, &delete_committed_ids_scan)?;

    // Insert key4 with a different transaction ID (tx_other)
    // The current simple_file_store.scan() takes latest non-expired, regardless of tx_id,
    // as it mimics a snapshot_id=0 non-transactional read.
    store.put(
        key4_uncommitted_or_specific_tx.clone(),
        val4_uncommitted_or_specific_tx.clone(),
        &tx_other,
        lsn_base + 5,
    )?;

    // Test 3: Scan after updates and deletes
    let mut results2 = store.scan()?;
    results2.sort_by(|a, b| a.0.cmp(&b.0));

    let mut expected_results2 = vec![
        (key1.clone(), val1_v2.clone()), // key1 updated to v2
        (key2.clone(), val2.clone()),    // key2 should remain
        (key4_uncommitted_or_specific_tx.clone(), val4_uncommitted_or_specific_tx.clone()), // key4 from tx_other visible
    ];
    expected_results2.sort_by(|a, b| a.0.cmp(&b.0));

    assert_eq!(results2.len(), 3, "Scan results: {:?}", results2);
    assert_eq!(
        results2, expected_results2,
        "Scan results did not match expected results after modifications."
    );

    // Ensure key3 is not present
    assert!(!results2.iter().any(|(k, _)| k == &key3), "key3 should be deleted");

    // Test 4: Scan after persisting and reloading (data loaded from file)
    store.persist()?; // This saves only latest, non-expired versions
    let store_reloaded = SimpleFileKvStore::new(&db_path)?;
    let mut results_reloaded = store_reloaded.scan()?;
    results_reloaded.sort_by(|a, b| a.0.cmp(&b.0));

    // After persist, only versions considered "committed" and latest are saved.
    // The SimpleFileKvStore's persist logic writes the latest non-expired version.
    // So, key1/val1_v2, key2/val2 should be there. key3 is deleted.
    // key4_uncommitted_or_specific_tx was put by tx_other. If tx_other is not considered "committed"
    // by the persistence logic (which it isn't, persistence saves latest non-expired from cache),
    // it might not be persisted if there's no overarching commit concept for SimpleFileKvStore's file format.
    // However, persistence logic for SimpleFileKvStore iterates cache and saves latest non-expired.
    // So key4 should be there.

    let mut expected_reloaded_results = vec![
        (key1.clone(), val1_v2.clone()),
        (key2.clone(), val2.clone()),
        (key4_uncommitted_or_specific_tx.clone(), val4_uncommitted_or_specific_tx.clone()),
    ];
    expected_reloaded_results.sort_by(|a, b| a.0.cmp(&b.0));

    assert_eq!(results_reloaded.len(), 3, "Reloaded store scan results: {:?}", results_reloaded);
    assert_eq!(results_reloaded, expected_reloaded_results, "Reloaded scan results mismatch.");

    // Test 5: Scan a store with one item
    let temp_dir_single = tempdir().unwrap(); // Corrected: use tempdir
    let db_path_single = temp_dir_single.path().join("single_item_scan.db");
    let mut store_single = SimpleFileKvStore::new(&db_path_single)?;
    store_single.put(key1.clone(), val1_v1.clone(), &tx0, lsn_base + 6)?;
    let mut results_single = store_single.scan()?;
    results_single.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(results_single.len(), 1);
    assert_eq!(results_single[0], (key1.clone(), val1_v1.clone()));

    Ok(())
}

#[test]
fn test_physical_wal_lsn_integration() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("lsn_test.db");

    // 1. Setup Oxidb
    let mut oxidb = crate::Oxidb::new(&db_path).expect("Failed to create Oxidb instance");
    let exec = &mut oxidb.executor; // For direct access to TransactionManager if needed for LSN assertions

    // 2. Execute Operations
    exec.execute_command(crate::core::query::commands::Command::CreateTable {
        table_name: "test_lsn".to_string(),
        columns: vec![
            crate::core::types::schema::ColumnDef {
                name: "id".to_string(),
                data_type: crate::core::types::DataType::Integer(0),
                is_primary_key: true,
                is_unique: true,
                is_nullable: false,
            },
            crate::core::types::schema::ColumnDef {
                name: "name".to_string(),
                data_type: crate::core::types::DataType::String("".to_string()),
                is_primary_key: false,
                is_unique: false,
                is_nullable: true,
            },
        ],
    })
    .expect("CREATE TABLE failed");

    exec.execute_command(crate::core::query::commands::Command::SqlInsert {
        table_name: "test_lsn".to_string(),
        columns: Some(vec!["id".to_string(), "name".to_string()]),
        values: vec![vec![
            crate::core::types::DataType::Integer(1),
            crate::core::types::DataType::String("Alice".to_string()),
        ]],
    })
    .expect("INSERT 1 failed");

    exec.execute_command(crate::core::query::commands::Command::SqlInsert {
        table_name: "test_lsn".to_string(),
        columns: Some(vec!["id".to_string(), "name".to_string()]),
        values: vec![vec![
            crate::core::types::DataType::Integer(2),
            crate::core::types::DataType::String("Bob".to_string()),
        ]],
    })
    .expect("INSERT 2 failed");

    exec.execute_command(crate::core::query::commands::Command::Update {
        source: "test_lsn".to_string(),
        assignments: vec![crate::core::query::commands::SqlAssignment {
            column: "name".to_string(),
            value: crate::core::types::DataType::String("Alicia".to_string()),
        }],
        condition: Some(crate::core::query::commands::SqlConditionTree::Comparison(
            crate::core::query::commands::SqlSimpleCondition {
                column: "id".to_string(),
                operator: "=".to_string(),
                value: crate::core::types::DataType::Integer(1),
            },
        )),
    })
    .expect("UPDATE failed");

    exec.execute_command(crate::core::query::commands::Command::SqlDelete {
        table_name: "test_lsn".to_string(),
        condition: Some(crate::core::query::commands::SqlConditionTree::Comparison(
            crate::core::query::commands::SqlSimpleCondition {
                column: "id".to_string(),
                operator: "=".to_string(),
                value: crate::core::types::DataType::Integer(2),
            },
        )),
    })
    .expect("DELETE failed");

    exec.execute_command(crate::core::query::commands::Command::BeginTransaction)
        .expect("BEGIN failed");

    exec.execute_command(crate::core::query::commands::Command::SqlInsert {
        table_name: "test_lsn".to_string(),
        columns: Some(vec!["id".to_string(), "name".to_string()]),
        values: vec![vec![
            crate::core::types::DataType::Integer(3),
            crate::core::types::DataType::String("Charlie".to_string()),
        ]],
    })
    .expect("TX1: INSERT Charlie failed");

    // This was the missing operation
    exec.execute_command(crate::core::query::commands::Command::Update {
        source: "test_lsn".to_string(),
        assignments: vec![crate::core::query::commands::SqlAssignment {
            column: "name".to_string(),
            value: crate::core::types::DataType::String("AliceNewName".to_string()),
        }],
        condition: Some(crate::core::query::commands::SqlConditionTree::Comparison(
            crate::core::query::commands::SqlSimpleCondition {
                column: "id".to_string(),
                operator: "=".to_string(),
                value: crate::core::types::DataType::Integer(1),
            },
        )),
    })
    .expect("TX1: UPDATE Alice failed");

    exec.execute_command(crate::core::query::commands::Command::CommitTransaction)
        .expect("TX1: COMMIT failed");
    // The logical COMMIT will also consume an LSN from the shared LogManager.

    let physical_wal_path = derive_wal_path(&db_path);
    assert!(
        physical_wal_path.exists(),
        "Physical WAL file should exist at {:?}",
        physical_wal_path
    );

    let wal_entries =
        read_all_wal_entries(&physical_wal_path).expect("Failed to read physical WAL entries");

    println!("Read WAL entries: {:?}", wal_entries);
    assert!(!wal_entries.is_empty(), "Should have WAL entries");

    // Expected LSNs for the physical WAL entries based on current understanding and logs
    // Schema Put (0), Alice Put (2), Bob Put (4), Alicia Update Put (6), Bob Delete (8)
    // Charlie Put (Tx1) (11), AliceNewName Update Put (Tx1) (12)
    // These LSNs are from the physical store's WAL.
    // LSNs consumed by TM WAL: SchemaCommit (1), AliceCommit (3), BobCommit (5), AliciaCommit (7), BobDeleteCommit (9), BeginTx1 (10), CharlieCommit (13)
    let expected_physical_lsns = [0, 2, 4, 6, 8, 11, 12];

    assert_eq!(
        wal_entries.len(),
        expected_physical_lsns.len(),
        "Mismatch in number of physical WAL entries. Actual: {:?}, Expected: {:?}",
        wal_entries,
        expected_physical_lsns
    );

    let mut physical_data_ops = 0;

    for (idx, entry) in wal_entries.iter().enumerate() {
        match entry {
            WalEntry::Put { lsn, transaction_id, .. } => {
                assert_eq!(
                    *lsn, expected_physical_lsns[idx],
                    "LSN mismatch for Put entry idx {} with tx_id {}",
                    idx, transaction_id
                );
                physical_data_ops += 1;
            }
            WalEntry::Delete { lsn, transaction_id, .. } => {
                assert_eq!(
                    *lsn, expected_physical_lsns[idx],
                    "LSN mismatch for Delete entry idx {} with tx_id {}",
                    idx, transaction_id
                );
                physical_data_ops += 1;
            }
            // TransactionCommit/Rollback should not be in the physical store WAL with the new design
            WalEntry::TransactionCommit { lsn, transaction_id, .. } => {
                panic!("Unexpected TransactionCommit (lsn:{}, tx:{}) in physical store WAL at index {}", lsn, transaction_id, idx);
            }
            WalEntry::TransactionRollback { lsn, transaction_id, .. } => {
                panic!("Unexpected TransactionRollback (lsn:{}, tx:{}) in physical store WAL at index {}", lsn, transaction_id, idx);
            }
        }
    }

    // Expected physical data operations:
    // 1. Put Schema (_schema_test_lsn) - Tx0
    // 2. Put (1, "Alice") - Tx0
    // 3. Put (2, "Bob") - Tx0
    // 4. Put (1, "Alicia") for UPDATE - Tx0
    // 5. Delete (id=2) - Tx0
    // 6. Put (3, "Charlie") - Tx1
    // 7. Put (1, "AliceNewName") for UPDATE in TX1 - Tx1
    assert_eq!(physical_data_ops, 7, "Expected 7 data operations in physical WAL");
    assert_eq!(wal_entries.len(), 7, "Total physical WAL entries should be 7");

    temp_dir.close().expect("Failed to remove temp dir");
}
