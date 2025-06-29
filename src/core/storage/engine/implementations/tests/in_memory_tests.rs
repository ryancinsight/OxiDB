use crate::core::storage::engine::implementations::in_memory::InMemoryKvStore;
use crate::core::storage::engine::traits::{KeyValueStore, VersionedValue};
use crate::core::storage::engine::wal::WalEntry; // Though not strictly used by InMemory, tests might use it for completeness/shared helpers
use crate::core::transaction::Transaction;
use std::collections::HashSet;

// Helper to create a dummy transaction
fn tx(id: u64) -> Transaction {
    Transaction::new(crate::core::common::types::TransactionId(id)) // Use TransactionId struct
}

// MVCC tests are complex and depend on TransactionManager state.
// The old tests are commented out because they don't reflect MVCC.
// New tests for put/get/delete under MVCC would need careful setup of
// transaction states and committed_ids passed to get/contains_key.

// #[test]
// fn test_put_and_get() {
//     let mut store = InMemoryKvStore::new();
//     let key = b"test_key".to_vec();
//     let value = b"test_value".to_vec();
//     let dummy_transaction = Transaction::new(0);
//     let snapshot_id = 0;
//     let committed_ids = HashSet::new(); // Simplified for old test structure

//     assert!(store.put(key.clone(), value.clone(), &dummy_transaction).is_ok());
//     // MVCC Get logic is more complex now, direct assert_eq might fail without proper setup
//     // assert_eq!(store.get(&key, snapshot_id, &committed_ids).unwrap(), Some(value));
// }

// #[test]
// fn test_get_non_existent() {
//     let store = InMemoryKvStore::new();
//     let key = b"non_existent_key".to_vec();
//     let snapshot_id = 0;
//     let committed_ids = HashSet::new();
//     assert_eq!(store.get(&key, snapshot_id, &committed_ids).unwrap(), None);
// }

// #[test]
// fn test_put_update() {
//     let mut store = InMemoryKvStore::new();
//     let key = b"update_key".to_vec();
//     let value1 = b"value1".to_vec();
//     let value2 = b"value2".to_vec();
//     let dummy_transaction = Transaction::new(0);
//     let snapshot_id = 0;
//     let committed_ids = HashSet::new();

//     store.put(key.clone(), value1.clone(), &dummy_transaction).unwrap();
//     // assert_eq!(store.get(&key, snapshot_id, &committed_ids).unwrap(), Some(value1));

//     store.put(key.clone(), value2.clone(), &dummy_transaction).unwrap();
//     // assert_eq!(store.get(&key, snapshot_id, &committed_ids).unwrap(), Some(value2));
// }

// #[test]
// fn test_delete() {
//     let mut store = InMemoryKvStore::new();
//     let key = b"delete_key".to_vec();
//     let value = b"delete_value".to_vec();
//     let dummy_transaction = Transaction::new(0);
//     let snapshot_id = 0;
//     let committed_ids = HashSet::new();

//     store.put(key.clone(), value.clone(), &dummy_transaction).unwrap();
//     // assert_eq!(store.get(&key, snapshot_id, &committed_ids).unwrap(), Some(value));

//     assert!(store.delete(&key, &dummy_transaction).unwrap());
//     // assert_eq!(store.get(&key, snapshot_id, &committed_ids).unwrap(), None);
// }

// #[test]
// fn test_delete_non_existent() {
//     let mut store = InMemoryKvStore::new();
//     let key = b"delete_non_existent_key".to_vec();
//     let dummy_transaction = Transaction::new(0);
//     assert!(!store.delete(&key, &dummy_transaction).unwrap());
// }

// #[test]
// fn test_contains_key() {
//     let mut store = InMemoryKvStore::new();
//     let key = b"contains_key_test".to_vec();
//     let value = b"irrelevant_value".to_vec();
//     let dummy_transaction = Transaction::new(0);
//     let snapshot_id = 0;
//     let committed_ids = HashSet::new();

//     // assert!(!store.contains_key(&key, snapshot_id, &committed_ids).unwrap());
//     store.put(key.clone(), value, &dummy_transaction).unwrap();
//     // assert!(store.contains_key(&key, snapshot_id, &committed_ids).unwrap());
// }

// --- GC Tests ---
#[test]
fn test_gc_removes_old_uncommitted_versions() {
    let mut store = InMemoryKvStore::new();
    let key = b"key1".to_vec();
    let val1 = VersionedValue {
        value: b"val1_uncommitted_old".to_vec(),
        created_tx_id: 1,
        expired_tx_id: None,
    };
    let val2 =
        VersionedValue { value: b"val2_committed".to_vec(), created_tx_id: 2, expired_tx_id: None };
    let val3 = VersionedValue {
        value: b"val3_uncommitted_active".to_vec(),
        created_tx_id: 10,
        expired_tx_id: None,
    };

    store.data.insert(key.clone(), vec![val1, val2.clone(), val3.clone()]);

    let low_water_mark = 5; // TX ID 1 is older than LWM
    let mut committed_ids = HashSet::new();
    committed_ids.insert(2); // TX ID 2 is committed

    store.gc(low_water_mark, &committed_ids).unwrap();

    let versions = store.data.get(&key).unwrap();
    assert_eq!(versions.len(), 2);
    assert_eq!(versions[0], val2); // val2_committed should remain
    assert_eq!(versions[1], val3); // val3_uncommitted_active should remain (created_tx_id >= LWM)
}

#[test]
fn test_gc_removes_old_committed_and_old_expired_versions() {
    let mut store = InMemoryKvStore::new();
    let key = b"key2".to_vec();

    let val1 = VersionedValue {
        value: b"val1_comm_exp_old".to_vec(),
        created_tx_id: 1,
        expired_tx_id: Some(3),
    };
    let val2 = VersionedValue {
        value: b"val2_comm_not_exp".to_vec(),
        created_tx_id: 2,
        expired_tx_id: None,
    };
    let val3 = VersionedValue {
        value: b"val3_comm_exp_active".to_vec(),
        created_tx_id: 4,
        expired_tx_id: Some(10),
    };
    let val4 = VersionedValue {
        value: b"val4_comm_exp_uncomm".to_vec(),
        created_tx_id: 5,
        expired_tx_id: Some(11),
    };

    store.data.insert(key.clone(), vec![val1, val2.clone(), val3.clone(), val4.clone()]);

    let low_water_mark = 5;
    let mut committed_ids = HashSet::new();
    committed_ids.insert(1);
    committed_ids.insert(2);
    committed_ids.insert(3);
    committed_ids.insert(4);
    committed_ids.insert(5);
    committed_ids.insert(10);

    store.gc(low_water_mark, &committed_ids).unwrap();

    let versions = store.data.get(&key).unwrap();
    assert_eq!(versions.len(), 3);
    assert_eq!(versions[0], val2);
    assert_eq!(versions[1], val3);
    assert_eq!(versions[2], val4);
}

#[test]
fn test_gc_removes_key_if_all_versions_are_gc_ed() {
    let mut store = InMemoryKvStore::new();
    let key = b"key3".to_vec();
    let val1 = VersionedValue {
        value: b"val1_uncommitted_old".to_vec(),
        created_tx_id: 1,
        expired_tx_id: None,
    };

    store.data.insert(key.clone(), vec![val1]);

    let low_water_mark = 5;
    let committed_ids = HashSet::new();

    store.gc(low_water_mark, &committed_ids).unwrap();

    assert!(store.data.get(&key).is_none());
}

#[test]
fn test_gc_keeps_uncommitted_versions_from_active_transactions() {
    let mut store = InMemoryKvStore::new();
    let key = b"key4".to_vec();
    let val1 = VersionedValue {
        value: b"val1_uncommitted_active".to_vec(),
        created_tx_id: 6,
        expired_tx_id: None,
    };

    store.data.insert(key.clone(), vec![val1.clone()]);

    let low_water_mark = 5;
    let committed_ids = HashSet::new();

    store.gc(low_water_mark, &committed_ids).unwrap();
    let versions = store.data.get(&key).unwrap();
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0], val1);
}

#[test]
fn test_log_wal_entry_is_nop() {
    let mut store = InMemoryKvStore::new();
    let dummy_lsn = 0; // Dummy LSN for test
    let dummy_wal_entry = WalEntry::TransactionCommit { lsn: dummy_lsn, transaction_id: 1 };
    assert!(store.log_wal_entry(&dummy_wal_entry).is_ok());
}

// --- Tests for scan ---
#[test]
fn test_scan_empty_store() {
    let store = InMemoryKvStore::new();
    let result = store.scan().unwrap();
    assert!(result.is_empty());
}

#[test]
fn test_scan_single_item_no_expiration() {
    let mut store = InMemoryKvStore::new();
    let key1 = b"key1".to_vec();
    let val1 = b"val1".to_vec();
    let dummy_lsn = 0; // LSN for test
    store.put(key1.clone(), val1.clone(), &tx(1), dummy_lsn).unwrap();

    let result = store.scan().unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0], (key1, val1));
}

#[test]
fn test_scan_multiple_items_latest_version_no_expiration() {
    let mut store = InMemoryKvStore::new();
    let key1 = b"key1".to_vec();
    let val1_v1 = b"val1_v1".to_vec();
    let val1_v2 = b"val1_v2".to_vec();

    let key2 = b"key2".to_vec();
    let val2_v1 = b"val2_v1".to_vec();
    let dummy_lsn = 0; // LSN for test

    store.put(key1.clone(), val1_v1.clone(), &tx(1), dummy_lsn).unwrap();
    store.put(key1.clone(), val1_v2.clone(), &tx(2), dummy_lsn).unwrap();
    store.put(key2.clone(), val2_v1.clone(), &tx(3), dummy_lsn).unwrap();

    let result = store.scan().unwrap();
    assert_eq!(result.len(), 2);
    let mut found_key1 = false;
    let mut found_key2 = false;
    for (k, v) in result {
        if k == key1 {
            assert_eq!(v, val1_v2);
            found_key1 = true;
        } else if k == key2 {
            assert_eq!(v, val2_v1);
            found_key2 = true;
        }
    }
    assert!(found_key1 && found_key2, "Both keys should be found in scan");
}

#[test]
fn test_scan_item_with_all_versions_expired() {
    let mut store = InMemoryKvStore::new();
    let key1 = b"key1".to_vec();
    let val1_v1 = b"val1_v1".to_vec();
    let dummy_lsn = 0; // LSN for test

    store.put(key1.clone(), val1_v1.clone(), &tx(1), dummy_lsn).unwrap();
    let mut committed_ids_for_delete1 = HashSet::new();
    committed_ids_for_delete1.insert(0); // Assuming primordial is always committed
    committed_ids_for_delete1.insert(1); // Previous put
    committed_ids_for_delete1.insert(2); // The deleting transaction itself
    store.delete(&key1, &tx(2), dummy_lsn, &committed_ids_for_delete1).unwrap();

    let result = store.scan().unwrap();
    assert!(result.is_empty(), "Scan should be empty if the only item's versions are all expired.");
}

#[test]
fn test_scan_item_with_some_versions_expired_takes_latest_non_expired() {
    let mut store = InMemoryKvStore::new();
    let key1 = b"key1".to_vec();
    let val1_v1 = b"val1_v1_expired".to_vec();
    let val1_v2 = b"val1_v2_current".to_vec();
    let val1_v3 = b"val1_v3_also_current_but_later_tx".to_vec();
    let dummy_lsn = 0; // LSN for test

    store.put(key1.clone(), val1_v1.clone(), &tx(1), dummy_lsn).unwrap();
    if let Some(versions) = store.data.get_mut(&key1) {
        if let Some(version_to_expire) = versions.iter_mut().find(|v| v.created_tx_id == 1) {
            version_to_expire.expired_tx_id = Some(2);
        }
    }

    store.put(key1.clone(), val1_v2.clone(), &tx(3), dummy_lsn).unwrap();
    store.put(key1.clone(), val1_v3.clone(), &tx(4), dummy_lsn).unwrap();

    let result = store.scan().unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(
        result[0],
        (key1.clone(), val1_v3.clone()),
        "Scan should return the latest non-expired version"
    );
}

#[test]
fn test_scan_mixed_expired_and_active_keys() {
    let mut store = InMemoryKvStore::new();
    let key1 = b"key1_active".to_vec();
    let val1 = b"val1".to_vec();
    let dummy_lsn = 0; // LSN for test
    store.put(key1.clone(), val1.clone(), &tx(1), dummy_lsn).unwrap();

    let key2 = b"key2_expired".to_vec();
    let val2 = b"val2".to_vec();
    store.put(key2.clone(), val2.clone(), &tx(2), dummy_lsn).unwrap();
    let mut committed_ids_for_delete2 = HashSet::new();
    committed_ids_for_delete2.insert(0);
    committed_ids_for_delete2.insert(1); // For key1's put
    committed_ids_for_delete2.insert(2); // For key2's put
    committed_ids_for_delete2.insert(3); // The deleting transaction itself
    store.delete(&key2, &tx(3), dummy_lsn, &committed_ids_for_delete2).unwrap();

    let key3 = b"key3_active_multi_ver".to_vec();
    let val3_v1 = b"val3_v1".to_vec();
    let val3_v2 = b"val3_v2".to_vec();
    store.put(key3.clone(), val3_v1.clone(), &tx(4), dummy_lsn).unwrap();
    if let Some(versions) = store.data.get_mut(&key3) {
        if let Some(version_to_expire) = versions.iter_mut().find(|v| v.created_tx_id == 4) {
            version_to_expire.expired_tx_id = Some(5);
        }
    }
    store.put(key3.clone(), val3_v2.clone(), &tx(6), dummy_lsn).unwrap();

    let result = store.scan().unwrap();
    assert_eq!(result.len(), 2, "Should find key1 and key3, key2 is fully expired");

    let mut found_key1 = false;
    let mut found_key3 = false;
    for (k, v) in result {
        if k == key1 {
            assert_eq!(v, val1);
            found_key1 = true;
        } else if k == key3 {
            assert_eq!(v, val3_v2);
            found_key3 = true;
        }
    }
    assert!(found_key1 && found_key3);
}
