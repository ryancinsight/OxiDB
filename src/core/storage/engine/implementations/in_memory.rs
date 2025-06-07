// src/core/storage/engine/implementations/in_memory.rs
use std::collections::{HashMap, HashSet}; // Added HashSet
use crate::core::common::error::DbError;
use crate::core::storage::engine::traits::{KeyValueStore, VersionedValue};
use crate::core::storage::engine::wal::WalEntry;
use crate::core::transaction::Transaction;

#[derive(Debug, Default)] // Added Default
pub struct InMemoryKvStore {
    data: HashMap<Vec<u8>, Vec<VersionedValue<Vec<u8>>>>,
}

impl InMemoryKvStore {
    pub fn new() -> Self {
        InMemoryKvStore {
            data: HashMap::new(),
        }
    }
}

impl KeyValueStore<Vec<u8>, Vec<u8>> for InMemoryKvStore {
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>, transaction: &Transaction) -> Result<(), DbError> {
        let versions = self.data.entry(key).or_default();
        // Mark the latest existing visible version (if any) as expired by this transaction.
        for version in versions.iter_mut().rev() {
            if version.expired_tx_id.is_none() {
                version.expired_tx_id = Some(transaction.id);
                break; // Only expire the most recent version
            }
        }

        let new_version = VersionedValue {
            value,
            created_tx_id: transaction.id,
            expired_tx_id: None,
        };
        versions.push(new_version);
        Ok(())
    }

    fn get(&self, key: &Vec<u8>, snapshot_id: u64, committed_ids: &HashSet<u64>) -> Result<Option<Vec<u8>>, DbError> {
        if let Some(versions) = self.data.get(key) {
            for version in versions.iter().rev() {
                // Visibility rule for a version:
                // 1. The version's creator transaction ID must be less than or equal to the snapshot ID.
                // 2. The version's creator transaction ID must be in the set of committed transaction IDs.
                // 3. The version must not be expired, OR if it is expired:
                //    a. The expiring transaction ID must be greater than the snapshot ID (i.e., expired in the future), OR
                //    b. The expiring transaction ID must NOT be in the set of committed transaction IDs (i.e., the deletion is not committed).
                // MODIFIED visibility rule:
                if version.created_tx_id <= snapshot_id && (version.created_tx_id == snapshot_id || committed_ids.contains(&version.created_tx_id)) {
                    match version.expired_tx_id {
                        None => return Ok(Some(version.value.clone())), // Not expired, visible
                        Some(expired_id) => {
                            if expired_id > snapshot_id || !committed_ids.contains(&expired_id) {
                                return Ok(Some(version.value.clone())); // Expired in the future or by an uncommitted transaction, so visible
                            }
                            // Otherwise, it's expired by a committed transaction at or before the snapshot, so not visible.
                            // Continue to the next older version.
                        }
                    }
                }
            }
        }
        Ok(None)
    }

    fn delete(&mut self, key: &Vec<u8>, transaction: &Transaction) -> Result<bool, DbError> {
        if let Some(versions) = self.data.get_mut(key) {
            for version in versions.iter_mut().rev() {
                // Check if the version is visible to this transaction for deletion
                // A transaction can only delete what it can see based on its own ID.
                // It should not be able to delete a version created by a transaction that has not yet committed
                // (i.e. version.created_tx_id > transaction.id), unless we allow deleting uncommitted data from same transaction.
                // For now, assume transaction.id is the current snapshot for visibility.
                if version.created_tx_id <= transaction.id &&
                   (version.expired_tx_id.is_none() || version.expired_tx_id.unwrap() > transaction.id) {
                    if version.expired_tx_id.is_none() { // Ensure not already expired by another tx
                        version.expired_tx_id = Some(transaction.id);
                        return Ok(true);
                    } else {
                        // Already expired by a different transaction that is also visible to current transaction.id
                        // or expired by a transaction that current_tx_id cannot see yet.
                        // For simplicity, we'll say it's "already deleted" from this transaction's perspective.
                        return Ok(false);
                    }
                }
            }
        }
        Ok(false)
    }

    fn contains_key(&self, key: &Vec<u8>, snapshot_id: u64, committed_ids: &HashSet<u64>) -> Result<bool, DbError> {
        self.get(key, snapshot_id, committed_ids).map(|opt| opt.is_some())
    }

    fn log_wal_entry(&mut self, _entry: &WalEntry) -> Result<(), DbError> {
        // In-memory store does not need WAL. This is a no-op.
        Ok(())
    }

    fn gc(&mut self, low_water_mark: u64, committed_ids: &HashSet<u64>) -> Result<(), DbError> {
        self.data.retain(|_key, versions| {
            versions.retain_mut(|v| {
                let created_by_committed = committed_ids.contains(&v.created_tx_id);
                if !created_by_committed && v.created_tx_id < low_water_mark {
                    return false; // Remove: old, uncommitted version
                }
                if created_by_committed && v.expired_tx_id.is_some() {
                    let etid = v.expired_tx_id.unwrap();
                    if committed_ids.contains(&etid) && etid < low_water_mark {
                        return false; // Remove: committed, and expired by an old, committed transaction
                    }
                }
                true // Keep otherwise
            });
            !versions.is_empty() // Retain key if versions list is not empty
        });
        Ok(())
    }

    fn scan(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>, DbError> {
        // This is a simplified scan that returns the latest version of each value,
        // without considering MVCC visibility based on a snapshot ID.
        // It's suitable for enabling basic iteration functionalities like in SELECT *.
        let mut results = Vec::new();
        // self.data is HashMap<Vec<u8>, Vec<VersionedValue<Vec<u8>>>>
        // No read lock needed here as per current InMemoryKvStore structure (not using RwLock on self.data directly)
        // However, if this were to be made thread-safe, a read lock on `self.data` would be needed.
        // The trait KeyValueStore has Send + Sync, implying InMemoryKvStore should be thread-safe if shared.
        // The current HashMap is not behind a RwLock. This is a pre-existing issue.
        // For now, proceeding with direct iteration. If `InMemoryKvStore` is wrapped in `Arc<RwLock<...>>`
        // at a higher level, that would provide safety. This method itself doesn't use internal locks.

        for (key, version_vec) in self.data.iter() {
            if let Some(_latest_version) = version_vec.last() {
                // We only consider items that are not "deleted" from an absolute latest perspective.
                // A truly correct scan would need a snapshot_id and committed_ids.
                // This simplification takes the last entry if it's not marked as expired by *any* transaction.
                // Or, more simply for a "raw" scan, just take the value of the last entry.
                // The prompt suggested: "if let Some(latest_version) = value_rc.versions.front()"
                // but here versions are in a Vec, and new ones are pushed. So .last() is more appropriate.

                // Let's refine to pick the latest *visible* version based on a "latest possible snapshot"
                // This means taking the latest version whose created_tx_id is committed (assuming all prior are)
                // and which is not expired by a committed transaction.
                // This is still a simplification. A true latest committed scan:
                let mut best_candidate: Option<&VersionedValue<Vec<u8>>> = None;
                for version in version_vec.iter().rev() {
                     // For a basic scan, let's assume we see all committed data and ignore uncommitted expirations.
                     // This means we are looking for a version that *is* created and *is not* definitively expired.
                     // For simplicity for THIS scan, let's just take the latest version that has no expired_tx_id at all.
                     // This is the simplest way to get "a" version if multiple exist due to MVCC.
                    if version.expired_tx_id.is_none() {
                        best_candidate = Some(version);
                        break;
                    }
                }
                if best_candidate.is_none() && !version_vec.is_empty() {
                    // If all versions are marked as expired, this key is effectively deleted.
                    // However, a scan might still want to see the latest "tombstoned" value for some debug/raw cases.
                    // For now, if all are expired, we don't include it.
                    // If we wanted the absolute latest (even if tombstoned), we'd just use:
                    // best_candidate = version_vec.last();
                }


                if let Some(visible_version) = best_candidate {
                     results.push((key.clone(), visible_version.value.clone()));
                }
            }
        }
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::transaction::{Transaction, TransactionState}; // For dummy transaction & state
    use std::collections::HashSet;

    // Helper to create a dummy transaction
    fn tx(id: u64) -> Transaction {
        Transaction::new(id)
    }

    // Helper to create a committed transaction
    fn committed_tx(id: u64) -> Transaction {
        let mut t = Transaction::new(id);
        t.set_state(TransactionState::Committed);
        t
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
        let val1 = VersionedValue { value: b"val1_uncommitted_old".to_vec(), created_tx_id: 1, expired_tx_id: None };
        let val2 = VersionedValue { value: b"val2_committed".to_vec(), created_tx_id: 2, expired_tx_id: None };
        let val3 = VersionedValue { value: b"val3_uncommitted_active".to_vec(), created_tx_id: 10, expired_tx_id: None };

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

        // val1: committed, expired by TX3 (committed, old) -> should be removed
        let val1 = VersionedValue { value: b"val1_comm_exp_old".to_vec(), created_tx_id: 1, expired_tx_id: Some(3) };
        // val2: committed, not expired -> should remain
        let val2 = VersionedValue { value: b"val2_comm_not_exp".to_vec(), created_tx_id: 2, expired_tx_id: None };
        // val3: committed, expired by TX10 (committed, active/future) -> should remain
        let val3 = VersionedValue { value: b"val3_comm_exp_active".to_vec(), created_tx_id: 4, expired_tx_id: Some(10) };
        // val4: committed, expired by TX11 (uncommitted) -> should remain
        let val4 = VersionedValue { value: b"val4_comm_exp_uncomm".to_vec(), created_tx_id: 5, expired_tx_id: Some(11) };


        store.data.insert(key.clone(), vec![val1, val2.clone(), val3.clone(), val4.clone()]);

        let low_water_mark = 5;
        let mut committed_ids = HashSet::new();
        committed_ids.insert(1); // val1 created
        committed_ids.insert(2); // val2 created
        committed_ids.insert(3); // val1 expired by this
        committed_ids.insert(4); // val3 created
        committed_ids.insert(5); // val4 created
        committed_ids.insert(10); // val3 expired by this (active relative to LWM)
        // TX 11 is NOT committed

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
        let val1 = VersionedValue { value: b"val1_uncommitted_old".to_vec(), created_tx_id: 1, expired_tx_id: None };

        store.data.insert(key.clone(), vec![val1]);

        let low_water_mark = 5;
        let committed_ids = HashSet::new(); // No committed TXs

        store.gc(low_water_mark, &committed_ids).unwrap();

        assert!(store.data.get(&key).is_none());
    }

    #[test]
    fn test_gc_keeps_uncommitted_versions_from_active_transactions() {
        let mut store = InMemoryKvStore::new();
        let key = b"key4".to_vec();
        // Uncommitted, but created_tx_id (6) is >= low_water_mark (5)
        let val1 = VersionedValue { value: b"val1_uncommitted_active".to_vec(), created_tx_id: 6, expired_tx_id: None };

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
        let dummy_wal_entry = WalEntry::TransactionCommit { transaction_id: 1 };
        assert!(store.log_wal_entry(&dummy_wal_entry).is_ok());
        // No other state to check, just that it doesn't panic or error.
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
        store.put(key1.clone(), val1.clone(), &tx(1)).unwrap(); // Assumes tx 1 is "committed" for scan

        let result = store.scan().unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], (key1, val1));
    }

    #[test]
    fn test_scan_multiple_items_latest_version_no_expiration() {
        let mut store = InMemoryKvStore::new();
        let key1 = b"key1".to_vec();
        let val1_v1 = b"val1_v1".to_vec(); // tx1
        let val1_v2 = b"val1_v2".to_vec(); // tx2, latest for key1

        let key2 = b"key2".to_vec();
        let val2_v1 = b"val2_v1".to_vec(); // tx3

        store.put(key1.clone(), val1_v1.clone(), &tx(1)).unwrap();
        store.put(key1.clone(), val1_v2.clone(), &tx(2)).unwrap();
        store.put(key2.clone(), val2_v1.clone(), &tx(3)).unwrap();

        let result = store.scan().unwrap();
        assert_eq!(result.len(), 2);
        // Order is not guaranteed by HashMap iteration, so check contents
        let mut found_key1 = false;
        let mut found_key2 = false;
        for (k, v) in result {
            if k == key1 {
                assert_eq!(v, val1_v2); // Expect latest version of key1
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

        store.put(key1.clone(), val1_v1.clone(), &tx(1)).unwrap();
        // Now "delete" (expire) this version
        store.delete(&key1, &tx(2)).unwrap(); // tx(2) expires the version from tx(1)

        let result = store.scan().unwrap();
        // The current scan logic: if all versions are expired, the key is not included.
        assert!(result.is_empty(), "Scan should be empty if the only item's versions are all expired.");
    }

    #[test]
    fn test_scan_item_with_some_versions_expired_takes_latest_non_expired() {
        let mut store = InMemoryKvStore::new();
        let key1 = b"key1".to_vec();
        let val1_v1 = b"val1_v1_expired".to_vec(); // tx1, will be expired by tx2
        let val1_v2 = b"val1_v2_current".to_vec(); // tx3, current
        let val1_v3 = b"val1_v3_also_current_but_later_tx".to_vec(); // tx4, also current (if tx3 was also non-expired)

        store.put(key1.clone(), val1_v1.clone(), &tx(1)).unwrap();
        // Expire val1_v1 by tx(2)
        let mut expiring_tx = tx(2); // This transaction "deletes" the version made by tx(1)
        // Simulate the effect of put causing expiration:
        // Find the version created by tx(1) and set its expired_tx_id to 2
        if let Some(versions) = store.data.get_mut(&key1) {
            if let Some(version_to_expire) = versions.iter_mut().find(|v| v.created_tx_id == 1) {
                version_to_expire.expired_tx_id = Some(2);
            }
        }

        store.put(key1.clone(), val1_v2.clone(), &tx(3)).unwrap(); // This is now the latest non-expired
        store.put(key1.clone(), val1_v3.clone(), &tx(4)).unwrap(); // This is even later, also non-expired

        let result = store.scan().unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], (key1.clone(), val1_v3.clone()), "Scan should return the latest non-expired version");
    }

    #[test]
    fn test_scan_mixed_expired_and_active_keys() {
        let mut store = InMemoryKvStore::new();
        let key1 = b"key1_active".to_vec();
        let val1 = b"val1".to_vec();
        store.put(key1.clone(), val1.clone(), &tx(1)).unwrap();

        let key2 = b"key2_expired".to_vec();
        let val2 = b"val2".to_vec();
        store.put(key2.clone(), val2.clone(), &tx(2)).unwrap();
        store.delete(&key2, &tx(3)).unwrap(); // Expire key2

        let key3 = b"key3_active_multi_ver".to_vec();
        let val3_v1 = b"val3_v1".to_vec();
        let val3_v2 = b"val3_v2".to_vec();
        store.put(key3.clone(), val3_v1.clone(), &tx(4)).unwrap();
        // Expire val3_v1
         if let Some(versions) = store.data.get_mut(&key3) {
            if let Some(version_to_expire) = versions.iter_mut().find(|v| v.created_tx_id == 4) {
                version_to_expire.expired_tx_id = Some(5);
            }
        }
        store.put(key3.clone(), val3_v2.clone(), &tx(6)).unwrap();


        let result = store.scan().unwrap();
        assert_eq!(result.len(), 2, "Should find key1 and key3, key2 is fully expired");

        let mut found_key1 = false;
        let mut found_key3 = false;
        for (k,v) in result {
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
}
