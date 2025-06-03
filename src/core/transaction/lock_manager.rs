// src/core/transaction/lock_manager.rs
use std::collections::{HashMap, HashSet};
use crate::core::common::error::DbError;

/// Represents the type of lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockType {
    /// Shared lock (read lock).
    Shared,
    /// Exclusive lock (write lock).
    Exclusive,
}

/// Represents the key for a lock in the lock table.
pub type LockTableKey = Vec<u8>;

/// Represents a request for a lock.
#[derive(Debug, Clone, PartialEq)]
pub struct LockRequest {
    /// The ID of the transaction requesting the lock.
    pub transaction_id: u64,
    /// The type of lock being requested.
    pub mode: LockType,
}

/// Manages locks for transactions.
#[derive(Debug)]
pub struct LockManager {
    /// Stores the lock queue for each resource.
    /// Key: LockTableKey (e.g., table name, row ID)
    /// Value: Vec<LockRequest> (queue of lock requests for that resource)
    lock_table: HashMap<LockTableKey, Vec<LockRequest>>,
    /// Stores the set of locks held by each transaction.
    /// Key: Transaction ID (u64)
    /// Value: HashSet<LockTableKey> (set of resources locked by the transaction)
    transaction_locks: HashMap<u64, HashSet<LockTableKey>>,
}

impl LockManager {
    /// Creates a new, empty LockManager.
    pub fn new() -> Self {
        LockManager {
            lock_table: HashMap::new(),
            transaction_locks: HashMap::new(),
        }
    }

    pub fn acquire_lock(&mut self, transaction_id: u64, key: &LockTableKey, requested_mode: LockType) -> Result<(), DbError> {
        let key_specific_locks = self.lock_table.entry(key.clone()).or_default();

        // Check for conflicting locks held by *other* transactions
        for existing_lock in key_specific_locks.iter() {
            if existing_lock.transaction_id != transaction_id {
                // Conflict if an existing lock from another transaction is Exclusive
                if existing_lock.mode == LockType::Exclusive {
                    return Err(DbError::LockConflict { key: key.clone(), current_tx: transaction_id, locked_by_tx: Some(existing_lock.transaction_id) });
                }
                // Conflict if requesting Exclusive and an existing lock from another transaction is Shared
                if requested_mode == LockType::Exclusive && existing_lock.mode == LockType::Shared {
                    return Err(DbError::LockConflict { key: key.clone(), current_tx: transaction_id, locked_by_tx: Some(existing_lock.transaction_id) });
                }
            }
        }

        // If we reach here, no conflicting locks from *other* transactions.
        // Now, manage locks for the *current* transaction.
        
        let mut tx_already_had_exclusive = false;
        let mut tx_had_shared_only = false; // Not strictly needed by the provided logic, but kept for clarity if logic evolves

        // Check existing locks for *this* transaction and remove them.
        // We will re-add the appropriate lock (potentially upgraded).
        key_specific_locks.retain(|lock| {
            if lock.transaction_id == transaction_id {
                match lock.mode {
                    LockType::Exclusive => tx_already_had_exclusive = true,
                    LockType::Shared => tx_had_shared_only = true,
                }
                false // Remove current transaction's existing lock(s) on this key
            } else {
                true // Keep locks from other transactions
            }
        });

        let final_mode_to_add: LockType;

        if tx_already_had_exclusive {
            // If TX already held Exclusive, it can continue to hold Exclusive.
            // Or if it's requesting Shared, it still holds Exclusive (strongest lock prevails).
            final_mode_to_add = LockType::Exclusive;
        } else if requested_mode == LockType::Exclusive {
            // Requesting Exclusive. It might have held Shared or nothing.
            // Since no *other* TX conflicts, it can acquire Exclusive.
            final_mode_to_add = LockType::Exclusive;
        } else { // Requested Shared
            // Requesting Shared. It might have held Shared or nothing.
            // No conflict from others. Can acquire Shared.
            final_mode_to_add = LockType::Shared;
        }
        
        key_specific_locks.push(LockRequest { transaction_id, mode: final_mode_to_add }); // LockType is Copy

        // Update transaction_locks map to reflect that this transaction holds a lock on this key.
        self.transaction_locks.entry(transaction_id).or_default().insert(key.clone());
        
        Ok(())
    }

    pub fn release_locks(&mut self, transaction_id: u64) {
        if let Some(locked_keys_for_tx) = self.transaction_locks.remove(&transaction_id) {
            for key in locked_keys_for_tx {
                if let Some(key_specific_locks) = self.lock_table.get_mut(&key) {
                    // Remove all lock requests for this transaction_id on this key.
                    // It's possible a transaction might have multiple (e.g., if logic allowed shared then exclusive, though current acquire_lock simplifies this).
                    // Retain ensures any such duplicates are removed.
                    key_specific_locks.retain(|req| req.transaction_id != transaction_id);
                    
                    // If no more locks are held on this key by any transaction, remove the key entry from the lock_table.
                    if key_specific_locks.is_empty() {
                        self.lock_table.remove(&key);
                    }
                }
            }
        }
        // The transaction_id is already removed from transaction_locks by .remove() above.
    }
}

#[cfg(test)]
mod tests {
    use super::*; // Imports LockManager, LockType, LockTableKey, LockRequest
    use crate::core::common::error::DbError; 

    #[test]
    fn test_new_lock_manager() {
        let manager = LockManager::new();
        assert!(manager.lock_table.is_empty());
        assert!(manager.transaction_locks.is_empty());
    }

    #[test]
    fn test_acquire_shared_lock_multiple_tx() {
        let mut manager = LockManager::new();
        let key1: LockTableKey = b"key1".to_vec();
        
        assert!(manager.acquire_lock(1, &key1, LockType::Shared).is_ok());
        assert!(manager.acquire_lock(2, &key1, LockType::Shared).is_ok());
        
        assert_eq!(manager.lock_table.get(&key1).map_or(0, |v| v.len()), 2);
        assert!(manager.transaction_locks.get(&1).unwrap().contains(&key1));
        assert!(manager.transaction_locks.get(&2).unwrap().contains(&key1));
        assert_eq!(manager.lock_table.get(&key1).unwrap()[0].mode, LockType::Shared);
        assert_eq!(manager.lock_table.get(&key1).unwrap()[1].mode, LockType::Shared);
    }

    #[test]
    fn test_acquire_shared_lock_same_tx_multiple_times() {
        let mut manager = LockManager::new();
        let key1: LockTableKey = b"key1".to_vec();

        assert!(manager.acquire_lock(1, &key1, LockType::Shared).is_ok());
        // Current logic removes previous lock and adds new one. If it's same type, effect is one lock.
        assert!(manager.acquire_lock(1, &key1, LockType::Shared).is_ok()); 
        
        assert_eq!(manager.lock_table.get(&key1).map_or(0, |v| v.len()), 1);
        assert_eq!(manager.lock_table.get(&key1).unwrap()[0].transaction_id, 1);
        assert_eq!(manager.lock_table.get(&key1).unwrap()[0].mode, LockType::Shared);
        assert!(manager.transaction_locks.get(&1).unwrap().contains(&key1));
    }

    #[test]
    fn test_acquire_exclusive_lock_success() {
        let mut manager = LockManager::new();
        let key1: LockTableKey = b"key1".to_vec();

        assert!(manager.acquire_lock(1, &key1, LockType::Exclusive).is_ok());
        assert_eq!(manager.lock_table.get(&key1).map_or(0, |v| v.len()), 1);
        assert_eq!(manager.lock_table.get(&key1).unwrap()[0].mode, LockType::Exclusive);
        assert!(manager.transaction_locks.get(&1).unwrap().contains(&key1));
    }

    #[test]
    fn test_acquire_exclusive_lock_conflict_with_existing_exclusive() {
        let mut manager = LockManager::new();
        let key1: LockTableKey = b"key1".to_vec();

        assert!(manager.acquire_lock(1, &key1, LockType::Exclusive).is_ok());
        
        match manager.acquire_lock(2, &key1, LockType::Exclusive) {
            Err(DbError::LockConflict { key, current_tx, locked_by_tx }) => {
                assert_eq!(key, key1);
                assert_eq!(current_tx, 2);
                assert_eq!(locked_by_tx, Some(1));
            }
            res => panic!("Expected LockConflict, got {:?}", res),
        }
    }

    #[test]
    fn test_acquire_shared_lock_conflict_with_existing_exclusive() {
        let mut manager = LockManager::new();
        let key1: LockTableKey = b"key1".to_vec();

        assert!(manager.acquire_lock(1, &key1, LockType::Exclusive).is_ok());

        match manager.acquire_lock(2, &key1, LockType::Shared) {
            Err(DbError::LockConflict { key, current_tx, locked_by_tx }) => {
                assert_eq!(key, key1);
                assert_eq!(current_tx, 2);
                assert_eq!(locked_by_tx, Some(1));
            }
            res => panic!("Expected LockConflict, got {:?}", res),
        }
    }
    
    #[test]
    fn test_acquire_exclusive_lock_same_tx_multiple_times() {
        let mut manager = LockManager::new();
        let key1: LockTableKey = b"key1".to_vec();

        assert!(manager.acquire_lock(1, &key1, LockType::Exclusive).is_ok());
        // Acquiring exclusive again by the same transaction should succeed and keep one exclusive lock
        assert!(manager.acquire_lock(1, &key1, LockType::Exclusive).is_ok());
        
        assert_eq!(manager.lock_table.get(&key1).map_or(0, |v| v.len()), 1);
        assert_eq!(manager.lock_table.get(&key1).unwrap()[0].transaction_id, 1);
        assert_eq!(manager.lock_table.get(&key1).unwrap()[0].mode, LockType::Exclusive);
        assert!(manager.transaction_locks.get(&1).unwrap().contains(&key1));
    }

    #[test]
    fn test_shared_lock_allows_other_shared() {
        let mut manager = LockManager::new();
        let key1: LockTableKey = b"key1".to_vec();

        assert!(manager.acquire_lock(1, &key1, LockType::Shared).is_ok());
        assert!(manager.acquire_lock(2, &key1, LockType::Shared).is_ok()); // TX2 gets Shared too

        assert_eq!(manager.lock_table.get(&key1).map_or(0, |v| v.len()), 2);
    }

    #[test]
    fn test_acquire_exclusive_lock_conflict_with_existing_shared() {
        let mut manager = LockManager::new();
        let key1: LockTableKey = b"key1".to_vec();

        assert!(manager.acquire_lock(1, &key1, LockType::Shared).is_ok());
        assert!(manager.acquire_lock(2, &key1, LockType::Shared).is_ok()); // TX2 also gets Shared

        // TX3 tries to get Exclusive
        match manager.acquire_lock(3, &key1, LockType::Exclusive) {
            Err(DbError::LockConflict { key, current_tx, locked_by_tx }) => {
                assert_eq!(key, key1);
                assert_eq!(current_tx, 3);
                // locked_by_tx could be Some(1) or Some(2) depending on iteration order.
                // Check if it's one of them.
                assert!(locked_by_tx == Some(1) || locked_by_tx == Some(2));
            }
            res => panic!("Expected LockConflict, got {:?}", res),
        }
    }
    
    #[test]
    fn test_lock_upgrade_shared_to_exclusive_by_same_tx() {
        let mut manager = LockManager::new();
        let key1: LockTableKey = b"key1".to_vec();

        // TX 1 acquires Shared
        assert!(manager.acquire_lock(1, &key1, LockType::Shared).is_ok());
        assert_eq!(manager.lock_table.get(&key1).unwrap()[0].mode, LockType::Shared);
        assert_eq!(manager.lock_table.get(&key1).unwrap()[0].transaction_id, 1);

        // TX 1 upgrades to Exclusive
        assert!(manager.acquire_lock(1, &key1, LockType::Exclusive).is_ok());
        assert_eq!(manager.lock_table.get(&key1).map_or(0, |v| v.len()), 1); 
        assert_eq!(manager.lock_table.get(&key1).unwrap()[0].mode, LockType::Exclusive);
        assert_eq!(manager.lock_table.get(&key1).unwrap()[0].transaction_id, 1);
    }

    #[test]
    fn test_lock_upgrade_shared_to_exclusive_conflict_with_other_shared() {
        let mut manager = LockManager::new();
        let key1: LockTableKey = b"key1".to_vec();

        assert!(manager.acquire_lock(1, &key1, LockType::Shared).is_ok());
        assert!(manager.acquire_lock(2, &key1, LockType::Shared).is_ok()); // TX2 also has Shared

        // TX1 tries to upgrade to Exclusive, should fail due to TX2's Shared lock
        match manager.acquire_lock(1, &key1, LockType::Exclusive) {
            Err(DbError::LockConflict { key, current_tx, locked_by_tx }) => {
                assert_eq!(key, key1);
                assert_eq!(current_tx, 1);
                assert_eq!(locked_by_tx, Some(2)); // Conflict with TX2
            }
            res => panic!("Expected LockConflict, got {:?}", res),
        }
         // Check state: TX1 should still hold its original Shared lock, TX2 also holds Shared
        let locks_on_key1 = manager.lock_table.get(&key1).unwrap();
        assert_eq!(locks_on_key1.len(), 2);
        assert!(locks_on_key1.iter().any(|l| l.transaction_id == 1 && l.mode == LockType::Shared));
        assert!(locks_on_key1.iter().any(|l| l.transaction_id == 2 && l.mode == LockType::Shared));
    }


    #[test]
    fn test_lock_request_exclusive_while_holding_exclusive_same_tx() {
        // If TX holds Exclusive, and requests Exclusive again, it keeps Exclusive.
        let mut manager = LockManager::new();
        let key1: LockTableKey = b"key1".to_vec();

        assert!(manager.acquire_lock(1, &key1, LockType::Exclusive).is_ok());
        assert_eq!(manager.lock_table.get(&key1).unwrap()[0].mode, LockType::Exclusive);

        assert!(manager.acquire_lock(1, &key1, LockType::Exclusive).is_ok()); // Request exclusive again
        assert_eq!(manager.lock_table.get(&key1).map_or(0, |v| v.len()), 1);
        assert_eq!(manager.lock_table.get(&key1).unwrap()[0].mode, LockType::Exclusive); 
    }
    
    #[test]
    fn test_lock_request_shared_while_holding_exclusive_same_tx() {
        // If TX holds Exclusive, and requests Shared, it effectively keeps Exclusive.
        let mut manager = LockManager::new();
        let key1: LockTableKey = b"key1".to_vec();

        assert!(manager.acquire_lock(1, &key1, LockType::Exclusive).is_ok());
        assert_eq!(manager.lock_table.get(&key1).unwrap()[0].mode, LockType::Exclusive);

        assert!(manager.acquire_lock(1, &key1, LockType::Shared).is_ok()); // Request shared
        assert_eq!(manager.lock_table.get(&key1).map_or(0, |v| v.len()), 1);
        assert_eq!(manager.lock_table.get(&key1).unwrap()[0].mode, LockType::Exclusive); // Still Exclusive
    }

    #[test]
    fn test_release_locks_basic_and_cleanup() {
        let mut manager = LockManager::new();
        let key1: LockTableKey = b"key1".to_vec();
        let key2: LockTableKey = b"key2".to_vec();

        manager.acquire_lock(1, &key1, LockType::Shared).unwrap();
        manager.acquire_lock(1, &key2, LockType::Exclusive).unwrap();
        manager.acquire_lock(2, &key1, LockType::Shared).unwrap(); 

        manager.release_locks(1);

        assert!(manager.transaction_locks.get(&1).is_none(), "TX1 should have no locks in transaction_locks");
        assert!(manager.lock_table.get(&key2).is_none(), "Key2 should be removed from lock_table as TX1 was the only one holding its lock");
        
        // Key1 should still be locked by TX2
        let key1_locks = manager.lock_table.get(&key1).expect("Key1 should still be in lock_table");
        assert_eq!(key1_locks.len(), 1, "Key1 should only have one lock remaining (TX2's)");
        assert_eq!(key1_locks[0].transaction_id, 2);
        assert_eq!(key1_locks[0].mode, LockType::Shared);
        assert!(manager.transaction_locks.get(&2).unwrap().contains(&key1), "TX2 should still list key1 in its locks");
    }

    #[test]
    fn test_release_locks_for_non_existent_transaction() {
        let mut manager = LockManager::new();
        let key1: LockTableKey = b"key1".to_vec();
        manager.acquire_lock(1, &key1, LockType::Shared).unwrap();
        
        manager.release_locks(99); // TX 99 holds no locks

        assert_eq!(manager.lock_table.get(&key1).map_or(0, |v| v.len()), 1); // TX1's lock should remain
        assert!(manager.transaction_locks.get(&1).is_some());
    }

    #[test]
    fn test_release_locks_empties_lock_table_for_key() {
        let mut manager = LockManager::new();
        let key1: LockTableKey = b"key1".to_vec();

        manager.acquire_lock(1, &key1, LockType::Exclusive).unwrap();
        manager.release_locks(1);

        assert!(manager.lock_table.get(&key1).is_none(), "Key1 entry should be removed from lock_table");
        assert!(manager.transaction_locks.get(&1).is_none());
    }

    #[test]
    fn test_acquire_after_release() {
        let mut manager = LockManager::new();
        let key1: LockTableKey = b"key1".to_vec();

        manager.acquire_lock(1, &key1, LockType::Exclusive).unwrap();
        manager.release_locks(1);

        assert!(manager.acquire_lock(2, &key1, LockType::Shared).is_ok(), "TX2 should be able to acquire Shared lock after TX1 released Exclusive");
        assert_eq!(manager.lock_table.get(&key1).map_or(0, |v| v.len()), 1);
        assert_eq!(manager.lock_table.get(&key1).unwrap()[0].transaction_id, 2);
    }

    #[test]
    fn test_multiple_locks_different_keys_same_tx_release() {
        let mut manager = LockManager::new();
        let key1: LockTableKey = b"key1".to_vec();
        let key2: LockTableKey = b"key2".to_vec();

        manager.acquire_lock(1, &key1, LockType::Shared).unwrap();
        manager.acquire_lock(1, &key2, LockType::Exclusive).unwrap();

        assert_eq!(manager.transaction_locks.get(&1).unwrap().len(), 2);
        
        manager.release_locks(1);
        assert!(manager.transaction_locks.get(&1).is_none());
        assert!(manager.lock_table.get(&key1).is_none());
        assert!(manager.lock_table.get(&key2).is_none());
    }
}
