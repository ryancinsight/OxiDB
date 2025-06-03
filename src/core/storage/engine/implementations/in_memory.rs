// src/core/storage/engine/implementations/in_memory.rs
use std::collections::HashMap;
use crate::core::common::error::DbError;
use crate::core::storage::engine::traits::KeyValueStore;
use crate::core::storage::engine::wal::WalEntry;
use crate::core::transaction::Transaction;

#[derive(Debug, Default)] // Added Default
pub struct InMemoryKvStore {
    data: HashMap<Vec<u8>, Vec<u8>>,
}

impl InMemoryKvStore {
    pub fn new() -> Self {
        InMemoryKvStore {
            data: HashMap::new(),
        }
    }
}

impl KeyValueStore<Vec<u8>, Vec<u8>> for InMemoryKvStore {
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>, _transaction: &Transaction) -> Result<(), DbError> {
        self.data.insert(key, value);
        Ok(())
    }

    fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, DbError> {
        Ok(self.data.get(key).cloned())
    }

    fn delete(&mut self, key: &Vec<u8>, _transaction: &Transaction) -> Result<bool, DbError> {
        Ok(self.data.remove(key).is_some())
    }

    fn contains_key(&self, key: &Vec<u8>) -> Result<bool, DbError> {
        Ok(self.data.contains_key(key))
    }

    fn log_wal_entry(&mut self, _entry: &WalEntry) -> Result<(), DbError> {
        // In-memory store does not need WAL. This is a no-op.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::transaction::Transaction; // For dummy transaction

    #[test]
    fn test_put_and_get() {
        let mut store = InMemoryKvStore::new();
        let key = b"test_key".to_vec();
        let value = b"test_value".to_vec();
        let dummy_transaction = Transaction::new(0);

        assert!(store.put(key.clone(), value.clone(), &dummy_transaction).is_ok());
        assert_eq!(store.get(&key).unwrap(), Some(value));
    }

    #[test]
    fn test_get_non_existent() {
        let store = InMemoryKvStore::new();
        let key = b"non_existent_key".to_vec();
        assert_eq!(store.get(&key).unwrap(), None);
    }

    #[test]
    fn test_put_update() {
        let mut store = InMemoryKvStore::new();
        let key = b"update_key".to_vec();
        let value1 = b"value1".to_vec();
        let value2 = b"value2".to_vec();
        let dummy_transaction = Transaction::new(0);

        store.put(key.clone(), value1.clone(), &dummy_transaction).unwrap();
        assert_eq!(store.get(&key).unwrap(), Some(value1));

        store.put(key.clone(), value2.clone(), &dummy_transaction).unwrap();
        assert_eq!(store.get(&key).unwrap(), Some(value2));
    }

    #[test]
    fn test_delete() {
        let mut store = InMemoryKvStore::new();
        let key = b"delete_key".to_vec();
        let value = b"delete_value".to_vec();
        let dummy_transaction = Transaction::new(0);

        store.put(key.clone(), value.clone(), &dummy_transaction).unwrap();
        assert_eq!(store.get(&key).unwrap(), Some(value));

        assert!(store.delete(&key, &dummy_transaction).unwrap());
        assert_eq!(store.get(&key).unwrap(), None);
    }

    #[test]
    fn test_delete_non_existent() {
        let mut store = InMemoryKvStore::new();
        let key = b"delete_non_existent_key".to_vec();
        let dummy_transaction = Transaction::new(0);
        assert!(!store.delete(&key, &dummy_transaction).unwrap());
    }

    #[test]
    fn test_contains_key() {
        let mut store = InMemoryKvStore::new();
        let key = b"contains_key_test".to_vec();
        let value = b"irrelevant_value".to_vec();
        let dummy_transaction = Transaction::new(0);

        assert!(!store.contains_key(&key).unwrap());
        store.put(key.clone(), value, &dummy_transaction).unwrap();
        assert!(store.contains_key(&key).unwrap());
    }

    #[test]
    fn test_log_wal_entry_is_nop() {
        let mut store = InMemoryKvStore::new();
        let dummy_wal_entry = WalEntry::TransactionCommit { transaction_id: 1 };
        assert!(store.log_wal_entry(&dummy_wal_entry).is_ok());
        // No other state to check, just that it doesn't panic or error.
    }
}
