// Assuming DbError will be defined in crate::core::common::error
// Adjust the path if error.rs is structured within a module e.g. crate::core::common::error::DbError
use crate::core::common::error::DbError;
use crate::core::transaction::Transaction;

/// Trait for basic key-value store operations.
pub trait KeyValueStore<K, V> {
    /// Inserts a key-value pair into the store.
    /// If the key already exists, its value is updated.
    fn put(&mut self, key: K, value: V, transaction: &Transaction) -> Result<(), DbError>;

    /// Retrieves the value associated with a key.
    /// Returns `Ok(Some(value))` if the key exists, `Ok(None)` otherwise.
    fn get(&self, key: &K) -> Result<Option<V>, DbError>;

    /// Deletes a key-value pair from the store.
    /// Returns `Ok(true)` if the key was found and deleted, `Ok(false)` otherwise.
    fn delete(&mut self, key: &K, transaction: &Transaction) -> Result<bool, DbError>;

    /// Checks if a key exists in the store.
    fn contains_key(&self, key: &K) -> Result<bool, DbError>;

    /// Logs a raw WAL entry. Used for transaction commit/rollback markers.
    fn log_wal_entry(&mut self, entry: &crate::core::storage::engine::wal::WalEntry) -> Result<(), DbError>;

    // Other potential methods:
    // fn scan(&self, key_prefix: &K) -> Result<Vec<(K, V)>, DbError>;
    // fn clear(&mut self) -> Result<(), DbError>;
}
