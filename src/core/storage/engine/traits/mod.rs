// Assuming DbError will be defined in crate::core::common::error
// Adjust the path if error.rs is structured within a module e.g. crate::core::common::error::DbError
use crate::core::common::error::DbError;
use crate::core::transaction::Transaction;
use std::collections::HashSet; // Added import

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionedValue<V> {
    pub value: V,
    pub created_tx_id: u64, // Transaction ID that created this version
    pub expired_tx_id: Option<u64>, // Transaction ID that "deleted" or superseded this version
                                 // None means it's currently the latest/valid for its creation cohort
}

/// Trait for basic key-value store operations.
// Added Send + Sync + 'static bounds for broader usability (e.g. with Arc<RwLock<T>>)
pub trait KeyValueStore<K, V>: Send + Sync + 'static {
    /// Inserts a key-value pair into the store.
    /// If the key already exists, its value is updated.
    fn put(&mut self, key: K, value: V, transaction: &Transaction) -> Result<(), DbError>;

    /// Retrieves the value associated with a key.
    /// Returns `Ok(Some(value))` if the key exists, `Ok(None)` otherwise.
    fn get(&self, key: &K, snapshot_id: u64, committed_ids: &HashSet<u64>) -> Result<Option<V>, DbError>;

    /// Deletes a key-value pair from the store.
    /// Returns `Ok(true)` if the key was found and deleted, `Ok(false)` otherwise.
    fn delete(&mut self, key: &K, transaction: &Transaction) -> Result<bool, DbError>;

    /// Checks if a key exists in the store.
    fn contains_key(&self, key: &K, snapshot_id: u64, committed_ids: &HashSet<u64>) -> Result<bool, DbError>;

    /// Logs a raw WAL entry. Used for transaction commit/rollback markers.
    fn log_wal_entry(&mut self, entry: &crate::core::storage::engine::wal::WalEntry) -> Result<(), DbError>;

    /// Performs garbage collection on the store.
    /// `low_water_mark`: The oldest transaction ID currently active.
    /// `committed_ids`: The set of all committed transaction IDs.
    fn gc(&mut self, low_water_mark: u64, committed_ids: &HashSet<u64>) -> Result<(), DbError>;

    // Other potential methods:
    // fn scan(&self, key_prefix: &K) -> Result<Vec<(K, V)>, DbError>;
    // fn clear(&mut self) -> Result<(), DbError>;

    /// Scans all key-value pairs in the store.
    ///
    /// This is a simple, non-MVCC scan for now, returning the latest versions of values.
    /// The order of returned pairs is not guaranteed.
    ///
    /// # Returns
    /// A `Result` containing a `Vec` of `(K, V)` tuples.
    ///
    /// # Type Bounds
    /// Requires `K: Clone` and `V: Clone` as it returns owned copies.
    fn scan(&self) -> Result<Vec<(K, V)>, DbError>
        where K: Clone, V: Clone;
}
