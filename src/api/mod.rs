//! # Oxidb API Module
//!
//! This module provides the public API for interacting with the Oxidb key-value store.
//! It exposes the `Oxidb` struct, which is the main entry point for database operations.

use crate::core::common::error::DbError;
use crate::core::query::commands::{Command, Key, Value};
use crate::core::query::executor::{execute_command, ExecutionResult}; // Added ExecutionResult
use crate::core::storage::engine::simple_file_kv_store::SimpleFileKvStore;
use std::path::Path;

/// `Oxidb` is the primary structure providing the public API for the key-value store.
///
/// It encapsulates a `SimpleFileKvStore` instance to manage data persistence
/// and provides methods for common database operations like insert, get, and delete.
pub struct Oxidb {
    store: SimpleFileKvStore,
}

impl Oxidb {
    /// Creates a new `Oxidb` instance or loads an existing one from the specified path.
    ///
    /// This method initializes the underlying `SimpleFileKvStore`. If a database file
    /// and/or WAL (Write-Ahead Log) file exists at the given path, they will be loaded.
    /// Otherwise, new files will be created.
    ///
    /// # Arguments
    /// * `path` - A path-like object (e.g., `&str`, `PathBuf`) specifying the location
    ///            of the database file.
    ///
    /// # Errors
    /// Returns `DbError` if the store cannot be initialized, for example, due to
    /// I/O errors or issues during WAL replay.
    pub fn new(path: impl AsRef<Path>) -> Result<Self, DbError> {
        let store = SimpleFileKvStore::new(path)?;
        Ok(Self { store })
    }

    /// Inserts a key-value pair into the database.
    ///
    /// If the key already exists, its value will be overwritten.
    /// This operation is first recorded in the Write-Ahead Log (WAL) for durability
    /// and then applied to the in-memory cache. The data is persisted to the main
    /// data file when `persist` is called or when the `Oxidb` instance (or its
    /// underlying `SimpleFileKvStore`) is dropped.
    ///
    /// # Arguments
    /// * `key` - The key (`Vec<u8>`) to insert.
    /// * `value` - The value (`Vec<u8>`) to associate with the key.
    ///
    /// # Errors
    /// Returns `DbError` if the operation fails, for instance, due to issues
    /// writing to the WAL.
    pub fn insert(&mut self, key: Key, value: Value) -> Result<(), DbError> {
        let command = Command::Insert { key, value };
        match execute_command(&mut self.store, command) {
            Ok(ExecutionResult::Success) => Ok(()),
            Ok(unexpected_result) => Err(DbError::InternalError(format!(
                "Insert: Expected Success, got {:?}",
                unexpected_result
            ))),
            Err(e) => Err(e),
        }
    }

    /// Retrieves the value associated with a given key.
    ///
    /// This method queries the in-memory cache for the key.
    ///
    /// # Arguments
    /// * `key` - The key (`Vec<u8>`) whose value is to be retrieved.
    ///
    /// # Returns
    /// * `Ok(Some(Value))` if the key is found, containing the associated value.
    /// * `Ok(None)` if the key is not found.
    /// * `Err(DbError)` if any other error occurs during the operation.
    pub fn get(&mut self, key: Key) -> Result<Option<Value>, DbError> {
        let command = Command::Get { key };
        match execute_command(&mut self.store, command) {
            Ok(ExecutionResult::Value(value_option)) => Ok(value_option),
            Ok(unexpected_result) => Err(DbError::InternalError(format!(
                "Get: Expected Value, got {:?}",
                unexpected_result
            ))),
            Err(e) => Err(e),
        }
    }

    /// Deletes a key-value pair from the database.
    ///
    /// This operation is first recorded in the Write-Ahead Log (WAL) and then
    /// applied to the in-memory cache. The deletion is made permanent in the main
    /// data file when `persist` is called or when the `Oxidb` instance is dropped.
    ///
    /// # Arguments
    /// * `key` - The key (`Vec<u8>`) to delete.
    ///
    /// # Returns
    /// * `Ok(true)` if the key was found and successfully deleted.
    /// * `Ok(false)` if the key was not found.
    /// * `Err(DbError)` if the operation fails, for example, due to WAL write issues.
    pub fn delete(&mut self, key: Key) -> Result<bool, DbError> {
        let command = Command::Delete { key };
        match execute_command(&mut self.store, command) {
            Ok(ExecutionResult::Deleted(status)) => Ok(status),
            Ok(unexpected_result) => Err(DbError::InternalError(format!(
                "Delete: Expected Deleted, got {:?}",
                unexpected_result
            ))),
            Err(e) => Err(e),
        }
    }

    /// Persists all current in-memory data to the main data file on disk.
    ///
    /// This method explicitly triggers the `save_to_disk` operation on the underlying
    /// `SimpleFileKvStore`. This process involves writing the current cache to a
    /// temporary file, then atomically replacing the main data file with this
    /// temporary file. Upon successful completion, the Write-Ahead Log (WAL) is cleared,
    /// as all its entries are now safely stored in the main data file.
    ///
    /// This is useful for ensuring data durability at specific points, rather than
    /// relying solely on the `Drop` implementation's automatic save.
    ///
    /// # Errors
    /// Returns `DbError` if any part of the saving process fails (e.g., I/O errors,
    /// serialization issues). See `SimpleFileKvStore::save_to_disk` for more details.
    pub fn persist(&mut self) -> Result<(), DbError> {
        self.store.save_to_disk()
    }
}

#[cfg(test)]
mod tests {
    use super::*; // Imports Oxidb, Key, Value etc.
    use tempfile::NamedTempFile; // For creating temporary db files
    use std::path::PathBuf; // Added for derive_wal_path_for_test
    use crate::core::storage::engine::traits::KeyValueStore; // Added for store.contains_key

    #[test]
    fn test_oxidb_insert_and_get() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut db = Oxidb::new(temp_file.path()).unwrap();

        let key = b"api_key_1".to_vec();
        let value = b"api_value_1".to_vec();

        // Test insert
        let insert_result = db.insert(key.clone(), value.clone());
        assert!(insert_result.is_ok());

        // Test get
        let get_result = db.get(key.clone());
        assert!(get_result.is_ok());
        assert_eq!(get_result.unwrap(), Some(value));
    }

    #[test]
    fn test_oxidb_get_non_existent() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut db = Oxidb::new(temp_file.path()).unwrap();
        let key = b"api_non_existent".to_vec();
        let get_result = db.get(key);
        assert!(get_result.is_ok());
        assert_eq!(get_result.unwrap(), None);
    }

    #[test]
    fn test_oxidb_delete() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut db = Oxidb::new(temp_file.path()).unwrap();
        let key = b"api_delete_key".to_vec();
        let value = b"api_delete_value".to_vec();

        db.insert(key.clone(), value.clone()).unwrap();
        let get_inserted_result = db.get(key.clone());
        assert!(get_inserted_result.is_ok());
        assert_eq!(get_inserted_result.unwrap(), Some(value)); // Verify insert

        let delete_result = db.delete(key.clone());
        assert!(delete_result.is_ok());
        assert_eq!(delete_result.unwrap(), true); // Key existed and was deleted

        let get_deleted_result = db.get(key.clone());
        assert!(get_deleted_result.is_ok());
        assert_eq!(get_deleted_result.unwrap(), None); // Verify deleted
    }
    
    #[test]
    fn test_oxidb_delete_non_existent() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut db = Oxidb::new(temp_file.path()).unwrap();
        let key = b"api_delete_non_existent".to_vec();

        let delete_result = db.delete(key.clone());
        assert!(delete_result.is_ok());
        assert_eq!(delete_result.unwrap(), false); // Key did not exist
    }

    #[test]
    fn test_oxidb_update() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut db = Oxidb::new(temp_file.path()).unwrap();
        let key = b"api_update_key".to_vec();
        let value1 = b"value1".to_vec();
        let value2 = b"value2".to_vec();

        db.insert(key.clone(), value1.clone()).unwrap();
        let get_v1_result = db.get(key.clone());
        assert!(get_v1_result.is_ok());
        assert_eq!(get_v1_result.unwrap(), Some(value1));

        db.insert(key.clone(), value2.clone()).unwrap(); // This is an update
        let get_v2_result = db.get(key.clone());
        assert!(get_v2_result.is_ok());
        assert_eq!(get_v2_result.unwrap(), Some(value2));
    }

    // Helper function to derive WAL path from DB path for testing
    fn derive_wal_path_for_test(db_path: &Path) -> PathBuf {
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

    #[test]
    fn test_oxidb_persist_method() {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path().to_path_buf(); // Keep PathBuf for derive_wal_path_for_test
        let wal_path = derive_wal_path_for_test(&db_path);

        let key = b"persist_key".to_vec();
        let value = b"persist_value".to_vec();

        {
            let mut db = Oxidb::new(&db_path).unwrap();
            db.insert(key.clone(), value.clone()).unwrap();
            // Data is in WAL and cache. Main file might be empty or have old data.
            // WAL file should exist if inserts happened.
            // (This check depends on SimpleFileKvStore's WAL behavior after insert)
            // For this test, we assume WAL is written to on put.
            if db.store.contains_key(&key).unwrap() { // Check if key is in cache, implying a put happened
                 assert!(wal_path.exists(), "WAL file should exist after insert before persist.");
            }


            let persist_result = db.persist();
            assert!(persist_result.is_ok());

            // After persist, WAL should be cleared, and data should be in the main file.
            assert!(!wal_path.exists(), "WAL file should not exist after persist.");
        }

        // Re-load the database
        let mut reloaded_db = Oxidb::new(&db_path).unwrap();
        let get_result = reloaded_db.get(key.clone());
        assert!(get_result.is_ok());
        assert_eq!(get_result.unwrap(), Some(value));
    }
}
