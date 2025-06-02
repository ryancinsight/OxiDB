// src/api/mod.rs

use crate::core::common::error::DbError;
use crate::core::query::commands::{Command, Key, Value};
use crate::core::query::executor::{execute_command, ExecutionResult}; // Added ExecutionResult
use crate::core::storage::engine::simple_file_kv_store::SimpleFileKvStore;
use std::path::Path; // For the 'new' method

pub struct Oxidb {
    store: SimpleFileKvStore,
}

impl Oxidb {
    /// Creates a new `Oxidb` instance, initializing or loading a `SimpleFileKvStore`
    /// from the given path.
    pub fn new(path: impl AsRef<Path>) -> Result<Self, DbError> {
        let store = SimpleFileKvStore::new(path)?;
        Ok(Self { store })
    }

    /// Inserts a key-value pair into the database.
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

    /// Retrieves the value associated with a key.
    /// Returns `Ok(Some(value))` if the key exists, `Ok(None)` otherwise.
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
    /// Returns `Ok(true)` if the key was found and deleted, `Ok(false)` otherwise.
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
}

#[cfg(test)]
mod tests {
    use super::*; // Imports Oxidb, Key, Value etc.
    use tempfile::NamedTempFile; // For creating temporary db files

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
}
