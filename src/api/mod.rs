//! # Oxidb API Module
//!
//! This module provides the public API for interacting with the Oxidb key-value store.
//! It exposes the `Oxidb` struct, which is the main entry point for database operations.

use crate::core::common::error::DbError;
use crate::core::query::commands::{Command, Key, Value};
use crate::core::query::executor::{QueryExecutor, ExecutionResult}; // Corrected import
use crate::core::query::parser::parse_query_string;
use crate::core::storage::engine::simple_file_kv_store::SimpleFileKvStore;
use std::path::Path;

/// `Oxidb` is the primary structure providing the public API for the key-value store.
///
/// It encapsulates a `QueryExecutor` instance to manage database operations,
/// which in turn uses a `SimpleFileKvStore` for persistence.
pub struct Oxidb {
    executor: QueryExecutor<SimpleFileKvStore>,
}

impl Oxidb {
    /// Creates a new `Oxidb` instance or loads an existing one from the specified path.
    ///
    /// This method initializes the underlying `SimpleFileKvStore` and wraps it in a
    /// `QueryExecutor`. If a database file and/or WAL (Write-Ahead Log) file exists
    /// at the given path, they will be loaded by the store. Otherwise, new files
    /// will be created.
    ///
    /// # Arguments
    /// * `path` - A path-like object (e.g., `&str`, `PathBuf`) specifying the location
    ///            of the database file.
    ///
    /// # Errors
    /// Returns `DbError` if the store cannot be initialized or the executor cannot be created.
    pub fn new(path: impl AsRef<Path>) -> Result<Self, DbError> {
        let store = SimpleFileKvStore::new(path)?;
        let executor = QueryExecutor::new(store);
        Ok(Self { executor })
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
        match self.executor.execute_command(command) { // Use self.executor
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
        match self.executor.execute_command(command) { // Use self.executor
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
        match self.executor.execute_command(command) { // Use self.executor
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
        self.executor.persist() // Use self.executor
    }

    /// Executes a raw query string against the database.
    ///
    /// This method provides a flexible way to interact with the database using string-based commands.
    /// It internally parses the `query_string` into a `Command` using `crate::core::query::parser::parse_query_string`
    /// and then executes this command via `crate::core::query::executor::execute_command`.
    ///
    /// # Arguments
    /// * `query_string` - A string slice representing the raw query. Supported commands are:
    ///     - `GET <key>`
    ///     - `INSERT <key> <value>` (value can be quoted for spaces, e.g., `INSERT name "John Doe"`)
    ///     - `DELETE <key>`
    ///
    /// # Returns
    /// * `Ok(ExecutionResult)`: If the query is successfully parsed and executed. The `ExecutionResult`
    ///   enum indicates the outcome of the command (e.g., `ExecutionResult::Value(Some(data))` for a successful GET,
    ///   `ExecutionResult::Success` for an INSERT, `ExecutionResult::Deleted(true)` for a successful DELETE).
    /// * `Err(DbError)`: If an error occurs at any stage. This can be:
    ///     - `DbError::InvalidQuery(String)`: If the `query_string` is malformed (e.g., unknown command,
    ///       incorrect number of arguments, unclosed quotes).
    ///     - Other `DbError` variants if the command execution itself fails (e.g., I/O errors during
    ///       storage operations).
    ///
    /// # Examples
    /// ```rust
    /// use oxidb::api::Oxidb;
    /// use oxidb::core::query::executor::ExecutionResult;
    /// use tempfile::NamedTempFile;
    ///
    /// let temp_file = NamedTempFile::new().unwrap();
    /// let mut db = Oxidb::new(temp_file.path()).unwrap();
    ///
    /// // Insert a value
    /// match db.execute_query_str("INSERT my_key my_value") {
    ///     Ok(ExecutionResult::Success) => println!("Insert successful!"),
    ///     Err(e) => eprintln!("Insert failed: {:?}", e),
    ///     _ => {} // Other ExecutionResult variants not expected for INSERT
    /// }
    ///
    /// // Get the value
    /// match db.execute_query_str("GET my_key") {
    ///     Ok(ExecutionResult::Value(Some(value))) => {
    ///         assert_eq!(value, b"my_value".to_vec());
    ///         println!("Got value: {:?}", String::from_utf8_lossy(&value));
    ///     },
    ///     Ok(ExecutionResult::Value(None)) => println!("Key not found."),
    ///     Err(e) => eprintln!("Get failed: {:?}", e),
    ///     _ => {} // Other ExecutionResult variants
    /// }
    ///
    /// // Insert a value with spaces
    /// db.execute_query_str("INSERT user \"Alice Wonderland\"").unwrap();
    /// match db.execute_query_str("GET user") {
    ///     Ok(ExecutionResult::Value(Some(value))) => {
    ///          assert_eq!(value, b"Alice Wonderland".to_vec());
    ///     },
    ///     _ => panic!("Should have found user Alice"),
    /// }
    ///
    /// // Delete the value
    /// match db.execute_query_str("DELETE my_key") {
    ///     Ok(ExecutionResult::Deleted(true)) => println!("Delete successful!"),
    ///     Ok(ExecutionResult::Deleted(false)) => println!("Key not found for deletion."),
    ///     Err(e) => eprintln!("Delete failed: {:?}", e),
    ///     _ => {} // Other ExecutionResult variants
    /// }
    ///
    /// // Attempt an invalid query
    /// match db.execute_query_str("INVALID COMMAND") {
    ///     Err(oxidb::core::common::error::DbError::InvalidQuery(msg)) => {
    ///         assert!(msg.contains("Unknown command"));
    ///         eprintln!("Invalid query as expected: {}", msg);
    ///     },
    ///     _ => panic!("Expected InvalidQuery error"),
    /// }
    /// ```
    pub fn execute_query_str(&mut self, query_string: &str) -> Result<ExecutionResult, DbError> {
        match parse_query_string(query_string) {
            Ok(command) => self.executor.execute_command(command), // Use self.executor
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*; // Imports Oxidb, Key, Value, ExecutionResult etc.
    use tempfile::NamedTempFile; // For creating temporary db files
    use std::path::PathBuf;
    //KeyValueStore might not be needed here anymore if tests don't directly interact with store details that require this trait in scope.
    // use crate::core::storage::engine::traits::KeyValueStore;


    // Helper function to create a NamedTempFile and return its path for tests
    // This avoids repeating NamedTempFile::new().unwrap().path()
    fn get_temp_db_path() -> PathBuf {
        NamedTempFile::new().unwrap().path().to_path_buf()
    }

    #[test]
    fn test_oxidb_insert_and_get() {
        let db_path = get_temp_db_path();
        let mut db = Oxidb::new(&db_path).unwrap();

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
        let db_path = get_temp_db_path();
        let mut db = Oxidb::new(&db_path).unwrap();
        let key = b"api_non_existent".to_vec();
        let get_result = db.get(key);
        assert!(get_result.is_ok());
        assert_eq!(get_result.unwrap(), None);
    }

    #[test]
    fn test_oxidb_delete() {
        let db_path = get_temp_db_path();
        let mut db = Oxidb::new(&db_path).unwrap();
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
        let db_path = get_temp_db_path();
        let mut db = Oxidb::new(&db_path).unwrap();
        let key = b"api_delete_non_existent".to_vec();

        let delete_result = db.delete(key.clone());
        assert!(delete_result.is_ok());
        assert_eq!(delete_result.unwrap(), false); // Key did not exist
    }

    #[test]
    fn test_oxidb_update() {
        let db_path = get_temp_db_path();
        let mut db = Oxidb::new(&db_path).unwrap();
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
        let db_path = get_temp_db_path();
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
            // Replace direct store access with API usage:
            if db.get(key.clone()).unwrap().is_some() {
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

    // Tests for execute_query_str
    #[test]
    fn test_execute_query_str_get_ok() {
        let db_path = get_temp_db_path();
        let mut db = Oxidb::new(&db_path).unwrap();
        db.insert(b"mykey".to_vec(), b"myvalue".to_vec()).unwrap();

        let result = db.execute_query_str("GET mykey");
        match result {
            Ok(ExecutionResult::Value(Some(val))) => assert_eq!(val, b"myvalue".to_vec()),
            _ => panic!("Expected Value(Some(...)), got {:?}", result),
        }
    }

    #[test]
    fn test_execute_query_str_get_not_found() {
        let db_path = get_temp_db_path();
        let mut db = Oxidb::new(&db_path).unwrap();
        let result = db.execute_query_str("GET nonkey");
        match result {
            Ok(ExecutionResult::Value(None)) => {} // Expected
            _ => panic!("Expected Value(None), got {:?}", result),
        }
    }

    #[test]
    fn test_execute_query_str_insert_ok() {
        let db_path = get_temp_db_path();
        let mut db = Oxidb::new(&db_path).unwrap();
        let result = db.execute_query_str("INSERT newkey newvalue");
        match result {
            Ok(ExecutionResult::Success) => {} // Expected
            _ => panic!("Expected Success, got {:?}", result),
        }
        assert_eq!(db.get(b"newkey".to_vec()).unwrap(), Some(b"newvalue".to_vec()));
    }

    #[test]
    fn test_execute_query_str_insert_with_quotes_ok() {
        let db_path = get_temp_db_path();
        let mut db = Oxidb::new(&db_path).unwrap();
        let result = db.execute_query_str("INSERT qkey \"quoted value\"");
        match result {
            Ok(ExecutionResult::Success) => {} // Expected
            _ => panic!("Expected Success, got {:?}", result),
        }
        assert_eq!(db.get(b"qkey".to_vec()).unwrap(), Some(b"quoted value".to_vec()));
    }


    #[test]
    fn test_execute_query_str_delete_ok() {
        let db_path = get_temp_db_path();
        let mut db = Oxidb::new(&db_path).unwrap();
        db.insert(b"delkey".to_vec(), b"delvalue".to_vec()).unwrap();
        let result = db.execute_query_str("DELETE delkey");
        match result {
            Ok(ExecutionResult::Deleted(true)) => {} // Expected
            _ => panic!("Expected Deleted(true), got {:?}", result),
        }
        assert_eq!(db.get(b"delkey".to_vec()).unwrap(), None);
    }

    #[test]
    fn test_execute_query_str_delete_not_found() {
        let db_path = get_temp_db_path();
        let mut db = Oxidb::new(&db_path).unwrap();
        let result = db.execute_query_str("DELETE nonkey");
        match result {
            Ok(ExecutionResult::Deleted(false)) => {} // Expected
            _ => panic!("Expected Deleted(false), got {:?}", result),
        }
    }

    #[test]
    fn test_execute_query_str_parse_error() {
        let db_path = get_temp_db_path();
        let mut db = Oxidb::new(&db_path).unwrap();
        let result = db.execute_query_str("GARBAGE COMMAND");
        match result {
            Err(DbError::InvalidQuery(_)) => {} // Expected
            _ => panic!("Expected InvalidQuery, got {:?}", result),
        }
    }

    #[test]
    fn test_execute_query_str_empty_query() {
        let db_path = get_temp_db_path();
        let mut db = Oxidb::new(&db_path).unwrap();
        let result = db.execute_query_str("");
        match result {
            Err(DbError::InvalidQuery(msg)) => assert_eq!(msg, "Input query string cannot be empty."),
            _ => panic!("Expected InvalidQuery for empty string, got {:?}", result),
        }
    }
}
