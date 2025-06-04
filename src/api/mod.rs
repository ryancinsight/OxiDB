//! # Oxidb API Module
//!
//! This module provides the public API for interacting with the Oxidb key-value store.
//! It exposes the `Oxidb` struct, which is the main entry point for database operations.

use crate::core::common::error::DbError;
use crate::core::config::Config; // Added
use crate::core::query::commands::{Command, Key}; // Value removed
use crate::core::types::DataType; // Added
use serde_json; // Added for JsonBlob stringification
use crate::core::query::executor::{QueryExecutor, ExecutionResult}; // Corrected import
use crate::core::query::parser::parse_query_string;
use crate::core::storage::engine::SimpleFileKvStore;
use std::path::Path; // Removed PathBuf

/// `Oxidb` is the primary structure providing the public API for the key-value store.
///
/// It encapsulates a `QueryExecutor` instance to manage database operations,
/// which in turn uses a `SimpleFileKvStore` for persistence.
#[derive(Debug)]
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
    /// * `config` - Configuration for the database.
    ///
    /// # Errors
    /// Returns `DbError` if the store cannot be initialized or the executor cannot be created.
    pub fn new_with_config(config: Config) -> Result<Self, DbError> {
        let store = SimpleFileKvStore::new(config.database_path())?;
        let executor = QueryExecutor::new(store, config.index_path())?;
        Ok(Self { executor })
    }

    /// Creates a new `Oxidb` instance or loads an existing one from the specified path,
    /// using default configuration for other settings.
    ///
    /// # Arguments
    /// * `db_path` - A path-like object (e.g., `&str`, `PathBuf`) specifying the location
    ///            of the database file. This will override `database_file_path` in the default config.
    ///
    /// # Errors
    /// Returns `DbError` if the store cannot be initialized or the executor cannot be created.
    pub fn new(db_path: impl AsRef<Path>) -> Result<Self, DbError> {
        let mut config = Config::default();
        config.database_file_path = db_path.as_ref().to_string_lossy().into_owned();
        // index_base_path will remain its default relative to the execution directory ("oxidb_indexes/")
        // or could be made relative to db_path if desired, for example:
        // if let Some(parent) = db_path.as_ref().parent() {
        //     config.index_base_path = parent.join("oxidb_indexes/").to_string_lossy().into_owned();
        // } else {
        //     config.index_base_path = "oxidb_indexes/".to_string();
        // }
        Self::new_with_config(config)
    }

    /// Creates a new `Oxidb` instance or loads an existing one using a configuration file.
    ///
    /// If the configuration file does not exist, default settings will be used.
    ///
    /// # Arguments
    /// * `config_path` - Path to the TOML configuration file.
    ///
    /// # Errors
    /// Returns `DbError` if the configuration file cannot be read/parsed or if the store cannot be initialized.
    pub fn new_from_config_file(config_path: impl AsRef<Path>) -> Result<Self, DbError> {
        let config = Config::load_from_file(config_path.as_ref())?;
        Self::new_with_config(config)
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
    /// * `value` - The value (`String`) to associate with the key. This will be stored as `DataType::String`.
    ///
    /// # Errors
    /// Returns `DbError` if the operation fails, for instance, due to issues
    /// writing to the WAL.
    pub fn insert(&mut self, key: Key, value: String) -> Result<(), DbError> {
        let command = Command::Insert { key, value: DataType::String(value) };
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
    /// * `Ok(Some(String))` if the key is found, containing the string representation of the associated value.
    /// * `Ok(None)` if the key is not found.
    /// * `Err(DbError)` if any other error occurs during the operation.
    pub fn get(&mut self, key: Key) -> Result<Option<String>, DbError> {
        let command = Command::Get { key };
        match self.executor.execute_command(command) { // Use self.executor
            Ok(ExecutionResult::Value(data_type_option)) => {
                // Convert DataType option to String option
                Ok(data_type_option.map(|dt| match dt {
                    DataType::Integer(i) => i.to_string(),
                    DataType::String(s) => s,
                    DataType::Boolean(b) => b.to_string(),
                    DataType::Float(f) => f.to_string(), // Added Float
                    DataType::Null => "NULL".to_string(), // Added Null
                    DataType::Map(map_val) => { // Added Map
                        serde_json::to_string(&map_val)
                            .unwrap_or_else(|e| format!("Error serializing Map: {}", e))
                    }
                    DataType::JsonBlob(json_val) => {
                        serde_json::to_string(&json_val)
                            .unwrap_or_else(|e| format!("Error serializing JsonBlob: {}", e))
                    }
                }))
            }
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
    /// use oxidb::core::types::DataType; // Ensure DataType is in scope for matching
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
    ///     Ok(ExecutionResult::Value(Some(DataType::String(s)))) => {
    ///         assert_eq!(s, "my_value");
    ///         println!("Got value: {:?}", s);
    ///     },
    ///     Ok(ExecutionResult::Value(None)) => println!("Key not found."),
    ///     Err(e) => eprintln!("Get failed: {:?}", e),
    ///     other => panic!("Unexpected result for GET my_key: {:?}", other)
    /// }
    ///
    /// // Insert a value with spaces
    /// db.execute_query_str("INSERT user \"Alice Wonderland\"").unwrap();
    /// match db.execute_query_str("GET user") {
    ///     Ok(ExecutionResult::Value(Some(DataType::String(s)))) => {
    ///          assert_eq!(s, "Alice Wonderland");
    ///     },
    ///     other => panic!("Should have found user Alice, got {:?}", other),
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
        let value_str = "api_value_1".to_string();

        // Test insert (now takes String)
        let insert_result = db.insert(key.clone(), value_str.clone());
        assert!(insert_result.is_ok());

        // Test get (now returns Option<String>)
        let get_result = db.get(key.clone());
        assert!(get_result.is_ok());
        assert_eq!(get_result.unwrap(), Some(value_str));
    }

    #[test]
    fn test_oxidb_get_non_existent() {
        let db_path = get_temp_db_path();
        let mut db = Oxidb::new(&db_path).unwrap();
        let key = b"api_non_existent".to_vec();
        let get_result = db.get(key);
        assert!(get_result.is_ok());
        assert_eq!(get_result.unwrap(), None); // Stays None
    }

    #[test]
    fn test_oxidb_delete() {
        let db_path = get_temp_db_path();
        let mut db = Oxidb::new(&db_path).unwrap();
        let key = b"api_delete_key".to_vec();
        let value_str = "api_delete_value".to_string();

        db.insert(key.clone(), value_str.clone()).unwrap();
        let get_inserted_result = db.get(key.clone());
        assert!(get_inserted_result.is_ok());
        assert_eq!(get_inserted_result.unwrap(), Some(value_str)); // Verify insert

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
        let value1_str = "value1".to_string();
        let value2_str = "value2".to_string();

        db.insert(key.clone(), value1_str.clone()).unwrap();
        let get_v1_result = db.get(key.clone());
        assert!(get_v1_result.is_ok());
        assert_eq!(get_v1_result.unwrap(), Some(value1_str));

        db.insert(key.clone(), value2_str.clone()).unwrap(); // This is an update
        let get_v2_result = db.get(key.clone());
        assert!(get_v2_result.is_ok());
        assert_eq!(get_v2_result.unwrap(), Some(value2_str));
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
        let value_str = "persist_value".to_string(); // Changed to String

        {
            let mut db = Oxidb::new(&db_path).unwrap();
            db.insert(key.clone(), value_str.clone()).unwrap(); // Use String value
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
        assert_eq!(get_result.unwrap(), Some(value_str)); // Assert against String value
    }

    // Tests for execute_query_str
    #[test]
    fn test_execute_query_str_get_ok() {
        let db_path = get_temp_db_path();
        let mut db = Oxidb::new(&db_path).unwrap();
        // Insert using the API's insert which now takes String and stores as DataType::String
        db.insert(b"mykey".to_vec(), "myvalue".to_string()).unwrap();

        let result = db.execute_query_str("GET mykey");
        match result {
            // Expecting DataType::String from the executor
            Ok(ExecutionResult::Value(Some(DataType::String(val_str)))) => assert_eq!(val_str, "myvalue"),
            _ => panic!("Expected Value(Some(DataType::String(...))), got {:?}", result),
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
        // The parser will turn "newvalue" into DataType::String("newvalue")
        let result = db.execute_query_str("INSERT newkey newvalue");
        match result {
            Ok(ExecutionResult::Success) => {} // Expected
            _ => panic!("Expected Success, got {:?}", result),
        }
        // db.get now returns Option<String>
        assert_eq!(db.get(b"newkey".to_vec()).unwrap(), Some("newvalue".to_string()));
    }

    #[test]
    fn test_execute_query_str_insert_with_quotes_ok() {
        let db_path = get_temp_db_path();
        let mut db = Oxidb::new(&db_path).unwrap();
        // Parser turns "\"quoted value\"" into DataType::String("quoted value")
        let result = db.execute_query_str("INSERT qkey \"quoted value\"");
        match result {
            Ok(ExecutionResult::Success) => {} // Expected
            _ => panic!("Expected Success, got {:?}", result),
        }
        assert_eq!(db.get(b"qkey".to_vec()).unwrap(), Some("quoted value".to_string()));
    }

    #[test]
    fn test_execute_query_str_insert_integer_via_parser() {
        let db_path = get_temp_db_path();
        let mut db = Oxidb::new(&db_path).unwrap();
        // Parser turns "123" into DataType::Integer(123)
        let result = db.execute_query_str("INSERT intkey 123");
        match result {
            Ok(ExecutionResult::Success) => {} // Expected
            _ => panic!("Expected Success, got {:?}", result),
        }
        // db.get now returns Option<String>
        assert_eq!(db.get(b"intkey".to_vec()).unwrap(), Some("123".to_string()));
    }

    #[test]
    fn test_execute_query_str_insert_boolean_via_parser() {
        let db_path = get_temp_db_path();
        let mut db = Oxidb::new(&db_path).unwrap();
        // Parser turns "true" into DataType::Boolean(true)
        let result = db.execute_query_str("INSERT boolkey true");
        match result {
            Ok(ExecutionResult::Success) => {} // Expected
            _ => panic!("Expected Success, got {:?}", result),
        }
        assert_eq!(db.get(b"boolkey".to_vec()).unwrap(), Some("true".to_string()));
    }


    #[test]
    fn test_execute_query_str_delete_ok() {
        let db_path = get_temp_db_path();
        let mut db = Oxidb::new(&db_path).unwrap();
        db.insert(b"delkey".to_vec(), "delvalue".to_string()).unwrap(); // Use String for insert
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
