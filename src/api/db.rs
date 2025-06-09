use crate::core::common::OxidbError;
use crate::core::config::Config;
use crate::core::query::commands::{Command, Key};
use crate::core::query::executor::{ExecutionResult, QueryExecutor};
use crate::core::query::parser::parse_query_string;
use crate::core::storage::engine::SimpleFileKvStore;
use crate::core::types::DataType;
use serde_json;
use std::path::{Path, PathBuf};

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
    /// Returns `OxidbError` if the store cannot be initialized or the executor cannot be created.
    pub fn new_with_config(config: Config) -> Result<Self, OxidbError> {
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
    /// Returns `OxidbError` if the store cannot be initialized or the executor cannot be created.
    pub fn new(db_path: impl AsRef<Path>) -> Result<Self, OxidbError> {
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
    /// Returns `OxidbError` if the configuration file cannot be read/parsed or if the store cannot be initialized.
    pub fn new_from_config_file(config_path: impl AsRef<Path>) -> Result<Self, OxidbError> {
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
    /// Returns `OxidbError` if the operation fails, for instance, due to issues
    /// writing to the WAL.
    pub fn insert(&mut self, key: Key, value: String) -> Result<(), OxidbError> {
        let command = Command::Insert { key, value: DataType::String(value) };
        match self.executor.execute_command(command) {
            // Use self.executor
            Ok(ExecutionResult::Success) => Ok(()),
            Ok(unexpected_result) => Err(OxidbError::Internal(format!( // Changed to Internal
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
    /// * `Err(OxidbError)` if any other error occurs during the operation.
    pub fn get(&mut self, key: Key) -> Result<Option<String>, OxidbError> {
        let command = Command::Get { key };
        match self.executor.execute_command(command) {
            // Use self.executor
            Ok(ExecutionResult::Value(data_type_option)) => {
                // Convert DataType option to String option
                Ok(data_type_option.map(|dt| match dt {
                    DataType::Integer(i) => i.to_string(),
                    DataType::String(s) => s,
                    DataType::Boolean(b) => b.to_string(),
                    DataType::Float(f) => f.to_string(), // Added Float
                    DataType::Null => "NULL".to_string(), // Added Null
                    DataType::Map(map_val) => {
                        // Added Map
                        serde_json::to_string(&map_val)
                            .unwrap_or_else(|e| format!("Error serializing Map: {}", e))
                    }
                    DataType::JsonBlob(json_val) => serde_json::to_string(&json_val)
                        .unwrap_or_else(|e| format!("Error serializing JsonBlob: {}", e)),
                }))
            }
            Ok(unexpected_result) => Err(OxidbError::Internal(format!( // Changed to Internal
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
    /// * `Err(OxidbError)` if the operation fails, for example, due to WAL write issues.
    pub fn delete(&mut self, key: Key) -> Result<bool, OxidbError> {
        let command = Command::Delete { key };
        match self.executor.execute_command(command) {
            // Use self.executor
            Ok(ExecutionResult::Deleted(status)) => Ok(status),
            Ok(unexpected_result) => Err(OxidbError::Internal(format!( // Changed to Internal
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
    /// Returns `OxidbError` if any part of the saving process fails (e.g., I/O errors,
    /// serialization issues). See `SimpleFileKvStore::save_to_disk` for more details.
    pub fn persist(&mut self) -> Result<(), OxidbError> {
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
    /// * `Err(OxidbError)`: If an error occurs at any stage. This can be:
    ///     - `OxidbError::SqlParsing(String)`: If the `query_string` is malformed (e.g., unknown command,
    ///       incorrect number of arguments, unclosed quotes).
    ///     - Other `OxidbError` variants if the command execution itself fails (e.g., I/O errors during
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
    ///     Err(oxidb::core::common::OxidbError::SqlParsing(msg)) => { // Changed to OxidbError::SqlParsing
    ///         assert!(msg.contains("SQL parse error: Unknown statement type at position 0"));
    ///         eprintln!("Invalid query as expected: {}", msg);
    ///     },
    ///     _ => panic!("Expected SqlParsing error"),
    /// }
    /// ```
    pub fn execute_query_str(&mut self, query_string: &str) -> Result<ExecutionResult, OxidbError> {
        match parse_query_string(query_string) {
            Ok(command) => self.executor.execute_command(command), // Use self.executor
            Err(e) => Err(e), // parse_query_string now returns OxidbError
        }
    }

    /// Returns the path to the main database file.
    pub fn database_path(&self) -> PathBuf {
        self.executor.store.read().unwrap().file_path().to_path_buf()
    }

    /// Returns the base path for index storage.
    pub fn index_path(&self) -> PathBuf {
        self.executor.index_base_path()
    }
}
