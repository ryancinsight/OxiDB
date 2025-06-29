// src/api/api_impl.rs
//! Contains the private implementation logic for the API layer.

use super::types::Oxidb; // To refer to the Oxidb struct in types.rs
use crate::core::common::OxidbError;
use crate::core::config::Config;
use crate::core::query::commands::{Command, Key};
use crate::core::query::executor::{ExecutionResult as CoreExecutionResult, QueryExecutor}; // QueryExecutor is needed for Oxidb::new_with_config
use crate::core::query::parser::parse_query_string;
use crate::core::storage::engine::SimpleFileKvStore; // SimpleFileKvStore is needed for Oxidb::new_with_config
use crate::core::types::DataType; // Removed JsonSafeMap
use crate::core::wal::log_manager::LogManager;
use super::types::Value; // Import the public Value
use crate::core::wal::writer::WalWriter;
// use serde_json; // No longer needed directly by get()
use std::path::{Path, PathBuf};
use std::sync::Arc;

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
        let store_path = config.database_path(); // Path for SFKS data file
        let store = SimpleFileKvStore::new(store_path.clone())?; // SFKS derives its physical WAL from store_path

        let wal_writer_config = crate::core::wal::writer::WalWriterConfig::default();

        // Use the wal_file_path from the config for the TransactionManager's WalWriter.
        // This path is distinct from the one SimpleFileKvStore derives for its physical data WAL.
        let tm_wal_path = config.wal_path(); // This is typically "<cwd>/oxidb.wal" or user-defined.
                                             // SimpleFileKvStore derives its WAL as "<store_path>.wal" or "<store_path>.<ext>.wal"

        eprintln!("[Oxidb::new_with_config] SFKS main DB path: {:?}", store_path);
        // Actual SFKS WAL path is derived internally by SFKS, e.g. store_path.with_extension(...)
        eprintln!(
            "[Oxidb::new_with_config] Using TM WAL path for QueryExecutor: {:?}",
            tm_wal_path
        );

        let tm_wal_writer = WalWriter::new(tm_wal_path, wal_writer_config);

        let log_manager = Arc::new(LogManager::new());
        let executor = QueryExecutor::new(store, config.index_path(), tm_wal_writer, log_manager)?;
        Ok(Self { executor })
    }

    /// Creates a new `Oxidb` instance or loads an existing one from the specified path,
    /// using default configuration for other settings.
    ///
    /// # Arguments
    /// * `db_path` - A path-like object (e.g., `&str`, `PathBuf`) specifying the location
    ///   of the database file. This will override `database_file_path` in the default config.
    ///
    /// # Errors
    /// Returns `OxidbError` if the store cannot be initialized or the executor cannot be created.
    pub fn new(db_path: impl AsRef<Path>) -> Result<Self, OxidbError> {
        let mut config = Config {
            // made mutable
            database_file_path: db_path.as_ref().to_string_lossy().into_owned(),
            ..Default::default()
        };
        // Make index_base_path relative to db_path's parent if default or empty
        if let Some(parent) = db_path.as_ref().parent() {
            if config.index_base_path.is_empty() || config.index_base_path == "oxidb_indexes/" {
                config.index_base_path =
                    parent.join("oxidb_indexes/").to_string_lossy().into_owned();
            }
        }
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
    /// Returns `ApiError` if the operation fails.
    pub fn insert(&mut self, key: Key, value: String) -> Result<Value, super::errors::ApiError> {
        let command = Command::Insert { key, value: DataType::String(value) };
        match self.executor.execute_command(command) {
            Ok(CoreExecutionResult::Success) => Ok(Value::Success),
            Ok(unexpected_result) => Err(super::errors::ApiError::CoreError(OxidbError::Internal(format!(
                "Insert: Expected Success, got {:?}",
                unexpected_result
            )))),
            Err(e) => Err(super::errors::ApiError::from(e)),
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
    /// * `Ok(Value::Single(Some(DataType)))` if the key is found.
    /// * `Ok(Value::Single(None))` if the key is not found.
    /// * `Err(ApiError)` if any other error occurs during the operation.
    pub fn get(&mut self, key: Key) -> Result<Value, super::errors::ApiError> {
        let command = Command::Get { key };
        match self.executor.execute_command(command) {
            Ok(CoreExecutionResult::Value(data_type_option)) => {
                Ok(Value::Single(data_type_option)) // Return the DataType directly
            }
            Ok(unexpected_result) => Err(super::errors::ApiError::CoreError(OxidbError::Internal(format!(
                "Get: Expected Value, got {:?}",
                unexpected_result
            )))),
            Err(e) => Err(super::errors::ApiError::from(e)),
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
    /// * `Ok(Value::Deleted(true))` if the key was found and successfully deleted.
    /// * `Ok(Value::Deleted(false))` if the key was not found.
    /// * `Err(ApiError)` if the operation fails.
    pub fn delete(&mut self, key: Key) -> Result<Value, super::errors::ApiError> {
        let command = Command::Delete { key };
        match self.executor.execute_command(command) {
            Ok(CoreExecutionResult::Deleted(status)) => Ok(Value::Deleted(status)),
            Ok(unexpected_result) => Err(super::errors::ApiError::CoreError(OxidbError::Internal(format!(
                "Delete: Expected Deleted, got {:?}",
                unexpected_result
            )))),
            Err(e) => Err(super::errors::ApiError::from(e)),
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
        self.executor.persist()
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
    /// * `Ok(Value)`: If the query is successfully parsed and executed. The `Value`
    ///   enum indicates the outcome of the command (e.g., `Value::Single(Some(data))` for a successful GET,
    ///   `Value::Success` for an INSERT, `Value::Deleted(true)` for a successful DELETE).
    /// * `Err(ApiError)`: If an error occurs at any stage. This can be:
    ///     - `ApiError::ParsingError(String)`: If the `query_string` is malformed.
    ///     - Other `ApiError` variants if the command execution itself fails.
    pub fn execute_query_str(&mut self, query_string: &str) -> Result<Value, super::errors::ApiError> {
        match parse_query_string(query_string) {
            Ok(command) => {
                match self.executor.execute_command(command) {
                    Ok(core_result) => Ok(Value::from(core_result)),
                    Err(oxidb_error) => Err(super::errors::ApiError::from(oxidb_error)),
                }
            }
            Err(oxidb_error) => Err(super::errors::ApiError::from(oxidb_error)),
        }
    }

    /// Returns the path to the main database file.
    #[allow(clippy::unwrap_used)] // Panicking on poisoned lock is acceptable here
    pub fn database_path(&self) -> PathBuf {
        self.executor.store.read().unwrap().file_path().to_path_buf()
    }

    /// Returns the base path for index storage.
    pub fn index_path(&self) -> PathBuf {
        self.executor.index_base_path()
    }

    /// Finds primary keys by an indexed value.
    ///
    /// # Arguments
    /// * `index_name` - The name of the index to search.
    /// * `value_to_find` - The `DataType` representing the value to search for in the index.
    ///
    /// # Returns
    /// * `Ok(Value::Multiple(Vec<DataType>))` containing found items (empty if none).
    /// * `Err(ApiError)` if any error occurs.
    pub fn find_by_index(
        &mut self,
        index_name: String,
        value_to_find: DataType,
    ) -> Result<Value, super::errors::ApiError> {
        // Serialize the DataType to Vec<u8> for the command
        let serialized_value =
            match crate::core::common::serialization::serialize_data_type(&value_to_find) {
                Ok(val) => val,
                Err(e) => {
                    // Convert OxidbError::Serialization to ApiError::SerializationError
                    return Err(super::errors::ApiError::SerializationError(format!(
                        "Failed to serialize value for index lookup: {}",
                        e
                    )));
                }
            };

        let command = Command::FindByIndex { index_name, value: serialized_value };

        match self.executor.execute_command(command) {
            Ok(CoreExecutionResult::Values(values_vec)) => {
                Ok(Value::Multiple(values_vec)) // Wrap the vec (even if empty) in Value::Multiple
            }
            Ok(unexpected_result) => Err(super::errors::ApiError::CoreError(OxidbError::Internal(format!(
                "FindByIndex: Expected Values, got {:?}",
                unexpected_result
            )))),
            Err(e) => Err(super::errors::ApiError::from(e)),
        }
    }
}
