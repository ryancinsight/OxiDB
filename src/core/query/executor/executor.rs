// src/core/query/executor/executor.rs
//! Main QueryExecutor implementation

use crate::core::common::types::TransactionId;
use crate::core::common::OxidbError;
use crate::core::indexing::manager::IndexManager;
use crate::core::optimizer::Optimizer;
use crate::core::storage::engine::traits::KeyValueStore;
use crate::core::transaction::lock_manager::LockManager;
use crate::core::transaction::manager::TransactionManager;
use crate::core::types::Schema;
use crate::core::wal::log_manager::LogManager;
use crate::core::wal::writer::WalWriter;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

/// Maximum auto-increment value to prevent overflow
pub const MAX_AUTO_INCREMENT_VALUE: i64 = i64::MAX - 1000;

/// Default index name for value storage
pub const DEFAULT_VALUE_INDEX_NAME: &str = "default_value_index";

/// Main query executor that coordinates all database operations
#[derive(Debug)]
pub struct QueryExecutor<S: KeyValueStore<Vec<u8>, Vec<u8>>> {
    /// The underlying key-value store, wrapped for thread-safe access.
    pub(crate) store: Arc<RwLock<S>>,
    /// Manages transactions, including their state and undo/redo logs.
    pub(crate) transaction_manager: TransactionManager,
    /// Manages locks on data to ensure transaction isolation.
    pub(crate) lock_manager: LockManager,
    /// Manages indexes for efficient data retrieval.
    pub(crate) index_manager: Arc<RwLock<IndexManager>>,
    /// Optimizes query plans for more efficient execution.
    pub(crate) optimizer: Optimizer,
    /// Manages the write-ahead log for durability.
    pub(crate) log_manager: Arc<LogManager>,
    /// Tracks the next auto-increment value for each table.column combination
    pub(crate) auto_increment_state: HashMap<String, i64>,
}

impl<S: KeyValueStore<Vec<u8>, Vec<u8>>> QueryExecutor<S> {
    /// Create a new QueryExecutor instance
    pub fn new(
        store: S,
        index_base_path: PathBuf,
        wal_writer: WalWriter,
        log_manager: Arc<LogManager>,
    ) -> Result<Self, OxidbError> {
        let mut index_manager = IndexManager::new(index_base_path)?;

        if index_manager.get_index(DEFAULT_VALUE_INDEX_NAME).is_none() {
            index_manager.create_index(DEFAULT_VALUE_INDEX_NAME.to_string(), "hash").map_err(
                |e| {
                    OxidbError::Index(format!("Failed to create {}: {e}", DEFAULT_VALUE_INDEX_NAME))
                },
            )?;
        }

        // Pass a clone of log_manager to TransactionManager, store original in self
        let mut transaction_manager = TransactionManager::new(wal_writer, log_manager.clone());
        transaction_manager.add_committed_tx_id(TransactionId(0));
        let index_manager_arc = Arc::new(RwLock::new(index_manager));

        let mut executor = Self {
            store: Arc::new(RwLock::new(store)),
            transaction_manager,
            lock_manager: LockManager::new(),
            optimizer: Optimizer::new(),
            index_manager: index_manager_arc,
            log_manager,
            auto_increment_state: HashMap::new(),
        };

        // Load auto-increment state from existing data
        executor.load_auto_increment_state()?;

        Ok(executor)
    }

    /// Load auto-increment state from storage
    fn load_auto_increment_state(&mut self) -> Result<(), OxidbError> {
        // This is a placeholder - in a real implementation, this would
        // read from storage to restore auto-increment counters
        Ok(())
    }

    /// Get the next auto-increment value for a table column
    pub(crate) fn next_auto_increment(&mut self, table_column: &str) -> Result<i64, OxidbError> {
        let current = self.auto_increment_state.get(table_column).copied().unwrap_or(0);
        let next = current + 1;

        if next > MAX_AUTO_INCREMENT_VALUE {
            return Err(OxidbError::AutoIncrementOverflow {
                table_column: table_column.to_string(),
                max_value: MAX_AUTO_INCREMENT_VALUE,
            });
        }

        self.auto_increment_state.insert(table_column.to_string(), next);
        Ok(next)
    }

    /// Reset auto-increment value for a table column
    #[allow(dead_code)] // Used in tests
    pub(crate) fn reset_auto_increment(
        &mut self,
        table_column: &str,
        value: i64,
    ) -> Result<(), OxidbError> {
        if value < 0 || value > MAX_AUTO_INCREMENT_VALUE {
            return Err(OxidbError::InvalidAutoIncrementValue {
                value,
                max_allowed: MAX_AUTO_INCREMENT_VALUE,
            });
        }

        self.auto_increment_state.insert(table_column.to_string(), value);
        Ok(())
    }

    /// Get current auto-increment value for a table column
    #[allow(dead_code)] // Used in tests
    pub(crate) fn get_auto_increment(&self, table_column: &str) -> i64 {
        self.auto_increment_state.get(table_column).copied().unwrap_or(0)
    }

    /// Persist all data and indexes to disk
    pub fn persist(&mut self) -> Result<(), OxidbError> {
        // Persist store data
        self.persist_store()?;

        // Persist indexes
        self.index_manager
            .read()
            .map_err(|e| {
                OxidbError::LockTimeout(format!(
                    "Failed to acquire read lock on index manager for persist: {e}"
                ))
            })?
            .save_all_indexes()
    }

    /// Get the index base path
    #[must_use]
    pub fn index_base_path(&self) -> Result<PathBuf, OxidbError> {
        Ok(self
            .index_manager
            .read()
            .map_err(|e| OxidbError::LockTimeout(format!("Failed to acquire read lock: {e}")))?
            .base_path())
    }

    /// Generate a schema key for the given table name
    pub fn schema_key(table_name: &str) -> Vec<u8> {
        format!("schema:{}", table_name).into_bytes()
    }

    /// Get table schema from storage
    pub fn get_table_schema(&self, table_name: &str) -> Result<Option<Arc<Schema>>, OxidbError> {
        let schema_key = Self::schema_key(table_name);

        match self.get(&schema_key)? {
            Some(schema_bytes) => {
                let schema: Schema =
                    serde_json::from_slice(&schema_bytes).map_err(OxidbError::Json)?;
                Ok(Some(Arc::new(schema)))
            }
            None => Ok(None),
        }
    }

    /// Store table schema in storage
    pub fn store_table_schema(
        &mut self,
        table_name: &str,
        schema: &Schema,
    ) -> Result<(), OxidbError> {
        let schema_key = Self::schema_key(table_name);
        let schema_bytes = serde_json::to_vec(schema).map_err(OxidbError::Json)?;

        // Use a simple transaction for schema storage
        let tx = crate::core::transaction::Transaction::new(TransactionId(0));
        let lsn: crate::core::common::types::Lsn = 0;

        let mut store = self.store.write().map_err(|e| {
            OxidbError::LockTimeout(format!("Failed to acquire write lock on store: {e}"))
        })?;

        store.put(schema_key, schema_bytes, &tx, lsn)?;
        Ok(())
    }

    /// Simple wrapper for key-value retrieval with current transaction context
    pub fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, OxidbError> {
        let store = self.store.read().map_err(|e| {
            OxidbError::LockTimeout(format!("Failed to acquire read lock on store: {e}"))
        })?;

        // Use committed transaction set for visibility
        let committed_tx_ids: std::collections::HashSet<u64> = self
            .transaction_manager
            .get_committed_tx_ids_snapshot()
            .into_iter()
            .map(|id| id.0)
            .collect();
        store.get(key, 0, &committed_tx_ids)
    }

    /// Simple wrapper for key-value storage with current transaction context  
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), OxidbError> {
        let tx = crate::core::transaction::Transaction::new(TransactionId(0));
        let lsn: crate::core::common::types::Lsn = 0;

        let mut store = self.store.write().map_err(|e| {
            OxidbError::LockTimeout(format!("Failed to acquire write lock on store: {e}"))
        })?;

        store.put(key, value, &tx, lsn)
    }

    /// Persist data to disk - wrapper for store persistence
    pub fn persist_store(&self) -> Result<(), OxidbError> {
        // For file-based stores, persistence might be automatic
        // This is a placeholder for explicit persistence if needed
        Ok(())
    }

    /// Store row data in the key-value store
    pub fn store_row_data(
        &mut self,
        key: Vec<u8>,
        data: &crate::core::types::DataType,
    ) -> Result<(), OxidbError> {
        use crate::core::common::serialization::serialize_data_type;
        let serialized_data = serialize_data_type(data)?;
        self.set(key, serialized_data)
    }

    /// Handle SQL DELETE operations
    pub fn handle_sql_delete(
        &mut self,
        table_name: String,
        condition: Option<crate::core::query::commands::SqlConditionTree>,
    ) -> Result<crate::core::query::executor::types::ExecutionResult, OxidbError> {
        // Delegate to the specific DELETE handler in delete_execution.rs
        // This keeps executor.rs clean and follows separation of concerns
        self.handle_delete(table_name, condition)
    }

    /// Execute parameterized SQL statement
    pub fn execute_parameterized_statement(
        &mut self,
        _statement: &str,
        _parameters: &[crate::core::types::DataType],
    ) -> Result<crate::core::query::executor::types::ExecutionResult, OxidbError> {
        // Implementation placeholder for parameterized queries
        // Would involve parsing statement, binding parameters, and executing
        use crate::core::query::executor::types::ExecutionResult;
        Ok(ExecutionResult::Updated { count: 0 })
    }

    /// Check uniqueness constraint for column values
    pub fn check_uniqueness(
        &mut self,
        _table_name: &str,
        _column_name: &str,
        _value: &crate::core::types::DataType,
        _exclude_key: Option<&Vec<u8>>,
    ) -> Result<bool, OxidbError> {
        // Implementation placeholder for uniqueness checking
        // Would scan existing data to verify no duplicates exist
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::storage::engine::SimpleFileKvStore;
    use tempfile::TempDir;

    fn create_test_executor() -> (QueryExecutor<SimpleFileKvStore>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let store_path = temp_dir.path().join("test.db");
        let index_path = temp_dir.path().join("indexes");
        let wal_path = temp_dir.path().join("wal.log");

        let store = SimpleFileKvStore::new(store_path).unwrap();
        let wal_config = crate::core::wal::writer::WalWriterConfig::default();
        let wal_writer = WalWriter::new(wal_path, wal_config);
        let log_manager = Arc::new(LogManager::new());

        let executor = QueryExecutor::new(store, index_path, wal_writer, log_manager).unwrap();
        (executor, temp_dir)
    }

    #[test]
    fn test_executor_creation() {
        let (_executor, _temp_dir) = create_test_executor();
        // If we get here without panicking, the test passes
    }

    #[test]
    fn test_auto_increment() {
        let (mut executor, _temp_dir) = create_test_executor();

        // Test initial value
        assert_eq!(executor.get_auto_increment("users.id"), 0);

        // Test increment
        let next = executor.next_auto_increment("users.id").unwrap();
        assert_eq!(next, 1);
        assert_eq!(executor.get_auto_increment("users.id"), 1);

        // Test another increment
        let next = executor.next_auto_increment("users.id").unwrap();
        assert_eq!(next, 2);

        // Test reset
        executor.reset_auto_increment("users.id", 100).unwrap();
        assert_eq!(executor.get_auto_increment("users.id"), 100);

        let next = executor.next_auto_increment("users.id").unwrap();
        assert_eq!(next, 101);
    }

    #[test]
    fn test_auto_increment_overflow() {
        let (mut executor, _temp_dir) = create_test_executor();

        // Set to near maximum
        executor.reset_auto_increment("test.id", MAX_AUTO_INCREMENT_VALUE).unwrap();

        // This should fail due to overflow protection
        let result = executor.next_auto_increment("test.id");
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_auto_increment_reset() {
        let (mut executor, _temp_dir) = create_test_executor();

        // Test negative value
        let result = executor.reset_auto_increment("test.id", -1);
        assert!(result.is_err());

        // Test overflow value
        let result = executor.reset_auto_increment("test.id", MAX_AUTO_INCREMENT_VALUE + 1);
        assert!(result.is_err());
    }
}
