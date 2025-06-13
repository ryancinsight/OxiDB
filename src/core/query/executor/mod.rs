// src/core/query/executor/mod.rs

// Module declarations
pub mod command_handlers;
pub mod ddl_handlers;
pub mod planner; // Added planner module
pub mod select_execution;
#[cfg(test)]
pub mod tests;
pub mod transaction_handlers;
pub mod update_execution;
pub mod utils;

// Re-export planner contents

// Necessary imports for struct definitions and the `new` method
use crate::core::common::OxidbError; // Changed
use crate::core::indexing::manager::IndexManager;
use crate::core::wal::writer::WalWriter; // Added for TransactionManager
use crate::core::optimizer::Optimizer;
use crate::core::storage::engine::traits::KeyValueStore;
use crate::core::storage::engine::SimpleFileKvStore;
use crate::core::transaction::lock_manager::LockManager;
use crate::core::transaction::manager::TransactionManager;
use crate::core::types::DataType;
use std::path::PathBuf;
use std::sync::{Arc, RwLock}; // Added RwLock // Added Optimizer

#[derive(Debug, PartialEq)]
pub enum ExecutionResult {
    Value(Option<DataType>),
    Success,
    Deleted(bool),
    Values(Vec<DataType>),
}

#[derive(Debug)]
pub struct QueryExecutor<S: KeyValueStore<Vec<u8>, Vec<u8>>> {
    pub(crate) store: Arc<RwLock<S>>,
    pub(crate) transaction_manager: TransactionManager,
    pub(crate) lock_manager: LockManager,
    pub(crate) index_manager: Arc<IndexManager>,
    pub(crate) optimizer: Optimizer, // Added optimizer field
}

// The `new` method remains here as it's tied to the struct definition visibility
impl<S: KeyValueStore<Vec<u8>, Vec<u8>>> QueryExecutor<S> {
    pub fn new(store: S, index_base_path: PathBuf, wal_writer: WalWriter) -> Result<Self, OxidbError> { // Changed
        let mut index_manager = IndexManager::new(index_base_path)?;

        if index_manager.get_index("default_value_index").is_none() {
            index_manager.create_index("default_value_index".to_string(), "hash").map_err(|e| {
                OxidbError::Index(format!("Failed to create default_value_index: {}", e)) // Changed
            })?;
        }

        let mut transaction_manager = TransactionManager::new(wal_writer);
        transaction_manager.add_committed_tx_id(0);
        let index_manager_arc = Arc::new(index_manager); // Create Arc for optimizer and self

        Ok(QueryExecutor {
            store: Arc::new(RwLock::new(store)),
            transaction_manager,
            lock_manager: LockManager::new(),
            optimizer: Optimizer::new(index_manager_arc.clone()), // Initialize optimizer
            index_manager: index_manager_arc,
        })
    }
}

// Methods specific to QueryExecutor when the store is SimpleFileKvStore
impl QueryExecutor<SimpleFileKvStore> {
    pub fn persist(&mut self) -> Result<(), OxidbError> { // Changed
        // Call the new persist method on SimpleFileKvStore
        self.store.read().unwrap().persist()?; // Use read lock if persist only needs to read cache
                                               // If persist needs to modify internal state of store (e.g. WAL writer),
                                               // then a write lock might be needed: self.store.write().unwrap().persist()?;
                                               // Given save_data_to_disk takes &self.cache, read lock is fine for cache access.
                                               // WalWriter is not directly used by persist() method of SimpleFileKvStore.
        self.index_manager.save_all_indexes()
    }

    pub fn index_base_path(&self) -> PathBuf {
        self.index_manager.base_path()
    }
}
