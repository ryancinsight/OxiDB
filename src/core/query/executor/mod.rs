// src/core/query/executor/mod.rs

// Module declarations
pub mod command_handlers;
pub mod utils;
pub mod select_execution;
pub mod update_execution;
pub mod transaction_handlers;
pub mod ddl_handlers;
pub mod planner; // Added planner module
#[cfg(test)]
pub mod tests;

pub use planner::*; // Re-export planner contents

// Necessary imports for struct definitions and the `new` method
use crate::core::common::error::DbError;
use crate::core::types::DataType;
use crate::core::storage::engine::SimpleFileKvStore;
use crate::core::storage::engine::traits::KeyValueStore;
use crate::core::indexing::manager::IndexManager;
use std::path::PathBuf;
use std::sync::{Arc, RwLock}; // Added RwLock
use crate::core::transaction::lock_manager::LockManager;
use crate::core::transaction::manager::TransactionManager;
use crate::core::optimizer::Optimizer; // Added Optimizer

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
    pub fn new(store: S, index_base_path: PathBuf) -> Result<Self, DbError> {
        let mut index_manager = IndexManager::new(index_base_path)?;

        if index_manager.get_index("default_value_index").is_none() {
            index_manager.create_index("default_value_index".to_string(), "hash")
                .map_err(|e| DbError::IndexError(format!("Failed to create default_value_index: {}", e.to_string())))?;
        }

        let mut transaction_manager = TransactionManager::new();
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
    pub fn persist(&mut self) -> Result<(), DbError> {
        self.store.write().unwrap().save_to_disk()?;
        self.index_manager.save_all_indexes()
    }
}
