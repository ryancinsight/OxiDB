//! Core QueryExecutor struct and essential methods
//!
//! This module contains the main QueryExecutor struct definition and core functionality
//! that doesn't fit into more specific categories.

use crate::core::common::types::TransactionId;
use crate::core::common::OxidbError;
use crate::core::indexing::manager::IndexManager;
use crate::core::optimizer::Optimizer;
use crate::core::storage::engine::traits::KeyValueStore;
use crate::core::transaction::lock_manager::LockManager;
use crate::core::transaction::manager::TransactionManager;
use crate::core::wal::log_manager::LogManager;
use crate::core::wal::writer::WalWriter;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

/// The main query executor structure
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

        if index_manager.get_index("default_value_index").is_none() {
            index_manager.create_index("default_value_index".to_string(), "hash").map_err(|e| {
                OxidbError::Index(format!("Failed to create default_value_index: {e}"))
            })?;
        }

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

    /// Get the index base path (placeholder for extracted method)
    pub fn index_base_path(&self) -> PathBuf {
        // This would contain the actual implementation
        PathBuf::from("placeholder")
    }

    /// Load auto-increment state (placeholder for extracted method)
    pub(crate) fn load_auto_increment_state(&mut self) -> Result<(), OxidbError> {
        // This would contain the actual implementation
        Ok(())
    }
}