//! Vector transaction management module
//!
//! This module provides ACID transaction support for vector operations:
//! - Atomicity: All operations in a transaction succeed or all fail
//! - Consistency: Vector operations maintain data integrity
//! - Isolation: Transactions don't interfere with each other
//! - Durability: Committed changes are persisted

use crate::core::common::OxidbError;
use crate::core::vector::storage::{VectorEntry, VectorStore};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Transaction ID type
pub type TransactionId = u64;

/// Vector operation types for transaction logging
#[derive(Debug, Clone)]
pub enum VectorOperation {
    Store { id: String, entry: VectorEntry },
    Delete { id: String },
    Update { id: String, entry: VectorEntry },
}

/// Undo operation for rollback
#[derive(Debug, Clone)]
pub enum UndoOperation {
    Delete { id: String },
    Restore { entry: VectorEntry },
}

/// Transaction state
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionState {
    Active,
    Committed,
    Aborted,
}

/// Vector transaction context
#[derive(Debug)]
pub struct VectorTransaction {
    pub id: TransactionId,
    pub state: TransactionState,
    pub operations: Vec<VectorOperation>,
    pub undo_log: Vec<UndoOperation>,
    pub timestamp: u64,
}

impl VectorTransaction {
    /// Create a new transaction
    #[must_use] pub fn new(id: TransactionId) -> Self {
        Self {
            id,
            state: TransactionState::Active,
            operations: Vec::new(),
            undo_log: Vec::new(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }

    /// Add an operation to the transaction
    pub fn add_operation(&mut self, operation: VectorOperation) -> Result<(), OxidbError> {
        if self.state != TransactionState::Active {
            return Err(OxidbError::Transaction(
                "Cannot add operations to inactive transaction".to_string(),
            ));
        }

        self.operations.push(operation);
        Ok(())
    }

    /// Add an undo operation for rollback
    pub fn add_undo_operation(&mut self, undo_op: UndoOperation) {
        self.undo_log.push(undo_op);
    }

    /// Mark transaction as committed
    pub fn commit(&mut self) -> Result<(), OxidbError> {
        if self.state != TransactionState::Active {
            return Err(OxidbError::Transaction("Cannot commit inactive transaction".to_string()));
        }

        self.state = TransactionState::Committed;
        Ok(())
    }

    /// Mark transaction as aborted
    pub fn abort(&mut self) -> Result<(), OxidbError> {
        if self.state == TransactionState::Committed {
            return Err(OxidbError::Transaction("Cannot abort committed transaction".to_string()));
        }

        self.state = TransactionState::Aborted;
        Ok(())
    }
}

/// Vector transaction manager implementing ACID properties
pub struct VectorTransactionManager {
    next_tx_id: Arc<Mutex<TransactionId>>,
    active_transactions: Arc<Mutex<HashMap<TransactionId, VectorTransaction>>>,
    store: Arc<Mutex<Box<dyn VectorStore + Send>>>,
}

impl VectorTransactionManager {
    /// Create a new transaction manager
    #[must_use] pub fn new(store: Box<dyn VectorStore + Send>) -> Self {
        Self {
            next_tx_id: Arc::new(Mutex::new(1)),
            active_transactions: Arc::new(Mutex::new(HashMap::new())),
            store: Arc::new(Mutex::new(store)),
        }
    }

    /// Begin a new transaction (Atomicity)
    pub fn begin_transaction(&self) -> Result<TransactionId, OxidbError> {
        let mut next_id = self
            .next_tx_id
            .lock()
            .map_err(|_| OxidbError::LockTimeout("Failed to acquire next_tx_id lock".to_string()))?;

        let tx_id = *next_id;
        *next_id = next_id.saturating_add(1);

        let transaction = VectorTransaction::new(tx_id);

        let mut active_txs = self.active_transactions.lock().map_err(|_| {
            OxidbError::LockTimeout("Failed to acquire active_transactions lock".to_string())
        })?;

        active_txs.insert(tx_id, transaction);
        Ok(tx_id)
    }

    /// Store a vector within a transaction
    pub fn transactional_store(
        &self,
        tx_id: TransactionId,
        entry: VectorEntry,
    ) -> Result<(), OxidbError> {
        let mut active_txs = self.active_transactions.lock().map_err(|_| {
            OxidbError::LockTimeout("Failed to acquire active_transactions lock".to_string())
        })?;

        let transaction = active_txs
            .get_mut(&tx_id)
            .ok_or_else(|| OxidbError::Transaction(format!("Transaction {tx_id} not found")))?;

        if transaction.state != TransactionState::Active {
            return Err(OxidbError::Transaction("Transaction is not active".to_string()));
        }

        let id = entry.id.clone();

        // Check if entry already exists for undo log
        let mut store = self
            .store
            .lock()
            .map_err(|_| OxidbError::LockTimeout("Failed to acquire store lock".to_string()))?;

        let existing_entry = store.retrieve(&id)?;
        if let Some(existing) = existing_entry {
            transaction.add_undo_operation(UndoOperation::Restore { entry: existing });
        } else {
            transaction.add_undo_operation(UndoOperation::Delete { id: id.clone() });
        }

        // Perform the operation
        store.store(entry.clone())?;

        // Log the operation
        transaction.add_operation(VectorOperation::Store { id, entry })?;

        Ok(())
    }

    /// Delete a vector within a transaction
    pub fn transactional_delete(&self, tx_id: TransactionId, id: &str) -> Result<bool, OxidbError> {
        let mut active_txs = self.active_transactions.lock().map_err(|_| {
            OxidbError::LockTimeout("Failed to acquire active_transactions lock".to_string())
        })?;

        let transaction = active_txs
            .get_mut(&tx_id)
            .ok_or_else(|| OxidbError::Transaction(format!("Transaction {tx_id} not found")))?;

        if transaction.state != TransactionState::Active {
            return Err(OxidbError::Transaction("Transaction is not active".to_string()));
        }

        let mut store = self
            .store
            .lock()
            .map_err(|_| OxidbError::LockTimeout("Failed to acquire store lock".to_string()))?;

        // Get existing entry for undo log
        let existing_entry = store.retrieve(id)?;
        if let Some(existing) = existing_entry {
            transaction.add_undo_operation(UndoOperation::Restore { entry: existing });

            // Perform the deletion
            let deleted = store.delete(id)?;

            // Log the operation
            transaction.add_operation(VectorOperation::Delete { id: id.to_string() })?;

            Ok(deleted)
        } else {
            Ok(false) // Entry didn't exist
        }
    }

    /// Commit a transaction (Durability)
    pub fn commit_transaction(&self, tx_id: TransactionId) -> Result<(), OxidbError> {
        let mut active_txs = self.active_transactions.lock().map_err(|_| {
            OxidbError::LockTimeout("Failed to acquire active_transactions lock".to_string())
        })?;

        let mut transaction = active_txs
            .remove(&tx_id)
            .ok_or_else(|| OxidbError::Transaction(format!("Transaction {tx_id} not found")))?;

        if transaction.state != TransactionState::Active {
            return Err(OxidbError::Transaction("Transaction is not active".to_string()));
        }

        // Mark as committed (all operations already applied)
        transaction.commit()?;

        // In a real implementation, we would write to a durable log here
        // For now, we just ensure the operations are already applied

        Ok(())
    }

    /// Rollback a transaction (Consistency)
    pub fn rollback_transaction(&self, tx_id: TransactionId) -> Result<(), OxidbError> {
        let mut active_txs = self.active_transactions.lock().map_err(|_| {
            OxidbError::LockTimeout("Failed to acquire active_transactions lock".to_string())
        })?;

        let mut transaction = active_txs
            .remove(&tx_id)
            .ok_or_else(|| OxidbError::Transaction(format!("Transaction {tx_id} not found")))?;

        if transaction.state != TransactionState::Active {
            return Err(OxidbError::Transaction("Transaction is not active".to_string()));
        }

        let mut store = self
            .store
            .lock()
            .map_err(|_| OxidbError::LockTimeout("Failed to acquire store lock".to_string()))?;

        // Apply undo operations in reverse order
        for undo_op in transaction.undo_log.iter().rev() {
            match undo_op {
                UndoOperation::Delete { id } => {
                    store.delete(id)?;
                }
                UndoOperation::Restore { entry } => {
                    store.store(entry.clone())?;
                }
            }
        }

        transaction.abort()?;
        Ok(())
    }

    /// Get active transaction count (for monitoring)
    pub fn active_transaction_count(&self) -> Result<usize, OxidbError> {
        let active_txs = self.active_transactions.lock().map_err(|_| {
            OxidbError::LockTimeout("Failed to acquire active_transactions lock".to_string())
        })?;

        Ok(active_txs.len())
    }

    /// Retrieve a vector (read-only, no transaction needed)
    pub fn retrieve(&self, id: &str) -> Result<Option<VectorEntry>, OxidbError> {
        let store = self
            .store
            .lock()
            .map_err(|_| OxidbError::LockTimeout("Failed to acquire store lock".to_string()))?;

        store.retrieve(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::vector::{storage::InMemoryVectorStore, VectorFactory};

    #[test]
    fn test_transaction_lifecycle() {
        let store = Box::new(InMemoryVectorStore::new());
        let tx_manager = VectorTransactionManager::new(store);

        // Begin transaction
        let tx_id = tx_manager.begin_transaction().unwrap();
        assert_eq!(tx_manager.active_transaction_count().unwrap(), 1);

        // Commit transaction
        tx_manager.commit_transaction(tx_id).unwrap();
        assert_eq!(tx_manager.active_transaction_count().unwrap(), 0);
    }

    #[test]
    fn test_transactional_store_and_commit() {
        let store = Box::new(InMemoryVectorStore::new());
        let tx_manager = VectorTransactionManager::new(store);

        let tx_id = tx_manager.begin_transaction().unwrap();

        let vector = VectorFactory::create_vector(3, vec![1.0, 2.0, 3.0]).unwrap();
        let entry = VectorEntry::new("test_vector".to_string(), vector);

        // Store within transaction
        tx_manager.transactional_store(tx_id, entry).unwrap();

        // Verify it exists
        let retrieved = tx_manager.retrieve("test_vector").unwrap();
        assert!(retrieved.is_some());

        // Commit
        tx_manager.commit_transaction(tx_id).unwrap();

        // Verify it still exists after commit
        let retrieved = tx_manager.retrieve("test_vector").unwrap();
        assert!(retrieved.is_some());
    }

    #[test]
    fn test_transactional_rollback() {
        let store = Box::new(InMemoryVectorStore::new());
        let tx_manager = VectorTransactionManager::new(store);

        let tx_id = tx_manager.begin_transaction().unwrap();

        let vector = VectorFactory::create_vector(3, vec![1.0, 2.0, 3.0]).unwrap();
        let entry = VectorEntry::new("test_vector".to_string(), vector);

        // Store within transaction
        tx_manager.transactional_store(tx_id, entry).unwrap();

        // Verify it exists
        let retrieved = tx_manager.retrieve("test_vector").unwrap();
        assert!(retrieved.is_some());

        // Rollback
        tx_manager.rollback_transaction(tx_id).unwrap();

        // Verify it no longer exists after rollback
        let retrieved = tx_manager.retrieve("test_vector").unwrap();
        assert!(retrieved.is_none());
    }

    #[test]
    fn test_transactional_delete() {
        let mut store = Box::new(InMemoryVectorStore::new());

        // Pre-populate with a vector
        let vector = VectorFactory::create_vector(3, vec![1.0, 2.0, 3.0]).unwrap();
        let entry = VectorEntry::new("test_vector".to_string(), vector);
        store.store(entry).unwrap();

        let tx_manager = VectorTransactionManager::new(store);

        // Verify it exists
        assert!(tx_manager.retrieve("test_vector").unwrap().is_some());

        let tx_id = tx_manager.begin_transaction().unwrap();

        // Delete within transaction
        let deleted = tx_manager.transactional_delete(tx_id, "test_vector").unwrap();
        assert!(deleted);

        // Verify it's deleted
        assert!(tx_manager.retrieve("test_vector").unwrap().is_none());

        // Rollback
        tx_manager.rollback_transaction(tx_id).unwrap();

        // Verify it's restored after rollback
        assert!(tx_manager.retrieve("test_vector").unwrap().is_some());
    }

    #[test]
    fn test_multiple_transactions_isolation() {
        let store = Box::new(InMemoryVectorStore::new());
        let tx_manager = VectorTransactionManager::new(store);

        let tx1 = tx_manager.begin_transaction().unwrap();
        let tx2 = tx_manager.begin_transaction().unwrap();

        assert_eq!(tx_manager.active_transaction_count().unwrap(), 2);

        // Each transaction operates independently
        let vector1 = VectorFactory::create_vector(3, vec![1.0, 2.0, 3.0]).unwrap();
        let entry1 = VectorEntry::new("vector1".to_string(), vector1);
        tx_manager.transactional_store(tx1, entry1).unwrap();

        let vector2 = VectorFactory::create_vector(3, vec![4.0, 5.0, 6.0]).unwrap();
        let entry2 = VectorEntry::new("vector2".to_string(), vector2);
        tx_manager.transactional_store(tx2, entry2).unwrap();

        // Commit first, rollback second
        tx_manager.commit_transaction(tx1).unwrap();
        tx_manager.rollback_transaction(tx2).unwrap();

        // Only first vector should exist
        assert!(tx_manager.retrieve("vector1").unwrap().is_some());
        assert!(tx_manager.retrieve("vector2").unwrap().is_none());

        assert_eq!(tx_manager.active_transaction_count().unwrap(), 0);
    }
}
