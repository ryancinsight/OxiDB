// src/core/storage/manager.rs
//! Storage Manager implementing SOLID, CUPID, GRASP, DRY, YAGNI, and ACID principles
//!
//! This module provides a high-level storage abstraction that ensures:
//! - Single Responsibility: Each component has one clear purpose
//! - Open/Closed: Extensible through traits without modification
//! - Liskov Substitution: Implementations are interchangeable
//! - Interface Segregation: Focused, minimal interfaces
//! - Dependency Inversion: Depends on abstractions, not concretions
//! - CUPID: Composable, Unix-like, Predictable, Idiomatic, Domain-focused
//! - GRASP: High cohesion, low coupling, information expert pattern
//! - DRY: No code duplication
//! - YAGNI: Only implement what's needed
//! - ACID: Atomicity, Consistency, Isolation, Durability

use crate::core::common::OxidbError;
use crate::core::types::{DataType, TransactionId};
use std::collections::HashMap;
use async_trait::async_trait;

/// Transaction state for ACID compliance
#[derive(Debug, Clone, PartialEq)]
pub enum TransactionState {
    Active,
    Preparing,
    Committed,
    Aborted,
}

/// Transaction metadata
#[derive(Debug, Clone)]
pub struct TransactionContext {
    pub id: TransactionId,
    pub state: TransactionState,
    pub operations: Vec<Operation>,
    pub isolation_level: IsolationLevel,
}

/// Isolation levels for transaction isolation
#[derive(Debug, Clone, PartialEq)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

/// Storage operations for transaction logging
#[derive(Debug, Clone)]
pub enum Operation {
    Insert { key: Vec<u8>, value: DataType },
    Update { key: Vec<u8>, old_value: DataType, new_value: DataType },
    Delete { key: Vec<u8>, value: DataType },
}

/// Core storage interface following Interface Segregation Principle
#[async_trait]
pub trait StorageEngine: Send + Sync {
    /// Get a value by key with transaction context
    async fn get(&self, key: &[u8], tx_context: &TransactionContext) -> Result<Option<DataType>, OxidbError>;
    
    /// Insert or update a value
    async fn put(&mut self, key: Vec<u8>, value: DataType, tx_context: &mut TransactionContext) -> Result<(), OxidbError>;
    
    /// Delete a value by key
    async fn delete(&mut self, key: &[u8], tx_context: &mut TransactionContext) -> Result<bool, OxidbError>;
    
    /// Persist changes to durable storage
    async fn flush(&mut self) -> Result<(), OxidbError>;
}

/// Transaction management interface
#[async_trait]
pub trait TransactionManager: Send + Sync {
    /// Begin a new transaction
    async fn begin_transaction(&mut self, isolation_level: IsolationLevel) -> Result<TransactionContext, OxidbError>;
    
    /// Commit a transaction (ACID compliance)
    async fn commit_transaction(&mut self, tx_context: &mut TransactionContext) -> Result<(), OxidbError>;
    
    /// Abort a transaction (ACID compliance)
    async fn abort_transaction(&mut self, tx_context: &mut TransactionContext) -> Result<(), OxidbError>;
    
    /// Check if transaction can proceed (isolation control)
    async fn can_read(&self, key: &[u8], tx_context: &TransactionContext) -> Result<bool, OxidbError>;
    
    /// Check if transaction can write (isolation control)
    async fn can_write(&self, key: &[u8], tx_context: &TransactionContext) -> Result<bool, OxidbError>;
}

/// Lock management for concurrency control
#[async_trait]
pub trait LockManager: Send + Sync {
    /// Acquire a read lock
    async fn acquire_read_lock(&mut self, key: &[u8], tx_id: TransactionId) -> Result<(), OxidbError>;
    
    /// Acquire a write lock
    async fn acquire_write_lock(&mut self, key: &[u8], tx_id: TransactionId) -> Result<(), OxidbError>;
    
    /// Release locks for a transaction
    async fn release_locks(&mut self, tx_id: TransactionId) -> Result<(), OxidbError>;
}

/// Durability manager for ACID compliance
#[async_trait]
pub trait DurabilityManager: Send + Sync {
    /// Write operation to WAL
    async fn log_operation(&mut self, operation: &Operation, tx_id: TransactionId) -> Result<(), OxidbError>;
    
    /// Ensure all logs are written to durable storage
    async fn sync_logs(&mut self) -> Result<(), OxidbError>;
    
    /// Recover from WAL after crash
    async fn recover(&mut self) -> Result<Vec<TransactionContext>, OxidbError>;
}

/// Main storage manager implementing all SOLID principles
pub struct EnhancedStorageManager<S, T, L, D>
where
    S: StorageEngine,
    T: TransactionManager,
    L: LockManager,
    D: DurabilityManager,
{
    storage_engine: S,
    transaction_manager: T,
    lock_manager: L,
    durability_manager: D,
    active_transactions: HashMap<TransactionId, TransactionContext>,
}

impl<S, T, L, D> EnhancedStorageManager<S, T, L, D>
where
    S: StorageEngine,
    T: TransactionManager,
    L: LockManager,
    D: DurabilityManager,
{
    /// Create a new storage manager (Dependency Inversion Principle)
    pub fn new(
        storage_engine: S,
        transaction_manager: T,
        lock_manager: L,
        durability_manager: D,
    ) -> Self {
        Self {
            storage_engine,
            transaction_manager,
            lock_manager,
            durability_manager,
            active_transactions: HashMap::new(),
        }
    }

    /// Begin a new transaction with ACID guarantees
    pub async fn begin_transaction(&mut self, isolation_level: IsolationLevel) -> Result<TransactionId, OxidbError> {
        let tx_context = self.transaction_manager.begin_transaction(isolation_level).await?;
        let tx_id = tx_context.id;
        self.active_transactions.insert(tx_id, tx_context);
        Ok(tx_id)
    }

    /// Commit transaction with full ACID compliance
    pub async fn commit_transaction(&mut self, tx_id: TransactionId) -> Result<(), OxidbError> {
        let mut tx_context = self.active_transactions.remove(&tx_id)
            .ok_or_else(|| OxidbError::Transaction("Transaction not found".to_string()))?;

        // Ensure durability before committing
        for operation in &tx_context.operations {
            self.durability_manager.log_operation(operation, tx_id).await?;
        }
        self.durability_manager.sync_logs().await?;

        // Commit the transaction
        self.transaction_manager.commit_transaction(&mut tx_context).await?;
        
        // Release all locks
        self.lock_manager.release_locks(tx_id).await?;
        
        // Flush storage engine
        self.storage_engine.flush().await?;

        Ok(())
    }

    /// Abort transaction with cleanup
    pub async fn abort_transaction(&mut self, tx_id: TransactionId) -> Result<(), OxidbError> {
        let mut tx_context = self.active_transactions.remove(&tx_id)
            .ok_or_else(|| OxidbError::Transaction("Transaction not found".to_string()))?;

        // Abort the transaction
        self.transaction_manager.abort_transaction(&mut tx_context).await?;
        
        // Release all locks
        self.lock_manager.release_locks(tx_id).await?;

        Ok(())
    }

    /// Read with transaction isolation
    pub async fn get(&self, key: &[u8], tx_id: TransactionId) -> Result<Option<DataType>, OxidbError> {
        let tx_context = self.active_transactions.get(&tx_id)
            .ok_or_else(|| OxidbError::Transaction("Transaction not found".to_string()))?;

        // Check read permissions
        if !self.transaction_manager.can_read(key, tx_context).await? {
            return Err(OxidbError::Transaction("Read not allowed due to isolation level".to_string()));
        }

        self.storage_engine.get(key, tx_context).await
    }

    /// Write with transaction isolation and locking
    pub async fn put(&mut self, key: Vec<u8>, value: DataType, tx_id: TransactionId) -> Result<(), OxidbError> {
        let tx_context = self.active_transactions.get_mut(&tx_id)
            .ok_or_else(|| OxidbError::Transaction("Transaction not found".to_string()))?;

        // Check write permissions
        if !self.transaction_manager.can_write(&key, tx_context).await? {
            return Err(OxidbError::Transaction("Write not allowed due to isolation level".to_string()));
        }

        // Acquire write lock
        self.lock_manager.acquire_write_lock(&key, tx_id).await?;

        // Perform the operation
        self.storage_engine.put(key.clone(), value.clone(), tx_context).await?;

        // Log the operation for durability
        let operation = Operation::Insert { key, value };
        tx_context.operations.push(operation);

        Ok(())
    }

    /// Delete with transaction isolation and locking
    pub async fn delete(&mut self, key: &[u8], tx_id: TransactionId) -> Result<bool, OxidbError> {
        let tx_context = self.active_transactions.get_mut(&tx_id)
            .ok_or_else(|| OxidbError::Transaction("Transaction not found".to_string()))?;

        // Check write permissions
        if !self.transaction_manager.can_write(key, tx_context).await? {
            return Err(OxidbError::Transaction("Write not allowed due to isolation level".to_string()));
        }

        // Acquire write lock
        self.lock_manager.acquire_write_lock(key, tx_id).await?;

        // Get current value for logging
        let current_value = self.storage_engine.get(key, tx_context).await?;

        // Perform the deletion
        let deleted = self.storage_engine.delete(key, tx_context).await?;

        if deleted {
            if let Some(value) = current_value {
                // Log the operation for durability
                let operation = Operation::Delete { 
                    key: key.to_vec(), 
                    value 
                };
                tx_context.operations.push(operation);
            }
        }

        Ok(deleted)
    }

    /// Recover from crash (ACID durability)
    pub async fn recover(&mut self) -> Result<(), OxidbError> {
        let recovered_transactions = self.durability_manager.recover().await?;
        
        for tx_context in recovered_transactions {
            // Replay committed transactions
            if tx_context.state == TransactionState::Committed {
                for operation in &tx_context.operations {
                    match operation {
                        Operation::Insert { key, value } => {
                            let mut temp_tx = tx_context.clone();
                            self.storage_engine.put(key.clone(), value.clone(), &mut temp_tx).await?;
                        }
                        Operation::Delete { key, .. } => {
                            let mut temp_tx = tx_context.clone();
                            self.storage_engine.delete(key, &mut temp_tx).await?;
                        }
                        Operation::Update { key, new_value, .. } => {
                            let mut temp_tx = tx_context.clone();
                            self.storage_engine.put(key.clone(), new_value.clone(), &mut temp_tx).await?;
                        }
                    }
                }
            }
        }

        self.storage_engine.flush().await?;
        Ok(())
    }
}

/// Builder pattern for creating storage managers (GRASP Creator pattern)
pub struct StorageManagerBuilder<S, T, L, D>
where
    S: StorageEngine,
    T: TransactionManager,
    L: LockManager,
    D: DurabilityManager,
{
    storage_engine: Option<S>,
    transaction_manager: Option<T>,
    lock_manager: Option<L>,
    durability_manager: Option<D>,
}

impl<S, T, L, D> Default for StorageManagerBuilder<S, T, L, D>
where
    S: StorageEngine,
    T: TransactionManager,
    L: LockManager,
    D: DurabilityManager,
{
    fn default() -> Self {
        Self {
            storage_engine: None,
            transaction_manager: None,
            lock_manager: None,
            durability_manager: None,
        }
    }
}

impl<S, T, L, D> StorageManagerBuilder<S, T, L, D>
where
    S: StorageEngine,
    T: TransactionManager,
    L: LockManager,
    D: DurabilityManager,
{
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_storage_engine(mut self, engine: S) -> Self {
        self.storage_engine = Some(engine);
        self
    }

    pub fn with_transaction_manager(mut self, manager: T) -> Self {
        self.transaction_manager = Some(manager);
        self
    }

    pub fn with_lock_manager(mut self, manager: L) -> Self {
        self.lock_manager = Some(manager);
        self
    }

    pub fn with_durability_manager(mut self, manager: D) -> Self {
        self.durability_manager = Some(manager);
        self
    }

    pub fn build(self) -> Result<EnhancedStorageManager<S, T, L, D>, OxidbError> {
        let storage_engine = self.storage_engine
            .ok_or_else(|| OxidbError::Configuration { section: "Unknown".to_string(), message: "Storage engine not provided".to_string( }))?;
        let transaction_manager = self.transaction_manager
            .ok_or_else(|| OxidbError::Configuration { section: "Unknown".to_string(), message: "Transaction manager not provided".to_string( }))?;
        let lock_manager = self.lock_manager
            .ok_or_else(|| OxidbError::Configuration { section: "Unknown".to_string(), message: "Lock manager not provided".to_string( }))?;
        let durability_manager = self.durability_manager
            .ok_or_else(|| OxidbError::Configuration { section: "Unknown".to_string(), message: "Durability manager not provided".to_string( }))?;

        Ok(EnhancedStorageManager::new(
            storage_engine,
            transaction_manager,
            lock_manager,
            durability_manager,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::collections::BTreeMap;

    // Mock implementations for testing
    struct MockStorageEngine {
        data: Arc<Mutex<BTreeMap<Vec<u8>, DataType>>>,
    }

    impl MockStorageEngine {
        fn new() -> Self {
            Self {
                data: Arc::new(Mutex::new(BTreeMap::new())),
            }
        }
    }

    #[async_trait]
    impl StorageEngine for MockStorageEngine {
        async fn get(&self, key: &[u8], _tx_context: &TransactionContext) -> Result<Option<DataType>, OxidbError> {
            let data = self.data.lock().map_err(|_| OxidbError::LockTimeout("Lock poisoned".to_string()))?;
            Ok(data.get(key).cloned())
        }

        async fn put(&mut self, key: Vec<u8>, value: DataType, _tx_context: &mut TransactionContext) -> Result<(), OxidbError> {
            let mut data = self.data.lock().map_err(|_| OxidbError::LockTimeout("Lock poisoned".to_string()))?;
            data.insert(key, value);
            Ok(())
        }

        async fn delete(&mut self, key: &[u8], _tx_context: &mut TransactionContext) -> Result<bool, OxidbError> {
            let mut data = self.data.lock().map_err(|_| OxidbError::LockTimeout("Lock poisoned".to_string()))?;
            Ok(data.remove(key).is_some())
        }

        async fn flush(&mut self) -> Result<(), OxidbError> {
            Ok(())
        }
    }

    struct MockTransactionManager {
        next_tx_id: TransactionId,
    }

    impl MockTransactionManager {
        fn new() -> Self {
            Self { next_tx_id: TransactionId(1) }
        }
    }

    #[async_trait]
    impl TransactionManager for MockTransactionManager {
        async fn begin_transaction(&mut self, isolation_level: IsolationLevel) -> Result<TransactionContext, OxidbError> {
            let tx_id = self.next_tx_id;
            self.next_tx_id = TransactionId(self.next_tx_id.0 + 1);
            
            Ok(TransactionContext {
                id: tx_id,
                state: TransactionState::Active,
                operations: Vec::new(),
                isolation_level,
            })
        }

        async fn commit_transaction(&mut self, tx_context: &mut TransactionContext) -> Result<(), OxidbError> {
            tx_context.state = TransactionState::Committed;
            Ok(())
        }

        async fn abort_transaction(&mut self, tx_context: &mut TransactionContext) -> Result<(), OxidbError> {
            tx_context.state = TransactionState::Aborted;
            Ok(())
        }

        async fn can_read(&self, _key: &[u8], _tx_context: &TransactionContext) -> Result<bool, OxidbError> {
            Ok(true)
        }

        async fn can_write(&self, _key: &[u8], _tx_context: &TransactionContext) -> Result<bool, OxidbError> {
            Ok(true)
        }
    }

    struct MockLockManager;

    #[async_trait]
    impl LockManager for MockLockManager {
        async fn acquire_read_lock(&mut self, _key: &[u8], _tx_id: TransactionId) -> Result<(), OxidbError> {
            Ok(())
        }

        async fn acquire_write_lock(&mut self, _key: &[u8], _tx_id: TransactionId) -> Result<(), OxidbError> {
            Ok(())
        }

        async fn release_locks(&mut self, _tx_id: TransactionId) -> Result<(), OxidbError> {
            Ok(())
        }
    }

    struct MockDurabilityManager;

    #[async_trait]
    impl DurabilityManager for MockDurabilityManager {
        async fn log_operation(&mut self, _operation: &Operation, _tx_id: TransactionId) -> Result<(), OxidbError> {
            Ok(())
        }

        async fn sync_logs(&mut self) -> Result<(), OxidbError> {
            Ok(())
        }

        async fn recover(&mut self) -> Result<Vec<TransactionContext>, OxidbError> {
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn test_storage_manager_transaction_lifecycle() {
        let mut manager = StorageManagerBuilder::new()
            .with_storage_engine(MockStorageEngine::new())
            .with_transaction_manager(MockTransactionManager::new())
            .with_lock_manager(MockLockManager)
            .with_durability_manager(MockDurabilityManager)
            .build()
            .unwrap();

        // Begin transaction
        let tx_id = manager.begin_transaction(IsolationLevel::ReadCommitted).await.unwrap();

        // Perform operations
        let key = b"test_key".to_vec();
        let value = DataType::String("test_value".to_string());
        
        manager.put(key.clone(), value.clone(), tx_id).await.unwrap();
        
        let retrieved = manager.get(&key, tx_id).await.unwrap();
        assert_eq!(retrieved, Some(value));

        // Commit transaction
        manager.commit_transaction(tx_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_storage_manager_abort_transaction() {
        let mut manager = StorageManagerBuilder::new()
            .with_storage_engine(MockStorageEngine::new())
            .with_transaction_manager(MockTransactionManager::new())
            .with_lock_manager(MockLockManager)
            .with_durability_manager(MockDurabilityManager)
            .build()
            .unwrap();

        // Begin transaction
        let tx_id = manager.begin_transaction(IsolationLevel::ReadCommitted).await.unwrap();

        // Perform operations
        let key = b"test_key".to_vec();
        let value = DataType::String("test_value".to_string());
        
        manager.put(key.clone(), value, tx_id).await.unwrap();

        // Abort transaction
        manager.abort_transaction(tx_id).await.unwrap();

        // Transaction should no longer exist
        let result = manager.get(&key, tx_id).await;
        assert!(result.is_err());
    }
}