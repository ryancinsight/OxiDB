// Content from transaction.rs
use crate::core::common::types::{Lsn, TransactionId}; // Added TransactionId import

// Define INVALID_LSN constant
pub const INVALID_LSN: Lsn = u64::MAX;

/// Represents the state of a transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionState {
    /// Transaction is currently active and ongoing.
    Active,
    /// Transaction has been successfully committed.
    Committed,
    /// Transaction has been aborted and changes rolled back.
    Aborted,
}

/// Represents a transaction in the system.
#[derive(Debug, Clone)]
pub struct Transaction {
    /// A unique identifier for the transaction.
    pub id: TransactionId, // Changed from u64
    /// The current state of the transaction.
    pub state: TransactionState,
    /// The LSN of the previous WAL record written by this transaction.
    pub prev_lsn: Lsn,
    pub undo_log: Vec<UndoOperation>,
    pub redo_log: Vec<RedoOperation>, // Added redo_log
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedoOperation {
    IndexInsert { key: Vec<u8>, value_for_index: Vec<u8> }, // Serialized value
    IndexDelete { key: Vec<u8>, old_value_for_index: Vec<u8> }, // Old serialized value
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UndoOperation {
    RevertInsert {
        key: Vec<u8>,
    },
    RevertUpdate {
        key: Vec<u8>,
        old_value: Vec<u8>,
    },
    RevertDelete {
        key: Vec<u8>,
        old_value: Vec<u8>,
    },
    // For reverting index changes
    IndexRevertInsert {
        index_name: String,
        key: Vec<u8>,
        value_for_index: Vec<u8>,
    }, // value_for_index is the serialized data that was indexed
    IndexRevertDelete {
        index_name: String,
        key: Vec<u8>,
        old_value_for_index: Vec<u8>,
    }, // old_value_for_index is the serialized data that was deleted from index
    IndexRevertUpdate {
        index_name: String,
        key: Vec<u8>,
        old_value_for_index: Vec<u8>,
        new_value_for_index: Vec<u8>,
    },
}

impl Transaction {
    /// Creates a new transaction with the given ID and an initial state of `Active`.
    #[must_use] pub const fn new(id: TransactionId) -> Self {
        // Changed id type from u64
        Self {
            id,
            state: TransactionState::Active,
            prev_lsn: INVALID_LSN, // Initialize prev_lsn with an invalid LSN
            undo_log: Vec::new(),
            redo_log: Vec::new(), // Initialize redo_log
        }
    }

    /// Sets the state of the transaction.
    pub fn set_state(&mut self, state: TransactionState) {
        self.state = state;
    }

    /// Adds an undo operation to the transaction's undo log.
    pub fn add_undo_operation(&mut self, op: UndoOperation) {
        self.undo_log.push(op);
    }

    /// Clones the transaction for storage operations, excluding logs.
    /// The store itself doesn't need to know about the undo/redo logs for its basic put/get/delete.
    #[must_use] pub fn clone_for_store(&self) -> Self {
        Self {
            id: self.id,
            state: self.state.clone(), // State might be relevant for some store implementations (e.g. MVCC visibility)
            prev_lsn: self.prev_lsn,   // Clone prev_lsn
            undo_log: Vec::new(),
            redo_log: Vec::new(),
        }
    }
}

// Original content of mod.rs (with modifications)
// This module will handle transaction management.
pub mod acid_manager; // Comprehensive ACID transaction manager
pub mod lock_manager;
pub mod manager;
// pub mod transaction; // This line is removed
// pub mod types; // Assuming types.rs might be added later or was a misunderstanding

pub use lock_manager::{LockManager, LockType};
pub use manager::TransactionManager;
// pub use transaction::{Transaction, TransactionState}; // This line is removed - already defined above
// TransactionError is now part of the main OxidbError enum
// pub use crate::core::common::error::TransactionError;
