use crate::core::wal::log_record::LogSequenceNumber;

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
    pub id: u64,
    /// The current state of the transaction.
    pub state: TransactionState,
    /// The Log Sequence Number of the last WAL record written by this transaction.
    pub last_lsn: LogSequenceNumber,
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
    RevertInsert { key: Vec<u8> },
    RevertUpdate { key: Vec<u8>, old_value: Vec<u8> },
    RevertDelete { key: Vec<u8>, old_value: Vec<u8> },
    // For reverting index changes
    IndexRevertInsert { index_name: String, key: Vec<u8>, value_for_index: Vec<u8> }, // value_for_index is the serialized data that was indexed
    IndexRevertDelete { index_name: String, key: Vec<u8>, old_value_for_index: Vec<u8> }, // old_value_for_index is the serialized data that was deleted from index
}

impl Transaction {
    /// Creates a new transaction with the given ID and an initial state of `Active`.
    pub fn new(id: u64) -> Self {
        Transaction {
            id,
            state: TransactionState::Active,
            last_lsn: 0, // Initialize last_lsn to 0 (or a more appropriate default LSN)
            undo_log: Vec::new(),
            redo_log: Vec::new(), // Initialize redo_log
        }
    }

    /// Sets the state of the transaction.
    pub fn set_state(&mut self, state: TransactionState) {
        self.state = state;
    }

    /// Clones the transaction for storage operations, excluding logs.
    /// The store itself doesn't need to know about the undo/redo logs for its basic put/get/delete.
    pub fn clone_for_store(&self) -> Self {
        Transaction {
            id: self.id,
            state: self.state.clone(), // State might be relevant for some store implementations (e.g. MVCC visibility)
            last_lsn: self.last_lsn, // Clone last_lsn
            undo_log: Vec::new(),
            redo_log: Vec::new(),
        }
    }
}
