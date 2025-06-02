// This module will handle transaction management.

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
}

impl Transaction {
    /// Creates a new transaction with the given ID and an initial state of `Active`.
    pub fn new(id: u64) -> Self {
        Transaction {
            id,
            state: TransactionState::Active,
        }
    }
}
