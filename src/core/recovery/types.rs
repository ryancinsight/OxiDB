//! Recovery Types
//!
//! This module defines the core types and enums used throughout the ARIES recovery process.
//! These types represent the state of transactions, recovery phases, and error conditions.

use crate::core::common::types::{Lsn, TransactionId};
use std::fmt;

/// Represents the state of a transaction during recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionState {
    /// Transaction is active (has begun but not committed/aborted)
    Active,
    /// Transaction has been committed
    Committed,
    /// Transaction has been aborted
    Aborted,
    /// Transaction is in the process of being undone during recovery
    Undoing,
}

impl fmt::Display for TransactionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransactionState::Active => write!(f, "Active"),
            TransactionState::Committed => write!(f, "Committed"),
            TransactionState::Aborted => write!(f, "Aborted"),
            TransactionState::Undoing => write!(f, "Undoing"),
        }
    }
}

/// Represents the current state of the recovery process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryState {
    /// Recovery has not started
    NotStarted,
    /// Currently in the Analysis phase
    Analysis,
    /// Currently in the Redo phase
    Redo,
    /// Currently in the Undo phase
    Undo,
    /// Recovery has completed successfully
    Completed,
    /// Recovery failed with an error
    Failed,
}

impl fmt::Display for RecoveryState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecoveryState::NotStarted => write!(f, "Not Started"),
            RecoveryState::Analysis => write!(f, "Analysis Phase"),
            RecoveryState::Redo => write!(f, "Redo Phase"),
            RecoveryState::Undo => write!(f, "Undo Phase"),
            RecoveryState::Completed => write!(f, "Completed"),
            RecoveryState::Failed => write!(f, "Failed"),
        }
    }
}

/// Information about a transaction during recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionInfo {
    /// The transaction ID
    pub tx_id: TransactionId,
    /// The current state of the transaction
    pub state: TransactionState,
    /// The LSN of the last log record for this transaction
    pub last_lsn: Lsn,
    /// The LSN to start undoing from (for active transactions)
    pub undo_next_lsn: Option<Lsn>,
}

impl TransactionInfo {
    /// Creates a new TransactionInfo for an active transaction.
    pub fn new_active(tx_id: TransactionId, last_lsn: Lsn) -> Self {
        Self {
            tx_id,
            state: TransactionState::Active,
            last_lsn,
            undo_next_lsn: Some(last_lsn),
        }
    }

    /// Creates a new TransactionInfo for a committed transaction.
    pub fn new_committed(tx_id: TransactionId, last_lsn: Lsn) -> Self {
        Self {
            tx_id,
            state: TransactionState::Committed,
            last_lsn,
            undo_next_lsn: None,
        }
    }

    /// Creates a new TransactionInfo for an aborted transaction.
    pub fn new_aborted(tx_id: TransactionId, last_lsn: Lsn) -> Self {
        Self {
            tx_id,
            state: TransactionState::Aborted,
            last_lsn,
            undo_next_lsn: None,
        }
    }

    /// Updates the last LSN for this transaction.
    pub fn update_last_lsn(&mut self, lsn: Lsn) {
        self.last_lsn = lsn;
        if self.state == TransactionState::Active {
            self.undo_next_lsn = Some(lsn);
        }
    }

    /// Marks the transaction as committed.
    pub fn commit(&mut self) {
        self.state = TransactionState::Committed;
        self.undo_next_lsn = None;
    }

    /// Marks the transaction as aborted.
    pub fn abort(&mut self) {
        self.state = TransactionState::Aborted;
        self.undo_next_lsn = None;
    }

    /// Returns true if the transaction needs to be undone during recovery.
    pub fn needs_undo(&self) -> bool {
        matches!(self.state, TransactionState::Active | TransactionState::Undoing)
    }
}

/// Errors that can occur during the recovery process.
#[derive(Debug, Clone)]
pub enum RecoveryError {
    /// Error reading from the WAL
    WalError(String),
    /// Error in the Analysis phase
    AnalysisError(String),
    /// Error in the Redo phase
    RedoError(String),
    /// Error in the Undo phase
    UndoError(String),
    /// Invalid log record encountered
    InvalidLogRecord(String),
    /// Inconsistent state detected
    InconsistentState(String),
    /// I/O error
    IoError(String),
    /// Configuration error
    ConfigError(String),
}

impl fmt::Display for RecoveryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecoveryError::WalError(msg) => write!(f, "WAL Error: {}", msg),
            RecoveryError::AnalysisError(msg) => write!(f, "Analysis Error: {}", msg),
            RecoveryError::RedoError(msg) => write!(f, "Redo Error: {}", msg),
            RecoveryError::UndoError(msg) => write!(f, "Undo Error: {}", msg),
            RecoveryError::InvalidLogRecord(msg) => write!(f, "Invalid Log Record: {}", msg),
            RecoveryError::InconsistentState(msg) => write!(f, "Inconsistent State: {}", msg),
            RecoveryError::IoError(msg) => write!(f, "I/O Error: {}", msg),
            RecoveryError::ConfigError(msg) => write!(f, "Configuration Error: {}", msg),
        }
    }
}

impl std::error::Error for RecoveryError {}

impl From<std::io::Error> for RecoveryError {
    fn from(error: std::io::Error) -> Self {
        RecoveryError::IoError(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::common::types::{TransactionId};

    #[test]
    fn test_transaction_info_creation() {
        let tx_id = TransactionId(123);
        let lsn = 100;

        let active_tx = TransactionInfo::new_active(tx_id, lsn);
        assert_eq!(active_tx.state, TransactionState::Active);
        assert_eq!(active_tx.last_lsn, lsn);
        assert_eq!(active_tx.undo_next_lsn, Some(lsn));
        assert!(active_tx.needs_undo());

        let committed_tx = TransactionInfo::new_committed(tx_id, lsn);
        assert_eq!(committed_tx.state, TransactionState::Committed);
        assert_eq!(committed_tx.undo_next_lsn, None);
        assert!(!committed_tx.needs_undo());

        let aborted_tx = TransactionInfo::new_aborted(tx_id, lsn);
        assert_eq!(aborted_tx.state, TransactionState::Aborted);
        assert_eq!(aborted_tx.undo_next_lsn, None);
        assert!(!aborted_tx.needs_undo());
    }

    #[test]
    fn test_transaction_info_updates() {
        let tx_id = TransactionId(123);
        let mut tx_info = TransactionInfo::new_active(tx_id, 100);

        tx_info.update_last_lsn(200);
        assert_eq!(tx_info.last_lsn, 200);
        assert_eq!(tx_info.undo_next_lsn, Some(200));

        tx_info.commit();
        assert_eq!(tx_info.state, TransactionState::Committed);
        assert_eq!(tx_info.undo_next_lsn, None);
        assert!(!tx_info.needs_undo());
    }

    #[test]
    fn test_transaction_state_display() {
        assert_eq!(TransactionState::Active.to_string(), "Active");
        assert_eq!(TransactionState::Committed.to_string(), "Committed");
        assert_eq!(TransactionState::Aborted.to_string(), "Aborted");
        assert_eq!(TransactionState::Undoing.to_string(), "Undoing");
    }

    #[test]
    fn test_recovery_state_display() {
        assert_eq!(RecoveryState::NotStarted.to_string(), "Not Started");
        assert_eq!(RecoveryState::Analysis.to_string(), "Analysis Phase");
        assert_eq!(RecoveryState::Redo.to_string(), "Redo Phase");
        assert_eq!(RecoveryState::Undo.to_string(), "Undo Phase");
        assert_eq!(RecoveryState::Completed.to_string(), "Completed");
        assert_eq!(RecoveryState::Failed.to_string(), "Failed");
    }

    #[test]
    fn test_recovery_error_display() {
        let error = RecoveryError::WalError("Test error".to_string());
        assert_eq!(error.to_string(), "WAL Error: Test error");

        let error = RecoveryError::AnalysisError("Analysis failed".to_string());
        assert_eq!(error.to_string(), "Analysis Error: Analysis failed");
    }

    #[test]
    fn test_recovery_error_from_io_error() {
        let io_error = std::io::Error::new(std::io::ErrorKind::NotFound, "File not found");
        let recovery_error = RecoveryError::from(io_error);
        
        match recovery_error {
            RecoveryError::IoError(msg) => assert!(msg.contains("File not found")),
            _ => panic!("Expected IoError"),
        }
    }
}