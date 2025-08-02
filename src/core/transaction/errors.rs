// src/core/transaction/errors.rs
//! Defines error types for the transaction management system.

use std::fmt;

#[derive(Debug)]
pub enum TransactionError {
    SerializationError(String),
    IoError(std::io::Error),
    Conflict(String),
    Deadlock(String),
    InvalidTransactionState(String),
    MvccError(String), // For Multi-Version Concurrency Control specific errors
    General(String),
}

impl fmt::Display for TransactionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SerializationError(s) => write!(f, "Serialization error: {}", s),
            Self::IoError(e) => write!(f, "I/O error: {}", e),
            Self::Conflict(s) => write!(f, "Transaction conflict: {}", s),
            Self::Deadlock(s) => write!(f, "Transaction deadlock detected: {}", s),
            Self::InvalidTransactionState(s) => write!(f, "Invalid transaction state: {}", s),
            Self::MvccError(s) => write!(f, "MVCC error: {}", s),
            Self::General(s) => write!(f, "General transaction error: {}", s),
        }
    }
}

impl std::error::Error for TransactionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::IoError(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for TransactionError {
    fn from(err: std::io::Error) -> Self {
        Self::IoError(err)
    }
}

// If we need to convert from a general OxidbError or other specific errors
// use crate::core::common::error::OxidbError;
// impl From<OxidbError> for TransactionError {
//     fn from(err: OxidbError) -> Self {
//         // Example: Convert based on the type of OxidbError
//         // match err {
//         //     OxidbError::SerializationError(s) => TransactionError::SerializationError(s),
//         //     OxidbError::IoError(e) => TransactionError::IoError(e),
//         //     _ => TransactionError::General(err.to_string()),
//         // }
//         TransactionError::General(err.to_string())
//     }
// }
