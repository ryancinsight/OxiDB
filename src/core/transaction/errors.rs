// src/core/transaction/errors.rs
//! Defines error types for the transaction management system.

use thiserror::Error;

#[derive(Error, Debug)]
pub enum TransactionError {
    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Transaction conflict: {0}")]
    Conflict(String),

    #[error("Transaction deadlock detected: {0}")]
    Deadlock(String),

    #[error("Invalid transaction state: {0}")]
    InvalidTransactionState(String),

    #[error("MVCC error: {0}")]
    MvccError(String), // For Multi-Version Concurrency Control specific errors

    #[error("General transaction error: {0}")]
    General(String),
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
