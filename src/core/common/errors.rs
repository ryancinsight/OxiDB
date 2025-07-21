//! Error types for Oxidb
//! 
//! This module defines error types following SOLID principles and providing
//! clear, composable error handling throughout the system.

use std::fmt;
use std::io;

/// Main error type for Oxidb operations
/// Follows SOLID's Single Responsibility Principle - each variant represents a specific error category
#[derive(Debug, Clone)]
pub enum OxidbError {
    /// IO related errors
    Io(String),
    
    /// Serialization/Deserialization errors
    Serialization(String),
    Deserialization(String),
    
    /// Transaction related errors
    TransactionError(String),
    TransactionNotFound(String),
    
    /// Lock related errors
    LockTimeout(String),
    DeadlockDetected(String),
    
    /// Storage related errors
    StorageError(String),
    
    /// Query related errors
    QueryError(String),
    ParseError(String),
    
    /// Table related errors
    TableNotFound(String),
    TableAlreadyExists(String),
    
    /// Index related errors
    IndexError(String),
    
    /// Configuration errors
    ConfigError(String),
    
    /// Network related errors
    NetworkError(String),
    
    /// Authentication/Authorization errors
    AuthError(String),
    
    /// General errors
    Other(String),
    
    /// Invalid operation
    InvalidOperation(String),
    
    /// Resource not found
    NotFound(String),
    
    /// Constraint violation
    ConstraintViolation(String),
}

impl fmt::Display for OxidbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OxidbError::Io(msg) => write!(f, "IO Error: {}", msg),
            OxidbError::Serialization(msg) => write!(f, "Serialization Error: {}", msg),
            OxidbError::Deserialization(msg) => write!(f, "Deserialization Error: {}", msg),
            OxidbError::TransactionError(msg) => write!(f, "Transaction Error: {}", msg),
            OxidbError::TransactionNotFound(msg) => write!(f, "Transaction Not Found: {}", msg),
            OxidbError::LockTimeout(msg) => write!(f, "Lock Timeout: {}", msg),
            OxidbError::DeadlockDetected(msg) => write!(f, "Deadlock Detected: {}", msg),
            OxidbError::StorageError(msg) => write!(f, "Storage Error: {}", msg),
            OxidbError::QueryError(msg) => write!(f, "Query Error: {}", msg),
            OxidbError::ParseError(msg) => write!(f, "Parse Error: {}", msg),
            OxidbError::TableNotFound(msg) => write!(f, "Table Not Found: {}", msg),
            OxidbError::TableAlreadyExists(msg) => write!(f, "Table Already Exists: {}", msg),
            OxidbError::IndexError(msg) => write!(f, "Index Error: {}", msg),
            OxidbError::ConfigError(msg) => write!(f, "Config Error: {}", msg),
            OxidbError::NetworkError(msg) => write!(f, "Network Error: {}", msg),
            OxidbError::AuthError(msg) => write!(f, "Auth Error: {}", msg),
            OxidbError::Other(msg) => write!(f, "Error: {}", msg),
            OxidbError::InvalidOperation(msg) => write!(f, "Invalid Operation: {}", msg),
            OxidbError::NotFound(msg) => write!(f, "Not Found: {}", msg),
            OxidbError::ConstraintViolation(msg) => write!(f, "Constraint Violation: {}", msg),
        }
    }
}

impl std::error::Error for OxidbError {}

// Implement From traits for common error types
impl From<io::Error> for OxidbError {
    fn from(error: io::Error) -> Self {
        OxidbError::Io(error.to_string())
    }
}

impl From<serde_json::Error> for OxidbError {
    fn from(error: serde_json::Error) -> Self {
        OxidbError::Serialization(error.to_string())
    }
}

impl From<bincode::Error> for OxidbError {
    fn from(error: bincode::Error) -> Self {
        OxidbError::Serialization(error.to_string())
    }
}

impl From<std::string::FromUtf8Error> for OxidbError {
    fn from(error: std::string::FromUtf8Error) -> Self {
        OxidbError::Deserialization(error.to_string())
    }
}

/// Transaction-specific error type
#[derive(Debug, Clone)]
pub enum TransactionError {
    /// Transaction already committed
    AlreadyCommitted,
    /// Transaction already aborted
    AlreadyAborted,
    /// Transaction not found
    NotFound,
    /// Deadlock detected
    Deadlock,
    /// Lock timeout
    LockTimeout,
    /// Invalid transaction state
    InvalidState(String),
    /// General transaction error
    Other(String),
}

impl fmt::Display for TransactionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransactionError::AlreadyCommitted => write!(f, "Transaction already committed"),
            TransactionError::AlreadyAborted => write!(f, "Transaction already aborted"),
            TransactionError::NotFound => write!(f, "Transaction not found"),
            TransactionError::Deadlock => write!(f, "Deadlock detected"),
            TransactionError::LockTimeout => write!(f, "Lock timeout"),
            TransactionError::InvalidState(msg) => write!(f, "Invalid transaction state: {}", msg),
            TransactionError::Other(msg) => write!(f, "Transaction error: {}", msg),
        }
    }
}

impl std::error::Error for TransactionError {}

impl From<TransactionError> for OxidbError {
    fn from(error: TransactionError) -> Self {
        OxidbError::TransactionError(error.to_string())
    }
}