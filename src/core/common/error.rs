use std::fmt;
use std::io;

/// Core error type for OxiDB - pure stdlib implementation
/// Following SOLID principles: Single Responsibility, Open/Closed
#[derive(Debug, Clone)]
pub enum OxidbError {
    /// I/O related errors
    Io { 
        kind: IoErrorKind,
        message: String,
    },
    
    /// Serialization/Deserialization errors
    Serialization { 
        context: String,
        details: String,
    },
    
    /// Query parsing errors
    Parse { 
        query: String,
        position: Option<usize>,
        expected: String,
    },
    
    /// Execution errors
    Execution { 
        operation: String,
        cause: String,
    },
    
    /// Storage layer errors
    Storage { 
        component: String,
        operation: String,
        details: String,
    },
    
    /// Transaction management errors
    Transaction { 
        tx_id: u64,
        operation: String,
        reason: String,
    },
    
    /// Lock management errors
    Lock { 
        key: Vec<u8>,
        tx_id: u64,
        conflict_type: LockConflictType,
    },
    
    /// Index operation errors
    Index { 
        index_name: String,
        operation: String,
        details: String,
    },
    
    /// Configuration errors
    Configuration { 
        parameter: String,
        value: String,
        reason: String,
    },
    
    /// Type system errors
    Type { 
        expected: String,
        found: String,
        context: String,
    },
    
    /// Resource not found
    NotFound { 
        resource_type: String,
        identifier: String,
    },
    
    /// Resource already exists
    AlreadyExists { 
        resource_type: String,
        identifier: String,
    },
    
    /// Feature not implemented
    NotImplemented { 
        feature: String,
        context: String,
    },
    
    /// Invalid input
    InvalidInput { 
        parameter: String,
        value: String,
        constraints: String,
    },
    
    /// Internal consistency errors
    Internal { 
        component: String,
        invariant: String,
        state: String,
    },
    
    /// Buffer pool errors
    BufferPool { 
        operation: String,
        page_id: Option<u64>,
        details: String,
    },
    
    /// Constraint violations
    ConstraintViolation { 
        constraint_type: String,
        table: String,
        details: String,
    },
    
    /// Vector operation errors
    Vector { 
        operation: String,
        dimension: Option<usize>,
        details: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoErrorKind {
    NotFound,
    PermissionDenied,
    ConnectionRefused,
    ConnectionReset,
    ConnectionAborted,
    NotConnected,
    AddrInUse,
    AddrNotAvailable,
    BrokenPipe,
    AlreadyExists,
    WouldBlock,
    InvalidInput,
    InvalidData,
    TimedOut,
    WriteZero,
    Interrupted,
    UnexpectedEof,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockConflictType {
    ReadWrite,
    WriteWrite,
    Timeout,
    Deadlock,
}

impl fmt::Display for OxidbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OxidbError::Io { kind, message } => {
                write!(f, "IO Error ({:?}): {}", kind, message)
            }
            OxidbError::Serialization { context, details } => {
                write!(f, "Serialization Error in {}: {}", context, details)
            }
            OxidbError::Parse { query, position, expected } => {
                match position {
                    Some(pos) => write!(f, "Parse Error at position {}: expected {} in query '{}'", pos, expected, query),
                    None => write!(f, "Parse Error: expected {} in query '{}'", expected, query),
                }
            }
            OxidbError::Execution { operation, cause } => {
                write!(f, "Execution Error during {}: {}", operation, cause)
            }
            OxidbError::Storage { component, operation, details } => {
                write!(f, "Storage Error in {} during {}: {}", component, operation, details)
            }
            OxidbError::Transaction { tx_id, operation, reason } => {
                write!(f, "Transaction Error (TX {}): {} failed - {}", tx_id, operation, reason)
            }
            OxidbError::Lock { key, tx_id, conflict_type } => {
                write!(f, "Lock Error: {:?} conflict for key {:?} in transaction {}", 
                       conflict_type, String::from_utf8_lossy(key), tx_id)
            }
            OxidbError::Index { index_name, operation, details } => {
                write!(f, "Index Error in '{}' during {}: {}", index_name, operation, details)
            }
            OxidbError::Configuration { parameter, value, reason } => {
                write!(f, "Configuration Error: parameter '{}' with value '{}' - {}", parameter, value, reason)
            }
            OxidbError::Type { expected, found, context } => {
                write!(f, "Type Error in {}: expected {}, found {}", context, expected, found)
            }
            OxidbError::NotFound { resource_type, identifier } => {
                write!(f, "{} not found: {}", resource_type, identifier)
            }
            OxidbError::AlreadyExists { resource_type, identifier } => {
                write!(f, "{} already exists: {}", resource_type, identifier)
            }
            OxidbError::NotImplemented { feature, context } => {
                write!(f, "Feature '{}' not implemented in {}", feature, context)
            }
            OxidbError::InvalidInput { parameter, value, constraints } => {
                write!(f, "Invalid input for parameter '{}': value '{}' violates constraints: {}", parameter, value, constraints)
            }
            OxidbError::Internal { component, invariant, state } => {
                write!(f, "Internal Error in {}: invariant '{}' violated, state: {}", component, invariant, state)
            }
            OxidbError::BufferPool { operation, page_id, details } => {
                match page_id {
                    Some(id) => write!(f, "Buffer Pool Error during {} on page {}: {}", operation, id, details),
                    None => write!(f, "Buffer Pool Error during {}: {}", operation, details),
                }
            }
            OxidbError::ConstraintViolation { constraint_type, table, details } => {
                write!(f, "Constraint Violation: {} constraint on table '{}' - {}", constraint_type, table, details)
            }
            OxidbError::Vector { operation, dimension, details } => {
                match dimension {
                    Some(dim) => write!(f, "Vector Error during {} (dimension {}): {}", operation, dim, details),
                    None => write!(f, "Vector Error during {}: {}", operation, details),
                }
            }
        }
    }
}

impl std::error::Error for OxidbError {}

// Zero-cost conversion from std::io::Error
impl From<io::Error> for OxidbError {
    fn from(err: io::Error) -> Self {
        let kind = match err.kind() {
            io::ErrorKind::NotFound => IoErrorKind::NotFound,
            io::ErrorKind::PermissionDenied => IoErrorKind::PermissionDenied,
            io::ErrorKind::ConnectionRefused => IoErrorKind::ConnectionRefused,
            io::ErrorKind::ConnectionReset => IoErrorKind::ConnectionReset,
            io::ErrorKind::ConnectionAborted => IoErrorKind::ConnectionAborted,
            io::ErrorKind::NotConnected => IoErrorKind::NotConnected,
            io::ErrorKind::AddrInUse => IoErrorKind::AddrInUse,
            io::ErrorKind::AddrNotAvailable => IoErrorKind::AddrNotAvailable,
            io::ErrorKind::BrokenPipe => IoErrorKind::BrokenPipe,
            io::ErrorKind::AlreadyExists => IoErrorKind::AlreadyExists,
            io::ErrorKind::WouldBlock => IoErrorKind::WouldBlock,
            io::ErrorKind::InvalidInput => IoErrorKind::InvalidInput,
            io::ErrorKind::InvalidData => IoErrorKind::InvalidData,
            io::ErrorKind::TimedOut => IoErrorKind::TimedOut,
            io::ErrorKind::WriteZero => IoErrorKind::WriteZero,
            io::ErrorKind::Interrupted => IoErrorKind::Interrupted,
            io::ErrorKind::UnexpectedEof => IoErrorKind::UnexpectedEof,
            _ => IoErrorKind::Other,
        };
        
        OxidbError::Io {
            kind,
            message: err.to_string(),
        }
    }
}

// Utility functions for common error patterns - zero cost abstractions
impl OxidbError {
    /// Create an IO error with context
    pub fn io_error(kind: IoErrorKind, message: impl Into<String>) -> Self {
        Self::Io {
            kind,
            message: message.into(),
        }
    }
    
    /// Create a storage error with component context
    pub fn storage_error(component: impl Into<String>, operation: impl Into<String>, details: impl Into<String>) -> Self {
        Self::Storage {
            component: component.into(),
            operation: operation.into(),
            details: details.into(),
        }
    }
    
    /// Create a transaction error
    pub fn transaction_error(tx_id: u64, operation: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::Transaction {
            tx_id,
            operation: operation.into(),
            reason: reason.into(),
        }
    }
    
    /// Create an index error
    pub fn index_error(index_name: impl Into<String>, operation: impl Into<String>, details: impl Into<String>) -> Self {
        Self::Index {
            index_name: index_name.into(),
            operation: operation.into(),
            details: details.into(),
        }
    }
    
    /// Create a parse error
    pub fn parse_error(query: impl Into<String>, position: Option<usize>, expected: impl Into<String>) -> Self {
        Self::Parse {
            query: query.into(),
            position,
            expected: expected.into(),
        }
    }
    
    /// Create an internal error
    pub fn internal_error(component: impl Into<String>, invariant: impl Into<String>, state: impl Into<String>) -> Self {
        Self::Internal {
            component: component.into(),
            invariant: invariant.into(),
            state: state.into(),
        }
    }
}

/// Result type alias for convenience
pub type Result<T> = std::result::Result<T, OxidbError>;

/// Chain errors with context - zero cost abstraction
pub trait ErrorContext<T> {
    fn with_context<F>(self, f: F) -> Result<T>
    where
        F: FnOnce() -> String;
        
    fn with_storage_context(self, component: &str, operation: &str) -> Result<T>;
    fn with_transaction_context(self, tx_id: u64, operation: &str) -> Result<T>;
}

impl<T> ErrorContext<T> for Result<T> {
    fn with_context<F>(self, f: F) -> Result<T>
    where
        F: FnOnce() -> String,
    {
        self.map_err(|e| match e {
            OxidbError::Internal { component, invariant, .. } => {
                OxidbError::Internal {
                    component,
                    invariant,
                    state: f(),
                }
            }
            other => other,
        })
    }
    
    fn with_storage_context(self, component: &str, operation: &str) -> Result<T> {
        self.map_err(|e| match e {
            OxidbError::Io { kind, message } => {
                OxidbError::storage_error(component, operation, format!("IO Error ({:?}): {}", kind, message))
            }
            other => other,
        })
    }
    
    fn with_transaction_context(self, tx_id: u64, operation: &str) -> Result<T> {
        self.map_err(|e| OxidbError::transaction_error(tx_id, operation, e.to_string()))
    }
}
