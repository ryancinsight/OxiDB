use std::fmt;

#[derive(Debug)]
pub enum OxidbError {
    Io(std::io::Error),
    Serialization(String),
    Deserialization(String),
    Json(serde_json::Error),
    Parsing(String),
    SqlParsing(String),
    Execution(String),
    Storage(String),
    Transaction(String),
    NotFound(String),
    InvalidNodeId,
    EntityNotFound(String),
    AlreadyExists { name: String },
    NotImplemented { feature: String },
    InvalidInput { message: String },
    Index(String),
    Lock(String),
    LockTimeout(String),
    Internal(String),
    BufferPool(String),
    ConstraintViolation(String),
    VectorDimensionMismatch { dim1: usize, dim2: usize },
    VectorMagnitudeZero,
    Other(String),
    TransactionError(String), // Deprecated: Use Transaction instead
    TransactionNotFound(String),
    DeadlockDetected(String),
    TableNotFound(String),
    NoActiveTransaction,
    LockConflict { message: String },
    LockAcquisitionTimeout { key: Vec<u8>, current_tx: u64 },
    Configuration(String),
    Type(String),
    TypeMismatch,
}

impl fmt::Display for OxidbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO Error: {}", e),
            Self::Serialization(s) => write!(f, "Serialization Error: {}", s),
            Self::Deserialization(s) => write!(f, "Deserialization Error: {}", s),
            Self::Json(e) => write!(f, "JSON Serialization/Deserialization Error: {}", e),
            Self::Parsing(s) => write!(f, "Parsing Error: {}", s),
            Self::SqlParsing(s) => write!(f, "SQL Parsing Error: {}", s),
            Self::Execution(s) => write!(f, "Execution Error: {}", s),
            Self::Storage(s) => write!(f, "Storage Error: {}", s),
            Self::Transaction(s) => write!(f, "Transaction Error: {}", s),
            Self::NotFound(s) => write!(f, "Not Found: {}", s),
            Self::InvalidNodeId => write!(f, "Invalid Node ID"),
            Self::EntityNotFound(s) => write!(f, "Entity Not Found: {}", s),
            Self::AlreadyExists { name } => write!(f, "Resource already exists: {}", name),
            Self::NotImplemented { feature } => write!(f, "Feature not implemented: {}", feature),
            Self::InvalidInput { message } => write!(f, "Invalid input: {}", message),
            Self::Index(s) => write!(f, "Index Error: {}", s),
            Self::Lock(s) => write!(f, "Lock Error: {}", s),
            Self::LockTimeout(s) => write!(f, "Lock Timeout: {}", s),
            Self::Internal(s) => write!(f, "Internal Error: {}", s),
            Self::BufferPool(s) => write!(f, "Buffer Pool Error: {}", s),
            Self::ConstraintViolation(s) => write!(f, "Constraint Violation: {}", s),
            Self::VectorDimensionMismatch { dim1, dim2 } => {
                write!(f, "Vector dimension mismatch: dim1 = {}, dim2 = {}", dim1, dim2)
            }
            Self::VectorMagnitudeZero => write!(f, "Vector magnitude is zero, cannot compute cosine similarity"),
            Self::Other(s) => write!(f, "Other: {}", s),
            Self::TransactionError(s) => write!(f, "Transaction error: {}", s),
            Self::TransactionNotFound(s) => write!(f, "Transaction not found: {}", s),
            Self::DeadlockDetected(s) => write!(f, "Deadlock detected: {}", s),
            Self::TableNotFound(s) => write!(f, "Table not found: {}", s),
            Self::NoActiveTransaction => write!(f, "No active transaction"),
            Self::LockConflict { message } => write!(f, "Lock conflict: {}", message),
            Self::LockAcquisitionTimeout { key, current_tx } => {
                write!(f, "Lock acquisition timeout for key {:?} on transaction {}", key, current_tx)
            }
            Self::Configuration(s) => write!(f, "Configuration error: {}", s),
            Self::Type(s) => write!(f, "Type Error: {}", s),
            Self::TypeMismatch => write!(f, "Type mismatch"),
        }
    }
}

impl std::error::Error for OxidbError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::Json(e) => Some(e),
            _ => None,
        }
    }
}

// Manual From implementations
impl From<std::io::Error> for OxidbError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<serde_json::Error> for OxidbError {
    fn from(err: serde_json::Error) -> Self {
        Self::Json(err)
    }
}

impl From<crate::core::indexing::btree::OxidbError> for OxidbError {
    fn from(err: crate::core::indexing::btree::OxidbError) -> Self {
        match err {
            crate::core::indexing::btree::OxidbError::Io(e) => Self::Io(e),
            crate::core::indexing::btree::OxidbError::Serialization(se) => {
                Self::Serialization(format!("BTree Node Serialization: {se:?}"))
            }
            crate::core::indexing::btree::OxidbError::NodeNotFound(page_id) => {
                Self::Index(format!("BTree Node not found on page: {page_id}"))
            }
            crate::core::indexing::btree::OxidbError::PageFull(s) => {
                Self::Index(format!("BTree PageFull: {s}"))
            }
            crate::core::indexing::btree::OxidbError::UnexpectedNodeType => {
                Self::Index("BTree Unexpected Node Type".to_string())
            }
            crate::core::indexing::btree::OxidbError::TreeLogicError(s) => {
                Self::Index(format!("BTree Logic Error: {s}"))
            }
            crate::core::indexing::btree::OxidbError::BorrowError(s) => {
                Self::LockTimeout(format!("BTree Borrow Error: {s}")) // Or a new specific variant
            }
            crate::core::indexing::btree::OxidbError::Generic(s) => {
                Self::Internal(format!("BTree Generic Error: {s}"))
            }
        }
    }
}

impl OxidbError {
    /// Create an IO error with a custom message
    /// This maintains compatibility with existing code
    #[must_use]
    pub fn io_error(message: String) -> Self {
        use std::io::{Error, ErrorKind};
        Self::Io(Error::new(ErrorKind::Other, message))
    }
}
