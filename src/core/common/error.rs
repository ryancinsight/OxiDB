use thiserror::Error;

#[derive(Error, Debug)]
pub enum OxidbError {
    #[error("IO Error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization Error: {0}")]
    Serialization(String),

    #[error("Deserialization Error: {0}")]
    Deserialization(String), // Kept separate from Serialization for now

    #[error("JSON Serialization/Deserialization Error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Parsing Error: {0}")]
    Parsing(String), // Generic parsing error

    #[error("SQL Parsing Error: {0}")]
    SqlParsing(String), // Specific for SQL, was InvalidQuery

    #[error("Execution Error: {0}")]
    Execution(String),

    #[error("Storage Error: {0}")]
    Storage(String), // Unified storage error variant

    #[error("Transaction Error: {0}")]
    Transaction(String), // Was TransactionError

    #[error("Not Found: {0}")]
    NotFound(String), // Was NotFoundError

    #[error("Resource already exists: {name}")]
    AlreadyExists { name: String },

    #[error("Feature not implemented: {feature}")]
    NotImplemented { feature: String }, // Was NotImplemented(String) and similar to UnsupportedOperation

    #[error("Invalid input: {message}")]
    InvalidInput { message: String },

    #[error("Index Error: {0}")]
    Index(String), // Was IndexError

    #[error("Lock Error: {0}")]
    Lock(String), // Was LockError

    #[error("Lock Timeout: {0}")]
    LockTimeout(String),

    #[error("No active transaction")]
    NoActiveTransaction,

    #[error("Lock conflict: {message}")]
    LockConflict { message: String },

    #[error("Lock acquisition timeout for key {key:?} on transaction {current_tx}")]
    LockAcquisitionTimeout { key: Vec<u8>, current_tx: u64 },

    #[error("Configuration error: {0}")]
    Configuration(String), // Unified configuration error variant

    #[error("Type Error: {0}")]
    Type(String), // Was TypeError

    #[error("Internal Error: {0}")]
    Internal(String), // Was InternalError and Internal(String)

    #[error("Buffer Pool Error: {0}")]
    BufferPool(String),

    #[error("Constraint Violation: {0}")]
    ConstraintViolation(String),

    #[error("Vector dimension mismatch: dim1 = {dim1}, dim2 = {dim2}")]
    VectorDimensionMismatch { dim1: usize, dim2: usize },

    #[error("Vector magnitude is zero, cannot compute cosine similarity")]
    VectorMagnitudeZero,

    // Additional error variants for compatibility
    #[error("Other: {0}")]
    Other(String),

    #[error("Transaction error: {0}")]
    TransactionError(String), // Deprecated: Use Transaction instead

    #[error("Transaction not found: {0}")]
    TransactionNotFound(String),

    #[error("Deadlock detected: {0}")]
    DeadlockDetected(String),

    #[error("Table not found: {0}")]
    TableNotFound(String),
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

// Note: Removed manual PartialEq. If needed, it should be added carefully,
// considering that std::io::Error and other wrapped errors might not implement PartialEq.
// For many error types, direct comparison isn't as common as matching on the variant.
// thiserror does not automatically derive PartialEq or Eq.
