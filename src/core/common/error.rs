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
    Storage(String), // Was StorageError

    #[error("Transaction Error: {0}")]
    Transaction(String), // Was TransactionError

    #[error("Key not found: {key}")]
    NotFound { key: String }, // Was NotFoundError

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

    #[error("No active transaction")]
    NoActiveTransaction,

    #[error("Lock conflict for key {key:?} on transaction {current_tx}. Locked by transaction {locked_by_tx:?}")]
    LockConflict { key: Vec<u8>, current_tx: u64, locked_by_tx: Option<u64> },

    #[error("Lock acquisition timeout for key {key:?} on transaction {current_tx}")]
    LockAcquisitionTimeout { key: Vec<u8>, current_tx: u64 },

    #[error("Configuration error: {0}")]
    Configuration(String), // Was ConfigError

    #[error("Type Error: {0}")]
    Type(String), // Was TypeError

    #[error("Internal Error: {0}")]
    Internal(String), // Was InternalError and Internal(String)

    #[error("Buffer Pool Error: {0}")]
    BufferPool(String),

    #[error("ConstraintViolation: {message}")]
    ConstraintViolation { message: String },
}

impl From<crate::core::indexing::btree::tree::OxidbError> for OxidbError {
    fn from(err: crate::core::indexing::btree::tree::OxidbError) -> Self {
        match err {
            crate::core::indexing::btree::tree::OxidbError::Io(e) => OxidbError::Io(e),
            crate::core::indexing::btree::tree::OxidbError::Serialization(se) => {
                OxidbError::Serialization(format!("BTree Node Serialization: {:?}", se))
            }
            crate::core::indexing::btree::tree::OxidbError::NodeNotFound(page_id) => {
                OxidbError::Index(format!("BTree Node not found on page: {}", page_id))
            }
            crate::core::indexing::btree::tree::OxidbError::PageFull(s) => {
                OxidbError::Index(format!("BTree PageFull: {}", s))
            }
            crate::core::indexing::btree::tree::OxidbError::UnexpectedNodeType => {
                OxidbError::Index("BTree Unexpected Node Type".to_string())
            }
            crate::core::indexing::btree::tree::OxidbError::TreeLogicError(s) => {
                OxidbError::Index(format!("BTree Logic Error: {}", s))
            }
            crate::core::indexing::btree::tree::OxidbError::BorrowError(s) => {
                OxidbError::Lock(format!("BTree Borrow Error: {}", s)) // Or a new specific variant
            }
        }
    }
}

// Note: Removed manual PartialEq. If needed, it should be added carefully,
// considering that std::io::Error and other wrapped errors might not implement PartialEq.
// For many error types, direct comparison isn't as common as matching on the variant.
// thiserror does not automatically derive PartialEq or Eq.
