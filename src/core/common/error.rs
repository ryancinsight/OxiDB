use thiserror::Error;

#[derive(Error, Debug)] // PartialEq will be implemented manually
pub enum DbError {
    #[error("IO Error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Serialization Error: {0}")]
    SerializationError(String), // Or a more specific error type from a serialization crate
    #[error("Deserialization Error: {0}")]
    DeserializationError(String), // Or a more specific error type
    #[error("Key not found: {key}")]
    NotFoundError { key: String },
    #[error("Invalid Query: {0}")]
    InvalidQuery(String),
    #[error("Transaction Error: {0}")]
    TransactionError(String),
    #[error("Storage Error: {0}")]
    StorageError(String),
    #[error("Internal Error: {0}")]
    InternalError(String), // For unexpected issues
    #[error("Index Error: {0}")]
    IndexError(String),    // Errors related to index operations
    #[error("Lock Error: {0}")]
    LockError(String),     // Errors related to RwLock or other synchronization primitives
    #[error("No active transaction")]
    NoActiveTransaction,
    #[error("Lock conflict for key {key:?} on transaction {current_tx}. Locked by transaction {locked_by_tx:?}")]
    LockConflict { key: Vec<u8>, current_tx: u64, locked_by_tx: Option<u64> },
    #[error("Lock acquisition timeout for key {key:?} on transaction {current_tx}")]
    LockAcquisitionTimeout { key: Vec<u8>, current_tx: u64 },
    // Add more variants as needed
}

impl PartialEq for DbError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (DbError::IoError(e1), DbError::IoError(e2)) => e1.kind() == e2.kind(),
            (DbError::SerializationError(s1), DbError::SerializationError(s2)) => s1 == s2,
            (DbError::DeserializationError(s1), DbError::DeserializationError(s2)) => s1 == s2,
            (DbError::NotFoundError{key: k1}, DbError::NotFoundError{key: k2}) => k1 == k2,
            (DbError::InvalidQuery(s1), DbError::InvalidQuery(s2)) => s1 == s2,
            (DbError::TransactionError(s1), DbError::TransactionError(s2)) => s1 == s2,
            (DbError::StorageError(s1), DbError::StorageError(s2)) => s1 == s2,
            (DbError::InternalError(s1), DbError::InternalError(s2)) => s1 == s2,
            (DbError::IndexError(s1), DbError::IndexError(s2)) => s1 == s2,
            (DbError::LockError(s1), DbError::LockError(s2)) => s1 == s2,
            (DbError::NoActiveTransaction, DbError::NoActiveTransaction) => true,
            (DbError::LockConflict { key: k1, current_tx: ct1, locked_by_tx: lbt1 },
             DbError::LockConflict { key: k2, current_tx: ct2, locked_by_tx: lbt2 }) => {
                k1 == k2 && ct1 == ct2 && lbt1 == lbt2
            }
            (DbError::LockAcquisitionTimeout { key: k1, current_tx: ct1 },
             DbError::LockAcquisitionTimeout { key: k2, current_tx: ct2 }) => {
                k1 == k2 && ct1 == ct2
            }
            _ => false, // Different variants are not equal
        }
    }
}

// Implement std::fmt::Display for DbError
