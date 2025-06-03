// Consider using the 'thiserror' crate if it simplifies things.
// For now, a manual definition:
#[derive(Debug)] // PartialEq will be implemented manually
pub enum DbError {
    IoError(std::io::Error),
    SerializationError(String), // Or a more specific error type from a serialization crate
    DeserializationError(String), // Or a more specific error type
    NotFoundError(String),
    InvalidQuery(String),
    TransactionError(String),
    StorageError(String),
    InternalError(String), // For unexpected issues
    IndexError(String),    // Errors related to index operations
    LockError(String),     // Errors related to RwLock or other synchronization primitives
    NoActiveTransaction,
    LockConflict { key: Vec<u8>, current_tx: u64, locked_by_tx: Option<u64> },
    LockAcquisitionTimeout { key: Vec<u8>, current_tx: u64 },
    // Add more variants as needed
}

impl PartialEq for DbError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (DbError::IoError(_), DbError::IoError(_)) => {
                // For tests, often comparing kind is enough.
                // Or, decide that two IoErrors are never equal unless they are identical instances,
                // which `PartialEq` can't guarantee. For now, let's say they are not equal
                // to avoid false positives in tests if error kinds match by coincidence.
                // A more robust way would be to compare error kinds if that's meaningful for tests.
                // For the purpose of passing current tests that assert specific non-IO errors,
                // treating IoErrors as non-equal to each other unless explicitly handled is safer.
                false // Or compare e1.kind() == e2.kind() if needed.
            }
            (DbError::SerializationError(s1), DbError::SerializationError(s2)) => s1 == s2,
            (DbError::DeserializationError(s1), DbError::DeserializationError(s2)) => s1 == s2,
            (DbError::NotFoundError(s1), DbError::NotFoundError(s2)) => s1 == s2,
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
impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DbError::IoError(e) => write!(f, "IO Error: {}", e),
            DbError::SerializationError(s) => write!(f, "Serialization Error: {}", s),
            DbError::DeserializationError(s) => write!(f, "Deserialization Error: {}", s),
            DbError::NotFoundError(s) => write!(f, "Not Found: {}", s),
            DbError::InvalidQuery(s) => write!(f, "Invalid Query: {}", s),
            DbError::TransactionError(s) => write!(f, "Transaction Error: {}", s),
            DbError::StorageError(s) => write!(f, "Storage Error: {}", s),
            DbError::InternalError(s) => write!(f, "Internal Error: {}", s),
            DbError::IndexError(s) => write!(f, "Index Error: {}", s),
            DbError::LockError(s) => write!(f, "Lock Error: {}", s),
            DbError::NoActiveTransaction => write!(f, "No active transaction"),
            DbError::LockConflict { key, current_tx, locked_by_tx } => {
                write!(f, "Lock conflict for key {:?} on transaction {}. Locked by transaction {:?}", key, current_tx, locked_by_tx)
            }
            DbError::LockAcquisitionTimeout { key, current_tx } => {
                write!(f, "Lock acquisition timeout for key {:?} on transaction {}", key, current_tx)
            }
        }
    }
}

// Implement std::error::Error for DbError
impl std::error::Error for DbError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            DbError::IoError(e) => Some(e),
            _ => None,
        }
    }
}

// Optional: Implement From<std::io::Error> for DbError
impl From<std::io::Error> for DbError {
    fn from(err: std::io::Error) -> Self {
        DbError::IoError(err)
    }
}
