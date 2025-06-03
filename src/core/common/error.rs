// Consider using the 'thiserror' crate if it simplifies things.
// For now, a manual definition:
#[derive(Debug)] // Add more derive macros as needed (e.g., PartialEq for testing)
pub enum DbError {
    IoError(std::io::Error),
    SerializationError(String), // Or a more specific error type from a serialization crate
    DeserializationError(String), // Or a more specific error type
    NotFoundError(String),
    InvalidQuery(String),
    TransactionError(String),
    StorageError(String),
    InternalError(String), // For unexpected issues
    NoActiveTransaction,
    // Add more variants as needed
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
            DbError::NoActiveTransaction => write!(f, "No active transaction"),
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
