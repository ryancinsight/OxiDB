use std::io;
use crate::core::indexing::btree::node::{PageId, SerializationError}; // Adjusted path

#[derive(Debug)]
pub enum OxidbError {
    Io(io::Error),
    Serialization(SerializationError),
    NodeNotFound(PageId),
    PageFull(String),
    UnexpectedNodeType,
    TreeLogicError(String),
    BorrowError(String), // For RefCell borrow errors
    Generic(String),     // For general string errors
}

impl std::fmt::Display for OxidbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OxidbError::Io(err) => write!(f, "BTree IO error: {}", err),
            OxidbError::Serialization(err) => write!(f, "BTree Serialization error: {:?}", err),
            OxidbError::NodeNotFound(page_id) => write!(f, "BTree Node not found: {}", page_id),
            OxidbError::PageFull(msg) => write!(f, "BTree Page full: {}", msg),
            OxidbError::UnexpectedNodeType => write!(f, "BTree Unexpected node type"),
            OxidbError::TreeLogicError(msg) => write!(f, "BTree logic error: {}", msg),
            OxidbError::BorrowError(msg) => write!(f, "BTree borrow error: {}", msg),
            OxidbError::Generic(msg) => write!(f, "BTree generic error: {}", msg),
        }
    }
}

impl From<&str> for OxidbError {
    fn from(s: &str) -> Self {
        OxidbError::Generic(s.to_string())
    }
}

impl From<std::cell::BorrowMutError> for OxidbError {
    fn from(err: std::cell::BorrowMutError) -> Self {
        OxidbError::BorrowError(err.to_string())
    }
}

impl From<io::Error> for OxidbError {
    fn from(err: io::Error) -> Self {
        OxidbError::Io(err)
    }
}

impl From<SerializationError> for OxidbError {
    fn from(err: SerializationError) -> Self {
        OxidbError::Serialization(err)
    }
}
