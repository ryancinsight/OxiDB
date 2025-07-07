use std::io;
use crate::core::indexing::blink_tree::node::{SerializationError, PageId};

#[derive(Debug)]
pub enum BlinkTreeError {
    Io(io::Error),
    Serialization(SerializationError),
    NodeNotFound(PageId),
    PageFull(String),
    UnexpectedNodeType,
    TreeLogicError(String),
    ConcurrencyError(String), // New for Blink tree - concurrent access issues
    BorrowError(String),      // For RefCell borrow errors
    Generic(String),          // For general string errors
}

impl std::fmt::Display for BlinkTreeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlinkTreeError::Io(err) => write!(f, "IO error: {}", err),
            BlinkTreeError::Serialization(err) => write!(f, "Serialization error: {:?}", err),
            BlinkTreeError::NodeNotFound(page_id) => write!(f, "Node not found: {}", page_id),
            BlinkTreeError::PageFull(msg) => write!(f, "Page full: {}", msg),
            BlinkTreeError::UnexpectedNodeType => write!(f, "Unexpected node type"),
            BlinkTreeError::TreeLogicError(msg) => write!(f, "Tree logic error: {}", msg),
            BlinkTreeError::ConcurrencyError(msg) => write!(f, "Concurrency error: {}", msg),
            BlinkTreeError::BorrowError(msg) => write!(f, "Borrow error: {}", msg),
            BlinkTreeError::Generic(msg) => write!(f, "Error: {}", msg),
        }
    }
}

impl std::error::Error for BlinkTreeError {}

impl From<&str> for BlinkTreeError {
    fn from(s: &str) -> Self {
        BlinkTreeError::Generic(s.to_string())
    }
}

impl From<std::cell::BorrowMutError> for BlinkTreeError {
    fn from(err: std::cell::BorrowMutError) -> Self {
        BlinkTreeError::BorrowError(err.to_string())
    }
}

impl From<io::Error> for BlinkTreeError {
    fn from(err: io::Error) -> Self {
        BlinkTreeError::Io(err)
    }
}

impl From<SerializationError> for BlinkTreeError {
    fn from(err: SerializationError) -> Self {
        BlinkTreeError::Serialization(err)
    }
} 