use crate::core::indexing::blink_tree::node::{PageId, SerializationError};
use std::io;

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
            Self::Io(err) => write!(f, "IO error: {err}"),
            Self::Serialization(err) => write!(f, "Serialization error: {err:?}"),
            Self::NodeNotFound(page_id) => write!(f, "Node not found: {page_id}"),
            Self::PageFull(msg) => write!(f, "Page full: {msg}"),
            Self::UnexpectedNodeType => write!(f, "Unexpected node type"),
            Self::TreeLogicError(msg) => write!(f, "Tree logic error: {msg}"),
            Self::ConcurrencyError(msg) => write!(f, "Concurrency error: {msg}"),
            Self::BorrowError(msg) => write!(f, "Borrow error: {msg}"),
            Self::Generic(msg) => write!(f, "Error: {msg}"),
        }
    }
}

impl std::error::Error for BlinkTreeError {}

impl From<&str> for BlinkTreeError {
    fn from(s: &str) -> Self {
        Self::Generic(s.to_string())
    }
}

impl From<std::cell::BorrowMutError> for BlinkTreeError {
    fn from(err: std::cell::BorrowMutError) -> Self {
        Self::BorrowError(err.to_string())
    }
}

impl From<io::Error> for BlinkTreeError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<SerializationError> for BlinkTreeError {
    fn from(err: SerializationError) -> Self {
        Self::Serialization(err)
    }
}
