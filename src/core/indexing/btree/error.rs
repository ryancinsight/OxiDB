use crate::core::indexing::btree::node::{PageId, SerializationError};
use std::io; // Adjusted path

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
            Self::Io(err) => write!(f, "BTree IO error: {err}"),
            Self::Serialization(err) => write!(f, "BTree Serialization error: {err:?}"),
            Self::NodeNotFound(page_id) => write!(f, "BTree Node not found: {page_id}"),
            Self::PageFull(msg) => write!(f, "BTree Page full: {msg}"),
            Self::UnexpectedNodeType => write!(f, "BTree Unexpected node type"),
            Self::TreeLogicError(msg) => write!(f, "BTree logic error: {msg}"),
            Self::BorrowError(msg) => write!(f, "BTree borrow error: {msg}"),
            Self::Generic(msg) => write!(f, "BTree generic error: {msg}"),
        }
    }
}

impl From<&str> for OxidbError {
    fn from(s: &str) -> Self {
        Self::Generic(s.to_string())
    }
}

impl From<std::cell::BorrowMutError> for OxidbError {
    fn from(err: std::cell::BorrowMutError) -> Self {
        Self::BorrowError(err.to_string())
    }
}

impl From<io::Error> for OxidbError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<SerializationError> for OxidbError {
    fn from(err: SerializationError) -> Self {
        Self::Serialization(err)
    }
}
