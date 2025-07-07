use std::io;

#[derive(Debug)]
pub enum RTreeError {
    Io(io::Error),
    Serialization(String),
    NodeNotFound(u64), // PageId
    InvalidGeometry(String),
    UnexpectedNodeType,
    TreeLogicError(String),
    Generic(String),
}

impl std::fmt::Display for RTreeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RTreeError::Io(err) => write!(f, "IO error: {}", err),
            RTreeError::Serialization(err) => write!(f, "Serialization error: {}", err),
            RTreeError::NodeNotFound(page_id) => write!(f, "R-tree node not found: {}", page_id),
            RTreeError::InvalidGeometry(msg) => write!(f, "Invalid geometry: {}", msg),
            RTreeError::UnexpectedNodeType => write!(f, "Unexpected R-tree node type"),
            RTreeError::TreeLogicError(msg) => write!(f, "R-tree logic error: {}", msg),
            RTreeError::Generic(msg) => write!(f, "R-tree error: {}", msg),
        }
    }
}

impl std::error::Error for RTreeError {}

impl From<io::Error> for RTreeError {
    fn from(err: io::Error) -> Self {
        RTreeError::Io(err)
    }
}

impl From<&str> for RTreeError {
    fn from(msg: &str) -> Self {
        RTreeError::Generic(msg.to_string())
    }
}

impl From<String> for RTreeError {
    fn from(msg: String) -> Self {
        RTreeError::Generic(msg)
    }
} 