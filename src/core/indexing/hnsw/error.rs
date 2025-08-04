#[derive(Debug)]
pub enum HnswError {
    Io(std::io::Error),
    Serialization(String),
    NodeNotFound(usize),
    InvalidVector(String),
    DimensionMismatch { expected: usize, actual: usize },
    GraphError(String),
    Generic(String),
    LayerIndexOutOfBounds { index: usize },
    MaxConnectionsExceeded { current: usize, max: usize },
    EmptyGraph,
    InvalidEntryPoint { node_id: usize },
}

impl std::fmt::Display for HnswError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::Serialization(e) => write!(f, "Serialization error: {e}"),
            Self::NodeNotFound(id) => write!(f, "Node not found: {id}"),
            Self::InvalidVector(msg) => write!(f, "Invalid vector: {msg}"),
            Self::DimensionMismatch { expected, actual } => {
                write!(f, "Vector dimension mismatch: expected {expected}, got {actual}")
            }
            Self::GraphError(msg) => write!(f, "Graph error: {msg}"),
            Self::Generic(msg) => write!(f, "HNSW error: {msg}"),
            Self::LayerIndexOutOfBounds { index } => write!(f, "Layer index out of bounds: {index}"),
            Self::MaxConnectionsExceeded { current, max } => {
                write!(f, "Maximum connections exceeded: {current}/{max}")
            }
            Self::EmptyGraph => write!(f, "Empty graph"),
            Self::InvalidEntryPoint { node_id } => write!(f, "Invalid entry point: {node_id}"),
        }
    }
}

impl std::error::Error for HnswError {}

impl From<std::io::Error> for HnswError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

// Result type alias for convenience
pub type HnswResult<T> = Result<T, HnswError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_creation() {
        let error = HnswError::NodeNotFound(42);
        assert!(error.to_string().contains("42"));

        let error = HnswError::DimensionMismatch { expected: 128, actual: 64 };
        assert!(error.to_string().contains("128"));
        assert!(error.to_string().contains("64"));
    }

    #[test]
    fn test_io_error_conversion() {
        let io_error = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let hnsw_error: HnswError = io_error.into();
        assert!(matches!(hnsw_error, HnswError::Io(_)));
    }
}
