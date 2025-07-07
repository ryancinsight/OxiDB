#[derive(Debug, thiserror::Error)]
pub enum HnswError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Node not found: {0}")]
    NodeNotFound(usize),

    #[error("Invalid vector: {0}")]
    InvalidVector(String),

    #[error("Vector dimension mismatch: expected {expected}, got {actual}")]
    DimensionMismatch { expected: usize, actual: usize },

    #[error("Graph error: {0}")]
    GraphError(String),

    #[error("HNSW error: {0}")]
    Generic(String),

    #[error("Layer index out of bounds: {index}")]
    LayerIndexOutOfBounds { index: usize },

    #[error("Maximum connections exceeded: {current}/{max}")]
    MaxConnectionsExceeded { current: usize, max: usize },

    #[error("Empty graph")]
    EmptyGraph,

    #[error("Invalid entry point: {node_id}")]
    InvalidEntryPoint { node_id: usize },
}

// thiserror automatically generates From<std::io::Error> implementation

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