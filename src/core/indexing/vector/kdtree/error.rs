// src/core/indexing/vector/kdtree/error.rs

use std::fmt;

/// Custom error types for KD-Tree operations.
#[derive(Debug)]
pub enum KdTreeError {
    /// Error when trying to build a tree from empty input.
    EmptyInput(String),
    /// Error when vector dimensions are inconsistent.
    DimensionMismatch(String),
    /// Error when an axis is out of bounds for a given dimension.
    AxisOutOfBounds(String),
    /// Error for general issues, e.g., during search or build.
    InternalError(String),
    /// Error when a search target is not found or k is invalid.
    SearchError(String),
}

impl fmt::Display for KdTreeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KdTreeError::EmptyInput(msg) => write!(f, "KD-Tree Empty Input: {}", msg),
            KdTreeError::DimensionMismatch(msg) => write!(f, "KD-Tree Dimension Mismatch: {}", msg),
            KdTreeError::AxisOutOfBounds(msg) => write!(f, "KD-Tree Axis Out Of Bounds: {}", msg),
            KdTreeError::InternalError(msg) => write!(f, "KD-Tree Internal Error: {}", msg),
            KdTreeError::SearchError(msg) => write!(f, "KD-Tree Search Error: {}", msg),
        }
    }
}

impl std::error::Error for KdTreeError {}

// Helper for creating a new error, e.g.
// pub fn new_empty_input_error(details: &str) -> KdTreeError {
//     KdTreeError::EmptyInput(details.to_string())
// }
//
// pub fn new_dimension_mismatch_error(details: &str) -> KdTreeError {
//     KdTreeError::DimensionMismatch(details.to_string())
// }
