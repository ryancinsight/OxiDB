// src/core/indexing/vector/mod.rs

use crate::core::common::OxidbError;
use crate::core::query::commands::Key as PrimaryKey; // Using common::Key
use crate::core::types::VectorData;
use std::fmt::Debug;

pub mod kdtree;

/// Trait for vector indexes capable of similarity search.
pub trait VectorIndex: Debug + Send + Sync {
    /// Returns the name of the index.
    fn name(&self) -> &str;

    /// Returns the dimension of vectors this index handles.
    fn dimension(&self) -> u32;

    /// Inserts a vector with its primary key into the index.
    /// Depending on implementation, this might add to a temporary store
    /// requiring a separate `build` step, or insert directly into the structure.
    fn insert(&mut self, vector: &VectorData, primary_key: &PrimaryKey) -> Result<(), OxidbError>;

    /// Deletes a vector associated with a primary key from the index.
    fn delete(&mut self, primary_key: &PrimaryKey) -> Result<(), OxidbError>;

    /// Performs a K-Nearest Neighbor search.
    /// Returns a list of (PrimaryKey, distance) tuples.
    fn search_knn(
        &self,
        query_vector: &VectorData,
        k: usize,
    ) -> Result<Vec<(PrimaryKey, f32)>, OxidbError>;

    /// Builds or rebuilds the index from all current data.
    /// This is crucial for indexes like KD-Tree that are typically bulk-loaded.
    /// Takes a collection of (PrimaryKey, VectorData) pairs.
    fn build(&mut self, all_data: &[(PrimaryKey, VectorData)]) -> Result<(), OxidbError>;

    /// Saves the index data to persistent storage.
    fn save(&self) -> Result<(), OxidbError>;

    /// Loads the index data from persistent storage.
    fn load(&mut self) -> Result<(), OxidbError>;
}

/// Enum for specific errors related to vector indexing operations.
#[derive(Debug)]
pub enum VectorIndexError {
    DimensionMismatch(String),
    NotFound(String),
    BuildError(String),
    SaveError(String),
    LoadError(String),
    InternalError(String),
    // Add other specific errors as needed
}

impl std::fmt::Display for VectorIndexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VectorIndexError::DimensionMismatch(s) => write!(f, "Dimension mismatch: {}", s),
            VectorIndexError::NotFound(s) => write!(f, "Not found: {}", s),
            VectorIndexError::BuildError(s) => write!(f, "Build error: {}", s),
            VectorIndexError::SaveError(s) => write!(f, "Save error: {}", s),
            VectorIndexError::LoadError(s) => write!(f, "Load error: {}", s),
            VectorIndexError::InternalError(s) => write!(f, "Internal error: {}", s),
        }
    }
}

impl std::error::Error for VectorIndexError {}

// Convert VectorIndexError to OxidbError for integration with IndexManager/API
impl From<VectorIndexError> for OxidbError {
    fn from(err: VectorIndexError) -> Self {
        OxidbError::VectorIndex(Box::new(err)) // Store the boxed error
    }
}

// Also need to ensure KdTreeError can be converted to OxidbError, likely via VectorIndexError
impl From<kdtree::KdTreeError> for VectorIndexError {
    fn from(kdt_err: kdtree::KdTreeError) -> Self {
        match kdt_err {
            kdtree::KdTreeError::DimensionMismatch(s) => VectorIndexError::DimensionMismatch(s),
            kdtree::KdTreeError::EmptyInput(s) => VectorIndexError::BuildError(format!("Empty input: {}",s)),
            kdtree::KdTreeError::AxisOutOfBounds(s) => VectorIndexError::InternalError(format!("Axis out of bounds: {}",s)),
            kdtree::KdTreeError::InternalError(s) => VectorIndexError::InternalError(s),
            kdtree::KdTreeError::SearchError(s) => VectorIndexError::InternalError(format!("Search error: {}", s)),
        }
    }
}
