// src/api/types.rs
//! Defines the data structures and enumerations used within the API layer.
// Required for Oxidb::new_with_config if it were here, but methods are in api_impl
use crate::core::query::executor::QueryExecutor;
use crate::core::storage::engine::SimpleFileKvStore;
// std::path::PathBuf is not directly used in the struct fields, but methods in api_impl might return it.
// No, Oxidb struct itself does not need PathBuf, Config, etc. directly.
// QueryExecutor and SimpleFileKvStore are essential.

/// `Oxidb` is the primary structure providing the public API for the key-value store.
///
/// It encapsulates a `QueryExecutor` instance to manage database operations,
/// which in turn uses a `SimpleFileKvStore` for persistence.
#[derive(Debug)]
pub struct Oxidb {
    /// The query executor responsible for handling database operations.
    /// Visible within the `api` module (crate::api) to allow `api_impl.rs` to access it.
    pub(crate) executor: QueryExecutor<SimpleFileKvStore>,
}

/// Represents the result of a query execution.
/// Placeholder: This will be expanded with more detailed variants.
#[derive(Debug)]
pub enum QueryResult {
    Success,             // Placeholder for successful operations that don't return data
    Data(String),        // Placeholder for operations that return data (e.g., SELECT)
    RowsAffected(usize), // For INSERT, UPDATE, DELETE
}

// Example (to be expanded later):
// pub struct ApiRequest { /* ... */ }
// pub enum ApiResponse { /* ... */ }
