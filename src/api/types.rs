// src/api/types.rs
//! Defines the data structures and enumerations used within the API layer.
// Required for Oxidb::new_with_config if it were here, but methods are in api_impl
use crate::core::query::executor::{QueryExecutor, ExecutionResult as CoreExecutionResult};
use crate::core::storage::engine::SimpleFileKvStore;
use crate::core::types::DataType;
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

// Represents the public result of a query execution.
#[derive(Debug, PartialEq)]
pub enum Value {
    Single(Option<DataType>),
    Multiple(Vec<DataType>),
    Success,
    Deleted(bool),
    Updated { count: usize },
    RankedResults(Vec<(f32, Vec<DataType>)>),
    Error(String), // Simplified error reporting for the public API
}

impl From<CoreExecutionResult> for Value {
    fn from(core_result: CoreExecutionResult) -> Self {
        match core_result {
            CoreExecutionResult::Value(opt_dt) => Value::Single(opt_dt),
            CoreExecutionResult::Values(vec_dt) => Value::Multiple(vec_dt),
            CoreExecutionResult::Success => Value::Success,
            CoreExecutionResult::Deleted(status) => Value::Deleted(status),
            CoreExecutionResult::Updated { count } => Value::Updated { count },
            CoreExecutionResult::RankedResults(results) => Value::RankedResults(results),
            // Note: OxidbError should be handled before this conversion,
            // typically resulting in Value::Error if it needs to be propagated through `Value`.
            // If an error occurs that leads to an ExecutionResult variant not listed,
            // it might indicate an unhandled case or that errors are meant to be transformed
            // into Value::Error at a higher level (e.g., in Oxidb::execute_query_str).
        }
    }
}


// Example (to be expanded later):
// pub struct ApiRequest { /* ... */ }
// pub enum ApiResponse { /* ... */ }
