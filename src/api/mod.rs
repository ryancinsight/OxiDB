//! Public API module for Oxidb database
//! 
//! This module provides the public-facing API for interacting with the Oxidb database.
//! The main entry point is the `Connection` struct which provides a modern, type-safe
//! interface for database operations.

pub mod connection;
pub mod types;

pub use connection::Connection;
pub use types::{QueryResult, RowSet, Row};

// Re-export key types/traits for easier use by external crates, if desired.
// For example:
// pub use self::types::{ApiRequest, ApiResponse};
// pub use self::traits::ApiRequestHandler; // Example, not activated yet

// If you want to make tests accessible from outside the api module (usually not needed)
// pub mod tests;
// Or, ensure tests are correctly configured within the api module if they are integration-like tests for the api module itself.
// For unit tests within db.rs, they would be in a `#[cfg(test)] mod tests { ... }` block in db.rs.
// The current tests in api/mod.rs are more like integration tests for the Oxidb struct.

