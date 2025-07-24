// src/api/mod.rs
//! # Oxidb API Module
//!
//! This module provides the public API for interacting with the Oxidb database.
//! It exposes both the legacy `Oxidb` struct and the new ergonomic `Connection` API.

pub mod implementation;
pub mod connection;
pub mod errors;
pub mod traits;
pub mod types;

pub use self::connection::Connection;
pub use self::errors::Error as ApiError;
pub use self::types::{Oxidb, QueryResult, QueryResultData, Row};

// Re-export key types/traits for easier use by external crates, if desired.
// For example:
// pub use self::types::{ApiRequest, ApiResponse};
// pub use self::traits::ApiRequestHandler; // Example, not activated yet

// If you want to make tests accessible from outside the api module (usually not needed)
// pub mod tests;
// Or, ensure tests are correctly configured within the api module if they are integration-like tests for the api module itself.
// For unit tests within db.rs, they would be in a `#[cfg(test)] mod tests { ... }` block in db.rs.
// The current tests in api/mod.rs are more like integration tests for the Oxidb struct.
// Let's assume they are module tests for now.
#[cfg(test)]
mod tests;
