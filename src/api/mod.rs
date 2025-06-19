// src/api/mod.rs
//! # Oxidb API Module
//!
//! This module provides the public API for interacting with the Oxidb key-value store.
//! It exposes the `Oxidb` struct, which is the main entry point for database operations.

// pub mod db; // Removed
pub mod types;
pub mod traits;
pub mod errors;
pub mod api_impl;

pub use self::types::Oxidb; // Changed from db::Oxidb
pub use self::errors::ApiError; // Added as per plan

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
