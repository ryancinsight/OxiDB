// src/api/errors.rs
//! Defines the error types specific to the API layer.

use thiserror::Error;

use crate::core::common::OxidbError; // Import OxidbError

#[derive(Error, Debug)]
pub enum ApiError {
    #[error("Invalid request: {0}")]
    InvalidRequest(String),

    #[error("Database core error: {0}")]
    CoreError(#[from] OxidbError), // This provides From<OxidbError>

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Deserialization error: {0}")]
    DeserializationError(String),
    // Example:
    // #[error("Resource not found: {0}")]
    // NotFound(String),
    // #[error("Internal server error: {0}")]
    // InternalError(#[from] Box<dyn std::error::Error + Send + Sync>),
}
