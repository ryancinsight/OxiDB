// src/api/errors.rs
//! Defines the error types specific to the API layer.

use thiserror::Error;

#[derive(Error, Debug)]
pub enum ApiError {
    #[error("Invalid request: {0}")]
    InvalidRequest(String),
    // Example:
    // #[error("Resource not found: {0}")]
    // NotFound(String),
    // #[error("Internal server error: {0}")]
    // InternalError(#[from] Box<dyn std::error::Error + Send + Sync>),
}
