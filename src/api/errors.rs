// src/api/errors.rs
//! Defines the error types specific to the API layer.

use std::fmt;

#[derive(Debug)]
pub enum Error {
    InvalidRequest(String),
    // Example:
    // NotFound(String),
    // InternalError(Box<dyn std::error::Error + Send + Sync>),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest(s) => write!(f, "Invalid request: {}", s),
        }
    }
}

impl std::error::Error for Error {}
