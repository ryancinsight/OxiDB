//! Public API module for Oxidb
//!
//! This module provides the public interface for interacting with the database,
//! including connection management and query execution.

mod connection;
pub mod types;

pub use connection::Connection;
pub use types::{QueryResult, Row};

// Re-export core types that users need
pub use crate::core::common::types::Value;
pub use crate::core::common::OxidbError;
pub use crate::core::config::Config;

