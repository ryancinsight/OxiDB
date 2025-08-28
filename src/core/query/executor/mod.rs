// src/core/query/executor/mod.rs

//! Query execution module
//! 
//! This module coordinates database query execution across different components:
//! - DDL operations (CREATE, DROP, ALTER)
//! - DML operations (SELECT, INSERT, UPDATE, DELETE)
//! - Transaction management
//! - Index management
//! - Query optimization

// Module declarations
pub mod command_handlers;
pub mod ddl_handlers;
pub mod executor;
pub mod planner;
pub mod processors;
pub mod select_execution;
#[cfg(test)]
pub mod tests;
pub mod transaction_handlers;
pub mod types;
pub mod update_execution;
pub mod utils;

// Re-export main types and structures
pub use executor::QueryExecutor;
pub use types::{ExecutionResult, ParameterContext, ValueConverter};

// Re-export constants
pub use executor::{DEFAULT_VALUE_INDEX_NAME, MAX_AUTO_INCREMENT_VALUE};
pub use types::MAX_PARAMETER_RECURSION_DEPTH;