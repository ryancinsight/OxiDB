// Will contain execution operator implementations
// use crate::core::common::error::DbError;
// use crate::core::types::DataType; // Make sure DataType is accessible
// use crate::core::execution::{ExecutionOperator, Tuple}; // Assuming Tuple is Vec<DataType>
// use crate::core::storage::engine::traits::KeyValueStore;
// use crate::core::indexing::manager::IndexManager;
// use crate::core::common::serialization::deserialize_data_type; // For deserializing store values
// use crate::core::query::commands::Key; // Or whatever your primary key type is
// use std::sync::Arc;
// use std::collections::HashSet; // Removed HashMap
//
// // Imports for FilterOperator / ProjectOperator
// use crate::core::optimizer::Expression; // Assuming these are in optimizer/mod.rs
// use crate::core::optimizer::JoinPredicate; // Ensure this is imported
// SimplePredicate is part of Expression enum based on previous setup.
// QueryPlanNode is not directly used by these operators, but Expression is.

// All individual operators have been moved to their own files.
// This file will now just declare the modules and re-export.

pub mod delete;
pub mod filter;
pub mod index_scan;
pub mod nested_loop_join;
pub mod project;
pub mod table_scan; // Added delete module

pub use delete::*;
pub use filter::*;
pub use index_scan::*;
pub use nested_loop_join::*;
pub use project::*;
pub use table_scan::*; // Added delete export
