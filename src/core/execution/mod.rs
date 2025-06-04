// This is the execution module.
// It defines the ExecutionOperator trait and related structs/type aliases.

pub mod operators; // Added operators module
// Optional: Re-export new operators
// pub use operators::{TableScanOperator, IndexScanOperator};

use crate::core::common::error::DbError;
use crate::core::types::DataType;
use std::collections::HashMap;

// Define Tuple type alias or struct
#[allow(dead_code)] // TODO: Remove this when Tuple is used
pub type Tuple = Vec<DataType>;

// Define Row struct (optional, for now Tuple is simpler)
#[allow(dead_code)] // TODO: Remove this when Row is used
pub struct Row {
    #[allow(dead_code)] // TODO: Remove this when columns is used
    pub columns: HashMap<String, DataType>,
}

// Define ExecutionOperator trait
#[allow(dead_code)] // TODO: Remove this when ExecutionOperator is used
pub trait ExecutionOperator {
    fn execute(&mut self) -> Result<Box<dyn Iterator<Item = Result<Tuple, DbError>> + Send + Sync>, DbError>;
}
