// This is the execution module.
// It defines the ExecutionOperator trait and related structs/type aliases.

pub mod operators;
// Optional: Re-export new operators
// pub use operators::{TableScanOperator, IndexScanOperator};

// Module for expression evaluation logic
pub mod expression_evaluator;

use crate::core::common::OxidbError;
use crate::core::common::types::{Schema, Value}; // Added Schema
use std::sync::Arc; // Added Arc

// Define Tuple type alias or struct
pub type Tuple = Vec<Value>; // Changed from Vec<DataType> to Vec<Value>

// Define ExecutionOperator trait
pub trait ExecutionOperator {
    fn execute(
        &mut self,
    ) -> Result<Box<dyn Iterator<Item = Result<Tuple, OxidbError>> + Send + Sync>, OxidbError>;

    /// Returns the schema of the tuples produced by this operator.
    fn get_output_schema(&self) -> Arc<Schema>;
}
