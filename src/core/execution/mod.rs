// This is the execution module.
// It defines the ExecutionOperator trait and related structs/type aliases.

pub mod operators; // Added operators module
                   // Optional: Re-export new operators
                   // pub use operators::{TableScanOperator, IndexScanOperator};

use crate::core::common::OxidbError; // Changed
use crate::core::types::DataType;

// Define Tuple type alias or struct
pub type Tuple = Vec<DataType>;

// Define ExecutionOperator trait
pub trait ExecutionOperator {
    fn execute(
        &mut self,
    ) -> Result<Box<dyn Iterator<Item = Result<Tuple, OxidbError>> + Send + Sync>, OxidbError>; // Changed
}
