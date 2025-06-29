// src/api/traits.rs
//! Defines abstract patterns of behavior (traits) for the API layer.

use crate::api::errors::ApiError;
use crate::api::types::Value; // Changed QueryResult to Value
use crate::core::query::commands::Command;
// use crate::core::types::schema::{Schema, TableSchema}; // Future use
// use crate::core::common::types::Value; // Future use

/// Defines the core operations for the Oxidb API.
pub trait OxidbApi {
    /// Executes a given Command.
    ///
    /// # Arguments
    ///
    /// * `statement`: A reference to a `Command` enum representing the operation to execute.
    ///
    /// # Returns
    ///
    /// A `Result` containing a `Value` on success, or an `ApiError` on failure.
    fn execute_statement(&self, statement: &Command) -> Result<Value, ApiError>;

    // == Future Placeholder Methods for schema and data type operations ==
    //
    // /// Creates a new table based on the provided schema.
    // fn create_table(&self, schema: &TableSchema) -> Result<(), ApiError>;
    //
    // /// Describes a table, returning its schema.
    // fn describe_table(&self, table_name: &str) -> Result<TableSchema, ApiError>;
    //
    // /// Inserts a row of data into the specified table.
    // /// Note: `Row` type would need to be defined, likely as Vec<Value>.
    // fn insert_data(&self, table_name: &str, data: &[Value]) -> Result<(), ApiError>;
    //
    // /// Retrieves all data from a table.
    // /// Note: This is a simplified example. Real-world scenarios would need pagination, filtering, etc.
    // fn get_all_data(&self, table_name: &str) -> Result<Vec<Vec<Value>>, ApiError>;
}

// Example of a concrete implementation (for illustration, not part of the trait itself):
// struct MyDbInstance { /* ... fields ... */ }
// impl OxidbApi for MyDbInstance {
//     fn execute_statement(&self, statement: &Command) -> Result<QueryResult, ApiError> {
//         // ... logic to process the command ...
//         unimplemented!()
//     }
//     // ... other methods ...
// }
