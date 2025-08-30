pub type Lsn = u64;

pub mod data_type;
pub mod ids;
pub mod row;
pub mod value; // Added ids module

pub use data_type::DataType;
pub use ids::{PageId, TransactionId};
pub use row::Row;
pub use value::Value; // Re-export PageId and TransactionId

// Re-export Schema types from the unified location
pub use crate::core::types::schema::{ColumnDef, Schema};

#[cfg(test)]
mod tests;
