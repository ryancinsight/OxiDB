pub type Lsn = u64;

pub mod data_type;
pub mod ids;
pub mod row;
pub mod schema;
pub mod value; // Added ids module

pub use data_type::DataType;
pub use ids::{PageId, TransactionId};
pub use row::Row;
pub use schema::{ColumnDef, Schema};
pub use value::Value; // Re-export PageId and TransactionId
                      // pub use self::Lsn; // Removed as pub type Lsn = u64; already makes it public.

#[cfg(test)]
mod tests;
