// This module will handle transaction management.

pub mod manager;
pub mod transaction;
// pub mod types; // Assuming types.rs might be added later or was a misunderstanding

pub use manager::TransactionManager;
pub use transaction::{Transaction, TransactionState};
// pub use types::{TransactionError, TransactionResult}; // Assuming these would be in types.rs
