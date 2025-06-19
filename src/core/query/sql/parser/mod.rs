//! SQL Parser module.

mod core;
mod expression;
mod statement;
// Potentially other modules for specific grammar rules if needed in future.

pub use core::SqlParser;
