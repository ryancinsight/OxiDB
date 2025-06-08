//! SQL Parser module.

mod core;
mod statement;
mod expression;
// Potentially other modules for specific grammar rules if needed in future.

pub use core::SqlParser;
