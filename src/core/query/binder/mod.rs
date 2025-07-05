// src/core/query/binder/mod.rs

pub mod binder;
pub mod expression; // Make expression module public

pub use self::binder::{BindError, Binder, BoundStatement}; // Re-export main items
pub use self::expression::BoundExpression; // Re-export BoundExpression
