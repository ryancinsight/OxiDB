//! Module for the `SimpleFileKvStore` implementation.

mod persistence; // Will be pub(super)
mod recovery;
pub mod store; // Will be pub(super)

pub use store::SimpleFileKvStore;
