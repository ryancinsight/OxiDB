//! Module for the SimpleFileKvStore implementation.

pub mod store;
mod persistence; // Will be pub(super)
mod recovery;    // Will be pub(super)

pub use store::SimpleFileKvStore;
