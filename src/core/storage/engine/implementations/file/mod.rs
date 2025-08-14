//! Module for the `FileKvStore` implementation.

mod persistence; // internal
mod recovery;
pub mod store;

pub use store::FileKvStore;