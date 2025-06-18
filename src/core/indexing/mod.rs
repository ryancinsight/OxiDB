pub mod btree; // Added
pub mod hash_index;
pub mod manager;
pub mod traits;

// Re-export IndexManager for convenience if other top-level modules use it.
pub use manager::IndexManager;
// Re-export the Index trait from traits.rs for convenience.
pub use traits::Index;
