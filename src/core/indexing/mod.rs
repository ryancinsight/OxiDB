pub mod blink_tree; // Blink tree implementation
pub mod btree;
pub mod hash;
pub mod hnsw; // HNSW (Hierarchical Navigable Small World) implementation
              // pub mod rtree; // R-tree implementation (commented out to avoid export conflicts for now)
pub mod manager;
pub mod traits;

// Re-export specific, non-conflicting types
pub use self::blink_tree::BlinkTreeIndex;
pub use self::btree::BPlusTreeIndex;
pub use self::hash::HashIndex;
pub use self::hnsw::HnswIndex;
pub use self::manager::IndexManager;
