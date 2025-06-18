//! B+-Tree Indexing Structures
//!
//! This module contains the implementation of a B+-Tree index, including the
//! node structure (`BPlusTreeNode`) and the tree management logic (`BPlusTreeIndex`).
//! It also provides the `Index` trait implementation for `BPlusTreeIndex`.

// Module declarations for node and tree logic
pub mod node;
pub mod tree;

// Re-export key structures for easier access from parent modules (e.g., `super::IndexManager`)
pub use node::BPlusTreeNode;
pub use tree::BPlusTreeIndex;
// pub use tree::OxidbError as BTreeError; // Keep BTreeError distinct for internal use
use std::io::{Seek, Read}; // Added Seek and Read

// Use the Index trait from the central traits module
use crate::core::indexing::traits::Index;
use crate::core::query::commands::Value as TraitValue; // Value type from the trait
use crate::core::query::commands::Key as TraitPrimaryKey; // PrimaryKey type from the trait (Key alias)
use crate::core::common::OxidbError as CommonError; // Common error type used by the Index trait
// Internal BTree methods will use node::PrimaryKey and node::KeyType (Vec<u8>)

/// Helper function to map internal BTree errors to common `OxidbError` type for the `Index` trait.
fn map_btree_error_to_common(btree_error: tree::OxidbError) -> CommonError {
    match btree_error {
        tree::OxidbError::Io(e) => CommonError::Io(e),
        tree::OxidbError::Serialization(se) => CommonError::Serialization(format!("BTree Serialization: {:?}", se)),
        tree::OxidbError::NodeNotFound(page_id) => CommonError::Index(format!("BTree Node not found on page: {}", page_id)),
        tree::OxidbError::PageFull(s) => CommonError::Index(format!("BTree PageFull: {}", s)),
        tree::OxidbError::UnexpectedNodeType => CommonError::Index("BTree Unexpected Node Type".to_string()),
        tree::OxidbError::TreeLogicError(s) => CommonError::Index(format!("BTree Logic Error: {}", s)),
        tree::OxidbError::BorrowError(s) => CommonError::Lock(format!("BTree Borrow Error: {}", s)),
    }
}

// Implementation of the Index trait for BPlusTreeIndex
impl Index for BPlusTreeIndex {
    fn name(&self) -> &str {
        &self.name
    }

    // Methods now use TraitValue and TraitPrimaryKey from query::commands
    fn insert(&mut self, value: &TraitValue, primary_key: &TraitPrimaryKey) -> Result<(), CommonError> {
        // TraitValue is &Vec<u8>, TraitPrimaryKey is &Vec<u8>
        // BPlusTreeIndex internal insert expects key: Vec<u8>, value: Vec<u8> (node::PrimaryKey)
        self.insert(value.clone(), primary_key.clone()).map_err(map_btree_error_to_common)
    }

    fn find(&self, value: &TraitValue) -> Result<Option<Vec<TraitPrimaryKey>>, CommonError> {
        // TraitValue is &Vec<u8>
        // BPlusTreeIndex internal find_primary_keys expects key: &Vec<u8> (node::KeyType)
        // It returns Result<Option<Vec<node::PrimaryKey>>, tree::OxidbError>
        // node::PrimaryKey is Vec<u8>, TraitPrimaryKey is Vec<u8>. So conversion is just type alias matching.
        self.find_primary_keys(value) // value is already &Vec<u8>
            .map_err(map_btree_error_to_common)
            // The inner map was an identity function, so it can be removed.
            // opt_vec_node_pk is Option<Vec<node::PrimaryKey>>
            // Since TraitPrimaryKey is an alias for node::PrimaryKey (Vec<u8>),
            // no further conversion is needed.
            // .map(|opt_vec_node_pk| {
            //     opt_vec_node_pk.map(|vec_node_pk| vec_node_pk)
            // })
    }

    fn save(&self) -> Result<(), CommonError> {
        // BPlusTreeIndex::write_metadata() takes &mut self and is for internal updates.
        // The Index trait's save() is for persisting the current state.
        // For BPlusTreeIndex, data is written as it's modified. Metadata is also updated.
        // So, save() primarily means ensuring everything is flushed to disk.
        // With Mutex, we lock to get access to the File object.
        self.file_handle.lock()
            .map_err(|e| CommonError::Lock(format!("Failed to lock file handle for save: {}", e)))?
            .sync_all()
            .map_err(CommonError::Io)
    }

    fn load(&mut self) -> Result<(), CommonError> {
        let mut file = self.file_handle.lock()
            .map_err(|e| CommonError::Lock(format!("Failed to lock file handle mutably for load: {}", e)))?;
        // This re-reads metadata. BPlusTreeIndex::new() handles the initial load.
        file.seek(std::io::SeekFrom::Start(0)).map_err(CommonError::Io)?;
        let mut u32_buf = [0u8; 4];
        let mut u64_buf = [0u8; 8];

        file.read_exact(&mut u32_buf).map_err(CommonError::Io)?;
        let loaded_order = u32::from_be_bytes(u32_buf) as usize;

        file.read_exact(&mut u64_buf).map_err(CommonError::Io)?;
        let root_page_id = u64::from_be_bytes(u64_buf);

        file.read_exact(&mut u64_buf).map_err(CommonError::Io)?;
        let next_available_page_id = u64::from_be_bytes(u64_buf);

        if self.order != loaded_order && self.order != 0 {
             eprintln!("Warning: Index order mismatch during load. File order: {}, current tree order: {}. Using file order.", loaded_order, self.order);
        }

        self.order = loaded_order;
        self.root_page_id = root_page_id;
        self.next_available_page_id = next_available_page_id;

        Ok(())
    }

    fn update(
        &mut self,
        old_value: &TraitValue,
        new_value: &TraitValue,
        primary_key: &TraitPrimaryKey,
    ) -> Result<(), CommonError> {
        // Types are: &Vec<u8>, &Vec<u8>, &Vec<u8>
        // Internal delete: key_to_delete: &KeyType, pk_to_remove: Option<&PrimaryKey>
        // Internal insert: key: KeyType, value: PrimaryKey
        self.delete(old_value, Some(primary_key)).map_err(map_btree_error_to_common)?;
        self.insert(new_value.clone(), primary_key.clone()).map_err(map_btree_error_to_common)
    }

    fn delete(&mut self, value: &TraitValue, primary_key_to_remove: Option<&TraitPrimaryKey>) -> Result<(), CommonError> {
        // value is &Vec<u8>, primary_key_to_remove is Option<&Vec<u8>>
        // Internal delete: key_to_delete: &KeyType, pk_to_remove: Option<&node::PrimaryKey>
        // These types match directly.
        self.delete(value, primary_key_to_remove)
            .map_err(map_btree_error_to_common)
            .map(|_was_removed| ())
    }
}

// Tests are now correctly using trait_val and trait_pk which should align with query::commands types.
// The internal_val and internal_pk are for direct btree calls or verifying byte content.
// The test assertions for find results (e.g. pks_apple, expected_pks_apple) might need adjustment
// if TraitPrimaryKey has a different structure than just Vec<u8> (e.g. if it's a struct).
// However, the trait defines it as `crate::core::query::commands::Key as PrimaryKey`.
// If `query::commands::Key` is `pub struct Key(pub Vec<u8>);`, then trait_pk and comparisons need care.
// Based on E0599, `query::commands::Value` and `query::commands::Key` are `Vec<u8>`.
#[cfg(test)]
mod tests {
    use super::*; // This brings BPlusTreeIndex, map_btree_error_to_common into scope
    // use crate::core::indexing::traits::Index; // Trait itself - Not directly used in these specific tests, but good for context
    use crate::core::query::commands::{Value as TestValue, Key as TestKey}; // Actual types for tests

    // use std::fs as std_fs; // Not directly used in these specific tests, but good for context
    // use tempfile::tempdir; // Not directly used in these specific tests

    // Helper functions for tests to create TraitValue and TraitPrimaryKey
    // These are simplified placeholders. Actual construction will depend on query::commands types.
    // Assuming query::commands::Value and Key are effectively Vec<u8> based on E0599 errors
    #[allow(dead_code)] // Potentially unused if tests only use trait methods
    fn trait_val(s: &str) -> TestValue { s.as_bytes().to_vec() }
    #[allow(dead_code)] // Potentially unused
    fn trait_pk(s: &str) -> TestKey { s.as_bytes().to_vec() }

    // These are for direct BTree calls if needed, or for expected values in find.
    #[allow(dead_code)] // Potentially unused
    fn internal_val(s: &str) -> Vec<u8> { s.as_bytes().to_vec() }
    #[allow(dead_code)] // Potentially unused
    fn internal_pk(s: &str) -> crate::core::indexing::btree::node::PrimaryKey { s.as_bytes().to_vec() }

// This comment and the duplicated tests block below will be removed.
}
