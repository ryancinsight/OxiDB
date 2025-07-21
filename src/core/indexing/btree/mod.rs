//! B+-Tree Indexing Structures
//!
//! This module contains the implementation of a B+-Tree index, including the
//! node structure (`BPlusTreeNode`) and the tree management logic (`BPlusTreeIndex`).
//! It also provides the `Index` trait implementation for `BPlusTreeIndex`.

// Module declarations
mod error; // Added error module
mod internal_tests;
pub mod node;
mod page_io; // Added page_io module
pub mod tree; // Keep existing internal_tests

// Re-export key structures for easier access from parent modules
pub use error::OxidbError; // Export our new OxidbError
pub use node::{BPlusTreeNode, KeyType, PageId, PrimaryKey, SerializationError}; // Made these public
pub use page_io::{PageManager, SENTINEL_PAGE_ID};
pub use tree::BPlusTreeIndex; // SENTINEL_PAGE_ID removed from here // Export PageManager and SENTINEL_PAGE_ID from page_io

// Removed: use std::io::{Read, Seek, SeekFrom};

// Use the Index trait from the central traits module
use crate::core::common::OxidbError as CommonError;
use crate::core::indexing::traits::Index;
use crate::core::query::commands::Key as TraitPrimaryKey; // PrimaryKey type from the trait (Key alias)
use crate::core::query::commands::Value as TraitValue; // Value type from the trait

/// Helper function to map internal BTree errors to common `OxidbError` type for the `Index` trait.
fn map_btree_error_to_common(btree_error: OxidbError) -> CommonError {
    // Changed to use the new OxidbError
    match btree_error {
        OxidbError::Io(e) => CommonError::Io(e),
        OxidbError::Serialization(se) => {
            CommonError::Serialization(format!("BTree Serialization: {:?}", se))
        }
        OxidbError::NodeNotFound(page_id) => {
            CommonError::Index(format!("BTree Node not found on page: {}", page_id))
        }
        OxidbError::PageFull(s) => CommonError::Index(format!("BTree PageFull: {}", s)),
        OxidbError::UnexpectedNodeType => {
            CommonError::Index("BTree Unexpected Node Type".to_string())
        }
        OxidbError::TreeLogicError(s) => CommonError::Index(format!("BTree Logic Error: {}", s)),
        OxidbError::BorrowError(s) => CommonError::Lock(format!("BTree Borrow Error: {}", s)),
        OxidbError::Generic(s) => CommonError::Internal(format!("BTree Generic Error: {}", s)),
    }
}

// Implementation of the Index trait for BPlusTreeIndex
impl Index for BPlusTreeIndex {
    fn name(&self) -> &str {
        &self.name
    }

    fn insert(
        &mut self,
        value: &TraitValue,
        primary_key: &TraitPrimaryKey,
    ) -> Result<(), CommonError> {
        self.insert(value.clone(), primary_key.clone()).map_err(map_btree_error_to_common)
    }

    fn find(&self, value: &TraitValue) -> Result<Option<Vec<TraitPrimaryKey>>, CommonError> {
        self.find_primary_keys(value).map_err(map_btree_error_to_common)
    }

    fn save(&self) -> Result<(), CommonError> {
        // Delegate to PageManager's sync_all_files method
        self.page_manager.sync_all_files().map_err(map_btree_error_to_common)
    }

    fn load(&mut self) -> Result<(), CommonError> {
        // Re-initialize the BPlusTreeIndex from its path.
        // This is similar to BPlusTreeIndex::new but modifies self in place.

        // Create a new PageManager. This will read metadata from the file.
        // We use the existing order as a hint, but PageManager::new will prioritize file's order if valid.
        let new_page_manager = PageManager::new(&self.path, self.order, false) // false: do not create if not exists for load
            .map_err(map_btree_error_to_common)?;

        let new_root_page_id = new_page_manager.get_root_page_id();
        let new_order = new_page_manager.get_order();

        // Check if the file was actually loadable and initialized
        if new_order == 0 {
            // Heuristic: PageManager::new might return order 0 if file was invalid/empty and create=false
            return Err(CommonError::Storage(format!(
                "Failed to load B+Tree from path {:?}: file may not exist or metadata invalid.",
                self.path
            )));
        }

        // Update self with the new state
        self.page_manager = new_page_manager;
        self.root_page_id = new_root_page_id;
        self.order = new_order;

        // It's crucial to ensure that the first node (root) is actually present if metadata suggests so.
        // BPlusTreeIndex::new handles creating an initial empty root if the tree is brand new.
        // If loading an existing tree, we assume the root node pointed to by metadata exists.
        // A check could be added here:
        // self.page_manager.read_node(self.root_page_id).map_err(map_btree_error_to_common)?;

        Ok(())
    }

    fn update(
        &mut self,
        old_value: &TraitValue,
        new_value: &TraitValue,
        primary_key: &TraitPrimaryKey,
    ) -> Result<(), CommonError> {
        self.delete(old_value, Some(primary_key)).map_err(map_btree_error_to_common)?;
        self.insert(new_value.clone(), primary_key.clone()).map_err(map_btree_error_to_common)
    }

    fn delete(
        &mut self,
        value: &TraitValue,
        primary_key_to_remove: Option<&TraitPrimaryKey>,
    ) -> Result<(), CommonError> {
        self.delete(value, primary_key_to_remove)
            .map_err(map_btree_error_to_common)
            .map(|_was_removed| ())
    }
}

#[cfg(test)]
mod tests {
    // For tests within mod.rs, direct use of items might be fine,
    // but for consistency with tree.rs tests, using crate path might be better.
    // use super::*; // This brings BPlusTreeIndex, OxidbError etc. from this mod.rs
    // use crate::core::indexing::traits::Index;
    use crate::core::indexing::btree::node::PrimaryKey;
    use crate::core::query::commands::{Key as TestKey, Value as TestValue};

    #[allow(dead_code)]
    fn trait_val(s: &str) -> TestValue {
        s.as_bytes().to_vec()
    }
    #[allow(dead_code)]
    fn trait_pk(s: &str) -> TestKey {
        s.as_bytes().to_vec()
    }

    #[allow(dead_code)]
    fn pk(s: &str) -> PrimaryKey {
        s.as_bytes().to_vec()
    }

    #[allow(dead_code)]
    fn internal_val(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }
    #[allow(dead_code)]
    fn internal_pk(s: &str) -> crate::core::indexing::btree::node::PrimaryKey {
        // Explicit path
        s.as_bytes().to_vec()
    }
}
