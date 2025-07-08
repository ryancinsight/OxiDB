use std::path::PathBuf;

use crate::core::indexing::blink_tree::error::BlinkTreeError;
use crate::core::indexing::blink_tree::node::{BlinkTreeNode, KeyType, PageId, PrimaryKey};
use crate::core::indexing::blink_tree::page_io::{BlinkPageManager, SENTINEL_PAGE_ID};

mod delete;
mod insert;
mod search;

pub use delete::*;

/// Blink Tree Index implementation with concurrent access support
///
/// Key features:
/// - Lock-free reads during splits using right-link pointers
/// - High keys for safe concurrent traversal
/// - Minimal locking for write operations
#[derive(Debug)]
pub struct BlinkTreeIndex {
    pub name: String,
    pub path: PathBuf,
    pub order: usize,
    pub(super) root_page_id: PageId,
    pub(super) page_manager: BlinkPageManager,
}

impl BlinkTreeIndex {
    /// Create a new Blink tree index
    pub fn new(name: String, path: PathBuf, order: usize) -> Result<Self, BlinkTreeError> {
        if order < 3 {
            return Err(BlinkTreeError::Generic(
                "Order must be at least 3 for a valid Blink tree".to_string(),
            ));
        }

        let page_manager = BlinkPageManager::new(&path, order, true)?;
        let root_page_id = page_manager.get_root_page_id();

        let mut tree = BlinkTreeIndex { name, path, order, root_page_id, page_manager };

        // If this is a new tree (no root), create the initial root leaf node
        if tree.root_page_id == SENTINEL_PAGE_ID {
            tree.create_initial_root()?;
        }

        Ok(tree)
    }

    /// Create the initial root node (leaf node)
    fn create_initial_root(&mut self) -> Result<(), BlinkTreeError> {
        let new_page_id = self.allocate_new_page_id()?;

        let root_node = BlinkTreeNode::Leaf {
            page_id: new_page_id,
            parent_page_id: None,
            keys: Vec::new(),
            values: Vec::new(),
            right_link: None, // No right sibling initially
            high_key: None,   // No high key for root
        };

        self.write_node(&root_node)?;
        self.root_page_id = new_page_id;
        self.page_manager.set_root_page_id(new_page_id)?;

        Ok(())
    }

    /// Lock-free search for primary keys (NEW for Blink tree)
    /// This is the core concurrent access feature - readers can traverse
    /// the tree without locks even during splits
    pub fn find_primary_keys(
        &self,
        key: &KeyType,
    ) -> Result<Option<Vec<PrimaryKey>>, BlinkTreeError> {
        if self.root_page_id == SENTINEL_PAGE_ID {
            return Ok(None);
        }

        // Start from root and traverse down
        let mut current_page_id = self.root_page_id;

        loop {
            let current_node = self.read_node(current_page_id)?;

            if current_node.is_leaf() {
                // We've reached a leaf node - search for the key
                return self.search_leaf_node(&current_node, key);
            } else {
                // Internal node - find next level to search
                current_page_id = self.find_next_page_in_internal(&current_node, key)?;
            }
        }
    }

    /// Search within a leaf node for a specific key
    fn search_leaf_node(
        &self,
        leaf_node: &BlinkTreeNode,
        search_key: &KeyType,
    ) -> Result<Option<Vec<PrimaryKey>>, BlinkTreeError> {
        match leaf_node {
            BlinkTreeNode::Leaf { keys, values, right_link, .. } => {
                // First check if this node is safe for our search key
                if !leaf_node.is_safe_for_key(search_key) {
                    // Key might be in right sibling due to concurrent split
                    if let Some(right_page_id) = right_link {
                        let right_node = self.read_node(*right_page_id)?;
                        return self.search_leaf_node(&right_node, search_key);
                    } else {
                        // No right sibling, key definitely not found
                        return Ok(None);
                    }
                }

                // Search within this node
                for (i, key) in keys.iter().enumerate() {
                    if key == search_key {
                        return Ok(Some(values[i].clone()));
                    }
                }

                Ok(None)
            }
            _ => Err(BlinkTreeError::UnexpectedNodeType),
        }
    }

    /// Find the next page to search in an internal node (with right-link following)
    fn find_next_page_in_internal(
        &self,
        internal_node: &BlinkTreeNode,
        search_key: &KeyType,
    ) -> Result<PageId, BlinkTreeError> {
        match internal_node {
            BlinkTreeNode::Internal { children, right_link, .. } => {
                // Check if this node is safe for our search key
                if !internal_node.is_safe_for_key(search_key) {
                    // Key might be in right sibling due to concurrent split
                    if let Some(right_page_id) = right_link {
                        let right_node = self.read_node(*right_page_id)?;
                        return self.find_next_page_in_internal(&right_node, search_key);
                    } else {
                        // No right sibling, follow rightmost child
                        return Ok(children[children.len() - 1]);
                    }
                }

                // Find appropriate child to follow
                let child_index = internal_node.find_child_index(search_key)?;
                Ok(children[child_index])
            }
            _ => Err(BlinkTreeError::UnexpectedNodeType),
        }
    }

    /// Helper methods for page management
    pub(super) fn allocate_new_page_id(&mut self) -> Result<PageId, BlinkTreeError> {
        self.page_manager.allocate_new_page_id()
    }

    pub(super) fn deallocate_page_id(&mut self, page_id: PageId) -> Result<(), BlinkTreeError> {
        self.page_manager.deallocate_page_id(page_id)
    }

    pub(super) fn read_node(&self, page_id: PageId) -> Result<BlinkTreeNode, BlinkTreeError> {
        self.page_manager.read_node(page_id)
    }

    pub(super) fn write_node(&mut self, node: &BlinkTreeNode) -> Result<(), BlinkTreeError> {
        self.page_manager.write_node(node)
    }

    /// Write metadata if root has changed
    pub(super) fn write_metadata_if_root_changed(
        &mut self,
        old_root_id: PageId,
    ) -> Result<(), BlinkTreeError> {
        if self.root_page_id != old_root_id {
            self.page_manager.set_root_page_id(self.root_page_id)
        } else {
            Ok(())
        }
    }

    /// Get the order of this tree
    pub fn get_order(&self) -> usize {
        self.order
    }

    /// Get the current root page ID
    pub fn get_root_page_id(&self) -> PageId {
        self.root_page_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn k(s: &str) -> KeyType {
        s.as_bytes().to_vec()
    }

    fn setup_tree(test_name: &str) -> (BlinkTreeIndex, PathBuf, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let tree_path = temp_dir.path().join(format!("{}.blink", test_name));
        let tree = BlinkTreeIndex::new("test_blink".to_string(), tree_path.clone(), 5).unwrap();
        (tree, tree_path, temp_dir)
    }

    #[test]
    fn test_new_blink_tree_creation() {
        let (tree, _path, _temp_dir) = setup_tree("test_new_creation");

        assert_eq!(tree.name, "test_blink");
        assert_eq!(tree.order, 5);
        assert_ne!(tree.root_page_id, SENTINEL_PAGE_ID);

        // Root should be a leaf node initially
        let root_node = tree.read_node(tree.root_page_id).unwrap();
        assert!(root_node.is_leaf());
        assert_eq!(root_node.get_keys().len(), 0);
    }

    #[test]
    fn test_search_empty_tree() {
        let (tree, _path, _temp_dir) = setup_tree("test_search_empty");

        let result = tree.find_primary_keys(&k("nonexistent")).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_concurrent_safety_methods() {
        let (tree, _path, _temp_dir) = setup_tree("test_concurrent_safety");

        // Test that search works on empty tree
        assert!(tree.find_primary_keys(&k("test")).unwrap().is_none());

        // Test node safety checking
        let root_node = tree.read_node(tree.root_page_id).unwrap();
        assert!(root_node.is_safe_for_key(&k("any_key"))); // Should be safe since no high key
    }
}
