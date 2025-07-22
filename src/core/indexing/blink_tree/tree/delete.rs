use super::BlinkTreeIndex;
use crate::core::indexing::blink_tree::error::BlinkTreeError;
use crate::core::indexing::blink_tree::node::{BlinkTreeNode, KeyType, PrimaryKey};

impl BlinkTreeIndex {
    /// Delete a key from the Blink tree
    /// If `primary_key` is specified, only remove that specific primary key
    /// If `primary_key` is None, remove all entries for the key
    pub fn delete(
        &mut self,
        key: &KeyType,
        primary_key: Option<&PrimaryKey>,
    ) -> Result<bool, BlinkTreeError> {
        if self.root_page_id == super::SENTINEL_PAGE_ID {
            return Ok(false); // Empty tree
        }

        // Find the leaf node containing this key
        let leaf_page_id = self.find_leaf_for_key(key)?;

        // Perform the deletion
        self.delete_from_leaf(leaf_page_id, key, primary_key)
    }

    /// Find the leaf node that should contain a given key
    fn find_leaf_for_key(&self, key: &KeyType) -> Result<super::PageId, BlinkTreeError> {
        let mut current_page_id = self.root_page_id;

        loop {
            let current_node = self.read_node(current_page_id)?;

            if current_node.is_leaf() {
                return Ok(current_page_id);
            }
            // Follow the appropriate child
            current_page_id = self.find_next_page_in_internal(&current_node, key)?;
        }
    }

    /// Delete from a leaf node
    /// Returns true if any deletion occurred, false if key was not found
    fn delete_from_leaf(
        &mut self,
        leaf_page_id: super::PageId,
        key: &KeyType,
        primary_key: Option<&PrimaryKey>,
    ) -> Result<bool, BlinkTreeError> {
        let mut leaf_node = self.read_node(leaf_page_id)?;

        // Check if this is the right node for our key (handle concurrent splits)
        if !leaf_node.is_safe_for_key(key) {
            if let Some(right_page_id) = leaf_node.get_right_link() {
                return self.delete_from_leaf(right_page_id, key, primary_key);
            }
            return Ok(false); // Key not found
        }

        match &mut leaf_node {
            BlinkTreeNode::Leaf { keys, values, .. } => {
                // Find the key
                for (i, existing_key) in keys.iter().enumerate() {
                    if existing_key == key {
                        if let Some(pk_to_remove) = primary_key {
                            // Remove specific primary key
                            values[i].retain(|pk| pk != pk_to_remove);

                            // If no primary keys left, remove the entire entry
                            if values[i].is_empty() {
                                keys.remove(i);
                                values.remove(i);
                            }
                        } else {
                            // Remove entire key entry
                            keys.remove(i);
                            values.remove(i);
                        }

                        // Write the updated node
                        self.write_node(&leaf_node)?;

                        // Note: In a Blink tree, we typically don't merge nodes immediately
                        // This simplifies concurrent access and reduces lock contention
                        // Periodic maintenance can handle empty or underflowing nodes

                        return Ok(true);
                    }
                }

                Ok(false) // Key not found
            }
            _ => Err(BlinkTreeError::UnexpectedNodeType),
        }
    }

    /// Check if a node is underflowing and might need maintenance
    /// In Blink trees, we're more lenient about underflow to support concurrency
    pub fn is_underflowing(&self, node: &BlinkTreeNode) -> bool {
        let min_keys = if node.is_leaf() {
            (self.order + 1) / 2 // Ceiling division
        } else {
            (self.order - 1 + 1) / 2 // Ceiling division for internal nodes
        };

        node.get_keys().len() < min_keys && !node.get_keys().is_empty()
    }

    /// Maintenance operation: Clean up underflowing nodes
    /// This can be run periodically to maintain tree balance
    /// In a concurrent system, this would be run by a background thread
    pub fn maintenance_cleanup(&mut self) -> Result<(), BlinkTreeError> {
        if self.root_page_id == super::SENTINEL_PAGE_ID {
            return Ok(());
        }

        self.cleanup_node(self.root_page_id)?;
        Ok(())
    }

    /// Recursively cleanup a node and its children
    fn cleanup_node(&mut self, page_id: super::PageId) -> Result<(), BlinkTreeError> {
        let node = self.read_node(page_id)?;

        match &node {
            BlinkTreeNode::Internal { children, .. } => {
                // First, recursively cleanup children
                for child_page_id in children {
                    self.cleanup_node(*child_page_id)?;
                }

                // Then handle this internal node if it's underflowing
                if self.is_underflowing(&node) {
                    self.handle_underflowing_internal(page_id)?;
                }
            }
            BlinkTreeNode::Leaf { .. } => {
                // Handle underflowing leaf
                if self.is_underflowing(&node) {
                    self.handle_underflowing_leaf(page_id)?;
                }
            }
        }

        Ok(())
    }

    /// Handle an underflowing leaf node
    /// In Blink trees, we can be more aggressive about leaving small nodes
    /// since concurrent access is more important than perfect balance
    fn handle_underflowing_leaf(
        &mut self,
        _leaf_page_id: super::PageId,
    ) -> Result<(), BlinkTreeError> {
        // For now, we'll leave underflowing leaves as-is
        // In a full implementation, we could:
        // 1. Try to borrow from siblings
        // 2. Merge with siblings if borrowing fails
        // 3. Update parent nodes accordingly
        //
        // However, these operations are complex in concurrent environments
        // and Blink trees are designed to work well even with some underflow

        Ok(())
    }

    /// Handle an underflowing internal node
    fn handle_underflowing_internal(
        &mut self,
        _internal_page_id: super::PageId,
    ) -> Result<(), BlinkTreeError> {
        // Similar to leaves, we'll defer complex merging operations
        // The tree will still function correctly even with some underflow

        Ok(())
    }

    /// Delete all entries (clear the tree)
    /// This is useful for testing and maintenance
    pub fn clear(&mut self) -> Result<(), BlinkTreeError> {
        if self.root_page_id == super::SENTINEL_PAGE_ID {
            return Ok(());
        }

        // Recursively deallocate all pages
        self.deallocate_subtree(self.root_page_id)?;

        // Reset to empty tree state
        self.root_page_id = super::SENTINEL_PAGE_ID;
        self.page_manager.set_root_page_id(super::SENTINEL_PAGE_ID)?;

        // Create new empty root
        self.create_initial_root()?;

        Ok(())
    }

    /// Recursively deallocate a subtree
    fn deallocate_subtree(&mut self, page_id: super::PageId) -> Result<(), BlinkTreeError> {
        let node = self.read_node(page_id)?;

        match &node {
            BlinkTreeNode::Internal { children, .. } => {
                // First deallocate all children
                for child_page_id in children {
                    self.deallocate_subtree(*child_page_id)?;
                }
            }
            BlinkTreeNode::Leaf { .. } => {
                // Leaf nodes have no children to deallocate
            }
        }

        // Deallocate this page
        self.deallocate_page_id(page_id)?;
        Ok(())
    }

    /// Get statistics about the tree structure
    /// Useful for monitoring and debugging
    pub fn get_tree_stats(&self) -> Result<BlinkTreeStats, BlinkTreeError> {
        if self.root_page_id == super::SENTINEL_PAGE_ID {
            return Ok(BlinkTreeStats::empty());
        }

        let mut stats = BlinkTreeStats::new();
        self.collect_stats(self.root_page_id, 0, &mut stats)?;
        Ok(stats)
    }

    /// Recursively collect statistics about the tree
    fn collect_stats(
        &self,
        page_id: super::PageId,
        depth: usize,
        stats: &mut BlinkTreeStats,
    ) -> Result<(), BlinkTreeError> {
        let node = self.read_node(page_id)?;

        stats.total_nodes += 1;
        stats.max_depth = stats.max_depth.max(depth);

        match &node {
            BlinkTreeNode::Internal { keys, children, .. } => {
                stats.internal_nodes += 1;
                stats.total_keys += keys.len();

                // Recursively collect stats from children
                for child_page_id in children {
                    self.collect_stats(*child_page_id, depth + 1, stats)?;
                }
            }
            BlinkTreeNode::Leaf { keys, values, .. } => {
                stats.leaf_nodes += 1;
                stats.total_keys += keys.len();

                // Count total primary keys
                for value_list in values {
                    stats.total_values += value_list.len();
                }
            }
        }

        Ok(())
    }
}

/// Statistics about the Blink tree structure
#[derive(Debug, Clone)]
pub struct BlinkTreeStats {
    pub total_nodes: usize,
    pub internal_nodes: usize,
    pub leaf_nodes: usize,
    pub total_keys: usize,
    pub total_values: usize,
    pub max_depth: usize,
}

impl BlinkTreeStats {
    const fn new() -> Self {
        Self {
            total_nodes: 0,
            internal_nodes: 0,
            leaf_nodes: 0,
            total_keys: 0,
            total_values: 0,
            max_depth: 0,
        }
    }

    const fn empty() -> Self {
        Self::new()
    }

    #[must_use] pub fn average_keys_per_node(&self) -> f64 {
        if self.total_nodes > 0 {
            self.total_keys as f64 / self.total_nodes as f64
        } else {
            0.0
        }
    }

    #[must_use] pub fn average_values_per_key(&self) -> f64 {
        if self.total_keys > 0 {
            self.total_values as f64 / self.total_keys as f64
        } else {
            0.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn k(s: &str) -> KeyType {
        s.as_bytes().to_vec()
    }

    fn pk(s: &str) -> PrimaryKey {
        s.as_bytes().to_vec()
    }

    fn setup_tree(test_name: &str) -> (BlinkTreeIndex, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let tree_path = temp_dir.path().join(format!("{}.blink", test_name));
        let tree = BlinkTreeIndex::new("test_blink".to_string(), tree_path, 5).unwrap();
        (tree, temp_dir)
    }

    #[test]
    fn test_delete_from_empty_tree() {
        let (mut tree, _temp_dir) = setup_tree("test_delete_empty");

        // Deleting from empty tree should return false
        let result = tree.delete(&k("nonexistent"), None).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_delete_existing_key() {
        let (mut tree, _temp_dir) = setup_tree("test_delete_existing");

        // Insert a key
        assert!(tree.insert(k("apple"), pk("pk1")).is_ok());

        // Verify it exists
        assert!(tree.find_primary_keys(&k("apple")).unwrap().is_some());

        // Delete it
        let result = tree.delete(&k("apple"), None).unwrap();
        assert!(result);

        // Verify it's gone
        assert!(tree.find_primary_keys(&k("apple")).unwrap().is_none());

        // Verify tree structure
        assert!(tree.verify_structure().is_ok());
    }

    #[test]
    fn test_delete_specific_primary_key() {
        let (mut tree, _temp_dir) = setup_tree("test_delete_specific_pk");

        // Insert same key with multiple primary keys
        assert!(tree.insert(k("apple"), pk("pk1")).is_ok());
        assert!(tree.insert(k("apple"), pk("pk2")).is_ok());

        // Delete specific primary key
        let result = tree.delete(&k("apple"), Some(&pk("pk1"))).unwrap();
        assert!(result);

        // Verify only pk2 remains
        let found = tree.find_primary_keys(&k("apple")).unwrap();
        assert!(found.is_some());
        let values = found.unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], pk("pk2"));

        // Delete the last primary key
        let result = tree.delete(&k("apple"), Some(&pk("pk2"))).unwrap();
        assert!(result);

        // Verify key is completely gone
        assert!(tree.find_primary_keys(&k("apple")).unwrap().is_none());

        // Verify tree structure
        assert!(tree.verify_structure().is_ok());
    }

    #[test]
    fn test_delete_nonexistent_key() {
        let (mut tree, _temp_dir) = setup_tree("test_delete_nonexistent");

        // Insert a key
        assert!(tree.insert(k("apple"), pk("pk1")).is_ok());

        // Try to delete different key
        let result = tree.delete(&k("banana"), None).unwrap();
        assert!(!result);

        // Original key should still exist
        assert!(tree.find_primary_keys(&k("apple")).unwrap().is_some());

        // Verify tree structure
        assert!(tree.verify_structure().is_ok());
    }

    #[test]
    fn test_tree_stats() {
        let (mut tree, _temp_dir) = setup_tree("test_stats");

        // Empty tree stats
        let stats = tree.get_tree_stats().unwrap();
        assert_eq!(stats.total_nodes, 1); // Root leaf
        assert_eq!(stats.leaf_nodes, 1);
        assert_eq!(stats.internal_nodes, 0);
        assert_eq!(stats.total_keys, 0);

        // Insert some keys
        let keys = ["apple", "banana", "cherry"];
        for (i, key) in keys.iter().enumerate() {
            assert!(tree.insert(k(key), pk(&format!("pk{}", i))).is_ok());
        }

        let stats = tree.get_tree_stats().unwrap();
        assert_eq!(stats.total_keys, 3);
        assert_eq!(stats.total_values, 3);
        assert!(stats.average_keys_per_node() > 0.0);
    }

    #[test]
    fn test_clear_tree() {
        let (mut tree, _temp_dir) = setup_tree("test_clear");

        // Insert some keys
        let keys = ["apple", "banana", "cherry"];
        for (i, key) in keys.iter().enumerate() {
            assert!(tree.insert(k(key), pk(&format!("pk{}", i))).is_ok());
        }

        // Verify keys exist
        for key in &keys {
            assert!(tree.find_primary_keys(&k(key)).unwrap().is_some());
        }

        // Clear the tree
        assert!(tree.clear().is_ok());

        // Verify all keys are gone
        for key in &keys {
            assert!(tree.find_primary_keys(&k(key)).unwrap().is_none());
        }

        // Verify tree structure is still valid
        assert!(tree.verify_structure().is_ok());

        // Should be able to insert again
        assert!(tree.insert(k("new_key"), pk("new_pk")).is_ok());
        assert!(tree.find_primary_keys(&k("new_key")).unwrap().is_some());
    }
}
