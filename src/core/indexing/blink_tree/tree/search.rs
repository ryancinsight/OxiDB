use super::BlinkTreeIndex;
use crate::core::indexing::blink_tree::error::BlinkTreeError;
use crate::core::indexing::blink_tree::node::{BlinkTreeNode, KeyType, PrimaryKey};

impl BlinkTreeIndex {
    /// Range scan operation - find all keys between start and end (inclusive)
    /// This showcases the power of Blink tree's concurrent traversal
    pub fn range_scan(
        &self,
        start_key: &KeyType,
        end_key: &KeyType,
    ) -> Result<Vec<(KeyType, Vec<PrimaryKey>)>, BlinkTreeError> {
        if self.root_page_id == super::SENTINEL_PAGE_ID {
            return Ok(Vec::new());
        }

        let mut results = Vec::new();

        // Find the leftmost leaf that might contain start_key
        let mut current_page_id = self.find_leftmost_leaf_for_key(start_key)?;

        while current_page_id != super::SENTINEL_PAGE_ID {
            let leaf_node = self.read_node(current_page_id)?;

            match &leaf_node {
                BlinkTreeNode::Leaf { keys, values, right_link, .. } => {
                    // Scan through keys in this leaf
                    for (i, key) in keys.iter().enumerate() {
                        if key >= start_key && key <= end_key {
                            results.push((key.clone(), values[i].clone()));
                        } else if key > end_key {
                            // We've gone past the end range
                            return Ok(results);
                        }
                    }

                    // Check if we need to continue to right sibling
                    if let Some(right_page_id) = right_link {
                        // Check if the right sibling might contain keys in our range
                        let right_node = self.read_node(*right_page_id)?;
                        if let BlinkTreeNode::Leaf { keys, .. } = &right_node {
                            if !keys.is_empty() && keys[0] <= *end_key {
                                current_page_id = *right_page_id;
                                continue;
                            }
                        }
                    }
                    break;
                }
                _ => return Err(BlinkTreeError::UnexpectedNodeType),
            }
        }

        Ok(results)
    }

    /// Find the leftmost leaf that might contain the given key
    fn find_leftmost_leaf_for_key(&self, key: &KeyType) -> Result<super::PageId, BlinkTreeError> {
        let mut current_page_id = self.root_page_id;

        loop {
            let current_node = self.read_node(current_page_id)?;

            if current_node.is_leaf() {
                return Ok(current_page_id);
            } else {
                // Find the leftmost child that might contain our key
                current_page_id = self.find_leftmost_child_for_key(&current_node, key)?;
            }
        }
    }

    /// Find the leftmost child in an internal node that might contain the key
    fn find_leftmost_child_for_key(
        &self,
        internal_node: &BlinkTreeNode,
        search_key: &KeyType,
    ) -> Result<super::PageId, BlinkTreeError> {
        match internal_node {
            BlinkTreeNode::Internal { keys, children, right_link, .. } => {
                // Check if this node is safe for our search key
                if !internal_node.is_safe_for_key(search_key) {
                    // Key might be in right sibling due to concurrent split
                    if let Some(right_page_id) = right_link {
                        let right_node = self.read_node(*right_page_id)?;
                        return self.find_leftmost_child_for_key(&right_node, search_key);
                    }
                }

                // Find the appropriate child
                for (i, key) in keys.iter().enumerate() {
                    if search_key < key {
                        return Ok(children[i]);
                    }
                }

                // Key is >= all keys in this node, go to rightmost child
                Ok(children[children.len() - 1])
            }
            _ => Err(BlinkTreeError::UnexpectedNodeType),
        }
    }

    /// Verify the structural integrity of the Blink tree
    /// This is useful for testing and debugging concurrent operations
    pub fn verify_structure(&self) -> Result<(), BlinkTreeError> {
        if self.root_page_id == super::SENTINEL_PAGE_ID {
            return Ok(()); // Empty tree is valid
        }

        self.verify_node_structure(self.root_page_id, None)?;
        Ok(())
    }

    /// Recursively verify a node and its subtree
    fn verify_node_structure(
        &self,
        page_id: super::PageId,
        expected_parent: Option<super::PageId>,
    ) -> Result<(), BlinkTreeError> {
        let node = self.read_node(page_id)?;

        // Verify parent relationship
        if node.get_parent_page_id() != expected_parent {
            return Err(BlinkTreeError::TreeLogicError(format!(
                "Node {} has incorrect parent: expected {:?}, got {:?}",
                page_id,
                expected_parent,
                node.get_parent_page_id()
            )));
        }

        match &node {
            BlinkTreeNode::Internal { keys, children, right_link, high_key, .. } => {
                // Verify key count vs children count
                if keys.len() + 1 != children.len() {
                    return Err(BlinkTreeError::TreeLogicError(format!(
                        "Internal node {} has {} keys but {} children",
                        page_id,
                        keys.len(),
                        children.len()
                    )));
                }

                // Verify keys are sorted
                for i in 1..keys.len() {
                    if keys[i - 1] >= keys[i] {
                        return Err(BlinkTreeError::TreeLogicError(format!(
                            "Internal node {} has unsorted keys",
                            page_id
                        )));
                    }
                }

                // Verify high key constraint
                if let Some(hkey) = high_key {
                    if let Some(last_key) = keys.last() {
                        if last_key > hkey {
                            return Err(BlinkTreeError::TreeLogicError(format!(
                                "Internal node {} violates high key constraint",
                                page_id
                            )));
                        }
                    }
                }

                // Recursively verify children
                for child_page_id in children {
                    self.verify_node_structure(*child_page_id, Some(page_id))?;
                }

                // Verify right link if present
                if let Some(right_page_id) = right_link {
                    let right_node = self.read_node(*right_page_id)?;
                    if right_node.get_parent_page_id() != expected_parent {
                        return Err(BlinkTreeError::TreeLogicError(format!(
                            "Right sibling {} has incorrect parent",
                            right_page_id
                        )));
                    }
                }
            }
            BlinkTreeNode::Leaf { keys, values, right_link, high_key, .. } => {
                // Verify key count vs value count
                if keys.len() != values.len() {
                    return Err(BlinkTreeError::TreeLogicError(format!(
                        "Leaf node {} has {} keys but {} values",
                        page_id,
                        keys.len(),
                        values.len()
                    )));
                }

                // Verify keys are sorted
                for i in 1..keys.len() {
                    if keys[i - 1] >= keys[i] {
                        return Err(BlinkTreeError::TreeLogicError(format!(
                            "Leaf node {} has unsorted keys",
                            page_id
                        )));
                    }
                }

                // Verify high key constraint
                if let Some(hkey) = high_key {
                    if let Some(last_key) = keys.last() {
                        if last_key > hkey {
                            return Err(BlinkTreeError::TreeLogicError(format!(
                                "Leaf node {} violates high key constraint",
                                page_id
                            )));
                        }
                    }
                }

                // Verify right link if present
                if let Some(right_page_id) = right_link {
                    let right_node = self.read_node(*right_page_id)?;
                    if right_node.get_parent_page_id() != expected_parent {
                        return Err(BlinkTreeError::TreeLogicError(format!(
                            "Right sibling {} has incorrect parent",
                            right_page_id
                        )));
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn k(s: &str) -> KeyType {
        s.as_bytes().to_vec()
    }

    fn setup_tree(test_name: &str) -> (BlinkTreeIndex, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let tree_path = temp_dir.path().join(format!("{}.blink", test_name));
        let tree = BlinkTreeIndex::new("test_blink".to_string(), tree_path, 5).unwrap();
        (tree, temp_dir)
    }

    #[test]
    fn test_range_scan_empty_tree() {
        let (tree, _temp_dir) = setup_tree("test_range_scan_empty");

        let results = tree.range_scan(&k("a"), &k("z")).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_verify_structure_empty_tree() {
        let (tree, _temp_dir) = setup_tree("test_verify_empty");

        // Empty tree should be valid
        assert!(tree.verify_structure().is_ok());
    }

    #[test]
    fn test_verify_structure_single_node() {
        let (tree, _temp_dir) = setup_tree("test_verify_single");

        // Single root leaf should be valid
        assert!(tree.verify_structure().is_ok());

        // Verify the root node exists and is a leaf
        let root_node = tree.read_node(tree.root_page_id).unwrap();
        assert!(root_node.is_leaf());
        assert_eq!(root_node.get_parent_page_id(), None);
    }
}
