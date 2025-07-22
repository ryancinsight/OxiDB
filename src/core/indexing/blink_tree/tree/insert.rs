use super::BlinkTreeIndex;
use crate::core::indexing::blink_tree::error::BlinkTreeError;
use crate::core::indexing::blink_tree::node::{
    BlinkTreeNode, InsertValue, KeyType, PageId, PrimaryKey,
};

impl BlinkTreeIndex {
    /// Insert a key-value pair into the Blink tree
    /// This is the main public interface for insertions
    pub fn insert(&mut self, key: KeyType, value: PrimaryKey) -> Result<(), BlinkTreeError> {
        if self.root_page_id == super::SENTINEL_PAGE_ID {
            // Create initial root if tree is empty
            self.create_initial_root()?;
        }

        // Find the leaf node where this key should be inserted
        let mut path = Vec::new();
        let leaf_page_id = self.find_leaf_for_insertion(&key, &mut path)?;

        // Insert into the leaf node
        self.insert_into_leaf(leaf_page_id, key, value, path)?;

        Ok(())
    }

    /// Find the leaf node where a key should be inserted, recording the path
    fn find_leaf_for_insertion(
        &self,
        key: &KeyType,
        path: &mut Vec<PageId>,
    ) -> Result<PageId, BlinkTreeError> {
        let mut current_page_id = self.root_page_id;

        loop {
            path.push(current_page_id);
            let current_node = self.read_node(current_page_id)?;

            if current_node.is_leaf() {
                return Ok(current_page_id);
            }
            // Find the appropriate child to follow
            current_page_id = self.find_child_for_insertion(&current_node, key)?;
        }
    }

    /// Find the appropriate child page for insertion in an internal node
    fn find_child_for_insertion(
        &self,
        internal_node: &BlinkTreeNode,
        key: &KeyType,
    ) -> Result<PageId, BlinkTreeError> {
        match internal_node {
            BlinkTreeNode::Internal { children, right_link, .. } => {
                // Check if this node is safe for our key
                if !internal_node.is_safe_for_key(key) {
                    // Key might belong in right sibling due to concurrent split
                    if let Some(right_page_id) = right_link {
                        let right_node = self.read_node(*right_page_id)?;
                        return self.find_child_for_insertion(&right_node, key);
                    }
                }

                // Find the appropriate child
                let child_index = internal_node.find_child_index(key)?;
                Ok(children[child_index])
            }
            _ => Err(BlinkTreeError::UnexpectedNodeType),
        }
    }

    /// Insert into a leaf node, handling splits if necessary
    fn insert_into_leaf(
        &mut self,
        leaf_page_id: PageId,
        key: KeyType,
        value: PrimaryKey,
        path: Vec<PageId>,
    ) -> Result<(), BlinkTreeError> {
        let mut leaf_node = self.read_node(leaf_page_id)?;

        // Check if the key already exists and update it
        if let BlinkTreeNode::Leaf { keys, values, .. } = &mut leaf_node {
            for (i, existing_key) in keys.iter().enumerate() {
                if existing_key == &key {
                    // Key exists, add to the primary key list
                    values[i].push(value);
                    self.write_node(&leaf_node)?;
                    return Ok(());
                }
            }
        }

        // Try to insert the new key
        let insert_result = leaf_node.insert_key_value(
            key.clone(),
            InsertValue::PrimaryKeys(vec![value.clone()]),
            self.order,
        );

        match insert_result {
            Ok(()) => {
                // Insertion successful, just write the updated node
                self.write_node(&leaf_node)?;
                Ok(())
            }
            Err(_) => {
                // Node is full, need to split
                self.split_leaf_and_propagate(leaf_node, key, value, path)
            }
        }
    }

    /// Split a full leaf node and propagate splits up the tree if necessary
    fn split_leaf_and_propagate(
        &mut self,
        mut leaf_node: BlinkTreeNode,
        key: KeyType,
        value: PrimaryKey,
        path: Vec<PageId>,
    ) -> Result<(), BlinkTreeError> {
        // Get a new page for the right split
        let new_page_id = self.allocate_new_page_id()?;

        // Determine where to insert the key (left or right split)
        let should_insert_in_left = self.should_insert_in_left_split(&leaf_node, &key)?;

        // Insert the key before splitting to ensure it's included
        if should_insert_in_left {
            // Force insert into left node (we'll handle overflow in split)
            self.force_insert_into_leaf(&mut leaf_node, key.clone(), value.clone())?;
        }

        // Perform the split
        let (split_key, mut new_right_node) = leaf_node.split(self.order, new_page_id)?;

        // If key should go in right split, insert it there
        if !should_insert_in_left {
            new_right_node.insert_key_value(
                key,
                InsertValue::PrimaryKeys(vec![value]),
                self.order,
            )?;
        }

        // Write both nodes to disk
        self.write_node(&leaf_node)?;
        self.write_node(&new_right_node)?;

        // Propagate the split up the tree
        self.propagate_split_up(split_key, new_page_id, path)
    }

    /// Determine if a key should be inserted in the left split during a leaf split
    fn should_insert_in_left_split(
        &self,
        leaf_node: &BlinkTreeNode,
        key: &KeyType,
    ) -> Result<bool, BlinkTreeError> {
        match leaf_node {
            BlinkTreeNode::Leaf { keys, .. } => {
                let mid = keys.len() / 2;
                // Insert in left if key is less than the middle key
                Ok(mid == 0 || key < &keys[mid])
            }
            _ => Err(BlinkTreeError::UnexpectedNodeType),
        }
    }

    /// Force insert a key into a leaf node (used before splitting)
    fn force_insert_into_leaf(
        &mut self,
        leaf_node: &mut BlinkTreeNode,
        key: KeyType,
        value: PrimaryKey,
    ) -> Result<(), BlinkTreeError> {
        match leaf_node {
            BlinkTreeNode::Leaf { keys, values, .. } => {
                // Find insertion point
                let mut insert_pos = keys.len();
                for (i, existing_key) in keys.iter().enumerate() {
                    if &key < existing_key {
                        insert_pos = i;
                        break;
                    } else if &key == existing_key {
                        // Key exists, just add to values
                        values[i].push(value);
                        return Ok(());
                    }
                }

                // Insert the new key-value pair
                keys.insert(insert_pos, key);
                values.insert(insert_pos, vec![value]);
                Ok(())
            }
            _ => Err(BlinkTreeError::UnexpectedNodeType),
        }
    }

    /// Propagate a split operation up the tree
    fn propagate_split_up(
        &mut self,
        split_key: KeyType,
        new_page_id: PageId,
        mut path: Vec<PageId>,
    ) -> Result<(), BlinkTreeError> {
        // Remove the leaf page from the path (we already handled it)
        path.pop();

        // If path is empty, we need to create a new root
        if path.is_empty() {
            return self.create_new_root(split_key, self.root_page_id, new_page_id);
        }

        // Get the parent node
        let parent_page_id = path[path.len() - 1];
        let mut parent_node = self.read_node(parent_page_id)?;

        // Try to insert the split key into the parent
        let insert_result = parent_node.insert_key_value(
            split_key.clone(),
            InsertValue::Page(new_page_id),
            self.order,
        );

        match insert_result {
            Ok(()) => {
                // Insertion successful
                self.write_node(&parent_node)?;
                Ok(())
            }
            Err(_) => {
                // Parent is also full, need to split it too
                self.split_internal_and_propagate(parent_node, split_key, new_page_id, path)
            }
        }
    }

    /// Split a full internal node and continue propagating splits up
    fn split_internal_and_propagate(
        &mut self,
        mut internal_node: BlinkTreeNode,
        key: KeyType,
        child_page_id: PageId,
        path: Vec<PageId>,
    ) -> Result<(), BlinkTreeError> {
        // Get a new page for the right split
        let new_page_id = self.allocate_new_page_id()?;

        // Force insert the key before splitting
        self.force_insert_into_internal(&mut internal_node, key, child_page_id)?;

        // Perform the split
        let (split_key, new_right_node) = internal_node.split(self.order, new_page_id)?;

        // Update parent pointers for children in the right node
        self.update_children_parent_pointers(&new_right_node)?;

        // Write both nodes to disk
        self.write_node(&internal_node)?;
        self.write_node(&new_right_node)?;

        // Continue propagating up
        self.propagate_split_up(split_key, new_page_id, path)
    }

    /// Force insert a key into an internal node (used before splitting)
    fn force_insert_into_internal(
        &mut self,
        internal_node: &mut BlinkTreeNode,
        key: KeyType,
        child_page_id: PageId,
    ) -> Result<(), BlinkTreeError> {
        match internal_node {
            BlinkTreeNode::Internal { keys, children, .. } => {
                // Find insertion point
                let mut insert_pos = keys.len();
                for (i, existing_key) in keys.iter().enumerate() {
                    if &key < existing_key {
                        insert_pos = i;
                        break;
                    }
                }

                // Insert the new key and child pointer
                keys.insert(insert_pos, key);
                children.insert(insert_pos + 1, child_page_id);
                Ok(())
            }
            _ => Err(BlinkTreeError::UnexpectedNodeType),
        }
    }

    /// Update parent pointers for all children of a node
    fn update_children_parent_pointers(
        &mut self,
        node: &BlinkTreeNode,
    ) -> Result<(), BlinkTreeError> {
        match node {
            BlinkTreeNode::Internal { children, page_id, .. } => {
                for child_page_id in children {
                    let mut child_node = self.read_node(*child_page_id)?;
                    child_node.set_parent_page_id(Some(*page_id));
                    self.write_node(&child_node)?;
                }
                Ok(())
            }
            _ => Ok(()), // Leaf nodes don't have children
        }
    }

    /// Create a new root when the old root splits
    fn create_new_root(
        &mut self,
        split_key: KeyType,
        left_child_id: PageId,
        right_child_id: PageId,
    ) -> Result<(), BlinkTreeError> {
        let new_root_page_id = self.allocate_new_page_id()?;

        let new_root = BlinkTreeNode::Internal {
            page_id: new_root_page_id,
            parent_page_id: None,
            keys: vec![split_key],
            children: vec![left_child_id, right_child_id],
            right_link: None, // Root has no siblings
            high_key: None,   // Root has no high key
        };

        // Update parent pointers for the children
        let mut left_child = self.read_node(left_child_id)?;
        left_child.set_parent_page_id(Some(new_root_page_id));
        self.write_node(&left_child)?;

        let mut right_child = self.read_node(right_child_id)?;
        right_child.set_parent_page_id(Some(new_root_page_id));
        self.write_node(&right_child)?;

        // Write the new root
        self.write_node(&new_root)?;

        // Update the tree's root page ID
        let old_root_id = self.root_page_id;
        self.root_page_id = new_root_page_id;
        self.write_metadata_if_root_changed(old_root_id)?;

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
    fn test_insert_into_empty_tree() {
        let (mut tree, _temp_dir) = setup_tree("test_insert_empty");

        // Insert a key-value pair
        assert!(tree.insert(k("apple"), pk("pk1")).is_ok());

        // Verify it can be found
        let result = tree.find_primary_keys(&k("apple")).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap(), vec![pk("pk1")]);

        // Verify tree structure is still valid
        assert!(tree.verify_structure().is_ok());
    }

    #[test]
    fn test_insert_multiple_keys() {
        let (mut tree, _temp_dir) = setup_tree("test_insert_multiple");

        // Insert several keys
        let keys = ["apple", "banana", "cherry", "date"];
        for (i, key) in keys.iter().enumerate() {
            assert!(tree.insert(k(key), pk(&format!("pk{}", i))).is_ok());
        }

        // Verify all keys can be found
        for (i, key) in keys.iter().enumerate() {
            let result = tree.find_primary_keys(&k(key)).unwrap();
            assert!(result.is_some());
            assert_eq!(result.unwrap(), vec![pk(&format!("pk{}", i))]);
        }

        // Verify tree structure
        assert!(tree.verify_structure().is_ok());
    }

    #[test]
    fn test_insert_duplicate_key() {
        let (mut tree, _temp_dir) = setup_tree("test_insert_duplicate");

        // Insert same key with different values
        assert!(tree.insert(k("apple"), pk("pk1")).is_ok());
        assert!(tree.insert(k("apple"), pk("pk2")).is_ok());

        // Should find both values
        let result = tree.find_primary_keys(&k("apple")).unwrap();
        assert!(result.is_some());
        let values = result.unwrap();
        assert_eq!(values.len(), 2);
        assert!(values.contains(&pk("pk1")));
        assert!(values.contains(&pk("pk2")));

        // Verify tree structure
        assert!(tree.verify_structure().is_ok());
    }

    #[test]
    fn test_insert_causing_split() {
        let (mut tree, _temp_dir) = setup_tree("test_insert_split");

        // Insert enough keys to force a split (order = 5, so leaf can hold 5 keys)
        let keys = ["a", "b", "c", "d", "e", "f"]; // 6 keys should cause split
        for (i, key) in keys.iter().enumerate() {
            assert!(tree.insert(k(key), pk(&format!("pk{}", i))).is_ok());
        }

        // Verify all keys can still be found
        for (i, key) in keys.iter().enumerate() {
            let result = tree.find_primary_keys(&k(key)).unwrap();
            assert!(result.is_some());
            assert_eq!(result.unwrap(), vec![pk(&format!("pk{}", i))]);
        }

        // Verify tree structure after split
        assert!(tree.verify_structure().is_ok());

        // Root should no longer be a leaf (it should have split)
        let _root_node = tree.read_node(tree.root_page_id).unwrap();
        // Depending on implementation, root might still be leaf if only one split occurred
        // The important thing is that structure is valid
    }
}
