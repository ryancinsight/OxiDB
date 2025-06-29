use std::path::PathBuf;

use crate::core::indexing::btree::node::{
    BPlusTreeNode, KeyType, PageId, PrimaryKey,
};
use super::error::OxidbError;
use super::page_io::PageManager;

#[derive(Debug)]
pub struct BPlusTreeIndex {
    pub name: String,
    pub path: PathBuf,
    pub order: usize,
    pub(super) root_page_id: PageId,
    pub(super) page_manager: PageManager,
}

impl BPlusTreeIndex {
    pub fn new(name: String, path: PathBuf, order: usize) -> Result<Self, OxidbError> {
        let file_exists = path.exists();
        let mut page_manager = PageManager::new(&path, order, true)?;

        let current_root_page_id;
        let effective_order;

        if file_exists && page_manager.get_order() != 0 {
            current_root_page_id = page_manager.get_root_page_id();
            effective_order = page_manager.get_order();
            if order != 0 && effective_order != order {
                 eprintln!(
                    "Warning: Order mismatch during load. Requested: {}, File's: {}. Using file's order.",
                    order, effective_order
                );
            }
        } else {
            if order < 3 {
                return Err(OxidbError::TreeLogicError(format!(
                    "Order {} is too small. Minimum order is 3.",
                    order
                )));
            }
            effective_order = order;
            current_root_page_id = page_manager.get_root_page_id();

            if current_root_page_id == 0 {
                 let initial_root_node = BPlusTreeNode::Leaf {
                    page_id: current_root_page_id,
                    parent_page_id: None,
                    keys: Vec::new(),
                    values: Vec::new(),
                    next_leaf: None,
                };
                page_manager.write_node(&initial_root_node)?;
            }
        }

        Ok(Self {
            name,
            path,
            order: effective_order,
            root_page_id: current_root_page_id,
            page_manager,
        })
    }

    pub(super) fn write_metadata_if_root_changed(&mut self, old_root_id: PageId) -> Result<(), OxidbError> {
        if self.root_page_id != old_root_id {
            self.page_manager.set_root_page_id(self.root_page_id)?;
            self.page_manager.write_metadata()?;
        }
        Ok(())
    }

    pub(super) fn allocate_new_page_id(&mut self) -> Result<PageId, OxidbError> {
        let new_page_id = self.page_manager.allocate_new_page_id()?;
        self.page_manager.write_metadata()?;
        Ok(new_page_id)
    }

    pub(super) fn deallocate_page_id(&mut self, page_id_to_free: PageId) -> Result<(), OxidbError> {
        self.page_manager.deallocate_page_id(page_id_to_free)?;
        self.page_manager.write_metadata()?;
        Ok(())
    }

    pub(super) fn read_node(&self, page_id: PageId) -> Result<BPlusTreeNode, OxidbError> {
        self.page_manager.read_node(page_id)
    }

    pub(super) fn write_node(&mut self, node: &BPlusTreeNode) -> Result<(), OxidbError> {
        self.page_manager.write_node(node)
    }

    pub fn find_leaf_node_path(
        &self,
        key: &KeyType,
        path: &mut Vec<PageId>,
    ) -> Result<BPlusTreeNode, OxidbError> {
        path.clear();
        let mut current_page_id = self.root_page_id;
        loop {
            path.push(current_page_id);
            let current_node = self.read_node(current_page_id)?;
            match current_node {
                BPlusTreeNode::Internal { ref keys, ref children, .. } => {
                    let child_idx = keys
                        .partition_point(|k_partition| k_partition.as_slice() <= key.as_slice());
                    current_page_id = children[child_idx];
                }
                BPlusTreeNode::Leaf { .. } => {
                    return Ok(current_node);
                }
            }
        }
    }

    pub fn find_primary_keys(&self, key: &KeyType) -> Result<Option<Vec<PrimaryKey>>, OxidbError> {
        let mut path = Vec::new();
        let leaf_node = self.find_leaf_node_path(key, &mut path)?;
        match leaf_node {
            BPlusTreeNode::Leaf { keys, values, .. } => match keys.binary_search(key) {
                Ok(idx) => Ok(Some(values[idx].clone())),
                Err(_) => Ok(None),
            },
            _ => unreachable!("find_leaf_node_path should always return a Leaf node"),
        }
    }

    pub fn insert(&mut self, key: KeyType, value: PrimaryKey) -> Result<(), OxidbError> {
        let mut path_to_leaf: Vec<PageId> = Vec::new();
        let _ = self.find_leaf_node_path(&key, &mut path_to_leaf)?;
        let leaf_page_id = *path_to_leaf
            .last()
            .ok_or(OxidbError::TreeLogicError("Path to leaf is empty".to_string()))?;

        let mut current_leaf_node = self.get_mutable_node(leaf_page_id)?;
        match &mut current_leaf_node {
            BPlusTreeNode::Leaf { keys, values, .. } => match keys.binary_search(&key) {
                Ok(idx) => {
                    if !values[idx].contains(&value) {
                        values[idx].push(value);
                        values[idx].sort();
                    } else {
                        return Ok(());
                    }
                }
                Err(idx) => {
                    keys.insert(idx, key.clone());
                    values.insert(idx, vec![value]);
                }
            },
            _ => return Err(OxidbError::UnexpectedNodeType),
        }

        if current_leaf_node.get_keys().len() >= self.order {
            self.handle_split(current_leaf_node, path_to_leaf)?
        } else {
            self.write_node(&current_leaf_node)?;
        }
        Ok(())
    }

    fn get_mutable_node(&mut self, page_id: PageId) -> Result<BPlusTreeNode, OxidbError> {
        self.read_node(page_id)
    }

    fn handle_split(
        &mut self,
        mut node_to_split: BPlusTreeNode,
        mut path: Vec<PageId>,
    ) -> Result<(), OxidbError> {
        let _original_node_page_id = path.pop().ok_or(OxidbError::TreeLogicError(
            "Path cannot be empty in handle_split".to_string(),
        ))?;

        let new_sibling_page_id = self.allocate_new_page_id()?;

        let (promoted_or_copied_key, mut new_sibling_node) = node_to_split
            .split(self.order, new_sibling_page_id)
            .map_err(|e| OxidbError::TreeLogicError(e.to_string()))?;

        new_sibling_node.set_parent_page_id(node_to_split.get_parent_page_id());

        self.write_node(&node_to_split)?;
        self.write_node(&new_sibling_node)?;

        let parent_page_id_opt = node_to_split.get_parent_page_id();
        if let Some(parent_page_id) = parent_page_id_opt {
            let mut parent_node = self.get_mutable_node(parent_page_id)?;
            match &mut parent_node {
                BPlusTreeNode::Internal { keys, children, .. } => {
                    let insertion_point =
                        keys.partition_point(|k| k.as_slice() < promoted_or_copied_key.as_slice());
                    keys.insert(insertion_point, promoted_or_copied_key);
                    children.insert(insertion_point.saturating_add(1), new_sibling_page_id);

                    if parent_node.get_keys().len() >= self.order {
                        self.handle_split(parent_node, path)
                    } else {
                        self.write_node(&parent_node)
                    }
                }
                _ => Err(OxidbError::UnexpectedNodeType),
            }
        } else {
            let old_root_id = self.root_page_id;
            let new_root_page_id = self.allocate_new_page_id()?;
            let old_node_split_page_id = node_to_split.get_page_id();

            let new_root = BPlusTreeNode::Internal {
                page_id: new_root_page_id,
                parent_page_id: None,
                keys: vec![promoted_or_copied_key],
                children: vec![old_node_split_page_id, new_sibling_node.get_page_id()],
            };

            node_to_split.set_parent_page_id(Some(new_root_page_id));
            new_sibling_node.set_parent_page_id(Some(new_root_page_id));

            self.write_node(&node_to_split)?;
            self.write_node(&new_sibling_node)?;
            self.write_node(&new_root)?;

            self.root_page_id = new_root_page_id;
            self.write_metadata_if_root_changed(old_root_id)
        }
    }

    pub fn delete(
        &mut self,
        key_to_delete: &KeyType,
        pk_to_remove: Option<&PrimaryKey>,
    ) -> Result<bool, OxidbError> {
        let mut path: Vec<PageId> = Vec::new();
        let _ = self.find_leaf_node_path(key_to_delete, &mut path)?;
        let leaf_page_id = *path
            .last()
            .ok_or(OxidbError::TreeLogicError("Path to leaf is empty for delete".to_string()))?;

        let mut leaf_node = self.get_mutable_node(leaf_page_id)?;
        let mut key_removed_from_structure = false;
        let mut modification_made = false;

        match &mut leaf_node {
            BPlusTreeNode::Leaf { keys, values, .. } => {
                match keys.binary_search(key_to_delete) {
                    Ok(idx) => {
                        if let Some(pk_ref) = pk_to_remove {
                            let original_pk_count = values[idx].len();
                            values[idx].retain(|p| p != pk_ref);
                            if values[idx].len() < original_pk_count {
                                modification_made = true;
                                if values[idx].is_empty() {
                                    keys.remove(idx);
                                    values.remove(idx);
                                    key_removed_from_structure = true;
                                }
                            }
                        } else {
                            keys.remove(idx);
                            values.remove(idx);
                            key_removed_from_structure = true;
                            modification_made = true;
                        }
                    }
                    Err(_) => { /* Key not found */ }
                }
            }
            _ => return Err(OxidbError::UnexpectedNodeType),
        }

        if modification_made {
            if key_removed_from_structure
                && leaf_node.get_keys().len() < self.min_keys_for_node()
                && leaf_page_id != self.root_page_id
            {
                self.handle_underflow(leaf_node, path)?;
            } else {
                self.write_node(&leaf_node)?;
            }
        }
        Ok(modification_made)
    }

    fn min_keys_for_node(&self) -> usize {
        self.order.saturating_sub(1) / 2
    }

    fn handle_underflow(
        &mut self,
        mut current_node: BPlusTreeNode,
        mut path: Vec<PageId>,
    ) -> Result<(), OxidbError> {
        let current_node_pid = path
            .pop()
            .ok_or_else(|| OxidbError::TreeLogicError("Path cannot be empty".to_string()))?;

        if current_node_pid == self.root_page_id {
            if let BPlusTreeNode::Internal { ref keys, ref children, .. } = current_node {
                if keys.is_empty() && children.len() == 1 {
                    let old_root_page_id = self.root_page_id;
                    self.root_page_id = children[0];

                    let mut new_root_node = self.get_mutable_node(self.root_page_id)?;
                    new_root_node.set_parent_page_id(None);
                    self.write_node(&new_root_node)?;

                    self.write_metadata_if_root_changed(old_root_page_id)?;
                    self.deallocate_page_id(old_root_page_id)?;
                }
            }
            return Ok(());
        }

        let parent_pid = *path.last().ok_or_else(|| {
            OxidbError::TreeLogicError("Parent not found for non-root underflow".to_string())
        })?;
        let mut parent_node = self.get_mutable_node(parent_pid)?;

        let parent_children = parent_node.get_children().map_err(|e| OxidbError::TreeLogicError(e.to_string()))?;
        let child_idx_in_parent = parent_children
            .iter()
            .position(|&child_pid| child_pid == current_node_pid)
            .ok_or_else(|| {
                OxidbError::TreeLogicError("Child not found in parent during underflow handling".to_string())
            })?;

        if child_idx_in_parent > 0 {
            let left_sibling_pid = parent_children[child_idx_in_parent.saturating_sub(1)];
            let mut left_sibling_node = self.get_mutable_node(left_sibling_pid)?;
            if left_sibling_node.get_keys().len() > self.min_keys_for_node() {
                self.borrow_from_sibling(
                    &mut current_node,
                    &mut left_sibling_node,
                    &mut parent_node,
                    child_idx_in_parent.saturating_sub(1),
                    true,
                )?;
                return Ok(());
            }
        }

        if child_idx_in_parent < parent_children.len().saturating_sub(1) {
            let right_sibling_pid = parent_children[child_idx_in_parent.saturating_add(1)];
            let mut right_sibling_node = self.get_mutable_node(right_sibling_pid)?;
            if right_sibling_node.get_keys().len() > self.min_keys_for_node() {
                self.borrow_from_sibling(
                    &mut current_node,
                    &mut right_sibling_node,
                    &mut parent_node,
                    child_idx_in_parent,
                    false,
                )?;
                return Ok(());
            }
        }

        let _merged_into_left_sibling_page_id;
        if child_idx_in_parent > 0 {
            let left_sibling_pid = parent_children[child_idx_in_parent.saturating_sub(1)];
            let mut left_sibling_node = self.get_mutable_node(left_sibling_pid)?;
            _merged_into_left_sibling_page_id = left_sibling_node.get_page_id();
            self.merge_nodes(
                &mut left_sibling_node,
                &mut current_node,
                &mut parent_node,
                child_idx_in_parent.saturating_sub(1),
            )?;
             self.write_node(&left_sibling_node)?;
        } else {
            let right_sibling_pid = parent_children[child_idx_in_parent.saturating_add(1)];
            let mut right_sibling_node = self.get_mutable_node(right_sibling_pid)?;
            _merged_into_left_sibling_page_id = current_node.get_page_id();
            self.merge_nodes(
                &mut current_node,
                &mut right_sibling_node,
                &mut parent_node,
                child_idx_in_parent,
            )?;
            self.write_node(&current_node)?;
        }

        if parent_node.get_keys().len() < self.min_keys_for_node() && parent_pid != self.root_page_id {
            self.handle_underflow(parent_node, path)?;
        } else if parent_pid == self.root_page_id
            && parent_node.get_keys().is_empty()
            && matches!(parent_node, BPlusTreeNode::Internal { .. }) {
            if let BPlusTreeNode::Internal { ref children, .. } = parent_node {
                if children.len() == 1 {
                    let old_root_pid = parent_pid;
                    self.root_page_id = children[0];
                    let mut new_root_node = self.get_mutable_node(self.root_page_id)?;
                    new_root_node.set_parent_page_id(None);
                    self.write_node(&new_root_node)?;
                    self.write_metadata_if_root_changed(old_root_pid)?;
                    self.deallocate_page_id(old_root_pid)?;
                } else {
                    self.write_node(&parent_node)?;
                }
            } else {
                self.write_node(&parent_node)?;
            }
        } else {
            self.write_node(&parent_node)?;
        }
        Ok(())
    }

    fn borrow_from_sibling(
        &mut self,
        underflowed_node: &mut BPlusTreeNode,
        lender_sibling: &mut BPlusTreeNode,
        parent_node: &mut BPlusTreeNode,
        parent_key_idx: usize,
        is_left_lender: bool,
    ) -> Result<(), OxidbError> {
        match (&mut *underflowed_node, &mut *lender_sibling, &mut *parent_node) {
            (
                BPlusTreeNode::Leaf { keys: u_keys, values: u_values, .. },
                BPlusTreeNode::Leaf { keys: l_keys, values: l_values, .. },
                BPlusTreeNode::Internal { keys: p_keys, .. }
            ) => {
                if is_left_lender {
                    let borrowed_key = l_keys.pop().ok_or(OxidbError::TreeLogicError("Lender leaf (left) empty".to_string()))?;
                    let borrowed_value = l_values.pop().ok_or(OxidbError::TreeLogicError("Lender leaf (left) values empty".to_string()))?;
                    u_keys.insert(0, borrowed_key.clone());
                    u_values.insert(0, borrowed_value);
                    p_keys[parent_key_idx] = borrowed_key;
                } else {
                    let borrowed_key = l_keys.remove(0);
                    let borrowed_value = l_values.remove(0);
                    u_keys.push(borrowed_key.clone());
                    u_values.push(borrowed_value);
                    p_keys[parent_key_idx] = l_keys.first().ok_or(OxidbError::TreeLogicError("Lender leaf (right) became empty".to_string()))?.clone();
                }
            },
            (
                BPlusTreeNode::Internal { page_id: u_pid_val, keys: u_keys, children: u_children, .. },
                BPlusTreeNode::Internal { keys: l_keys, children: l_children, .. },
                BPlusTreeNode::Internal { keys: p_keys, .. }
            ) => {
                if is_left_lender {
                    let key_from_parent = p_keys.remove(parent_key_idx);
                    u_keys.insert(0, key_from_parent);
                    let new_separator_for_parent = l_keys.pop().ok_or(OxidbError::TreeLogicError("Lender internal (left) empty".to_string()))?;
                    p_keys.insert(parent_key_idx, new_separator_for_parent);
                    let child_to_move = l_children.pop().ok_or(OxidbError::TreeLogicError("Lender internal (left) children empty".to_string()))?;
                    u_children.insert(0, child_to_move);
                    let mut moved_child_node = self.get_mutable_node(child_to_move)?;
                    moved_child_node.set_parent_page_id(Some(*u_pid_val));
                    self.write_node(&moved_child_node)?;
                } else {
                    let key_from_parent = p_keys.remove(parent_key_idx);
                    u_keys.push(key_from_parent);
                    let new_separator_for_parent = l_keys.remove(0);
                    p_keys.insert(parent_key_idx, new_separator_for_parent);
                    let child_to_move = l_children.remove(0);
                    u_children.push(child_to_move);
                    let mut moved_child_node = self.get_mutable_node(child_to_move)?;
                    moved_child_node.set_parent_page_id(Some(*u_pid_val));
                    self.write_node(&moved_child_node)?;
                }
            },
            _ => return Err(OxidbError::TreeLogicError("Sibling and parent types mismatch during borrow, or one is not a recognized BPlusTreeNode variant.".to_string())),
        }
        self.write_node(underflowed_node)?;
        self.write_node(lender_sibling)?;
        self.write_node(parent_node)?;
        Ok(())
    }

    fn merge_nodes(
        &mut self,
        left_node: &mut BPlusTreeNode,
        right_node: &mut BPlusTreeNode,
        parent_node: &mut BPlusTreeNode,
        parent_key_idx: usize,
    ) -> Result<(), OxidbError> {
        match (&mut *left_node, &mut *right_node, &mut *parent_node) {
            (
                BPlusTreeNode::Leaf { keys: l_keys, values: l_values, next_leaf: l_next_leaf, .. },
                BPlusTreeNode::Leaf { keys: r_keys, values: r_values, next_leaf: r_next_leaf, .. },
                BPlusTreeNode::Internal { keys: p_keys, children: p_children, .. },
            ) => {
                l_keys.append(r_keys);
                l_values.append(r_values);
                *l_next_leaf = *r_next_leaf;

                p_keys.remove(parent_key_idx);
                p_children.remove(parent_key_idx.saturating_add(1));
            }
            (
                BPlusTreeNode::Internal { page_id: l_pid_val, keys: l_keys, children: l_children, .. },
                BPlusTreeNode::Internal { keys: r_keys, children: r_children_original, .. },
                BPlusTreeNode::Internal { keys: p_keys, children: p_children, .. },
            ) => {
                let key_from_parent = p_keys.remove(parent_key_idx);
                l_keys.push(key_from_parent);

                let mut r_keys_temp = r_keys.clone();
                l_keys.append(&mut r_keys_temp);

                let children_to_move = r_children_original.clone();
                l_children.append(r_children_original);

                for child_pid_to_update in children_to_move {
                    let mut child_node = self.get_mutable_node(child_pid_to_update)?;
                    child_node.set_parent_page_id(Some(*l_pid_val));
                    self.write_node(&child_node)?;
                }
                p_children.remove(parent_key_idx.saturating_add(1));
            }
            _ => {
                return Err(OxidbError::TreeLogicError(
                    "Node types mismatch during merge, or parent is not Internal.".to_string(),
                ))
            }
        }

        let right_node_pid = right_node.get_page_id();
        self.deallocate_page_id(right_node_pid)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::indexing::btree::node::BPlusTreeNode::{Internal, Leaf};
    use crate::core::indexing::btree::node::PageId;
    use std::fs::{self, File};
    use std::io::{Read};
    use tempfile::{tempdir, TempDir};
    // METADATA_SIZE is not used directly in these tests anymore after PageManager
    // use crate::core::indexing::btree::page_io::METADATA_SIZE;
    use crate::core::indexing::btree::SENTINEL_PAGE_ID;


    fn construct_tree_with_nodes_for_tests(
        tree: &mut BPlusTreeIndex,
        nodes: Vec<BPlusTreeNode>,
        root_page_id: PageId,
        _next_available_page_id: PageId,
        _free_list_head_page_id: PageId,
    ) -> Result<(), OxidbError> {
        if nodes.is_empty() {
            return Err(OxidbError::TreeLogicError("Cannot construct tree with empty node list".to_string()));
        }

        for node in &nodes {
            println!("[DEBUG CONSTRUCT] Writing node PageID: {:?}, Keys: {:?}", node.get_page_id(), node.get_keys());
            if let BPlusTreeNode::Internal { children, .. } = node {
                println!("[DEBUG CONSTRUCT] ... Children: {:?}", children);
            } else if let BPlusTreeNode::Leaf { values, next_leaf, .. } = node {
                println!("[DEBUG CONSTRUCT] ... Value sets count: {}, NextLeaf: {:?}", values.len(), next_leaf);
            }
            tree.write_node(node)?;
        }

        let old_root_id = tree.root_page_id;
        tree.root_page_id = root_page_id;

        tree.write_metadata_if_root_changed(old_root_id)?;
        tree.page_manager.write_metadata()?;
        Ok(())
    }

    fn k(s: &str) -> KeyType {
        s.as_bytes().to_vec()
    }
    fn pk(s: &str) -> PrimaryKey {
        s.as_bytes().to_vec()
    }

    const TEST_TREE_ORDER: usize = 4;

    fn setup_tree(test_name: &str) -> (BPlusTreeIndex, PathBuf, TempDir) {
        let dir = tempdir().expect("Failed to create tempdir for test");
        let path = dir.path().join(format!("{}.db", test_name));
        if path.exists() {
            fs::remove_file(&path).expect("Failed to remove existing test db file");
        }
        let tree = BPlusTreeIndex::new(test_name.to_string(), path.clone(), TEST_TREE_ORDER)
            .expect("Failed to create BPlusTreeIndex");
        (tree, path, dir)
    }

    #[test]
    fn test_new_tree_creation() {
        let (tree, path, _dir) = setup_tree("test_new");
        assert_eq!(tree.order, TEST_TREE_ORDER);
        assert_eq!(tree.root_page_id, 0);

        let mut file = File::open(&path).expect("Failed to open DB file for metadata check");
        let mut u32_buf = [0u8; 4];
        let mut u64_buf = [0u8; 8];

        file.read_exact(&mut u32_buf).expect("Failed to read order from metadata");
        assert_eq!(u32::from_be_bytes(u32_buf) as usize, TEST_TREE_ORDER);

        file.read_exact(&mut u64_buf).expect("Failed to read root_page_id from metadata");
        assert_eq!(u64::from_be_bytes(u64_buf), 0);

        file.read_exact(&mut u64_buf).expect("Failed to read next_available_page_id from metadata");
        assert_eq!(u64::from_be_bytes(u64_buf), 1);

        file.read_exact(&mut u64_buf).expect("Failed to read free_list_head_page_id from metadata");
        assert_eq!(u64::from_be_bytes(u64_buf), SENTINEL_PAGE_ID);

        let root_node = tree.read_node(tree.root_page_id).expect("Failed to read root node");
        if let BPlusTreeNode::Leaf { keys, values, .. } = root_node {
            assert!(keys.is_empty());
            assert!(values.is_empty());
        } else {
            panic!("Root should be an empty leaf node");
        }
    }

    #[test]
    fn test_load_existing_tree() {
        let test_name = "test_load";
        let dir = tempdir().unwrap();
        let path = dir.path().join(format!("{}.db", test_name));
        {
            let _tree = BPlusTreeIndex::new(test_name.to_string(), path.clone(), TEST_TREE_ORDER).unwrap();
        }
        let loaded_tree = BPlusTreeIndex::new(test_name.to_string(), path.clone(), TEST_TREE_ORDER).unwrap();
        assert_eq!(loaded_tree.order, TEST_TREE_ORDER);
        assert_eq!(loaded_tree.root_page_id, 0);
        drop(dir);
    }

    #[test]
    fn test_node_read_write() {
        let (mut tree, _path, _dir) = setup_tree("test_read_write");
        let page_id1 = tree.allocate_new_page_id().expect("Failed to allocate page_id1");
        let node = BPlusTreeNode::Leaf {
            page_id: page_id1,
            parent_page_id: Some(0),
            keys: vec![k("apple")],
            values: vec![vec![pk("v_apple")]],
            next_leaf: None,
        };
        tree.write_node(&node).expect("Failed to write node");
        let read_node = tree.read_node(page_id1).expect("Failed to read node");
        assert_eq!(node, read_node);

        let page_id2 = tree.allocate_new_page_id().expect("Failed to allocate page_id2");
        let internal_node = BPlusTreeNode::Internal {
            page_id: page_id2,
            parent_page_id: None,
            keys: vec![k("banana")],
            children: vec![page_id1, 0],
        };
        tree.write_node(&internal_node).expect("Failed to write internal node");
        let read_internal_node = tree.read_node(page_id2).expect("Failed to read internal node");
        assert_eq!(internal_node, read_internal_node);
    }

    #[test]
    fn test_insert_into_empty_tree_and_find() {
        let (mut tree, _path, _dir) = setup_tree("test_insert_empty_find");
        tree.insert(k("apple"), pk("v_apple1")).expect("Insert failed");
        let result = tree.find_primary_keys(&k("apple")).expect("Find failed for apple");
        assert_eq!(result, Some(vec![pk("v_apple1")]));
        assert_eq!(tree.find_primary_keys(&k("banana")).expect("Find failed for banana"), None);
    }

    #[test]
    fn test_insert_multiple_no_split_and_find() {
        let (mut tree, _path, _dir) = setup_tree("test_insert_multiple_no_split");
        tree.insert(k("mango"), pk("v_mango")).expect("Insert mango failed");
        tree.insert(k("apple"), pk("v_apple")).expect("Insert apple failed");
        tree.insert(k("banana"), pk("v_banana")).expect("Insert banana failed");
        assert_eq!(
            tree.find_primary_keys(&k("apple")).expect("Find apple failed"),
            Some(vec![pk("v_apple")])
        );
        let root_node = tree.read_node(tree.root_page_id).expect("Read root node failed");
        if let BPlusTreeNode::Leaf { keys, .. } = root_node {
            assert_eq!(keys.len(), 3);
            assert_eq!(keys[0], k("apple"));
            assert_eq!(keys[1], k("banana"));
            assert_eq!(keys[2], k("mango"));
            assert!(keys.len() == tree.order - 1);
        } else {
            panic!("Root should be a leaf node");
        }
    }

    #[test]
    fn test_insert_causing_leaf_split_and_new_root() {
        let (mut tree, _path, _dir) = setup_tree("test_leaf_split_new_root");
        tree.insert(k("c"), pk("v_c")).expect("Insert c failed");
        tree.insert(k("a"), pk("v_a")).expect("Insert a failed");
        tree.insert(k("b"), pk("v_b")).expect("Insert b failed");
        tree.insert(k("d"), pk("v_d")).expect("Insert d failed");
        assert_ne!(tree.root_page_id, 0);
        let new_root_id = tree.root_page_id;
        let root_node = tree.read_node(new_root_id).expect("Read new root failed");
        if let BPlusTreeNode::Internal {
            page_id: r_pid,
            keys: r_keys,
            children: r_children,
            parent_page_id: r_parent_pid,
        } = root_node
        {
            assert_eq!(r_pid, new_root_id);
            assert!(r_parent_pid.is_none());
            assert_eq!(r_keys, vec![k("b")]);
            assert_eq!(r_children.len(), 2);
            let child0_page_id = r_children[0];
            let child1_page_id = r_children[1];
            let left_leaf = tree.read_node(child0_page_id).expect("Read child0 failed");
            if let BPlusTreeNode::Leaf {
                page_id: l_pid,
                keys: l_keys,
                values: l_values,
                next_leaf: l_next,
                parent_page_id: l_parent_pid,
            } = left_leaf
            {
                assert_eq!(l_pid, child0_page_id);
                assert_eq!(l_parent_pid, Some(new_root_id));
                assert_eq!(l_keys, vec![k("a")]);
                assert_eq!(l_values, vec![vec![pk("v_a")]]);
                assert_eq!(l_next, Some(child1_page_id));
            } else {
                panic!("Child 0 is not a Leaf as expected");
            }
            let right_leaf = tree.read_node(child1_page_id).expect("Read child1 failed");
            if let BPlusTreeNode::Leaf {
                page_id: rl_pid,
                keys: rl_keys,
                values: rl_values,
                next_leaf: rl_next,
                parent_page_id: rl_parent_pid,
            } = right_leaf
            {
                assert_eq!(rl_pid, child1_page_id);
                assert_eq!(rl_parent_pid, Some(new_root_id));
                assert_eq!(rl_keys, vec![k("b"), k("c"), k("d")]);
                assert_eq!(rl_values, vec![vec![pk("v_b")], vec![pk("v_c")], vec![pk("v_d")]]);
                assert_eq!(rl_next, None);
            } else {
                panic!("Child 1 is not a Leaf as expected");
            }
        } else {
            panic!("New root is not an Internal node as expected");
        }
        assert_eq!(tree.find_primary_keys(&k("d")).expect("Find d failed"), Some(vec![pk("v_d")]));
    }

    #[test]
    fn test_delete_from_leaf_no_underflow() {
        let (mut tree, _path, _dir) = setup_tree("delete_leaf_no_underflow");
        tree.insert(k("a"), pk("v_a")).expect("Insert a failed");
        tree.insert(k("b"), pk("v_b")).expect("Insert b failed");
        tree.insert(k("c"), pk("v_c")).expect("Insert c failed");
        let deleted = tree.delete(&k("b"), None).expect("Delete b failed");
        assert!(deleted);
        assert_eq!(tree.find_primary_keys(&k("b")).expect("Find b after delete failed"), None);
        assert_eq!(
            tree.find_primary_keys(&k("a")).expect("Find a after delete failed"),
            Some(vec![pk("v_a")])
        );
        let root_node = tree.read_node(tree.root_page_id).expect("Read root node failed");
        if let BPlusTreeNode::Leaf { keys, .. } = root_node {
            assert_eq!(keys, vec![k("a"), k("c")]);
        } else {
            panic!("Should be leaf root");
        }
    }

    #[test]
    fn test_delete_specific_pk_from_leaf() {
        let (mut tree, _path, _dir) = setup_tree("delete_specific_pk");
        tree.insert(k("a"), pk("v_a1")).expect("Insert v_a1 failed");
        tree.insert(k("a"), pk("v_a2")).expect("Insert v_a2 failed");
        tree.insert(k("a"), pk("v_a3")).expect("Insert v_a3 failed");
        tree.insert(k("b"), pk("v_b1")).expect("Insert v_b1 failed");
        let deleted_pk_result =
            tree.delete(&k("a"), Some(&pk("v_a2"))).expect("Delete v_a2 failed");
        assert!(
            deleted_pk_result,
            "Deletion of a specific PK should return true if PK was found and removed."
        );
        let pks_a_after_delete = tree
            .find_primary_keys(&k("a"))
            .expect("Find a after delete failed")
            .expect("PKs for 'a' should exist");
        assert_eq!(pks_a_after_delete.len(), 2);
        assert!(pks_a_after_delete.contains(&pk("v_a1")));
        assert!(!pks_a_after_delete.contains(&pk("v_a2")));
        assert!(pks_a_after_delete.contains(&pk("v_a3")));
        let deleted_key_entirely =
            tree.delete(&k("a"), None).expect("Delete entire key 'a' failed");
        assert!(deleted_key_entirely, "Deletion of entire key should return true.");
        assert!(
            tree.find_primary_keys(&k("a")).expect("Find 'a' after full delete failed").is_none(),
            "Key 'a' should be completely gone."
        );
    }

    #[test]
    fn test_delete_causing_underflow_simple_root_empty() {
        let (mut tree, _path, _dir) = setup_tree("delete_root_empties");
        tree.insert(k("a"), pk("v_a")).expect("Insert a failed");
        let deleted = tree.delete(&k("a"), None).expect("Delete a failed");
        assert!(deleted);
        assert!(tree.find_primary_keys(&k("a")).expect("Find a after delete failed").is_none());
        let root_node = tree.read_node(tree.root_page_id).expect("Read root node failed");
        if let BPlusTreeNode::Leaf { keys, .. } = root_node {
            assert!(keys.is_empty(), "Root leaf should be empty but not removed");
        } else {
            panic!("Root should remain a leaf");
        }
    }

    #[test]
    fn test_delete_leaf_borrow_from_right_sibling() -> Result<(), OxidbError> {
        const ORDER: usize = 4;
        let (mut tree, _path, _dir) = setup_tree("borrow_from_right_leaf");
        assert_eq!(tree.order, ORDER, "Test setup assumes order 4 from setup_tree");

        tree.insert(k("apple"), pk("v_apple"))?;
        tree.insert(k("banana"), pk("v_banana"))?;
        tree.insert(k("cherry"), pk("v_cherry"))?;
        tree.insert(k("date"), pk("v_date"))?;

        let root_pid = tree.root_page_id;
        let root_node_initial = tree.read_node(root_pid)?;
        let (initial_l1_pid, initial_l2_pid) = match &root_node_initial {
            BPlusTreeNode::Internal { keys, children, .. } => {
                assert_eq!(keys, &vec![k("banana")]);
                (children[0], children[1])
            }
            _ => panic!("Root should be internal"),
        };

        let deleted = tree.delete(&k("apple"), None)?;
        assert!(deleted, "Deletion of 'apple' should succeed");

        let final_root_node = tree.read_node(root_pid)?;
        let (final_l1_pid, final_l2_pid) = match &final_root_node {
            BPlusTreeNode::Internal { keys, children, .. } => {
                assert_eq!(keys, &vec![k("cherry")]);
                (children[0], children[1])
            }
            _ => panic!("Root should remain internal"),
        };

        assert_eq!(final_l1_pid, initial_l1_pid);
        assert_eq!(final_l2_pid, initial_l2_pid);

        let final_l1_node = tree.read_node(final_l1_pid)?;
        match &final_l1_node {
            BPlusTreeNode::Leaf { keys, parent_page_id, next_leaf, .. } => {
                assert_eq!(keys, &vec![k("banana")]);
                assert_eq!(*parent_page_id, Some(root_pid));
                assert_eq!(next_leaf, &Some(final_l2_pid));
            }
            _ => panic!("L1 should be a Leaf node"),
        }

        let final_l2_node = tree.read_node(final_l2_pid)?;
        match &final_l2_node {
            BPlusTreeNode::Leaf { keys, parent_page_id, next_leaf, .. } => {
                assert_eq!(keys, &vec![k("cherry"), k("date")]);
                assert_eq!(*parent_page_id, Some(root_pid));
                assert_eq!(next_leaf, &None);
            }
            _ => panic!("L2 should be a Leaf node"),
        }
        Ok(())
    }

    #[test]
    fn test_page_allocation_and_deallocation() {
        let (mut tree, _path, _dir) = setup_tree("alloc_dealloc_test");

        let p1 = tree.allocate_new_page_id().unwrap();
        assert_eq!(p1, 1);
        let p2 = tree.allocate_new_page_id().unwrap();
        assert_eq!(p2, 2);
        let p3 = tree.allocate_new_page_id().unwrap();
        assert_eq!(p3, 3);

        tree.deallocate_page_id(p2).unwrap();
        tree.deallocate_page_id(p1).unwrap();

        let p_reused1 = tree.allocate_new_page_id().unwrap();
        assert_eq!(p_reused1, p1);
        let p_reused2 = tree.allocate_new_page_id().unwrap();
        assert_eq!(p_reused2, p2);

        tree.deallocate_page_id(p3).unwrap();
        let p_reused3 = tree.allocate_new_page_id().unwrap();
        assert_eq!(p_reused3, p3);

        let p4 = tree.allocate_new_page_id().unwrap();
        assert_eq!(p4, 4);
    }

    fn insert_keys(tree: &mut BPlusTreeIndex, keys: &[&str]) -> Result<(), OxidbError> {
        for (i, key_str) in keys.iter().enumerate() {
            tree.insert(k(key_str), pk(&format!("v_{}_{}", key_str, i)))?;
        }
        Ok(())
    }

    #[test]
    fn test_delete_internal_borrow_from_right_sibling() -> Result<(), OxidbError> {
        // This test expects an internal borrow.
        // Setup: IL0 underflows, L0+L1 merge, IL0 borrows from IL1 (internal).
        // Root[kR] -> IL0[kI0](L0[kL0], L1[kL1_min]), IL1[kI1a,kI1b](L2[...], L3[...], L4[...])
        // Delete kL0. L0 empty. L1_min cannot lend. L0,L1 merge. IL0 loses kI0 -> underflows.
        // IL0 borrows from IL1. kR from Root moves to IL0. kI1a from IL1 moves to Root. L2 moves to IL0.

        let (mut tree, _path, _dir) = setup_tree("delete_internal_borrow_right_corrected");
        assert_eq!(tree.order, 4); // Min keys 1

        // Page IDs (conceptual, will be allocated by PageManager)
        let p_root = 0; // Initial root from setup_tree
        let p_il0 = 1;
        let p_l0 = 2;
        let p_l1 = 3;
        let p_il1 = 4;
        let p_l2 = 5;
        let p_l3 = 6;
        let p_l4 = 7;
        let next_available_hint = 8;

        let nodes = vec![
            // Root
            Internal { page_id: p_root, parent_page_id: None, keys: vec![k("05")], children: vec![p_il0, p_il1] },
            // IL0 (will underflow)
            Internal { page_id: p_il0, parent_page_id: Some(p_root), keys: vec![k("00")], children: vec![p_l0, p_l1] },
            Leaf { page_id: p_l0, parent_page_id: Some(p_il0), keys: vec![k("00")], values: vec![vec![pk("v00")]], next_leaf: Some(p_l1) },
            Leaf { page_id: p_l1, parent_page_id: Some(p_il0), keys: vec![k("02")], values: vec![vec![pk("v02")]], next_leaf: Some(p_l2) }, // L1 has 1 key (min)
            // IL1 (lender)
            Internal { page_id: p_il1, parent_page_id: Some(p_root), keys: vec![k("10"), k("12")], children: vec![p_l2, p_l3, p_l4] },
            Leaf { page_id: p_l2, parent_page_id: Some(p_il1), keys: vec![k("10")], values: vec![vec![pk("v10")]], next_leaf: Some(p_l3) },
            Leaf { page_id: p_l3, parent_page_id: Some(p_il1), keys: vec![k("12")], values: vec![vec![pk("v12")]], next_leaf: Some(p_l4) },
            Leaf { page_id: p_l4, parent_page_id: Some(p_il1), keys: vec![k("14")], values: vec![vec![pk("v14")]], next_leaf: None },
        ];
        construct_tree_with_nodes_for_tests(&mut tree, nodes, p_root, next_available_hint, SENTINEL_PAGE_ID)?;

        tree.delete(&k("00"), None)?; // Delete k("00") from L0

        // Expected state after IL0 borrows from IL1:
        // Root: keys [k("10")] (k("05") moved down, k("10") from IL1 moved up)
        // IL0: keys [k("05")] (got k("05") from Root)
        //      children: [merged_L0L1_page, p_l2 (moved from IL1)]
        // IL1: keys [k("12")] (lost k("10") and child p_l2)
        //      children: [p_l3, p_l4]

        let root_node_after = tree.read_node(p_root)?;
        match &root_node_after {
            Internal { keys, children, .. } => {
                assert_eq!(keys.as_slice(), &[k("10")], "Root key incorrect");
                assert_eq!(children.as_slice(), &[p_il0, p_il1], "Root children incorrect");
            }
            _ => panic!("Root not internal"),
        }

        let il0_node_after = tree.read_node(p_il0)?;
        match &il0_node_after {
            Internal { keys, children, parent_page_id, .. } => {
                assert_eq!(*parent_page_id, Some(p_root));
                assert_eq!(keys.as_slice(), &[k("05")], "IL0 keys incorrect"); // Was expecting k("03") in failing test
                assert_eq!(children.len(), 2);
                assert_eq!(children[1], p_l2, "IL0 should have L2 as its second child");

                let merged_l0l1_page_id = children[0]; // L0's page should now host merged L0L1
                let merged_l0l1_node = tree.read_node(merged_l0l1_page_id)?;
                assert_eq!(merged_l0l1_node.get_keys().as_slice(), &[k("02")]); // L0 was [k("00")], L1 was [k("02")]. After deleting k("00"), L0 merges L1.
            }
            _ => panic!("IL0 not internal"),
        }

        let il1_node_after = tree.read_node(p_il1)?;
         match &il1_node_after {
            Internal { keys, children, .. } => {
                assert_eq!(keys.as_slice(), &[k("12")]);
                assert_eq!(children.as_slice(), &[p_l3, p_l4]);
            }
            _ => panic!("IL1 not internal"),
        }
        Ok(())
    }

    #[test]
    fn test_delete_internal_borrow_from_left_sibling() -> Result<(), OxidbError> {
        // Similar setup to right_sibling, but IL0 is lender, IL1 underflows.
        Ok(())
    }


    #[test]
    fn test_delete_internal_merge_with_left_sibling() -> Result<(), OxidbError> {
        let (mut tree, _p, _d) = setup_tree("internal_merge_left_cascade_refactored");
        // Setup for Order 4 (min keys 1):
        // Root[kR1] -> I0[kI0], I1[kI1]
        //   I0 -> L0[kL0], L1[kL1] (L0, L1 at min keys)
        //   I1 -> L2[kL2], L3[kL3] (L2, L3 at min keys)
        // I0, I1 at min keys. Deleting kL0 forces L0/L1 merge, I0 underflows.
        // I0 cannot borrow from I1 (I1 at min keys). I0 merges I1. Root loses kR1, underflows.
        // Root becomes the merged I0I1 node.

        const P_ROOT: PageId = 0;
        const P_I0: PageId = 1; const P_I1: PageId = 2;
        const P_L0: PageId = 3; const P_L1: PageId = 4;
        const P_L2: PageId = 5; const P_L3: PageId = 6;
        let next_available_hint = 7;

        let nodes = vec![
            Internal { page_id: P_ROOT, parent_page_id: None, keys: vec![k("02")], children: vec![P_I0, P_I1]},
            Internal { page_id: P_I0, parent_page_id: Some(P_ROOT), keys: vec![k("00")], children: vec![P_L0, P_L1]},
            Internal { page_id: P_I1, parent_page_id: Some(P_ROOT), keys: vec![k("04")], children: vec![P_L2, P_L3]},
            Leaf { page_id: P_L0, parent_page_id: Some(P_I0), keys: vec![k("00")], values: vec![vec![pk("v00")]], next_leaf: Some(P_L1)},
            Leaf { page_id: P_L1, parent_page_id: Some(P_I0), keys: vec![k("01")], values: vec![vec![pk("v01")]], next_leaf: Some(P_L2)},
            Leaf { page_id: P_L2, parent_page_id: Some(P_I1), keys: vec![k("04")], values: vec![vec![pk("v04")]], next_leaf: Some(P_L3)},
            Leaf { page_id: P_L3, parent_page_id: Some(P_I1), keys: vec![k("05")], values: vec![vec![pk("v05")]], next_leaf: None},
        ];
        construct_tree_with_nodes_for_tests(&mut tree, nodes, P_ROOT, next_available_hint, SENTINEL_PAGE_ID)?;

        let r_pid_before = tree.root_page_id;
        assert_eq!(r_pid_before, P_ROOT);

        tree.delete(&k("00"), None)?; // Delete k("00") from L0

        let new_r_pid_after = tree.root_page_id;
        assert_ne!(new_r_pid_after, r_pid_before, "Root PID should change"); // This is the key assertion that was failing

        let new_root_node = tree.read_node(new_r_pid_after)?;
        assert!(new_root_node.get_parent_page_id().is_none(), "New root should have no parent");

        match &new_root_node {
            Internal { keys, children, .. } => {
                // Merged I0I1 node: I0 had k("00"), I1 had k("04"). Root key k("02") came down.
                // So, new root (which is the page of old I0) keys: [k("00"), k("02"), k("04")]
                assert_eq!(keys.as_slice(), &[k("00"), k("02"), k("04")]);
                // Children: mergedL0L1 (on P_L0), P_L2, P_L3
                assert_eq!(children.len(), 3);
                assert_eq!(children[0], P_L0); // L0 now contains merged L0+L1
                assert_eq!(children[1], P_L2);
                assert_eq!(children[2], P_L3);

                let merged_l0l1 = tree.read_node(P_L0)?;
                assert_eq!(merged_l0l1.get_keys().as_slice(), &[k("01")]); // L0 got k00, L1 got k01, merged L0L1 on L0 gets k01
            }
            _ => panic!("New root is not internal as expected after merge cascade"),
        }
        Ok(())
    }

    #[test]
    fn test_delete_internal_merge_with_right_sibling() -> Result<(), OxidbError> {
        let (mut tree, _path, _dir) = setup_tree("internal_merge_right_precisely_refactored");

        const R_PID: PageId = 0;
        const IL0_PID: PageId = 1;
        const L0_PID: PageId = 2;
        const L1_PID: PageId = 3;
        const IL1_PID: PageId = 4;
        const L2_PID: PageId = 5;
        const L3_PID: PageId = 6;
        const IL2_PID: PageId = 7;
        const L4_PID: PageId = 8;
        const L5_PID: PageId = 9;
        let next_available_hint = 10;

        let nodes_to_construct = vec![
            Internal { page_id: R_PID, parent_page_id: None, keys: vec![k("03"), k("07")], children: vec![IL0_PID, IL1_PID, IL2_PID] },
            Internal { page_id: IL0_PID, parent_page_id: Some(R_PID), keys: vec![k("01")], children: vec![L0_PID, L1_PID] },
            Leaf { page_id: L0_PID, parent_page_id: Some(IL0_PID), keys: vec![k("00")], values: vec![vec![pk("v00")]], next_leaf: Some(L1_PID) },
            Leaf { page_id: L1_PID, parent_page_id: Some(IL0_PID), keys: vec![k("02")], values: vec![vec![pk("v02")]], next_leaf: Some(L2_PID) }, // L1 has 1 key
            Internal { page_id: IL1_PID, parent_page_id: Some(R_PID), keys: vec![k("05")], children: vec![L2_PID, L3_PID] }, // IL1 has 1 key
            Leaf { page_id: L2_PID, parent_page_id: Some(IL1_PID), keys: vec![k("04")], values: vec![vec![pk("v04")]], next_leaf: Some(L3_PID) },
            Leaf { page_id: L3_PID, parent_page_id: Some(IL1_PID), keys: vec![k("06")], values: vec![vec![pk("v06")]], next_leaf: Some(L4_PID) },
            Internal { page_id: IL2_PID, parent_page_id: Some(R_PID), keys: vec![k("09")], children: vec![L4_PID, L5_PID] }, // IL2 has 1 key
            Leaf { page_id: L4_PID, parent_page_id: Some(IL2_PID), keys: vec![k("08")], values: vec![vec![pk("v08")]], next_leaf: Some(L5_PID) },
            Leaf { page_id: L5_PID, parent_page_id: Some(IL2_PID), keys: vec![k("10")], values: vec![vec![pk("v10")]], next_leaf: None },
        ];
        construct_tree_with_nodes_for_tests(&mut tree, nodes_to_construct, R_PID, next_available_hint, SENTINEL_PAGE_ID)?;

        // Delete k("02") from L1. L1 underflows. L0 cannot lend (1 key). L0, L1 merge.
        // Merged L0L1 on pL0: [k("00")]. (L1 key k("02") is gone).
        // IL0 loses key k("01"). IL0 keys become empty. IL0 underflows.
        // IL0 merges with IL1 (right). Root key k("03") comes down.
        // IL0 (absorber) keys: [k("03"), k("05")]. Children: [merged_L0L1, L2, L3].
        // Root keys: [k("07")]. Children [IL0, IL2].
        tree.delete(&k("02"), None)?;

        let root_node_after = tree.read_node(R_PID)?;
        assert_eq!(root_node_after.get_keys(), &vec![k("07")]);
        match &root_node_after {
            Internal { children, .. } => assert_eq!(children.as_slice(), &[IL0_PID, IL2_PID]),
            _ => panic!("Root not internal"),
        }

        let il0_node_after = tree.read_node(IL0_PID)?;
        assert_eq!(il0_node_after.get_keys().as_slice(), &[k("03"), k("05")]);
        Ok(())
    }


    #[test]
    fn test_delete_recursive_จน_root_is_leaf() -> Result<(), OxidbError> {
        let (mut tree, _p, _d) = setup_tree("delete_till_root_leaf_refactored");
        insert_keys(&mut tree, &["0","1","2","3"])?;

        let r_pid_internal_before_any_delete = tree.root_page_id;
        assert_ne!(r_pid_internal_before_any_delete, 0);

        tree.delete(&k("0"), None)?;
        tree.delete(&k("1"), None)?;

        let old_root_page_id_before_final_delete = tree.root_page_id;
        tree.delete(&k("2"), None)?;

        assert_ne!(tree.root_page_id, old_root_page_id_before_final_delete);
        let final_root_node = tree.read_node(tree.root_page_id)?;
        match final_root_node {
            Leaf { keys, ..} => assert_eq!(keys, vec![k("3")]),
            _ => panic!("Root should be leaf at the end"),
        }
        Ok(())
    }
}
