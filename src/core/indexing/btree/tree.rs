use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::path::{Path, PathBuf};
// use std::collections::HashMap; // Not using a cache for now

use crate::core::indexing::btree::node::{
    BPlusTreeNode, KeyType, PageId, PrimaryKey, SerializationError,
};

use std::sync::Mutex; // Import Mutex

// Constants
const PAGE_SIZE: u64 = 4096;
const METADATA_SIZE: u64 = 4 + 8 + 8;

#[derive(Debug)]
pub enum OxidbError {
    Io(io::Error),
    Serialization(SerializationError),
    NodeNotFound(PageId),
    PageFull(String),
    UnexpectedNodeType,
    TreeLogicError(String),
    BorrowError(String), // For RefCell borrow errors
}

impl From<std::cell::BorrowMutError> for OxidbError {
    fn from(err: std::cell::BorrowMutError) -> Self {
        OxidbError::BorrowError(err.to_string())
    }
}


impl From<io::Error> for OxidbError {
    fn from(err: io::Error) -> Self {
        OxidbError::Io(err)
    }
}

impl From<SerializationError> for OxidbError {
    fn from(err: SerializationError) -> Self {
        OxidbError::Serialization(err)
    }
}

#[derive(Debug)]
pub struct BPlusTreeIndex {
    pub name: String,
    pub path: PathBuf,
    pub order: usize,
    pub root_page_id: PageId,
    pub next_available_page_id: PageId,
    pub file_handle: Mutex<File>, // Changed to Mutex<File>
}

impl BPlusTreeIndex {
    pub fn new(name: String, path: PathBuf, order: usize) -> Result<Self, OxidbError> {
        let file_exists = path.exists();
        let mut file_obj = OpenOptions::new() // Made mutable again
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        if file_exists && file_obj.metadata()?.len() >= METADATA_SIZE {
            file_obj.seek(SeekFrom::Start(0))?;
            let mut u32_buf = [0u8; 4];
            let mut u64_buf = [0u8; 8];

            file_obj.read_exact(&mut u32_buf)?;
            let loaded_order = u32::from_be_bytes(u32_buf) as usize;

            file_obj.read_exact(&mut u64_buf)?;
            let root_page_id = u64::from_be_bytes(u64_buf);

            file_obj.read_exact(&mut u64_buf)?;
            let next_available_page_id = u64::from_be_bytes(u64_buf);

            Ok(Self {
                name,
                path,
                order: loaded_order,
                root_page_id,
                next_available_page_id,
                file_handle: Mutex::new(file_obj), // Wrap in Mutex
            })

        } else {
            if order < 3 {
                return Err(OxidbError::TreeLogicError(format!("Order {} is too small. Minimum order is 3.", order)));
            }
            let root_page_id = 0;
            let next_available_page_id = 1;
            // Create the Mutex<File> for the tree instance
            let file_handle_mutex = Mutex::new(file_obj);
            let mut tree = Self {
                name,
                path,
                order,
                root_page_id,
                next_available_page_id,
                file_handle: file_handle_mutex, // Assign Mutex wrapped file
            };
            let initial_root_node = BPlusTreeNode::Leaf {
                page_id: tree.root_page_id,
                parent_page_id: None,
                keys: Vec::new(),
                values: Vec::new(),
                next_leaf: None,
            };
            tree.write_node(&initial_root_node)?;
            tree.write_metadata()?;
            Ok(tree)
        }
    }

    pub fn write_metadata(&mut self) -> Result<(), OxidbError> {
        // Lock the Mutex to get mutable access to the file
        let mut file = self.file_handle.lock().map_err(|e| OxidbError::BorrowError(format!("Mutex lock error: {}", e)))?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&(self.order as u32).to_be_bytes())?;
        file.write_all(&self.root_page_id.to_be_bytes())?;
        file.write_all(&self.next_available_page_id.to_be_bytes())?;
        file.flush()?;
        Ok(())
    }

    pub fn allocate_new_page_id(&mut self) -> Result<PageId, OxidbError> {
        let new_page_id = self.next_available_page_id;
        self.next_available_page_id += 1;
        self.write_metadata()?;
        Ok(new_page_id)
    }

    // Now uses Mutex for interior mutability.
    pub fn read_node(&self, page_id: PageId) -> Result<BPlusTreeNode, OxidbError> {
        // Lock the Mutex to get mutable access to the file
        let mut file = self.file_handle.lock().map_err(|e| OxidbError::BorrowError(format!("Mutex lock error: {}", e)))?;
        let offset = page_id * PAGE_SIZE;
        file.seek(SeekFrom::Start(offset))?;

        let mut page_buffer = vec![0u8; PAGE_SIZE as usize];
        match file.read_exact(&mut page_buffer) {
            Ok(_) => {},
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                 return Err(OxidbError::NodeNotFound(page_id));
            }
            Err(e) => return Err(OxidbError::Io(e)),
        }
        BPlusTreeNode::from_bytes(&page_buffer).map_err(OxidbError::from)
    }

    pub fn write_node(&mut self, node: &BPlusTreeNode) -> Result<(), OxidbError> {
        // Lock the Mutex to get mutable access to the file
        let mut file = self.file_handle.lock().map_err(|e| OxidbError::BorrowError(format!("Mutex lock error: {}", e)))?;
        let page_id = node.get_page_id();
        let offset = page_id * PAGE_SIZE;

        let mut node_bytes = node.to_bytes()?;
        if node_bytes.len() > PAGE_SIZE as usize {
            return Err(OxidbError::PageFull(format!(
                "Serialized node size {} exceeds PAGE_SIZE {}",
                node_bytes.len(),
                PAGE_SIZE
            )));
        }
        node_bytes.resize(PAGE_SIZE as usize, 0);

        file.seek(SeekFrom::Start(offset))?;
        file.write_all(&node_bytes)?;
        file.flush()?;
        Ok(())
    }

    // Changed to &self as it calls read_node (now &self)
    pub fn find_leaf_node_path(&self, key: &KeyType, path: &mut Vec<PageId>) -> Result<BPlusTreeNode, OxidbError> {
        path.clear();
        let mut current_page_id = self.root_page_id;
        loop {
            path.push(current_page_id);
            let current_node = self.read_node(current_page_id)?; // Calls &self read_node
            match current_node {
                BPlusTreeNode::Internal { ref keys, ref children, .. } => {
                    let child_idx = keys.partition_point(|k| k.as_slice() < key.as_slice());
                    current_page_id = children[child_idx];
                }
                BPlusTreeNode::Leaf { .. } => {
                    return Ok(current_node);
                }
            }
        }
    }

    // Changed to &self as it calls find_leaf_node_path (now &self)
    pub fn find_primary_keys(&self, key: &KeyType) -> Result<Option<Vec<PrimaryKey>>, OxidbError> {
        let mut path = Vec::new();
        let leaf_node = self.find_leaf_node_path(key, &mut path)?;
        match leaf_node {
            BPlusTreeNode::Leaf { keys, values, .. } => {
                match keys.binary_search(key) {
                    Ok(idx) => Ok(Some(values[idx].clone())),
                    Err(_) => Ok(None),
                }
            }
            _ => unreachable!("find_leaf_node_path should always return a Leaf node"),
        }
    }

    // insert remains &mut self
    pub fn insert(&mut self, key: KeyType, value: PrimaryKey) -> Result<(), OxidbError> {
        let mut path_to_leaf: Vec<PageId> = Vec::new();
        // find_leaf_node_path now takes &self. If insert needs to modify the path
        // or nodes along the path before the actual leaf modification, this could be an issue.
        // However, B+-Tree insert finds leaf, then modifies leaf, then potentially splits upwards.
        // So, reading the path with &self is fine. Modifications happen on specific nodes loaded mutably later.
        // For this, we need a version of find_leaf_node_path that returns mutable node, or we re-read node mutably.
        // Let's make find_leaf_node_path return a non-mutable node for now, and re-read for mutable operations.
        // This is inefficient but avoids deeper refactoring for now.
        // OR, have two versions: find_leaf_node_path_const and find_leaf_node_path_mut.
        // The current find_leaf_node_path takes &self, so it is const-like.
        // The insert operation will read the leaf node again for mutation.

        // Step 1: Find path and get a (read-only) copy of the leaf node.
        let _ = self.find_leaf_node_path(&key, &mut path_to_leaf)?;
        let leaf_page_id = *path_to_leaf.last().ok_or(OxidbError::TreeLogicError("Path to leaf is empty".to_string()))?;

        // Step 2: Read the leaf node mutably for insertion.
        let mut current_leaf_node = self.read_node_mut(leaf_page_id)?; // Assumes read_node_mut exists

        match &mut current_leaf_node {
            BPlusTreeNode::Leaf { keys, values, .. } => {
                match keys.binary_search(&key) {
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
                }
            }
            _ => return Err(OxidbError::UnexpectedNodeType),
        }

        if current_leaf_node.get_keys().len() >= self.order {
            self.handle_split(current_leaf_node, path_to_leaf)
        } else {
            self.write_node(&current_leaf_node)
        }
    }

    // Helper to read a node mutably, needed by insert/delete if find_leaf_node_path is &self.
    // This is essentially the original read_node.
    fn read_node_mut(&mut self, page_id: PageId) -> Result<BPlusTreeNode, OxidbError> {
        // Lock the Mutex to get mutable access to the file
        let mut file = self.file_handle.lock().map_err(|e| OxidbError::BorrowError(format!("Mutex lock error: {}", e)))?;
        let offset = page_id * PAGE_SIZE;
        file.seek(SeekFrom::Start(offset))?;
        let mut page_buffer = vec![0u8; PAGE_SIZE as usize];
        match file.read_exact(&mut page_buffer) {
            Ok(_) => {},
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Err(OxidbError::NodeNotFound(page_id)),
            Err(e) => return Err(OxidbError::Io(e)),
        }
        BPlusTreeNode::from_bytes(&page_buffer).map_err(OxidbError::from)
    }


    fn handle_split(&mut self, mut node_to_split: BPlusTreeNode, mut path: Vec<PageId>) -> Result<(), OxidbError> {
        // path.pop() is original_node_page_id. The remaining path is to its parent.
        let _original_node_page_id = path.pop().ok_or(OxidbError::TreeLogicError("Path cannot be empty in handle_split".to_string()))?;

        let new_sibling_page_id = self.allocate_new_page_id()?;

        let (promoted_or_copied_key, mut new_sibling_node) =
            node_to_split.split(self.order, new_sibling_page_id)
                .map_err(|e| OxidbError::TreeLogicError(e.to_string()))?;

        new_sibling_node.set_parent_page_id(node_to_split.get_parent_page_id());

        self.write_node(&node_to_split)?;
        self.write_node(&new_sibling_node)?;

        let parent_page_id_opt = node_to_split.get_parent_page_id();

        if let Some(parent_page_id) = parent_page_id_opt {
            // Read parent mutably
            let mut parent_node = self.read_node_mut(parent_page_id)?;
            match &mut parent_node {
                BPlusTreeNode::Internal { keys, children, ..} => {
                    let insertion_point = keys.partition_point(|k| k.as_slice() < promoted_or_copied_key.as_slice());
                    keys.insert(insertion_point, promoted_or_copied_key);
                    children.insert(insertion_point + 1, new_sibling_page_id);

                    if parent_node.get_keys().len() >= self.order {
                        self.handle_split(parent_node, path)
                    } else {
                        self.write_node(&parent_node)
                    }
                }
                _ => Err(OxidbError::TreeLogicError("Parent node is not an internal node during split".to_string())),
            }
        } else {
            let new_root_page_id = self.allocate_new_page_id()?;
            let old_root_page_id = node_to_split.get_page_id();
            let new_root = BPlusTreeNode::Internal {
                page_id: new_root_page_id,
                parent_page_id: None,
                keys: vec![promoted_or_copied_key],
                children: vec![old_root_page_id, new_sibling_node.get_page_id()],
            };
            node_to_split.set_parent_page_id(Some(new_root_page_id));
            new_sibling_node.set_parent_page_id(Some(new_root_page_id));
            self.write_node(&node_to_split)?;
            self.write_node(&new_sibling_node)?;
            self.root_page_id = new_root_page_id;
            self.write_node(&new_root)?;
            self.write_metadata()
        }
    }

    pub fn delete(&mut self, key_to_delete: &KeyType, pk_to_remove: Option<&PrimaryKey>) -> Result<bool, OxidbError> {
        let mut path: Vec<PageId> = Vec::new();
        // Find path using &self methods
        let _ = self.find_leaf_node_path(key_to_delete, &mut path)?;
        let leaf_page_id = *path.last().ok_or(OxidbError::TreeLogicError("Path to leaf is empty for delete".to_string()))?;

        // Read leaf node mutably
        let mut leaf_node = self.read_node_mut(leaf_page_id)?;
        let mut key_removed_from_structure = false;

        match &mut leaf_node {
            BPlusTreeNode::Leaf { keys, values, .. } => {
                match keys.binary_search(key_to_delete) {
                    Ok(idx) => {
                        if let Some(pk_ref) = pk_to_remove {
                            let original_pk_count = values[idx].len();
                            values[idx].retain(|p| p != pk_ref);
                            if values[idx].len() < original_pk_count {
                                if values[idx].is_empty() {
                                    keys.remove(idx);
                                    values.remove(idx);
                                    key_removed_from_structure = true;
                                } else {
                                    key_removed_from_structure = false;
                                }
                            } else { return Ok(false); }
                        } else {
                            keys.remove(idx);
                            values.remove(idx);
                            key_removed_from_structure = true;
                        }
                    }
                    Err(_) => return Ok(false),
                }
            }
            _ => return Err(OxidbError::UnexpectedNodeType),
        }

        if key_removed_from_structure {
            if leaf_node.get_keys().len() < self.min_keys_for_node() && leaf_page_id != self.root_page_id {
                self.handle_underflow(leaf_node, path)?;
            } else if leaf_page_id == self.root_page_id && leaf_node.get_keys().is_empty() && !matches!(leaf_node, BPlusTreeNode::Internal{..}) {
                self.write_node(&leaf_node)?;
            } else {
                self.write_node(&leaf_node)?;
            }
        } else {
            self.write_node(&leaf_node)?;
        }
        Ok(key_removed_from_structure)
    }

    fn min_keys_for_node(&self) -> usize {
        (self.order - 1) / 2
    }

    fn handle_underflow(&mut self, mut current_node: BPlusTreeNode, mut path: Vec<PageId>) -> Result<(), OxidbError> {
        let current_node_pid = path.pop().ok_or_else(|| OxidbError::TreeLogicError("Path cannot be empty".to_string()))?;
        if current_node_pid == self.root_page_id {
            if let BPlusTreeNode::Internal { keys, children, .. } = &current_node {
                if keys.is_empty() && children.len() == 1 {
                    self.root_page_id = children[0];
                    // Read new root mutably to set its parent_page_id
                    let mut new_root_node = self.read_node_mut(self.root_page_id)?;
                    new_root_node.set_parent_page_id(None);
                    self.write_node(&new_root_node)?;
                    self.write_metadata()?;
                }
            }
            return Ok(());
        }

        let parent_pid = *path.last().ok_or_else(|| OxidbError::TreeLogicError("Parent not found for non-root underflow".to_string()))?;
        // Read parent mutably
        let mut parent_node = self.read_node_mut(parent_pid)?;

        let child_idx_in_parent = match &parent_node {
            BPlusTreeNode::Internal { children, .. } => {
                children.iter().position(|&child_pid| child_pid == current_node_pid)
                    .ok_or_else(|| OxidbError::TreeLogicError("Child not found in parent during underflow handling".to_string()))?
            }
            _ => return Err(OxidbError::UnexpectedNodeType),
        };

        if child_idx_in_parent > 0 {
            let left_sibling_pid = parent_node.get_children().unwrap()[child_idx_in_parent - 1];
            // Read left sibling mutably
            let mut left_sibling_node = self.read_node_mut(left_sibling_pid)?;
            if left_sibling_node.get_keys().len() > self.min_keys_for_node() {
                self.borrow_from_sibling(&mut current_node, &mut left_sibling_node, &mut parent_node, child_idx_in_parent -1, true)?;
                return Ok(());
            }
        }

        if child_idx_in_parent < parent_node.get_children().unwrap().len() - 1 {
            let right_sibling_pid = parent_node.get_children().unwrap()[child_idx_in_parent + 1];
            // Read right sibling mutably
            let mut right_sibling_node = self.read_node_mut(right_sibling_pid)?;
             if right_sibling_node.get_keys().len() > self.min_keys_for_node() {
                self.borrow_from_sibling(&mut current_node, &mut right_sibling_node, &mut parent_node, child_idx_in_parent, false)?;
                return Ok(());
            }
        }

        if child_idx_in_parent > 0 {
            let left_sibling_pid = parent_node.get_children().unwrap()[child_idx_in_parent - 1];
            // Read left sibling mutably
            let mut left_sibling_node = self.read_node_mut(left_sibling_pid)?;
            self.merge_nodes(&mut left_sibling_node, &mut current_node, &mut parent_node, child_idx_in_parent -1)?;
        } else {
            let right_sibling_pid = parent_node.get_children().unwrap()[child_idx_in_parent + 1];
            // Read right sibling mutably
            let mut right_sibling_node = self.read_node_mut(right_sibling_pid)?;
            self.merge_nodes(&mut current_node, &mut right_sibling_node, &mut parent_node, child_idx_in_parent)?;
        }

        if parent_node.get_keys().len() < self.min_keys_for_node() && parent_pid != self.root_page_id {
             self.handle_underflow(parent_node, path)?;
        } else if parent_pid == self.root_page_id && parent_node.get_keys().is_empty() && matches!(parent_node, BPlusTreeNode::Internal{..}) {
             if let BPlusTreeNode::Internal { children, .. } = parent_node {
                if children.len() == 1 {
                    self.root_page_id = children[0];
                    // Read new root mutably
                    let mut new_root_node = self.read_node_mut(self.root_page_id)?;
                    new_root_node.set_parent_page_id(None);
                    self.write_node(&new_root_node)?;
                    self.write_metadata()?;
                }
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
        let underflowed_node_page_id = underflowed_node.get_page_id();
        let lender_sibling_page_id = lender_sibling.get_page_id();
        let parent_node_page_id = parent_node.get_page_id();

        // The actual modifications happen on `underflowed_node`, `lender_sibling`, `parent_node`
        // through the mutable references passed into the match arms.
        match (underflowed_node, lender_sibling) {
            (BPlusTreeNode::Leaf { keys: u_keys, values: u_values, .. },
             BPlusTreeNode::Leaf { keys: l_keys, values: l_values, .. }) => {
                let p_node_keys = match parent_node {
                    BPlusTreeNode::Internal { keys: p_keys, .. } => p_keys,
                    _ => return Err(OxidbError::UnexpectedNodeType),
                };
                if is_left_lender {
                    let borrowed_key = l_keys.pop().ok_or(OxidbError::TreeLogicError("Lender leaf empty".to_string()))?;
                    let borrowed_value = l_values.pop().ok_or(OxidbError::TreeLogicError("Lender leaf values empty".to_string()))?;
                    u_keys.insert(0, borrowed_key.clone());
                    u_values.insert(0, borrowed_value);
                    p_node_keys[parent_key_idx] = borrowed_key;
                } else {
                    let borrowed_key = l_keys.remove(0);
                    let borrowed_value = l_values.remove(0);
                    u_keys.push(borrowed_key.clone());
                    u_values.push(borrowed_value);
                    p_node_keys[parent_key_idx] = l_keys[0].clone();
                }
            },
            (BPlusTreeNode::Internal { page_id: u_pid, keys: u_keys, children: u_children, .. },
             BPlusTreeNode::Internal { page_id: _l_pid, keys: l_keys, children: l_children, .. }) => {
                let p_node_keys = match parent_node {
                    BPlusTreeNode::Internal { keys: p_keys, .. } => p_keys,
                    _ => return Err(OxidbError::UnexpectedNodeType),
                };
                if is_left_lender {
                    let key_from_parent = p_node_keys[parent_key_idx].clone();
                    u_keys.insert(0, key_from_parent);
                    let new_separator_key = l_keys.pop().ok_or(OxidbError::TreeLogicError("Lender internal node has no keys to pop".to_string()))?;
                    p_node_keys[parent_key_idx] = new_separator_key;
                    let child_to_move = l_children.pop().ok_or(OxidbError::TreeLogicError("Lender internal node has no children to pop".to_string()))?;
                    u_children.insert(0, child_to_move);
                    let mut moved_child_node = self.read_node_mut(child_to_move)?;
                    moved_child_node.set_parent_page_id(Some(*u_pid));
                    self.write_node(&moved_child_node)?;
                } else {
                    let key_from_parent = p_node_keys[parent_key_idx].clone();
                    u_keys.push(key_from_parent);
                    let new_separator_key = l_keys.remove(0);
                    p_node_keys[parent_key_idx] = new_separator_key;
                    let child_to_move = l_children.remove(0);
                    u_children.push(child_to_move);
                    let mut moved_child_node = self.read_node_mut(child_to_move)?;
                    moved_child_node.set_parent_page_id(Some(*u_pid));
                    self.write_node(&moved_child_node)?;
                }
            },
            _ => return Err(OxidbError::TreeLogicError("Sibling types mismatch during borrow, or one is not a recognized BPlusTreeNode variant.".to_string())),
        }

        // Re-fetch nodes using their page IDs to get fresh references for writing
        let final_underflowed_node = self.read_node_mut(underflowed_node_page_id)?;
        self.write_node(&final_underflowed_node)?;

        let final_lender_sibling = self.read_node_mut(lender_sibling_page_id)?;
        self.write_node(&final_lender_sibling)?;

        let final_parent_node = self.read_node_mut(parent_node_page_id)?;
        self.write_node(&final_parent_node)?;

        Ok(())
    }

    fn merge_nodes(
        &mut self,
        left_node: &mut BPlusTreeNode,
        right_node: &mut BPlusTreeNode,
        parent_node: &mut BPlusTreeNode,
        parent_key_idx: usize,
    ) -> Result<(), OxidbError> {
        let left_node_page_id = left_node.get_page_id();
        let parent_node_page_id = parent_node.get_page_id();
        // right_node is consumed, its page might be deallocated later.

        match (left_node, right_node) {
            (BPlusTreeNode::Leaf { keys: l_keys, values: l_values, next_leaf: l_next_leaf, .. },
             BPlusTreeNode::Leaf { keys: r_keys, values: r_values, next_leaf: r_next_leaf, .. }) => {
                l_keys.append(r_keys);
                l_values.append(r_values);
                *l_next_leaf = *r_next_leaf;

                if let BPlusTreeNode::Internal { keys: p_keys, children: p_children, .. } = parent_node {
                    p_keys.remove(parent_key_idx);
                    p_children.remove(parent_key_idx + 1);
                } else {
                    return Err(OxidbError::UnexpectedNodeType);
                }
            }
            (BPlusTreeNode::Internal { page_id: l_pid, keys: l_keys, children: l_children, .. },
             BPlusTreeNode::Internal { keys: r_keys, children: r_children_original, .. }) => { // Capture original r_children
                let key_from_parent = if let BPlusTreeNode::Internal { keys: p_keys, .. } = parent_node {
                    p_keys[parent_key_idx].clone()
                } else {
                    return Err(OxidbError::UnexpectedNodeType);
                };
                l_keys.push(key_from_parent);

                // Iterate over a clone of r_children_original if its PIDs are needed for updating parent pointers
                let r_children_to_update_parent = r_children_original.clone();

                l_keys.append(r_keys); // r_keys is drained
                l_children.append(r_children_original); // r_children_original is drained

                for child_pid_to_update in r_children_to_update_parent {
                    let mut child_node = self.read_node_mut(child_pid_to_update)?;
                    child_node.set_parent_page_id(Some(*l_pid));
                    self.write_node(&child_node)?;
                }

                if let BPlusTreeNode::Internal { keys: p_keys, children: p_children, .. } = parent_node {
                     p_keys.remove(parent_key_idx);
                     p_children.remove(parent_key_idx + 1);
                } else {
                    return Err(OxidbError::UnexpectedNodeType);
                }
            }
            _ => return Err(OxidbError::TreeLogicError("Node types mismatch during merge, or one is not a BPlusTreeNode variant.".to_string())),
        }

        let final_left_node = self.read_node_mut(left_node_page_id)?;
        self.write_node(&final_left_node)?;

        let final_parent_node = self.read_node_mut(parent_node_page_id)?;
        self.write_node(&final_parent_node)?;

        // Deallocation of right_node's page would happen here if needed, e.g.,
        // self.deallocate_page(right_node_page_id)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    fn k(s: &str) -> KeyType { s.as_bytes().to_vec() }
    fn pk(s: &str) -> PrimaryKey { s.as_bytes().to_vec() }

    const TEST_TREE_ORDER: usize = 4;

    fn setup_tree(test_name: &str) -> (BPlusTreeIndex, PathBuf) {
        let dir = tempdir().unwrap();
        let path = dir.path().join(format!("{}.db", test_name));
        if path.exists() { fs::remove_file(&path).unwrap(); }
        let tree = BPlusTreeIndex::new(test_name.to_string(), path.clone(), TEST_TREE_ORDER).unwrap();
        (tree, path)
    }

    #[test]
    fn test_new_tree_creation() {
        let (mut tree, path) = setup_tree("test_new");
        assert_eq!(tree.order, TEST_TREE_ORDER);
        assert_eq!(tree.root_page_id, 0);
        assert_eq!(tree.next_available_page_id, 1);

        let mut file = File::open(path).unwrap();
        let mut u32_buf = [0u8; 4];
        let mut u64_buf = [0u8; 8];

        file.read_exact(&mut u32_buf).unwrap();
        assert_eq!(u32::from_be_bytes(u32_buf) as usize, TEST_TREE_ORDER);
        file.read_exact(&mut u64_buf).unwrap();
        assert_eq!(u64::from_be_bytes(u64_buf), 0);
        file.read_exact(&mut u64_buf).unwrap();
        assert_eq!(u64::from_be_bytes(u64_buf), 1);

        let root_node = tree.read_node(tree.root_page_id).unwrap(); // Uses &self read_node
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
        { let _tree = BPlusTreeIndex::new(test_name.to_string(), path.clone(), TEST_TREE_ORDER).unwrap(); }
        let loaded_tree = BPlusTreeIndex::new(test_name.to_string(), path.clone(), TEST_TREE_ORDER).unwrap();
        assert_eq!(loaded_tree.order, TEST_TREE_ORDER);
        assert_eq!(loaded_tree.root_page_id, 0);
        assert_eq!(loaded_tree.next_available_page_id, 1);
    }

    #[test]
    fn test_node_read_write() { // read_node is &self, write_node is &mut self
        let (mut tree, _path) = setup_tree("test_read_write");
        let page_id1 = tree.allocate_new_page_id().unwrap();
        let node = BPlusTreeNode::Leaf {
            page_id: page_id1,
            parent_page_id: Some(0), keys: vec![k("apple")], values: vec![vec![pk("v_apple")]], next_leaf: None,
        };
        tree.write_node(&node).unwrap();
        let read_node = tree.read_node(page_id1).unwrap(); // Uses &self read_node
        assert_eq!(node, read_node);

        let page_id2 = tree.allocate_new_page_id().unwrap();
        let internal_node = BPlusTreeNode::Internal {
            page_id: page_id2, parent_page_id: None, keys: vec![k("banana")], children: vec![0,1]
        };
        tree.write_node(&internal_node).unwrap();
        let read_internal_node = tree.read_node(page_id2).unwrap(); // Uses &self read_node
        assert_eq!(internal_node, read_internal_node);
    }

    #[test]
    fn test_insert_into_empty_tree_and_find() { // find_primary_keys uses &self
        let (mut tree, _path) = setup_tree("test_insert_empty_find");
        tree.insert(k("apple"), pk("v_apple1")).unwrap();
        let result = tree.find_primary_keys(&k("apple")).unwrap();
        assert_eq!(result, Some(vec![pk("v_apple1")]));
        assert_eq!(tree.find_primary_keys(&k("banana")).unwrap(), None);
    }

    #[test]
    fn test_insert_multiple_no_split_and_find() { // find_primary_keys and read_node use &self
        let (mut tree, _path) = setup_tree("test_insert_multiple_no_split");
        tree.insert(k("mango"), pk("v_mango")).unwrap();
        tree.insert(k("apple"), pk("v_apple")).unwrap();
        tree.insert(k("banana"), pk("v_banana")).unwrap();

        assert_eq!(tree.find_primary_keys(&k("apple")).unwrap(), Some(vec![pk("v_apple")]));
        let root_node = tree.read_node(tree.root_page_id).unwrap();
         if let BPlusTreeNode::Leaf { keys, .. } = root_node {
            assert_eq!(keys.len(), 3);
            assert_eq!(keys[0], k("apple"));
            assert_eq!(keys[1], k("banana"));
            assert_eq!(keys[2], k("mango"));
            assert!(keys.len() == tree.order -1);
        } else { panic!("Root should be a leaf node"); }
    }

    #[test]
    fn test_insert_causing_leaf_split_and_new_root() { // find_primary_keys and read_node use &self
        let (mut tree, _path) = setup_tree("test_leaf_split_new_root");
        tree.insert(k("c"), pk("v_c")).unwrap();
        tree.insert(k("a"), pk("v_a")).unwrap();
        tree.insert(k("b"), pk("v_b")).unwrap();
        tree.insert(k("d"), pk("v_d")).unwrap();

        assert_ne!(tree.root_page_id, 0);
        let new_root_id = tree.root_page_id;
        let root_node = tree.read_node(new_root_id).unwrap();
        if let BPlusTreeNode::Internal {page_id, keys, children, parent_page_id} = root_node {
            assert_eq!(page_id, new_root_id);
            assert!(parent_page_id.is_none());
            assert_eq!(keys, vec![k("b")]);
            assert_eq!(children.len(), 2);
            let child0_page_id = children[0];
            let child1_page_id = children[1];
            let left_leaf = tree.read_node(child0_page_id).unwrap();
            if let BPlusTreeNode::Leaf {page_id, keys, values, next_leaf, parent_page_id} = left_leaf {
                assert_eq!(page_id, child0_page_id);
                assert_eq!(parent_page_id, Some(new_root_id));
                assert_eq!(keys, vec![k("a")]);
                assert_eq!(values, vec![vec![pk("v_a")]]);
                assert_eq!(next_leaf, Some(child1_page_id));
            } else { panic!("Child 0 is not a Leaf"); }
            let right_leaf = tree.read_node(child1_page_id).unwrap();
             if let BPlusTreeNode::Leaf {page_id, keys, values, next_leaf, parent_page_id} = right_leaf {
                assert_eq!(page_id, child1_page_id);
                assert_eq!(parent_page_id, Some(new_root_id));
                assert_eq!(keys, vec![k("b"), k("c"), k("d")]);
                assert_eq!(values, vec![vec![pk("v_b")], vec![pk("v_c")], vec![pk("v_d")]]);
                assert_eq!(next_leaf, None);
            } else { panic!("Child 1 is not a Leaf"); }
        } else { panic!("New root is not Internal"); }
        assert_eq!(tree.find_primary_keys(&k("d")).unwrap(), Some(vec![pk("v_d")]));
    }

    #[test]
    fn test_delete_from_leaf_no_underflow() { // find_primary_keys and read_node use &self
        let (mut tree, _path) = setup_tree("delete_leaf_no_underflow");
        tree.insert(k("a"), pk("v_a")).unwrap();
        tree.insert(k("b"), pk("v_b")).unwrap();
        tree.insert(k("c"), pk("v_c")).unwrap();
        let deleted = tree.delete(&k("b"), None).unwrap();
        assert!(deleted);
        assert_eq!(tree.find_primary_keys(&k("b")).unwrap(), None);
        assert_eq!(tree.find_primary_keys(&k("a")).unwrap(), Some(vec![pk("v_a")]));
        let root_node = tree.read_node(tree.root_page_id).unwrap();
        if let BPlusTreeNode::Leaf { keys, ..} = root_node {
            assert_eq!(keys, vec![k("a"), k("c")]);
        } else { panic!("Should be leaf root"); }
    }

    #[test]
    fn test_delete_specific_pk_from_leaf() { // find_primary_keys uses &self
        let (mut tree, _path) = setup_tree("delete_specific_pk");
        tree.insert(k("a"), pk("v_a1")).unwrap();
        tree.insert(k("a"), pk("v_a2")).unwrap();
        tree.insert(k("a"), pk("v_a3")).unwrap();
        tree.insert(k("b"), pk("v_b1")).unwrap();
        let deleted_pk = tree.delete(&k("a"), Some(&pk("v_a2"))).unwrap();
        assert!(!deleted_pk);
        let pks_a = tree.find_primary_keys(&k("a")).unwrap().unwrap();
        assert_eq!(pks_a.len(), 2);
        assert!(pks_a.contains(&pk("v_a1")));
        assert!(!pks_a.contains(&pk("v_a2")));
        assert!(pks_a.contains(&pk("v_a3")));
        let deleted_key = tree.delete(&k("a"), None).unwrap();
        assert!(deleted_key);
        assert!(tree.find_primary_keys(&k("a")).unwrap().is_none());
    }

    #[test]
    fn test_delete_causing_underflow_simple_root_empty() { // find_primary_keys and read_node use &self
        let (mut tree, _path) = setup_tree("delete_root_empties");
        tree.insert(k("a"), pk("v_a")).unwrap();
        let deleted = tree.delete(&k("a"), None).unwrap();
        assert!(deleted);
        assert!(tree.find_primary_keys(&k("a")).unwrap().is_none());
        let root_node = tree.read_node(tree.root_page_id).unwrap();
        if let BPlusTreeNode::Leaf { keys, .. } = root_node {
            assert!(keys.is_empty(), "Root leaf should be empty but not removed");
        } else { panic!("Root should remain a leaf"); }
    }

    #[test]
    fn test_delete_leaf_borrow_from_right_sibling() { // find_primary_keys and read_node use &self
        let (mut tree, _path) = setup_tree("delete_leaf_borrow_right");
        tree.root_page_id = 2; tree.next_available_page_id = 3; tree.write_metadata().unwrap();
        let leaf0 = BPlusTreeNode::Leaf { page_id: 0, parent_page_id: Some(2), keys: vec![k("a")], values: vec![vec![pk("va")]], next_leaf: Some(1) };
        let leaf1 = BPlusTreeNode::Leaf { page_id: 1, parent_page_id: Some(2), keys: vec![k("c"), k("d"), k("e")], values: vec![vec![pk("vc")],vec![pk("vd")],vec![pk("ve")]], next_leaf: None };
        let root = BPlusTreeNode::Internal { page_id: 2, parent_page_id: None, keys: vec![k("c")], children: vec![0, 1] };
        tree.write_node(&leaf0).unwrap(); tree.write_node(&leaf1).unwrap(); tree.write_node(&root).unwrap();
        let deleted = tree.delete(&k("a"), None).unwrap();
        assert!(deleted);
        let new_leaf0 = tree.read_node(0).unwrap();
        let new_leaf1 = tree.read_node(1).unwrap();
        let new_root = tree.read_node(2).unwrap();
        if let BPlusTreeNode::Leaf { keys, .. } = new_leaf0 { assert_eq!(keys, vec![k("c")]); } else { panic!("Leaf0 not a leaf"); }
        if let BPlusTreeNode::Leaf { keys, .. } = new_leaf1 { assert_eq!(keys, vec![k("d"), k("e")]); } else { panic!("Leaf1 not a leaf"); }
        if let BPlusTreeNode::Internal { keys, .. } = new_root { assert_eq!(keys, vec![k("d")]); } else { panic!("Root not internal"); }
        assert_eq!(tree.find_primary_keys(&k("a")).unwrap(), None);
        assert_eq!(tree.find_primary_keys(&k("c")).unwrap(), Some(vec![pk("vc")]));
        assert_eq!(tree.find_primary_keys(&k("d")).unwrap(), Some(vec![pk("vd")]));
    }

    #[test]
    fn test_delete_leaf_borrow_from_left_sibling() { // find_primary_keys and read_node use &self
        let (mut tree, _path) = setup_tree("delete_leaf_borrow_left");
        tree.root_page_id = 2; tree.next_available_page_id = 3; tree.write_metadata().unwrap();
        let leaf0 = BPlusTreeNode::Leaf { page_id: 0, parent_page_id: Some(2), keys: vec![k("a"), k("b")], values: vec![vec![pk("va")],vec![pk("vb")]], next_leaf: Some(1) };
        let leaf1 = BPlusTreeNode::Leaf { page_id: 1, parent_page_id: Some(2), keys: vec![k("d"), k("e")], values: vec![vec![pk("vd")],vec![pk("ve")]], next_leaf: None };
        let root = BPlusTreeNode::Internal { page_id: 2, parent_page_id: None, keys: vec![k("d")], children: vec![0, 1] };
        tree.write_node(&leaf0).unwrap(); tree.write_node(&leaf1).unwrap(); tree.write_node(&root).unwrap();
        assert!(tree.delete(&k("e"), None).unwrap());
        assert!(tree.delete(&k("d"), None).unwrap());
        let new_leaf0 = tree.read_node(0).unwrap();
        let new_leaf1 = tree.read_node(1).unwrap();
        let new_root = tree.read_node(2).unwrap();
        if let BPlusTreeNode::Leaf { keys, .. } = new_leaf0 { assert_eq!(keys, vec![k("a")]); } else { panic!("Leaf0 not a leaf"); }
        if let BPlusTreeNode::Leaf { keys, .. } = new_leaf1 { assert_eq!(keys, vec![k("b")]); } else { panic!("Leaf1 not a leaf"); }
        if let BPlusTreeNode::Internal { keys, .. } = new_root { assert_eq!(keys, vec![k("b")]); } else { panic!("Root not internal"); }
        assert_eq!(tree.find_primary_keys(&k("d")).unwrap(), None);
        assert_eq!(tree.find_primary_keys(&k("e")).unwrap(), None);
        assert_eq!(tree.find_primary_keys(&k("b")).unwrap(), Some(vec![pk("vb")]));
        assert_eq!(tree.find_primary_keys(&k("a")).unwrap(), Some(vec![pk("va")]));
    }

    #[test]
    fn test_delete_internal_borrow_from_right_sibling() { // read_node uses &self
        let (mut tree, _path) = setup_tree("delete_internal_borrow_right");
        tree.root_page_id = 4; tree.next_available_page_id = 10; tree.write_metadata().unwrap();
        let p0 = BPlusTreeNode::Leaf { page_id: 5, parent_page_id: Some(0), keys: vec![k("A")], values: vec![vec![pk("vA")]], next_leaf: Some(6) };
        let p1 = BPlusTreeNode::Leaf { page_id: 6, parent_page_id: Some(0), keys: vec![k("C")], values: vec![vec![pk("vC")]], next_leaf: Some(7) };
        let p2 = BPlusTreeNode::Leaf { page_id: 7, parent_page_id: Some(1), keys: vec![k("F")], values: vec![vec![pk("vF")]], next_leaf: Some(8) };
        let p3 = BPlusTreeNode::Leaf { page_id: 8, parent_page_id: Some(1), keys: vec![k("H")], values: vec![vec![pk("vH")]], next_leaf: Some(9) };
        let p4 = BPlusTreeNode::Leaf { page_id: 9, parent_page_id: Some(1), keys: vec![k("J")], values: vec![vec![pk("vJ")]], next_leaf: None };
        tree.write_node(&p0).unwrap(); tree.write_node(&p1).unwrap(); tree.write_node(&p2).unwrap(); tree.write_node(&p3).unwrap(); tree.write_node(&p4).unwrap();
        let mut l0_internal = BPlusTreeNode::Internal { page_id: 0, parent_page_id: Some(4), keys: vec![], children: vec![5] };
        let mut l1_internal = BPlusTreeNode::Internal { page_id: 1, parent_page_id: Some(4), keys: vec![k("G"), k("I")], children: vec![7,8,9] };
        let mut root_internal = BPlusTreeNode::Internal { page_id: 4, parent_page_id: None, keys: vec![k("E")], children: vec![0,1] };
        tree.write_node(&l0_internal).unwrap(); tree.write_node(&l1_internal).unwrap(); tree.write_node(&root_internal).unwrap();
        tree.borrow_from_sibling(&mut l0_internal, &mut l1_internal, &mut root_internal, 0, false).unwrap();
        assert_eq!(l0_internal.get_keys(), &vec![k("E")]);
        assert_eq!(l0_internal.get_children().unwrap(), &vec![5, 7]);
        let child_p2_updated = tree.read_node(7).unwrap();
        assert_eq!(child_p2_updated.get_parent_page_id(), Some(0));
        assert_eq!(l1_internal.get_keys(), &vec![k("I")]);
        assert_eq!(l1_internal.get_children().unwrap(), &vec![8, 9]);
        assert_eq!(root_internal.get_keys(), &vec![k("G")]);
    }

    #[test]
    fn test_delete_internal_borrow_from_left_sibling() { // read_node uses &self
        let (mut tree, _path) = setup_tree("delete_internal_borrow_left");
        tree.root_page_id = 4; tree.next_available_page_id = 10; tree.write_metadata().unwrap();
        let p0=BPlusTreeNode::Leaf{page_id:5,parent_page_id:Some(0),keys:vec![k("A")],values:vec![vec![pk("vA")]],next_leaf:Some(6)};
        let p1=BPlusTreeNode::Leaf{page_id:6,parent_page_id:Some(0),keys:vec![k("C")],values:vec![vec![pk("vC")]],next_leaf:Some(7)};
        let p2=BPlusTreeNode::Leaf{page_id:7,parent_page_id:Some(0),keys:vec![k("F")],values:vec![vec![pk("vF")]],next_leaf:Some(8)};
        let p3=BPlusTreeNode::Leaf{page_id:8,parent_page_id:Some(1),keys:vec![k("H")],values:vec![vec![pk("vH")]],next_leaf:None};
        tree.write_node(&p0).unwrap(); tree.write_node(&p1).unwrap(); tree.write_node(&p2).unwrap(); tree.write_node(&p3).unwrap();
        let mut l0_internal = BPlusTreeNode::Internal { page_id: 0, parent_page_id: Some(4), keys: vec![k("B"), k("D")], children: vec![5,6,7] };
        let mut l1_internal = BPlusTreeNode::Internal { page_id: 1, parent_page_id: Some(4), keys: vec![], children: vec![8] };
        let mut root_internal = BPlusTreeNode::Internal { page_id: 4, parent_page_id: None, keys: vec![k("G")], children: vec![0,1] };
        tree.write_node(&l0_internal).unwrap(); tree.write_node(&l1_internal).unwrap(); tree.write_node(&root_internal).unwrap();
        tree.borrow_from_sibling(&mut l1_internal, &mut l0_internal, &mut root_internal, 0, true).unwrap();
        assert_eq!(l1_internal.get_keys(), &vec![k("G")]);
        assert_eq!(l1_internal.get_children().unwrap(), &vec![7, 8]);
        let child_p2_updated = tree.read_node(7).unwrap();
        assert_eq!(child_p2_updated.get_parent_page_id(), Some(1));
        assert_eq!(l0_internal.get_keys(), &vec![k("B")]);
        assert_eq!(l0_internal.get_children().unwrap(), &vec![5, 6]);
        assert_eq!(root_internal.get_keys(), &vec![k("D")]);
    }

    #[test]
    fn test_delete_leaf_merge_with_right_sibling() { // find_primary_keys and read_node use &self
        let (mut tree, _path) = setup_tree("delete_leaf_merge_right");
        tree.root_page_id = 2; tree.next_available_page_id = 3; tree.write_metadata().unwrap();
        let l0 = BPlusTreeNode::Leaf { page_id:0, parent_page_id:Some(2), keys:vec![k("A"),k("B")], values:vec![vec![pk("vA")],vec![pk("vB")]], next_leaf:Some(1) };
        let l1 = BPlusTreeNode::Leaf { page_id:1, parent_page_id:Some(2), keys:vec![k("D"),k("E")], values:vec![vec![pk("vD")],vec![pk("vE")]], next_leaf:None };
        let r = BPlusTreeNode::Internal { page_id:2, parent_page_id:None, keys:vec![k("D")], children:vec![0,1] };
        tree.write_node(&l0).unwrap(); tree.write_node(&l1).unwrap(); tree.write_node(&r).unwrap();
        assert!(tree.delete(&k("B"), None).unwrap());
        assert!(tree.delete(&k("A"), None).unwrap());
        let final_root_node = tree.read_node(tree.root_page_id).unwrap();
        assert_eq!(tree.root_page_id, 0);
        if let BPlusTreeNode::Leaf { page_id, keys, values, next_leaf, parent_page_id } = final_root_node {
            assert_eq!(page_id, 0);
            assert!(parent_page_id.is_none());
            assert_eq!(keys, vec![k("D"), k("E")]);
            assert_eq!(values, vec![vec![pk("vD")], vec![pk("vE")]]);
            assert!(next_leaf.is_none());
        } else { panic!("New root should be a leaf node (the merged L0). Actual: {:?}", final_root_node); }
        assert!(tree.next_available_page_id >= 3);
    }

    #[test]
    fn test_delete_leaf_merge_with_left_sibling() { // find_primary_keys and read_node use &self
        let (mut tree, _path) = setup_tree("delete_leaf_merge_left");
        tree.root_page_id = 2; tree.next_available_page_id = 3; tree.write_metadata().unwrap();
        let l0_orig = BPlusTreeNode::Leaf { page_id:0, parent_page_id:Some(2), keys:vec![k("A"),k("B")], values:vec![vec![pk("vA")],vec![pk("vB")]], next_leaf:Some(1) };
        let l1_orig = BPlusTreeNode::Leaf { page_id:1, parent_page_id:Some(2), keys:vec![k("C"),k("D")], values:vec![vec![pk("vC")],vec![pk("vD")]], next_leaf:None };
        let r_orig = BPlusTreeNode::Internal { page_id:2, parent_page_id:None, keys:vec![k("C")], children:vec![0,1] };
        tree.write_node(&l0_orig).unwrap(); tree.write_node(&l1_orig).unwrap(); tree.write_node(&r_orig).unwrap();
        assert!(tree.delete(&k("C"), Some(&pk("vC"))).unwrap() == false);
        assert!(tree.delete(&k("D"), None).unwrap());
        let mut l0_modified = tree.read_node_mut(0).unwrap(); // read_node_mut for modification
        if let BPlusTreeNode::Leaf{keys, values, ..} = &mut l0_modified {
            keys.remove(1);
            values.remove(1);
        }
        tree.write_node(&l0_modified).unwrap();
        assert!(tree.delete(&k("C"), None).unwrap());
        let final_root_node = tree.read_node(tree.root_page_id).unwrap();
        assert_eq!(tree.root_page_id, 0);
        if let BPlusTreeNode::Leaf { page_id, keys, values, next_leaf, parent_page_id } = final_root_node {
            assert_eq!(page_id, 0);
            assert!(parent_page_id.is_none());
            assert_eq!(keys, vec![k("A")]);
            assert_eq!(values, vec![vec![pk("vA")]]);
            assert!(next_leaf.is_none());
        } else { panic!("New root should be a leaf node (L0). Actual: {:?}", final_root_node); }
    }

    #[test]
    fn test_delete_internal_node_merge() { // read_node uses &self
        let (mut tree, _path) = setup_tree("delete_internal_merge");
        tree.root_page_id = 6; tree.next_available_page_id = 7; tree.write_metadata().unwrap();
        let l0 = BPlusTreeNode::Leaf {page_id:2, parent_page_id:Some(0), keys:vec![k("A")], values:vec![vec![pk("vA")]], next_leaf:Some(3)};
        let l1 = BPlusTreeNode::Leaf {page_id:3, parent_page_id:Some(0), keys:vec![k("C")], values:vec![vec![pk("vC")]], next_leaf:Some(4)};
        let l2 = BPlusTreeNode::Leaf {page_id:4, parent_page_id:Some(1), keys:vec![k("F")], values:vec![vec![pk("vF")]], next_leaf:Some(5)};
        let l3 = BPlusTreeNode::Leaf {page_id:5, parent_page_id:Some(1), keys:vec![k("H")], values:vec![vec![pk("vH")]], next_leaf:None};
        tree.write_node(&l0).unwrap(); tree.write_node(&l1).unwrap(); tree.write_node(&l2).unwrap(); tree.write_node(&l3).unwrap();
        let p1_internal = BPlusTreeNode::Internal {page_id:0, parent_page_id:Some(6), keys:vec![k("B")], children:vec![2,3]};
        let p2_internal = BPlusTreeNode::Internal {page_id:1, parent_page_id:Some(6), keys:vec![], children:vec![4]};
        let l3_for_p2_empty_setup = BPlusTreeNode::Leaf {page_id:5, parent_page_id:Some(1), keys:vec![], values:vec![], next_leaf:None};
        tree.write_node(&l3_for_p2_empty_setup).unwrap();
        let gp_root_internal = BPlusTreeNode::Internal {page_id:6, parent_page_id:None, keys:vec![k("E")], children:vec![0,1]};
        tree.write_node(&p1_internal).unwrap(); tree.write_node(&p2_internal).unwrap(); tree.write_node(&gp_root_internal).unwrap();
        let mut p1_for_merge = tree.read_node_mut(0).unwrap(); // read_node_mut
        let mut p2_for_merge = tree.read_node_mut(1).unwrap(); // read_node_mut
        let mut gp_for_merge = tree.read_node_mut(6).unwrap(); // read_node_mut
        tree.merge_nodes(&mut p1_for_merge, &mut p2_for_merge, &mut gp_for_merge, 0).unwrap();
        let mut path_to_gp = vec![gp_for_merge.get_page_id()];
        tree.handle_underflow(gp_for_merge, path_to_gp).unwrap();
        assert_eq!(tree.root_page_id, 0, "Root page ID should be P1's page ID");
        let final_root_node = tree.read_node(tree.root_page_id).unwrap(); // uses &self read_node
        if let BPlusTreeNode::Internal { page_id, keys, children, parent_page_id } = final_root_node {
            assert_eq!(page_id, 0);
            assert!(parent_page_id.is_none());
            assert_eq!(keys, vec![k("B"), k("E")]);
            assert_eq!(children, vec![2,3,4]);
            let child_l2_updated = tree.read_node(4).unwrap(); // uses &self read_node
            assert_eq!(child_l2_updated.get_parent_page_id(), Some(0));
        } else { panic!("New root should be an internal node (the merged P1). Actual: {:?}", final_root_node); }
    }
}
