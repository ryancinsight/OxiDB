use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::path::PathBuf; // Path is not directly used, PathBuf is sufficient
// use std::collections::HashMap; // Not using a cache for now

use crate::core::indexing::btree::node::{
    BPlusTreeNode, KeyType, PageId, PrimaryKey, SerializationError,
};

use std::sync::Mutex; // Import Mutex

// Constants
/// The size of a page in bytes.
const PAGE_SIZE: u64 = 4096;
/// Sentinel Page ID to signify the end of the free list or no page.
const SENTINEL_PAGE_ID: PageId = u64::MAX;
/// The size of the metadata stored at the beginning of the B+Tree file.
/// order (u32) + root_page_id (u64) + next_available_page_id (u64) + free_list_head_page_id (u64)
const METADATA_SIZE: u64 = 4 + 8 + 8 + 8;

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
    pub free_list_head_page_id: PageId, // Changed to PageId, SENTINEL_PAGE_ID for None
    pub file_handle: Mutex<File>,
}

impl BPlusTreeIndex {
    pub fn new(name: String, path: PathBuf, order: usize) -> Result<Self, OxidbError> {
        let file_exists = path.exists();
        let mut file_obj = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(!file_exists) // If creating new, truncate. Otherwise, preserve for loading.
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

            file_obj.read_exact(&mut u64_buf)?;
            let free_list_head_page_id = u64::from_be_bytes(u64_buf);

            Ok(Self {
                name,
                path,
                order: loaded_order,
                root_page_id,
                next_available_page_id,
                free_list_head_page_id,
                file_handle: Mutex::new(file_obj),
            })

        } else {
            if order < 3 {
                return Err(OxidbError::TreeLogicError(format!("Order {} is too small. Minimum order is 3.", order)));
            }
            let root_page_id = 0; // Root always starts at page 0
            let next_available_page_id = 1; // Page 0 is root, so next is 1
            let free_list_head_page_id = SENTINEL_PAGE_ID; // No free pages initially
            let file_handle_mutex = Mutex::new(file_obj);
            let mut tree = Self {
                name,
                path,
                order,
                root_page_id,
                next_available_page_id,
                free_list_head_page_id,
                file_handle: file_handle_mutex,
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
        let mut file = self.file_handle.lock().map_err(|e| OxidbError::BorrowError(format!("Mutex lock error: {}", e)))?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&(u32::try_from(self.order).map_err(|_| OxidbError::Serialization(SerializationError::InvalidFormat("Order too large for u32".to_string())))?).to_be_bytes())?;
        file.write_all(&self.root_page_id.to_be_bytes())?;
        file.write_all(&self.next_available_page_id.to_be_bytes())?;
        file.write_all(&self.free_list_head_page_id.to_be_bytes())?;
        file.flush()?;
        Ok(())
    }

    pub fn allocate_new_page_id(&mut self) -> Result<PageId, OxidbError> {
        if self.free_list_head_page_id != SENTINEL_PAGE_ID {
            let new_page_id = self.free_list_head_page_id;
            // Read the first 8 bytes of this page to get the next free page ID
            let mut file = self.file_handle.lock().map_err(|e| OxidbError::BorrowError(format!("Mutex lock error for allocate (read free list): {}", e)))?;
            let offset = PAGE_SIZE.saturating_add(new_page_id.saturating_mul(PAGE_SIZE));
            file.seek(SeekFrom::Start(offset))?;
            let mut next_free_buf = [0u8; 8];
            file.read_exact(&mut next_free_buf)?;
            self.free_list_head_page_id = PageId::from_be_bytes(next_free_buf);
            // No need to explicitly clear the rest of the page, it will be overwritten by new node data.
            // Drop file lock before calling write_metadata
            drop(file);
            self.write_metadata()?;
            Ok(new_page_id)
        } else {
            let new_page_id = self.next_available_page_id;
            self.next_available_page_id = self.next_available_page_id.saturating_add(1);
            self.write_metadata()?; // This locks and unlocks the file handle itself.
            Ok(new_page_id)
        }
    }

    fn deallocate_page_id(&mut self, page_id_to_free: PageId) -> Result<(), OxidbError> {
        if page_id_to_free == SENTINEL_PAGE_ID {
            return Err(OxidbError::TreeLogicError("Cannot deallocate sentinel page ID".to_string()));
        }
        // The page_id_to_free will now point to the current head of the free list.
        // Its first 8 bytes should store the *next* free page, which is the current free_list_head_page_id.
        let mut file = self.file_handle.lock().map_err(|e| OxidbError::BorrowError(format!("Mutex lock error for deallocate: {}", e)))?;
        let offset = PAGE_SIZE.saturating_add(page_id_to_free.saturating_mul(PAGE_SIZE));
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(&self.free_list_head_page_id.to_be_bytes())?; // Write old head as next pointer

        // The new head of the free list is the page we just deallocated.
        self.free_list_head_page_id = page_id_to_free;
        // Drop file lock before calling write_metadata
        drop(file);
        self.write_metadata()?;
        Ok(())
    }

    pub fn read_node(&self, page_id: PageId) -> Result<BPlusTreeNode, OxidbError> {
        let mut file = self.file_handle.lock().map_err(|e| OxidbError::BorrowError(format!("Mutex lock error: {}", e)))?;
        let offset = PAGE_SIZE.saturating_add(page_id.saturating_mul(PAGE_SIZE));
        file.seek(SeekFrom::Start(offset))?;
        let page_size_usize = usize::try_from(PAGE_SIZE).map_err(|_| OxidbError::Serialization(SerializationError::InvalidFormat("PAGE_SIZE too large for usize".to_string())))?;
        let mut page_buffer = vec![0u8; page_size_usize];
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
        let mut file = self.file_handle.lock().map_err(|e| OxidbError::BorrowError(format!("Mutex lock error: {}", e)))?;
        let page_id = node.get_page_id();
        let offset = PAGE_SIZE.saturating_add(page_id.saturating_mul(PAGE_SIZE));
        let mut node_bytes = node.to_bytes()?;
        let page_size_usize = usize::try_from(PAGE_SIZE).map_err(|_| OxidbError::Serialization(SerializationError::InvalidFormat("PAGE_SIZE too large for usize".to_string())))?;
        if node_bytes.len() > page_size_usize {
            return Err(OxidbError::PageFull(format!(
                "Serialized node size {} exceeds PAGE_SIZE {}",
                node_bytes.len(),
                PAGE_SIZE
            )));
        }
        node_bytes.resize(page_size_usize, 0);
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(&node_bytes)?;
        file.flush()?;
        Ok(())
    }

    /// Finds the leaf node for a given key and records the path taken.
    pub fn find_leaf_node_path(&self, key: &KeyType, path: &mut Vec<PageId>) -> Result<BPlusTreeNode, OxidbError> {
        path.clear();
        let mut current_page_id = self.root_page_id;
        loop {
            path.push(current_page_id);
            let current_node = self.read_node(current_page_id)?;
            match current_node {
                BPlusTreeNode::Internal { ref keys, ref children, .. } => {
                    let child_idx = keys.partition_point(|k_partition| k_partition.as_slice() <= key.as_slice());
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
            BPlusTreeNode::Leaf { keys, values, .. } => {
                match keys.binary_search(key) {
                    Ok(idx) => Ok(Some(values[idx].clone())),
                    Err(_) => Ok(None),
                }
            }
            _ => unreachable!("find_leaf_node_path should always return a Leaf node"),
        }
    }

    pub fn insert(&mut self, key: KeyType, value: PrimaryKey) -> Result<(), OxidbError> {
        let mut path_to_leaf: Vec<PageId> = Vec::new();
        let _ = self.find_leaf_node_path(&key, &mut path_to_leaf)?; // This populates path_to_leaf
        let leaf_page_id = *path_to_leaf.last().ok_or(OxidbError::TreeLogicError("Path to leaf is empty".to_string()))?;
        let mut current_leaf_node = self.read_node_mut(leaf_page_id)?;
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
            self.handle_split(current_leaf_node, path_to_leaf)?
        } else {
            self.write_node(&current_leaf_node)?;
        }
        Ok(())
    }

    /// Reads a node from disk, making it mutable.
    fn read_node_mut(&mut self, page_id: PageId) -> Result<BPlusTreeNode, OxidbError> {
        let mut file = self.file_handle.lock().map_err(|e| OxidbError::BorrowError(format!("Mutex lock error: {}", e)))?;
        let offset = PAGE_SIZE.saturating_add(page_id.saturating_mul(PAGE_SIZE));
        file.seek(SeekFrom::Start(offset))?;
        let page_size_usize = usize::try_from(PAGE_SIZE).map_err(|_| OxidbError::Serialization(SerializationError::InvalidFormat("PAGE_SIZE too large for usize".to_string())))?;
        let mut page_buffer = vec![0u8; page_size_usize];
        match file.read_exact(&mut page_buffer) {
            Ok(_) => {},
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Err(OxidbError::NodeNotFound(page_id)),
            Err(e) => return Err(OxidbError::Io(e)),
        }
        BPlusTreeNode::from_bytes(&page_buffer).map_err(OxidbError::from)
    }

    /// Handles splitting a node when it becomes full.
    /// This involves creating a new sibling, distributing keys/children,
    /// and updating the parent or creating a new root if necessary.
    fn handle_split(&mut self, mut node_to_split: BPlusTreeNode, mut path: Vec<PageId>) -> Result<(), OxidbError> {
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
            let mut parent_node = self.read_node_mut(parent_page_id)?;
            match &mut parent_node {
                BPlusTreeNode::Internal { keys, children, ..} => {
                    let insertion_point = keys.partition_point(|k| k.as_slice() < promoted_or_copied_key.as_slice());
                    keys.insert(insertion_point, promoted_or_copied_key);
                    children.insert(insertion_point.saturating_add(1), new_sibling_page_id);
                    if parent_node.get_keys().len() >= self.order {
                        self.handle_split(parent_node, path)
                    } else {
                        self.write_node(&parent_node)
                    }
                }
                _ => Err(OxidbError::UnexpectedNodeType), // Parent must be internal
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
        let _ = self.find_leaf_node_path(key_to_delete, &mut path)?; // Populates path
        let leaf_page_id = *path.last().ok_or(OxidbError::TreeLogicError("Path to leaf is empty for delete".to_string()))?;
        let mut leaf_node = self.read_node_mut(leaf_page_id)?;
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
                            } // else: pk_ref not found, modification_made remains false
                        } else { // pk_to_remove is None, remove all PKs for this key
                            keys.remove(idx);
                            values.remove(idx);
                            key_removed_from_structure = true;
                            modification_made = true;
                        }
                    }
                    Err(_) => { /* Key not found, modification_made remains false */ }
                }
            }
            _ => return Err(OxidbError::UnexpectedNodeType),
        }

        if modification_made {
            if key_removed_from_structure && leaf_node.get_keys().len() < self.min_keys_for_node() && leaf_page_id != self.root_page_id {
                self.handle_underflow(leaf_node, path)?;
            } else {
                // This covers:
                // 1. Not underflow (even if key_removed_from_structure is true).
                // 2. Is root (and not an internal node that needs shrinking).
                // 3. Only a PK was removed, not the whole key (key_removed_from_structure is false).
                self.write_node(&leaf_node)?;
            }
        }
        Ok(modification_made)
    }

    /// Calculates the minimum number of keys a non-root node should have.
    fn min_keys_for_node(&self) -> usize {
        self.order.saturating_sub(1) / 2
    }

    /// Handles node underflow after a delete operation.
    /// This might involve borrowing from a sibling or merging with a sibling.
    /// It can recursively call itself if the parent node also underflows.
    fn handle_underflow(&mut self, mut current_node: BPlusTreeNode, mut path: Vec<PageId>) -> Result<(), OxidbError> {
        let current_node_pid = path.pop().ok_or_else(|| OxidbError::TreeLogicError("Path cannot be empty".to_string()))?;
        if current_node_pid == self.root_page_id {
            // If the root itself is underflowing (e.g., an internal root with only one child after a merge)
            if let BPlusTreeNode::Internal { ref keys, ref children, .. } = current_node {
                if keys.is_empty() && children.len() == 1 {
                    let old_root_page_id = self.root_page_id;
                    self.root_page_id = children[0]; // The only child becomes the new root
                    let mut new_root_node = self.read_node_mut(self.root_page_id)?;
                    new_root_node.set_parent_page_id(None);
                    self.write_node(&new_root_node)?;
                    self.write_metadata()?; // Persist change to root_page_id
                    self.deallocate_page_id(old_root_page_id)?; // Deallocate the old root page
                } else if keys.is_empty() && children.is_empty() && !matches!(current_node, BPlusTreeNode::Leaf{..}) {
                    // This case should ideally not happen if merge logic is correct (root becomes leaf).
                    // But if it does, and root is internal and completely empty, it's a problem.
                    // For now, we'll assume the above case (one child) or root becoming leaf is handled.
                }
            } // If root is a leaf, it can be empty. No action needed for deallocation unless it's merged away (not possible for root).
            return Ok(());
        }

        let parent_pid = *path.last().ok_or_else(|| OxidbError::TreeLogicError("Parent not found for non-root underflow".to_string()))?;
        let mut parent_node = self.read_node_mut(parent_pid)?;

        let parent_children = parent_node.get_children().map_err(|e| OxidbError::TreeLogicError(e.to_string()))?;
        let child_idx_in_parent = parent_children.iter().position(|&child_pid| child_pid == current_node_pid)
            .ok_or_else(|| OxidbError::TreeLogicError("Child not found in parent during underflow handling".to_string()))?;

        // Try to borrow from left sibling
        if child_idx_in_parent > 0 {
            let left_sibling_pid = parent_children[child_idx_in_parent.saturating_sub(1)];
            let mut left_sibling_node = self.read_node_mut(left_sibling_pid)?;
            if left_sibling_node.get_keys().len() > self.min_keys_for_node() {
                self.borrow_from_sibling(&mut current_node, &mut left_sibling_node, &mut parent_node, child_idx_in_parent.saturating_sub(1), true)?;
                return Ok(());
            }
        }

        // Try to borrow from right sibling
        if child_idx_in_parent < parent_children.len().saturating_sub(1) {
            let right_sibling_pid = parent_children[child_idx_in_parent.saturating_add(1)];
            let mut right_sibling_node = self.read_node_mut(right_sibling_pid)?;
            if right_sibling_node.get_keys().len() > self.min_keys_for_node() {
                self.borrow_from_sibling(&mut current_node, &mut right_sibling_node, &mut parent_node, child_idx_in_parent, false)?;
                return Ok(());
            }
        }

        // Merge if borrowing failed
        if child_idx_in_parent > 0 { // Merge with left sibling
            let left_sibling_pid = parent_children[child_idx_in_parent.saturating_sub(1)];
            let mut left_sibling_node = self.read_node_mut(left_sibling_pid)?;
            self.merge_nodes(&mut left_sibling_node, &mut current_node, &mut parent_node, child_idx_in_parent.saturating_sub(1))?;
        } else { // Merge with right sibling
            let right_sibling_pid = parent_children[child_idx_in_parent.saturating_add(1)];
            let mut right_sibling_node = self.read_node_mut(right_sibling_pid)?;
            self.merge_nodes(&mut current_node, &mut right_sibling_node, &mut parent_node, child_idx_in_parent)?;
        }

        // After merge, parent might underflow
        if parent_node.get_keys().len() < self.min_keys_for_node() && parent_pid != self.root_page_id {
            self.handle_underflow(parent_node, path)?;
        } else if parent_pid == self.root_page_id && parent_node.get_keys().is_empty() && matches!(parent_node, BPlusTreeNode::Internal{..}) {
            // If parent was root and became empty internal node
            if let BPlusTreeNode::Internal { ref children, .. } = parent_node {
                if children.len() == 1 { // Root internal node has only one child left
                    let old_root_pid = parent_pid; // parent_pid is the root_page_id here
                    self.root_page_id = children[0];
                    let mut new_root_node = self.read_node_mut(self.root_page_id)?;
                    new_root_node.set_parent_page_id(None);
                    self.write_node(&new_root_node)?;
                    self.write_metadata()?;
                    self.deallocate_page_id(old_root_pid)?;
                } else { // Root internal node still has enough children or is not empty
                     self.write_node(&parent_node)?;
                }
            } else { // Parent is root leaf or non-empty internal root (should have been written if modified)
                 self.write_node(&parent_node)?; // Ensure it's written if modified
            }
        } else { // Parent is not root and did not underflow, or is root leaf (and not empty)
            self.write_node(&parent_node)?;
        }
        Ok(())
    }

    /// Borrows a key from a sibling node to resolve underflow.
    fn borrow_from_sibling(
        &mut self,
        underflowed_node: &mut BPlusTreeNode,
        lender_sibling: &mut BPlusTreeNode,
        parent_node: &mut BPlusTreeNode,
        parent_key_idx: usize, // Index of the key in parent that separates underflowed_node and lender_sibling
        is_left_lender: bool,
    ) -> Result<(), OxidbError> {
        match (&mut *underflowed_node, &mut *lender_sibling, &mut *parent_node) {
            ( // Both are Leaf nodes
                BPlusTreeNode::Leaf { keys: u_keys, values: u_values, .. },
                BPlusTreeNode::Leaf { keys: l_keys, values: l_values, .. },
                BPlusTreeNode::Internal { keys: p_keys, .. }
            ) => {
                if is_left_lender { // Borrow from left sibling
                    let borrowed_key = l_keys.pop().ok_or(OxidbError::TreeLogicError("Lender leaf (left) empty".to_string()))?;
                    let borrowed_value = l_values.pop().ok_or(OxidbError::TreeLogicError("Lender leaf (left) values empty".to_string()))?;
                    u_keys.insert(0, borrowed_key.clone());
                    u_values.insert(0, borrowed_value);
                    p_keys[parent_key_idx] = borrowed_key; // Update parent separator key
                } else { // Borrow from right sibling
                    let borrowed_key = l_keys.remove(0); // Key from sibling
                    let borrowed_value = l_values.remove(0); // Value from sibling
                    u_keys.push(borrowed_key.clone());
                    u_values.push(borrowed_value);
                    // The new separator key in parent is the smallest key in the right sibling (lender)
                    p_keys[parent_key_idx] = l_keys.first().ok_or(OxidbError::TreeLogicError("Lender leaf (right) became empty".to_string()))?.clone();
                }
            },
            ( // Both are Internal nodes
                BPlusTreeNode::Internal { page_id: u_pid_val, keys: u_keys, children: u_children, .. },
                BPlusTreeNode::Internal { keys: l_keys, children: l_children, .. },
                BPlusTreeNode::Internal { keys: p_keys, .. }
            ) => {
                if is_left_lender { // Borrow from left sibling
                    // Key from parent comes down to underflowed node
                    let key_from_parent = p_keys.remove(parent_key_idx); // This is the key separating left_lender and underflowed_node
                    u_keys.insert(0, key_from_parent);
                    // Rightmost key from left_lender goes up to parent
                    let new_separator_for_parent = l_keys.pop().ok_or(OxidbError::TreeLogicError("Lender internal (left) empty".to_string()))?;
                    p_keys.insert(parent_key_idx, new_separator_for_parent);
                    // Rightmost child of left_lender becomes leftmost child of underflowed_node
                    let child_to_move = l_children.pop().ok_or(OxidbError::TreeLogicError("Lender internal (left) children empty".to_string()))?;
                    u_children.insert(0, child_to_move);
                    // Update parent of moved child
                    let mut moved_child_node = self.read_node_mut(child_to_move)?;
                    moved_child_node.set_parent_page_id(Some(*u_pid_val));
                    self.write_node(&moved_child_node)?;
                } else { // Borrow from right sibling
                    // Key from parent comes down to underflowed node
                    let key_from_parent = p_keys.remove(parent_key_idx); // This is the key separating underflowed_node and right_lender
                    u_keys.push(key_from_parent);
                    // Leftmost key from right_lender goes up to parent
                    let new_separator_for_parent = l_keys.remove(0);
                    p_keys.insert(parent_key_idx, new_separator_for_parent);
                     // Leftmost child of right_lender becomes rightmost child of underflowed_node
                    let child_to_move = l_children.remove(0);
                    u_children.push(child_to_move);
                    // Update parent of moved child
                    let mut moved_child_node = self.read_node_mut(child_to_move)?;
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

    /// Merges two sibling nodes with a key from the parent.
    fn merge_nodes(
        &mut self,
        left_node: &mut BPlusTreeNode, // This node will absorb the right_node
        right_node: &mut BPlusTreeNode, // This node will be absorbed and effectively deleted
        parent_node: &mut BPlusTreeNode,
        parent_key_idx: usize, // Index of the key in parent that separates left_node and right_node
    ) -> Result<(), OxidbError> {
        match (&mut *left_node, &mut *right_node, &mut *parent_node) {
            ( // Both are Leaf nodes
                BPlusTreeNode::Leaf { keys: l_keys, values: l_values, next_leaf: l_next_leaf, .. },
                BPlusTreeNode::Leaf { keys: r_keys, values: r_values, next_leaf: r_next_leaf, .. },
                BPlusTreeNode::Internal { keys: p_keys, children: p_children, .. }
            ) => {
                // Key from parent is NOT added to leaf nodes during merge.
                l_keys.append(r_keys);
                l_values.append(r_values);
                *l_next_leaf = *r_next_leaf; // Update linked list

                p_keys.remove(parent_key_idx);
                p_children.remove(parent_key_idx.saturating_add(1)); // Remove pointer to the right_node
            },
            ( // Both are Internal nodes
                BPlusTreeNode::Internal { page_id: l_pid_val, keys: l_keys, children: l_children, .. },
                BPlusTreeNode::Internal { keys: r_keys, children: r_children_original, .. },
                BPlusTreeNode::Internal { keys: p_keys, children: p_children, .. }
            ) => {
                // Key from parent comes down into the merged left_node
                let key_from_parent = p_keys.remove(parent_key_idx);
                l_keys.push(key_from_parent);
                l_keys.append(r_keys);

                let children_to_move = r_children_original.clone(); // Clone to avoid borrow checker issues
                l_children.append(r_children_original); // Move children from right to left

                // Update parent_page_id for all moved children
                for child_pid_to_update in children_to_move {
                    let mut child_node = self.read_node_mut(child_pid_to_update)?;
                    child_node.set_parent_page_id(Some(*l_pid_val));
                    self.write_node(&child_node)?;
                }

                p_children.remove(parent_key_idx.saturating_add(1)); // Remove pointer to the right_node
            },
            _ => return Err(OxidbError::TreeLogicError("Node types mismatch during merge, or parent is not Internal.".to_string())),
        }
        self.write_node(left_node)?; // Write modified left_node (which absorbed right_node)
        // parent_node is written by the caller handle_underflow or its recursive calls
        // self.write_node(parent_node)?; // Write modified parent_node - this is done by the caller

        // Deallocate the page of the right_node which has been merged.
        let right_node_pid = right_node.get_page_id();
        self.deallocate_page_id(right_node_pid)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::{tempdir, TempDir};

    fn k(s: &str) -> KeyType { s.as_bytes().to_vec() }
    fn pk(s: &str) -> PrimaryKey { s.as_bytes().to_vec() }

    const TEST_TREE_ORDER: usize = 4;

    fn setup_tree(test_name: &str) -> (BPlusTreeIndex, PathBuf, TempDir) {
        let dir = tempdir().expect("Failed to create tempdir for test");
        let path = dir.path().join(format!("{}.db", test_name));
        if path.exists() { fs::remove_file(&path).expect("Failed to remove existing test db file"); }
        let tree = BPlusTreeIndex::new(test_name.to_string(), path.clone(), TEST_TREE_ORDER).expect("Failed to create BPlusTreeIndex");
        (tree, path, dir)
    }

    #[test]
    fn test_new_tree_creation() {
        let (tree, path, _dir) = setup_tree("test_new");
        assert_eq!(tree.order, TEST_TREE_ORDER);
        assert_eq!(tree.root_page_id, 0);
        assert_eq!(tree.next_available_page_id, 1); // Initial root is page 0, next is 1
        assert_eq!(tree.free_list_head_page_id, SENTINEL_PAGE_ID); // Initially no free pages

        let mut file = File::open(&path).expect("Failed to open DB file for metadata check");
        let mut u32_buf = [0u8; 4];
        let mut u64_buf = [0u8; 8];

        file.read_exact(&mut u32_buf).expect("Failed to read order from metadata");
        assert_eq!(u32::from_be_bytes(u32_buf) as usize, TEST_TREE_ORDER);

        file.read_exact(&mut u64_buf).expect("Failed to read root_page_id from metadata");
        assert_eq!(u64::from_be_bytes(u64_buf), 0); // Initial root_page_id

        file.read_exact(&mut u64_buf).expect("Failed to read next_available_page_id from metadata");
        assert_eq!(u64::from_be_bytes(u64_buf), 1); // Initial next_available_page_id

        file.read_exact(&mut u64_buf).expect("Failed to read free_list_head_page_id from metadata");
        assert_eq!(u64::from_be_bytes(u64_buf), SENTINEL_PAGE_ID); // Initial free_list_head_page_id

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
        assert_eq!(loaded_tree.next_available_page_id, 1);
        assert_eq!(loaded_tree.free_list_head_page_id, SENTINEL_PAGE_ID);
        drop(dir);
    }

    #[test]
    fn test_node_read_write() {
        let (mut tree, _path, _dir) = setup_tree("test_read_write");
        let page_id1 = tree.allocate_new_page_id().expect("Failed to allocate page_id1");
        let node = BPlusTreeNode::Leaf {
            page_id: page_id1,
            parent_page_id: Some(0), keys: vec![k("apple")], values: vec![vec![pk("v_apple")]], next_leaf: None,
        };
        tree.write_node(&node).expect("Failed to write node");
        let read_node = tree.read_node(page_id1).expect("Failed to read node");
        assert_eq!(node, read_node);
        let page_id2 = tree.allocate_new_page_id().expect("Failed to allocate page_id2");
        let internal_node = BPlusTreeNode::Internal {
            page_id: page_id2, parent_page_id: None, keys: vec![k("banana")], children: vec![0,1]
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
        assert_eq!(tree.find_primary_keys(&k("apple")).expect("Find apple failed"), Some(vec![pk("v_apple")]));
        let root_node = tree.read_node(tree.root_page_id).expect("Read root node failed");
         if let BPlusTreeNode::Leaf { keys, .. } = root_node {
            assert_eq!(keys.len(), 3);
            assert_eq!(keys[0], k("apple"));
            assert_eq!(keys[1], k("banana"));
            assert_eq!(keys[2], k("mango"));
            assert!(keys.len() == tree.order -1);
        } else { panic!("Root should be a leaf node"); }
    }

    #[test]
    fn test_insert_causing_leaf_split_and_new_root() {
        let (mut tree, _path, _dir) = setup_tree("test_leaf_split_new_root");
        tree.insert(k("c"), pk("v_c")).expect("Insert c failed");
        tree.insert(k("a"), pk("v_a")).expect("Insert a failed");
        tree.insert(k("b"), pk("v_b")).expect("Insert b failed");
        tree.insert(k("d"), pk("v_d")).expect("Insert d failed"); // This should cause a split
        assert_ne!(tree.root_page_id, 0); // Root should have changed
        let new_root_id = tree.root_page_id;
        let root_node = tree.read_node(new_root_id).expect("Read new root failed");
        if let BPlusTreeNode::Internal {page_id: r_pid, keys: r_keys, children: r_children, parent_page_id: r_parent_pid} = root_node {
            assert_eq!(r_pid, new_root_id);
            assert!(r_parent_pid.is_none());
            assert_eq!(r_keys, vec![k("b")]);
            assert_eq!(r_children.len(), 2);
            let child0_page_id = r_children[0];
            let child1_page_id = r_children[1];
            let left_leaf = tree.read_node(child0_page_id).expect("Read child0 failed");
            if let BPlusTreeNode::Leaf {page_id: l_pid, keys: l_keys, values: l_values, next_leaf: l_next, parent_page_id: l_parent_pid} = left_leaf {
                assert_eq!(l_pid, child0_page_id);
                assert_eq!(l_parent_pid, Some(new_root_id));
                assert_eq!(l_keys, vec![k("a")]);
                assert_eq!(l_values, vec![vec![pk("v_a")]]);
                assert_eq!(l_next, Some(child1_page_id));
            } else { panic!("Child 0 is not a Leaf as expected"); }
            let right_leaf = tree.read_node(child1_page_id).expect("Read child1 failed");
             if let BPlusTreeNode::Leaf {page_id: rl_pid, keys: rl_keys, values: rl_values, next_leaf: rl_next, parent_page_id: rl_parent_pid} = right_leaf {
                assert_eq!(rl_pid, child1_page_id);
                assert_eq!(rl_parent_pid, Some(new_root_id));
                assert_eq!(rl_keys, vec![k("b"), k("c"), k("d")]);
                assert_eq!(rl_values, vec![vec![pk("v_b")], vec![pk("v_c")], vec![pk("v_d")]]);
                assert_eq!(rl_next, None);
            } else { panic!("Child 1 is not a Leaf as expected"); }
        } else { panic!("New root is not an Internal node as expected"); }
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
        assert_eq!(tree.find_primary_keys(&k("a")).expect("Find a after delete failed"), Some(vec![pk("v_a")]));
        let root_node = tree.read_node(tree.root_page_id).expect("Read root node failed");
        if let BPlusTreeNode::Leaf { keys, ..} = root_node {
            assert_eq!(keys, vec![k("a"), k("c")]);
        } else { panic!("Should be leaf root"); }
    }

    #[test]
    fn test_delete_specific_pk_from_leaf() {
        let (mut tree, _path, _dir) = setup_tree("delete_specific_pk");
        tree.insert(k("a"), pk("v_a1")).expect("Insert v_a1 failed");
        tree.insert(k("a"), pk("v_a2")).expect("Insert v_a2 failed");
        tree.insert(k("a"), pk("v_a3")).expect("Insert v_a3 failed");
        tree.insert(k("b"), pk("v_b1")).expect("Insert v_b1 failed");
        let deleted_pk_result = tree.delete(&k("a"), Some(&pk("v_a2"))).expect("Delete v_a2 failed");
        assert!(deleted_pk_result, "Deletion of a specific PK should return true if PK was found and removed.");
        let pks_a_after_delete = tree.find_primary_keys(&k("a")).expect("Find a after delete failed").expect("PKs for 'a' should exist");
        assert_eq!(pks_a_after_delete.len(), 2);
        assert!(pks_a_after_delete.contains(&pk("v_a1")));
        assert!(!pks_a_after_delete.contains(&pk("v_a2")));
        assert!(pks_a_after_delete.contains(&pk("v_a3")));
        let deleted_key_entirely = tree.delete(&k("a"), None).expect("Delete entire key 'a' failed");
        assert!(deleted_key_entirely, "Deletion of entire key should return true.");
        assert!(tree.find_primary_keys(&k("a")).expect("Find 'a' after full delete failed").is_none(), "Key 'a' should be completely gone.");
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
        } else { panic!("Root should remain a leaf"); }
    }

    #[test]
    fn test_delete_leaf_borrow_from_right_sibling() -> Result<(), OxidbError> {
        const ORDER: usize = 4; // Min keys = (4-1)/2 = 1
        let (mut tree, _path, _dir) = setup_tree("borrow_from_right_leaf");
        assert_eq!(tree.order, ORDER, "Test setup assumes order 4 from setup_tree which uses TEST_TREE_ORDER");

        // Setup:
        // Root (P0) with key "banana"
        //  |-- Left Leaf (P1) with key "apple" (min keys)
        //  |-- Right Leaf (P2) with keys "cherry", "date" (can lend)

        // Insert initial data to create structure: "apple", "banana", "cherry", "date"
        // This will create:
        // Root: [banana]
        // L1: [apple] (next P2)
        // L2: [banana, cherry, date] (next None) -- this is not what we want.
        // Let's manually construct the state after splits to be precise.

        // Target state before delete:
        // Root (P_ROOT, e.g. P2 after splits) keys: ["banana"] children: [P_L1, P_L2]
        // Leaf L1 (P_L1, e.g. P0) keys: ["apple"] values: [pk("v_apple")] parent: P_ROOT, next: P_L2
        // Leaf L2 (P_L2, e.g. P1) keys: ["cherry", "date"] values: [pk("v_cherry"), pk("v_date")] parent: P_ROOT, next: None

        // To achieve this with order 4:
        // 1. Insert "apple", "banana", "cherry", "date"
        //    - "apple" -> L(P0) [apple]
        //    - "banana" -> L(P0) [apple, banana]
        //    - "cherry" -> L(P0) [apple, banana, cherry] (full)
        //    - "date" -> causes split.
        //      - Promoted/Copied: "banana"
        //      - New Root (P1): keys: ["banana"], children: [P0, P2]
        //      - Left Leaf (P0): keys: ["apple"], parent: P1, next: P2
        //      - Right Leaf (P2): keys: ["banana", "cherry", "date"], parent: P1, next: None

        tree.insert(k("apple"), pk("v_apple"))?;
        tree.insert(k("banana"), pk("v_banana"))?;
        tree.insert(k("cherry"), pk("v_cherry"))?;
        tree.insert(k("date"), pk("v_date"))?; // Split occurs here

        // Verify initial structure (as per split logic)
        let root_pid = tree.root_page_id;
        let root_node_initial = tree.read_node(root_pid)?;
        let (initial_l1_pid, initial_l2_pid) = match &root_node_initial {
            BPlusTreeNode::Internal { keys, children, .. } => {
                assert_eq!(keys, &vec![k("banana")]);
                (children[0], children[1])
            }
            _ => panic!("Root should be internal after insertions leading to split"),
        };

        let initial_l1_node = tree.read_node(initial_l1_pid)?;
        match &initial_l1_node {
            BPlusTreeNode::Leaf { keys, parent_page_id, next_leaf, .. } => {
                assert_eq!(keys, &vec![k("apple")]);
                assert_eq!(*parent_page_id, Some(root_pid));
                assert_eq!(*next_leaf, Some(initial_l2_pid));
            }
            _ => panic!("Child 0 should be leaf L1"),
        }

        let initial_l2_node = tree.read_node(initial_l2_pid)?;
        match &initial_l2_node {
            BPlusTreeNode::Leaf { keys, parent_page_id, next_leaf, .. } => {
                assert_eq!(keys, &vec![k("banana"), k("cherry"), k("date")]);
                assert_eq!(*parent_page_id, Some(root_pid));
                assert_eq!(*next_leaf, None);
            }
            _ => panic!("Child 1 should be leaf L2"),
        }
        // Now, we need to modify L2 to have ["cherry", "date"] and L1 to have ["apple"]
        // And parent to have ["banana"] separating them. This is already the case.
        // L2 has 3 keys, it can lend. L1 has 1 key (min_keys for order 4).

        // Delete "apple" from L1 (page initial_l1_pid). This causes L1 to underflow.
        let deleted = tree.delete(&k("apple"), None)?;
        assert!(deleted, "Deletion of 'apple' should succeed");

        // --- Verification after borrow from right ---
        // Expected structure:
        // Root (P_ROOT) keys: ["cherry"] children: [P_L1, P_L2]
        // Leaf L1 (P_L1) keys: ["banana"] values: [pk("v_banana")] parent: P_ROOT, next: P_L2
        // Leaf L2 (P_L2) keys: ["date"] values: [pk("v_date")] parent: P_ROOT, next: None

        let final_root_node = tree.read_node(root_pid)?; // Root PID should not change
        let (final_l1_pid, final_l2_pid) = match &final_root_node {
            BPlusTreeNode::Internal { keys, children, parent_page_id, .. } => {
                assert!(parent_page_id.is_none(), "Root's parent should be None");
                assert_eq!(keys, &vec![k("cherry")], "Root key should be 'cherry' after borrow");
                assert_eq!(children.len(), 2, "Root should still have 2 children");
                (children[0], children[1])
            }
            _ => panic!("Root should remain internal"),
        };

        assert_eq!(final_l1_pid, initial_l1_pid, "L1 page ID should not change");
        assert_eq!(final_l2_pid, initial_l2_pid, "L2 page ID should not change");

        let final_l1_node = tree.read_node(final_l1_pid)?;
        match &final_l1_node {
            BPlusTreeNode::Leaf { page_id, keys, values, parent_page_id, next_leaf } => {
                assert_eq!(*page_id, final_l1_pid);
                assert_eq!(keys, &vec![k("banana")], "L1 keys should be ['banana']");
                assert_eq!(values.len(), 1);
                assert_eq!(values[0], vec![pk("v_banana")]);
                assert_eq!(*parent_page_id, Some(root_pid), "L1 parent should be root");
                assert_eq!(*next_leaf, Some(final_l2_pid), "L1 next should point to L2");
            }
            _ => panic!("L1 should be a Leaf node"),
        }

        let final_l2_node = tree.read_node(final_l2_pid)?;
        match &final_l2_node {
            BPlusTreeNode::Leaf { page_id, keys, values, parent_page_id, next_leaf } => {
                assert_eq!(*page_id, final_l2_pid);
                assert_eq!(keys, &vec![k("cherry"), k("date")], "L2 keys should be ['cherry', 'date']");
                assert_eq!(values.len(), 2);
                assert_eq!(values[0], vec![pk("v_cherry")]);
                assert_eq!(values[1], vec![pk("v_date")]);
                assert_eq!(*parent_page_id, Some(root_pid), "L2 parent should be root");
                assert_eq!(*next_leaf, None, "L2 next should be None");
            }
            _ => panic!("L2 should be a Leaf node"),
        }

        // Ensure original key "apple" is gone
        assert!(tree.find_primary_keys(&k("apple"))?.is_none(), "'apple' should not be found");
        // Ensure "banana" is found in L1
        assert_eq!(tree.find_primary_keys(&k("banana"))?, Some(vec![pk("v_banana")]));
        // Ensure "cherry" is found in L2 (after borrowing, it moved from L2 to parent, then new L2 smallest is "cherry" if logic was different)
        // With current logic: "banana" moved from L2 to L1. "cherry" became parent. L2 still has "cherry", "date".
        // No, wait. Parent became "cherry". L2's first key "banana" went to L1. L2 should be ["cherry", "date"].
        // Parent key updated to l_keys.first() from L2. L2 had ["banana", "cherry", "date"].
        // L1 gets "banana". L2 becomes ["cherry", "date"]. Parent becomes "cherry". This is correct.
        assert_eq!(tree.find_primary_keys(&k("cherry"))?, Some(vec![pk("v_cherry")]));
        // Ensure "date" is found in L2
        assert_eq!(tree.find_primary_keys(&k("date"))?, Some(vec![pk("v_date")]));

        Ok(())
    }

    #[test]
    fn test_page_allocation_and_deallocation() {
        let (mut tree, _path, _dir) = setup_tree("alloc_dealloc_test");

        // 1. Initial state
        assert_eq!(tree.next_available_page_id, 1); // Root is 0
        assert_eq!(tree.free_list_head_page_id, SENTINEL_PAGE_ID);

        // 2. Allocate some pages
        let p1 = tree.allocate_new_page_id().unwrap(); // Should be 1
        assert_eq!(p1, 1);
        assert_eq!(tree.next_available_page_id, 2);
        assert_eq!(tree.free_list_head_page_id, SENTINEL_PAGE_ID);

        let p2 = tree.allocate_new_page_id().unwrap(); // Should be 2
        assert_eq!(p2, 2);
        assert_eq!(tree.next_available_page_id, 3);

        let p3 = tree.allocate_new_page_id().unwrap(); // Should be 3
        assert_eq!(p3, 3);
        assert_eq!(tree.next_available_page_id, 4);

        // 3. Deallocate p2
        tree.deallocate_page_id(p2).unwrap();
        assert_eq!(tree.free_list_head_page_id, p2); // p2 is now head of free list
        // Check content of p2 (it should point to SENTINEL_PAGE_ID)
        let mut file = tree.file_handle.lock().unwrap();
        let offset = PAGE_SIZE + p2 * PAGE_SIZE;
        file.seek(SeekFrom::Start(offset)).unwrap();
        let mut next_free_buf = [0u8; 8];
        file.read_exact(&mut next_free_buf).unwrap();
        assert_eq!(PageId::from_be_bytes(next_free_buf), SENTINEL_PAGE_ID);
        drop(file);

        // 4. Deallocate p1
        tree.deallocate_page_id(p1).unwrap();
        assert_eq!(tree.free_list_head_page_id, p1); // p1 is now head
        // Check content of p1 (it should point to p2)
        let mut file = tree.file_handle.lock().unwrap();
        let offset = PAGE_SIZE + p1 * PAGE_SIZE;
        file.seek(SeekFrom::Start(offset)).unwrap();
        let mut next_free_buf = [0u8; 8];
        file.read_exact(&mut next_free_buf).unwrap();
        assert_eq!(PageId::from_be_bytes(next_free_buf), p2); // p1 points to p2
        drop(file);


        // 5. Allocate again - should get p1
        let p_reused1 = tree.allocate_new_page_id().unwrap();
        assert_eq!(p_reused1, p1);
        assert_eq!(tree.free_list_head_page_id, p2); // Free list head should now be p2
        assert_eq!(tree.next_available_page_id, 4); // next_available_page_id should not have changed

        // 6. Allocate again - should get p2
        let p_reused2 = tree.allocate_new_page_id().unwrap();
        assert_eq!(p_reused2, p2);
        assert_eq!(tree.free_list_head_page_id, SENTINEL_PAGE_ID); // Free list should be empty
        assert_eq!(tree.next_available_page_id, 4);

        // 7. Allocate again - should get p3 (from free list, this test logic was slightly off before)
        // No, p3 was never deallocated. So it should come from next_available_page_id if free list is empty
        tree.deallocate_page_id(p3).unwrap(); // Deallocate p3
        assert_eq!(tree.free_list_head_page_id, p3);

        let p_reused3 = tree.allocate_new_page_id().unwrap();
        assert_eq!(p_reused3, p3);
        assert_eq!(tree.free_list_head_page_id, SENTINEL_PAGE_ID);
        assert_eq!(tree.next_available_page_id, 4);


        // 8. Allocate again - should get a new page (4)
        let p4 = tree.allocate_new_page_id().unwrap();
        assert_eq!(p4, 4);
        assert_eq!(tree.next_available_page_id, 5);
        assert_eq!(tree.free_list_head_page_id, SENTINEL_PAGE_ID);
    }
}
