use super::super::error::OxidbError;
use crate::core::indexing::btree::node::{BPlusTreeNode, KeyType, PageId, PrimaryKey};
// use std::io::Write;

use super::BPlusTreeIndex;

impl BPlusTreeIndex {
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

        let parent_children =
            parent_node.get_children().map_err(|e| OxidbError::TreeLogicError(e.to_string()))?;
        let child_idx_in_parent = parent_children
            .iter()
            .position(|&child_pid| child_pid == current_node_pid)
            .ok_or_else(|| {
                OxidbError::TreeLogicError(
                    "Child not found in parent during underflow handling".to_string(),
                )
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

        if child_idx_in_parent > 0 {
            let left_sibling_pid = parent_children[child_idx_in_parent.saturating_sub(1)];
            let mut left_sibling_node = self.get_mutable_node(left_sibling_pid)?;
            let current_node_pid = current_node.get_page_id();
            self.merge_nodes(
                &mut left_sibling_node,
                &mut current_node,
                &mut parent_node,
                child_idx_in_parent.saturating_sub(1),
            )?;
            self.write_node(&left_sibling_node)?;
            self.deallocate_page_id(current_node_pid)?;
        } else {
            let right_sibling_pid = parent_children[child_idx_in_parent.saturating_add(1)];
            let mut right_sibling_node = self.get_mutable_node(right_sibling_pid)?;
            self.merge_nodes(
                &mut current_node,
                &mut right_sibling_node,
                &mut parent_node,
                child_idx_in_parent,
            )?;
            self.write_node(&current_node)?;
            self.deallocate_page_id(right_sibling_pid)?;
        }

        if parent_node.get_keys().len() < self.min_keys_for_node()
            && parent_pid != self.root_page_id
        {
            self.handle_underflow(parent_node, path)?;
        } else if parent_pid == self.root_page_id
            && parent_node.get_keys().is_empty()
            && matches!(parent_node, BPlusTreeNode::Internal { .. })
        {
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
                    let key_from_parent = p_keys[parent_key_idx].clone();
                    u_keys.insert(0, key_from_parent);
                    let new_separator_for_parent = l_keys.pop().ok_or(OxidbError::TreeLogicError("Lender internal (left) empty".to_string()))?;
                    p_keys[parent_key_idx] = new_separator_for_parent;
                    let child_to_move = l_children.pop().ok_or(OxidbError::TreeLogicError("Lender internal (left) children empty".to_string()))?;
                    u_children.insert(0, child_to_move);
                    let mut moved_child_node = self.get_mutable_node(child_to_move)?;
                    moved_child_node.set_parent_page_id(Some(*u_pid_val));
                    self.write_node(&moved_child_node)?;
                } else {
                    let key_from_parent = p_keys[parent_key_idx].clone();
                    u_keys.push(key_from_parent);
                    let new_separator_for_parent = l_keys.remove(0);
                    p_keys[parent_key_idx] = new_separator_for_parent;
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
                BPlusTreeNode::Leaf {
                    keys: l_keys, values: l_values, next_leaf: l_next_leaf, ..
                },
                BPlusTreeNode::Leaf {
                    keys: r_keys, values: r_values, next_leaf: r_next_leaf, ..
                },
                BPlusTreeNode::Internal { keys: p_keys, children: p_children, .. },
            ) => {
                // Standard B+ tree leaf merge: merge right into left
                l_keys.append(r_keys);
                l_values.append(r_values);
                *l_next_leaf = *r_next_leaf;

                // Always remove separator key - this is the PostgreSQL/SQL standard approach
                // The separator key becomes redundant when leaves are merged
                p_keys.remove(parent_key_idx);
                p_children.remove(parent_key_idx + 1);
            }
            (
                BPlusTreeNode::Internal {
                    page_id: l_pid_val,
                    keys: l_keys,
                    children: l_children,
                    ..
                },
                BPlusTreeNode::Internal { keys: r_keys, children: r_children_original, .. },
                BPlusTreeNode::Internal { keys: p_keys, children: p_children, .. },
            ) => {
                // Standard B+ tree internal merge: pull down parent key and merge
                let key_from_parent = p_keys.remove(parent_key_idx);
                l_keys.push(key_from_parent);

                // Append all keys from right node to left
                let mut r_keys_temp = r_keys.clone();
                l_keys.append(&mut r_keys_temp);

                // Move all children from right to left and update their parent pointers
                let children_to_move = r_children_original.clone();
                l_children.append(r_children_original);

                for child_pid_to_update in children_to_move {
                    let mut child_node = self.get_mutable_node(child_pid_to_update)?;
                    child_node.set_parent_page_id(Some(*l_pid_val));
                    self.write_node(&child_node)?;
                }
                p_children.remove(parent_key_idx + 1);
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