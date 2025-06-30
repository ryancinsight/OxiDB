use std::path::PathBuf;

mod delete;
mod insert;

use super::error::OxidbError;
use super::page_io::PageManager;
use crate::core::indexing::btree::node::{BPlusTreeNode, KeyType, PageId, PrimaryKey};

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

    pub(super) fn write_metadata_if_root_changed(
        &mut self,
        old_root_id: PageId,
    ) -> Result<(), OxidbError> {
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
                    let child_idx =
                        keys.partition_point(|k_partition| k_partition.as_slice() < key.as_slice());
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
}

#[cfg(test)]
mod tests;
