use crate::core::indexing::btree::{
    error::OxidbError,
    node::{BPlusTreeNode, KeyType, PageId, PrimaryKey},
};

use super::BPlusTreeIndex;

impl BPlusTreeIndex {
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
            self.handle_split(current_leaf_node, path_to_leaf)?;
        } else {
            self.write_node(&current_leaf_node)?;
        }
        Ok(())
    }

    pub(super) fn get_mutable_node(
        &mut self,
        page_id: PageId,
    ) -> Result<BPlusTreeNode, OxidbError> {
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
}
