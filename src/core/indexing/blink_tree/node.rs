use serde::{Deserialize, Serialize};
use std::io::{Cursor, Read, Write};

pub type KeyType = Vec<u8>;
pub type PageId = u64; // Represents a page ID or offset in a file
pub type PrimaryKey = Vec<u8>; // Represents the primary key of a record

/// Blink Tree Node with concurrent access support
/// Key differences from B+ tree:
/// 1. `right_link`: Points to right sibling for concurrent traversal
/// 2. `high_key`: Highest key in this subtree (for safe concurrent access)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BlinkTreeNode {
    Internal {
        page_id: PageId,
        parent_page_id: Option<PageId>,
        keys: Vec<KeyType>,
        children: Vec<PageId>,      // Pointers to child nodes
        right_link: Option<PageId>, // NEW: Link to right sibling
        high_key: Option<KeyType>,  // NEW: Highest key in subtree
    },
    Leaf {
        page_id: PageId,
        parent_page_id: Option<PageId>,
        keys: Vec<KeyType>,
        values: Vec<Vec<PrimaryKey>>, // For leaf nodes, values are lists of PKs
        right_link: Option<PageId>,   // NEW: Link to right sibling
        high_key: Option<KeyType>,    // NEW: Highest key in this node
    },
}

#[derive(Debug)]
pub enum SerializationError {
    IoError(String),
    InvalidFormat(String),
    UnknownNodeType(u8),
}

impl From<std::io::Error> for SerializationError {
    fn from(err: std::io::Error) -> Self {
        Self::IoError(err.to_string())
    }
}

impl BlinkTreeNode {
    /// Get the page ID of this node
    #[must_use]
    pub const fn get_page_id(&self) -> PageId {
        match self {
            Self::Internal { page_id, .. } => *page_id,
            Self::Leaf { page_id, .. } => *page_id,
        }
    }

    /// Get the parent page ID
    #[must_use]
    pub const fn get_parent_page_id(&self) -> Option<PageId> {
        match self {
            Self::Internal { parent_page_id, .. } => *parent_page_id,
            Self::Leaf { parent_page_id, .. } => *parent_page_id,
        }
    }

    /// Set the parent page ID
    pub fn set_parent_page_id(&mut self, parent_id: Option<PageId>) {
        match self {
            Self::Internal { parent_page_id, .. } => *parent_page_id = parent_id,
            Self::Leaf { parent_page_id, .. } => *parent_page_id = parent_id,
        }
    }

    /// Get the keys in this node
    #[must_use]
    pub const fn get_keys(&self) -> &Vec<KeyType> {
        match self {
            Self::Internal { keys, .. } => keys,
            Self::Leaf { keys, .. } => keys,
        }
    }

    /// Check if this is a leaf node
    #[must_use]
    pub const fn is_leaf(&self) -> bool {
        matches!(self, Self::Leaf { .. })
    }

    /// Get children (only for internal nodes)
    pub const fn get_children(&self) -> Result<&Vec<PageId>, &'static str> {
        match self {
            Self::Internal { children, .. } => Ok(children),
            Self::Leaf { .. } => Err("Leaf nodes don't have children"),
        }
    }

    /// Get right link (NEW for Blink tree)
    #[must_use]
    pub const fn get_right_link(&self) -> Option<PageId> {
        match self {
            Self::Internal { right_link, .. } => *right_link,
            Self::Leaf { right_link, .. } => *right_link,
        }
    }

    /// Set right link (NEW for Blink tree)
    pub fn set_right_link(&mut self, link: Option<PageId>) {
        match self {
            Self::Internal { right_link, .. } => *right_link = link,
            Self::Leaf { right_link, .. } => *right_link = link,
        }
    }

    /// Get high key (NEW for Blink tree)
    #[must_use]
    pub const fn get_high_key(&self) -> Option<&KeyType> {
        match self {
            Self::Internal { high_key, .. } => high_key.as_ref(),
            Self::Leaf { high_key, .. } => high_key.as_ref(),
        }
    }

    /// Set high key (NEW for Blink tree)
    pub fn set_high_key(&mut self, key: Option<KeyType>) {
        match self {
            Self::Internal { high_key, .. } => *high_key = key,
            Self::Leaf { high_key, .. } => *high_key = key,
        }
    }

    /// Check if node is safe for concurrent access (NEW for Blink tree)
    /// A node is safe if the search key is <= `high_key` or `high_key` is None
    #[must_use]
    pub fn is_safe_for_key(&self, search_key: &KeyType) -> bool {
        match self.get_high_key() {
            Some(high_key) => search_key <= high_key,
            None => true, // No high key means this node handles all keys >= its min
        }
    }

    /// Check if this node is full
    #[must_use]
    pub fn is_full(&self, order: usize) -> bool {
        match self {
            Self::Internal { keys, .. } => keys.len() >= order - 1,
            Self::Leaf { keys, .. } => keys.len() >= order,
        }
    }

    /// Check if this node can lend a key or should be merged
    #[must_use]
    pub fn can_lend_or_merge(&self, order: usize) -> bool {
        let min_keys = if self.is_leaf() {
            (order + 1) / 2 // Ceiling division for leaf nodes
        } else {
            (order - 1 + 1) / 2 // Ceiling division for internal nodes
        };

        self.get_keys().len() <= min_keys
    }

    /// Find the index of the child that should contain the given key
    pub fn find_child_index(&self, key: &KeyType) -> Result<usize, &'static str> {
        match self {
            Self::Internal { keys, .. } => {
                // For internal nodes, find the rightmost child whose key is <= search key
                let mut index = 0;
                for (i, node_key) in keys.iter().enumerate() {
                    if key >= node_key {
                        index = i + 1;
                    } else {
                        break;
                    }
                }
                Ok(index)
            }
            Self::Leaf { .. } => Err("Leaf nodes don't have children"),
        }
    }

    /// Insert a key-value pair into this node
    pub fn insert_key_value(
        &mut self,
        key: KeyType,
        value: InsertValue,
        order: usize,
    ) -> Result<(), &'static str> {
        if self.is_full(order) {
            return Err("Node is full");
        }

        match self {
            Self::Internal { keys, children, .. } => {
                if let InsertValue::Page(page_id) = value {
                    // Find insertion point
                    let mut insert_pos = keys.len();
                    for (i, existing_key) in keys.iter().enumerate() {
                        if &key < existing_key {
                            insert_pos = i;
                            break;
                        }
                    }

                    keys.insert(insert_pos, key);
                    children.insert(insert_pos + 1, page_id);
                    Ok(())
                } else {
                    Err("Internal nodes require PageId values")
                }
            }
            Self::Leaf { keys, values, .. } => {
                if let InsertValue::PrimaryKeys(pk_list) = value {
                    // Find insertion point
                    let mut insert_pos = keys.len();
                    for (i, existing_key) in keys.iter().enumerate() {
                        if &key < existing_key {
                            insert_pos = i;
                            break;
                        } else if &key == existing_key {
                            // Key already exists, append to existing PKs
                            values[i].extend(pk_list);
                            return Ok(());
                        }
                    }

                    keys.insert(insert_pos, key);
                    values.insert(insert_pos, pk_list);
                    Ok(())
                } else {
                    Err("Leaf nodes require PrimaryKey values")
                }
            }
        }
    }

    /// Split this node, returning the split key and new right node
    pub fn split(
        &mut self,
        _order: usize,
        new_page_id: PageId,
    ) -> Result<(KeyType, Self), &'static str> {
        match self {
            Self::Internal { keys, children, parent_page_id, right_link, high_key, .. } => {
                let mid = keys.len() / 2;
                let split_key = keys[mid].clone();

                // Split keys and children
                let right_keys = keys.split_off(mid + 1);
                let right_children = children.split_off(mid + 1);
                keys.pop(); // Remove the split key from left node

                // Create new right node
                let new_right_node = Self::Internal {
                    page_id: new_page_id,
                    parent_page_id: *parent_page_id,
                    keys: right_keys,
                    children: right_children,
                    right_link: *right_link, // New node gets old right link
                    high_key: high_key.clone(), // New node gets the high key
                };

                // Update current node's right link and high key
                *right_link = Some(new_page_id);
                *high_key = Some(split_key.clone());

                Ok((split_key, new_right_node))
            }
            Self::Leaf { keys, values, parent_page_id, right_link, high_key, .. } => {
                let mid = keys.len() / 2;

                // Split keys and values
                let right_keys = keys.split_off(mid);
                let right_values = values.split_off(mid);
                let split_key = right_keys[0].clone(); // First key of right node goes up

                // Create new right node
                let new_right_node = Self::Leaf {
                    page_id: new_page_id,
                    parent_page_id: *parent_page_id,
                    keys: right_keys,
                    values: right_values,
                    right_link: *right_link, // New node gets old right link
                    high_key: high_key.clone(), // New node gets the high key
                };

                // Update current node's right link and high key
                *right_link = Some(new_page_id);
                *high_key = Some(split_key.clone());

                Ok((split_key, new_right_node))
            }
        }
    }

    /// Serialize the node to bytes
    pub fn to_bytes(&self) -> Result<Vec<u8>, SerializationError> {
        let mut buffer = Vec::new();

        match self {
            Self::Internal { page_id, parent_page_id, keys, children, right_link, high_key } => {
                // Write node type (0 = Internal)
                buffer.write_all(&[0u8])?;

                // Write page_id
                buffer.write_all(&page_id.to_le_bytes())?;

                // Write parent_page_id
                buffer.write_all(&[u8::from(parent_page_id.is_some())])?;
                if let Some(parent_id) = parent_page_id {
                    buffer.write_all(&parent_id.to_le_bytes())?;
                }

                // Write right_link
                buffer.write_all(&[u8::from(right_link.is_some())])?;
                if let Some(link) = right_link {
                    buffer.write_all(&link.to_le_bytes())?;
                }

                // Write high_key
                buffer.write_all(&[u8::from(high_key.is_some())])?;
                if let Some(hkey) = high_key {
                    buffer.write_all(&(hkey.len() as u32).to_le_bytes())?;
                    buffer.write_all(hkey)?;
                }

                // Write keys
                buffer.write_all(&(keys.len() as u32).to_le_bytes())?;
                for key in keys {
                    buffer.write_all(&(key.len() as u32).to_le_bytes())?;
                    buffer.write_all(key)?;
                }

                // Write children
                buffer.write_all(&(children.len() as u32).to_le_bytes())?;
                for &child in children {
                    buffer.write_all(&child.to_le_bytes())?;
                }
            }
            Self::Leaf { page_id, parent_page_id, keys, values, right_link, high_key } => {
                // Write node type (1 = Leaf)
                buffer.write_all(&[1u8])?;

                // Write page_id
                buffer.write_all(&page_id.to_le_bytes())?;

                // Write parent_page_id
                buffer.write_all(&[u8::from(parent_page_id.is_some())])?;
                if let Some(parent_id) = parent_page_id {
                    buffer.write_all(&parent_id.to_le_bytes())?;
                }

                // Write right_link
                buffer.write_all(&[u8::from(right_link.is_some())])?;
                if let Some(link) = right_link {
                    buffer.write_all(&link.to_le_bytes())?;
                }

                // Write high_key
                buffer.write_all(&[u8::from(high_key.is_some())])?;
                if let Some(hkey) = high_key {
                    buffer.write_all(&(hkey.len() as u32).to_le_bytes())?;
                    buffer.write_all(hkey)?;
                }

                // Write keys
                buffer.write_all(&(keys.len() as u32).to_le_bytes())?;
                for key in keys {
                    buffer.write_all(&(key.len() as u32).to_le_bytes())?;
                    buffer.write_all(key)?;
                }

                // Write values
                buffer.write_all(&(values.len() as u32).to_le_bytes())?;
                for value_list in values {
                    buffer.write_all(&(value_list.len() as u32).to_le_bytes())?;
                    for pk in value_list {
                        buffer.write_all(&(pk.len() as u32).to_le_bytes())?;
                        buffer.write_all(pk)?;
                    }
                }
            }
        }

        Ok(buffer)
    }

    /// Deserialize a node from bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, SerializationError> {
        let mut cursor = Cursor::new(bytes);

        // Read node type
        let node_type = read_u8(&mut cursor)?;

        // Read page_id
        let page_id = read_u64(&mut cursor)?;

        // Read parent_page_id
        let has_parent = read_u8(&mut cursor)? != 0;
        let parent_page_id = if has_parent { Some(read_u64(&mut cursor)?) } else { None };

        // Read right_link
        let has_right_link = read_u8(&mut cursor)? != 0;
        let right_link = if has_right_link { Some(read_u64(&mut cursor)?) } else { None };

        // Read high_key
        let has_high_key = read_u8(&mut cursor)? != 0;
        let high_key = if has_high_key {
            let key_len = read_u32(&mut cursor)?;
            Some(read_vec_u8(&mut cursor, key_len as usize)?)
        } else {
            None
        };

        match node_type {
            0 => {
                // Internal node
                // Read keys
                let keys_count = read_u32(&mut cursor)?;
                let mut keys = Vec::with_capacity(keys_count as usize);
                for _ in 0..keys_count {
                    let key_len = read_u32(&mut cursor)?;
                    keys.push(read_vec_u8(&mut cursor, key_len as usize)?);
                }

                // Read children
                let children_count = read_u32(&mut cursor)?;
                let mut children = Vec::with_capacity(children_count as usize);
                for _ in 0..children_count {
                    children.push(read_u64(&mut cursor)?);
                }

                Ok(Self::Internal { page_id, parent_page_id, keys, children, right_link, high_key })
            }
            1 => {
                // Leaf node
                // Read keys
                let keys_count = read_u32(&mut cursor)?;
                let mut keys = Vec::with_capacity(keys_count as usize);
                for _ in 0..keys_count {
                    let key_len = read_u32(&mut cursor)?;
                    keys.push(read_vec_u8(&mut cursor, key_len as usize)?);
                }

                // Read values
                let values_count = read_u32(&mut cursor)?;
                let mut values = Vec::with_capacity(values_count as usize);
                for _ in 0..values_count {
                    let pk_list_len = read_u32(&mut cursor)?;
                    let mut pk_list = Vec::with_capacity(pk_list_len as usize);
                    for _ in 0..pk_list_len {
                        let pk_len = read_u32(&mut cursor)?;
                        pk_list.push(read_vec_u8(&mut cursor, pk_len as usize)?);
                    }
                    values.push(pk_list);
                }

                Ok(Self::Leaf { page_id, parent_page_id, keys, values, right_link, high_key })
            }
            _ => Err(SerializationError::UnknownNodeType(node_type)),
        }
    }
}

/// Enum for inserting either `PageId` (internal) or `PrimaryKeys` (leaf)
pub enum InsertValue {
    Page(PageId),
    PrimaryKeys(Vec<PrimaryKey>),
}

// Helper functions for reading from cursor
fn read_u8(cursor: &mut Cursor<&[u8]>) -> Result<u8, SerializationError> {
    let mut buf = [0u8; 1];
    cursor.read_exact(&mut buf)?;
    Ok(buf[0])
}

fn read_u32(cursor: &mut Cursor<&[u8]>) -> Result<u32, SerializationError> {
    let mut buf = [0u8; 4];
    cursor.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn read_u64(cursor: &mut Cursor<&[u8]>) -> Result<u64, SerializationError> {
    let mut buf = [0u8; 8];
    cursor.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}

fn read_vec_u8(cursor: &mut Cursor<&[u8]>, len: usize) -> Result<Vec<u8>, SerializationError> {
    let mut buf = vec![0u8; len];
    cursor.read_exact(&mut buf)?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn k(s: &str) -> KeyType {
        s.as_bytes().to_vec()
    }

    fn pk(s: &str) -> PrimaryKey {
        s.as_bytes().to_vec()
    }

    #[test]
    fn test_blink_internal_node_creation_and_props() {
        let node = BlinkTreeNode::Internal {
            page_id: 1,
            parent_page_id: None,
            keys: vec![k("key1"), k("key2")],
            children: vec![10, 20, 30],
            right_link: Some(2),
            high_key: Some(k("key2")),
        };

        assert_eq!(node.get_page_id(), 1);
        assert_eq!(node.get_parent_page_id(), None);
        assert!(!node.is_leaf());
        assert_eq!(node.get_right_link(), Some(2));
        assert_eq!(node.get_high_key(), Some(&k("key2")));
        assert!(node.is_safe_for_key(&k("key1")));
        assert!(!node.is_safe_for_key(&k("key3")));
    }

    #[test]
    fn test_blink_leaf_node_creation_and_props() {
        let node = BlinkTreeNode::Leaf {
            page_id: 1,
            parent_page_id: Some(5),
            keys: vec![k("apple"), k("banana")],
            values: vec![vec![pk("pk1")], vec![pk("pk2"), pk("pk3")]],
            right_link: Some(3),
            high_key: Some(k("banana")),
        };

        assert_eq!(node.get_page_id(), 1);
        assert_eq!(node.get_parent_page_id(), Some(5));
        assert!(node.is_leaf());
        assert_eq!(node.get_right_link(), Some(3));
        assert_eq!(node.get_high_key(), Some(&k("banana")));
        assert!(node.is_safe_for_key(&k("apple")));
        assert!(!node.is_safe_for_key(&k("cherry")));
    }

    #[test]
    fn test_blink_serialization_deserialization() {
        let original = BlinkTreeNode::Internal {
            page_id: 42,
            parent_page_id: Some(7),
            keys: vec![k("alpha"), k("beta")],
            children: vec![100, 200, 300],
            right_link: Some(43),
            high_key: Some(k("gamma")),
        };

        let serialized = original.to_bytes().unwrap();
        let deserialized = BlinkTreeNode::from_bytes(&serialized).unwrap();

        assert_eq!(original.get_page_id(), deserialized.get_page_id());
        assert_eq!(original.get_parent_page_id(), deserialized.get_parent_page_id());
        assert_eq!(original.get_right_link(), deserialized.get_right_link());
        assert_eq!(original.get_high_key(), deserialized.get_high_key());
        assert_eq!(original.get_keys(), deserialized.get_keys());
    }

    #[test]
    fn test_concurrent_safety_with_high_key() {
        let mut node = BlinkTreeNode::Leaf {
            page_id: 1,
            parent_page_id: None,
            keys: vec![k("dog"), k("elephant")],
            values: vec![vec![pk("pk1")], vec![pk("pk2")]],
            right_link: Some(2),
            high_key: Some(k("elephant")),
        };

        // Keys within range should be safe
        assert!(node.is_safe_for_key(&k("dog")));
        assert!(node.is_safe_for_key(&k("elephant")));

        // Keys beyond high_key should not be safe
        assert!(!node.is_safe_for_key(&k("fox")));

        // Test with no high key (rightmost node)
        node.set_high_key(None);
        assert!(node.is_safe_for_key(&k("zebra"))); // Should be safe now
    }
}
