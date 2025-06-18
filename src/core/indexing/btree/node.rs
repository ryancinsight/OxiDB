use std::cmp::Ordering;

// Type Aliases
pub type KeyType = Vec<u8>;
pub type PageId = u64; // Represents a page ID or offset in a file
pub type PrimaryKey = Vec<u8>; // Represents the primary key of a record

#[derive(Debug, PartialEq, Clone)]
pub enum BPlusTreeNode {
    Internal {
        page_id: PageId, // ID of this node's page
        parent_page_id: Option<PageId>,
        keys: Vec<KeyType>,
        children: Vec<PageId>, // Pointers to child nodes
    },
    Leaf {
        page_id: PageId, // ID of this node's page
        parent_page_id: Option<PageId>,
        keys: Vec<KeyType>,
        values: Vec<Vec<PrimaryKey>>, // For leaf nodes, values are lists of PKs
        next_leaf: Option<PageId>,   // Pointer to the next leaf node
    },
}

#[derive(Debug, PartialEq)]
pub enum SerializationError {
    IoError(String),
    InvalidFormat(String),
    UnknownNodeType(u8),
}

impl From<std::io::Error> for SerializationError {
    fn from(err: std::io::Error) -> Self {
        SerializationError::IoError(err.to_string())
    }
}

impl BPlusTreeNode {
    // --- Node Properties ---
    pub fn get_page_id(&self) -> PageId {
        match self {
            BPlusTreeNode::Internal { page_id, .. } => *page_id,
            BPlusTreeNode::Leaf { page_id, .. } => *page_id,
        }
    }

    pub fn get_parent_page_id(&self) -> Option<PageId> {
        match self {
            BPlusTreeNode::Internal { parent_page_id, .. } => *parent_page_id,
            BPlusTreeNode::Leaf { parent_page_id, .. } => *parent_page_id,
        }
    }

    pub fn set_parent_page_id(&mut self, parent_id: Option<PageId>) {
        match self {
            BPlusTreeNode::Internal { parent_page_id, .. } => *parent_page_id = parent_id,
            BPlusTreeNode::Leaf { parent_page_id, .. } => *parent_page_id = parent_id,
        }
    }

    pub fn get_keys(&self) -> &Vec<KeyType> {
        match self {
            BPlusTreeNode::Internal { keys, .. } => keys,
            BPlusTreeNode::Leaf { keys, .. } => keys,
        }
    }

    pub fn is_leaf(&self) -> bool {
        matches!(self, BPlusTreeNode::Leaf { .. })
    }

    pub fn is_full(&self, order: usize) -> bool {
        match self {
            BPlusTreeNode::Internal { keys, .. } => keys.len() >= order -1, // Max keys for internal node
            BPlusTreeNode::Leaf { keys, .. } => keys.len() >= order -1, // Max keys for leaf node (can be different, but often same as internal for simplicity)
        }
    }

    pub fn can_lend_or_merge(&self, order: usize) -> bool {
        let min_keys = (order -1) / 2;
         match self {
            BPlusTreeNode::Internal { keys, .. } => keys.len() > min_keys,
            BPlusTreeNode::Leaf { keys, .. } => keys.len() > min_keys,
        }
    }


    // --- Operations ---

    /// For internal nodes, finds the index of the child pointer to follow for a given key.
    /// Returns the index `i` such that `keys[i-1] <= key < keys[i]`.
    /// If `key < keys[0]`, returns 0.
    /// If `key >= keys[len-1]`, returns `len`.
    pub fn find_child_index(&self, key: &KeyType) -> Result<usize, &'static str> {
        match self {
            BPlusTreeNode::Internal { keys, .. } => {
                // Perform a binary search or linear scan for the appropriate child index
                // `partition_point` returns the index of the first element `x` for which `f(x)` is true.
                // We want to find the first key `k_i` such that `key < k_i`.
                // The child pointer to follow will be at that index `i`.
                // If `key >= all keys`, then the index will be `keys.len()`.
                Ok(keys.partition_point(|k| k.as_slice() < key.as_slice()))
            }
            BPlusTreeNode::Leaf { .. } => Err("find_child_index is only applicable to Internal nodes"),
        }
    }

    /// Inserts a key and a child page ID into an internal node.
    /// Assumes the node is not full and the key should be inserted at the given index.
    fn checked_insert_internal(&mut self, key: KeyType, child_page_id: PageId, index: usize) -> Result<(), &'static str>{
         match self {
            BPlusTreeNode::Internal { keys, children, .. } => {
                keys.insert(index, key);
                children.insert(index + 1, child_page_id);
                Ok(())
            }
            BPlusTreeNode::Leaf { .. } => Err("Cannot insert child PageId into a Leaf node."),
        }
    }

    /// Inserts a key-value pair into a leaf node.
    /// Assumes the node is not full and the key should be inserted at the given index.
    fn checked_insert_leaf(&mut self, key: KeyType, value: Vec<PrimaryKey>, index: usize) -> Result<(), &'static str> {
        match self {
            BPlusTreeNode::Leaf { keys, values, .. } => {
                keys.insert(index, key);
                values.insert(index, value);
                Ok(())
            }
            BPlusTreeNode::Internal { .. } => Err("Cannot insert PK value into an Internal node."),
        }
    }


    /// Inserts a key and corresponding value (PageId for Internal, Vec<PrimaryKey> for Leaf)
    /// into the node, maintaining sorted order of keys. This is a simplified version
    /// that does not handle splits. It's intended for use when it's known that the
    /// node has space.
    ///
    /// For Internal nodes, `value` must be a `PageId`. The key is inserted, and the `PageId`
    /// becomes the right child of that key.
    /// For Leaf nodes, `value` must be a `Vec<PrimaryKey>`.
    pub fn insert_key_value(
        &mut self,
        key: KeyType,
        value: InsertValue, // Enum to hold either PageId or Vec<PrimaryKey>
        order: usize, // Max number of children for internal, or items for leaf
    ) -> Result<(), &'static str> {
        if self.is_full(order) {
            return Err("Node is full. Split required before insertion.");
        }

        let keys_vec = match self {
            BPlusTreeNode::Internal { keys, .. } => keys,
            BPlusTreeNode::Leaf { keys, .. } => keys,
        };

        // Find the correct position to insert the key to maintain sorted order.
        // `partition_point` gives the index of the first element greater than or equal to `key`.
        // For our B-Tree logic:
        // - In internal nodes, keys[i] is separator for children[i] and children[i+1].
        //   If key < keys[0], it goes to children[0].
        //   If keys[i-1] <= key < keys[i], it goes to children[i].
        // - In leaf nodes, keys are stored in sorted order.
        let insertion_point = keys_vec.partition_point(|k| k.as_slice() < key.as_slice());

        match (self, value) {
            (BPlusTreeNode::Internal { keys, children, .. }, InsertValue::Page(page_id)) => {
                keys.insert(insertion_point, key);
                // The new page_id becomes the right child of the newly inserted key.
                // So, it's inserted at insertion_point + 1 in the children vector.
                children.insert(insertion_point + 1, page_id);
                Ok(())
            }
            (BPlusTreeNode::Leaf { keys, values, .. }, InsertValue::PrimaryKeys(pk_vec)) => {
                keys.insert(insertion_point, key);
                values.insert(insertion_point, pk_vec);
                Ok(())
            }
            (BPlusTreeNode::Internal { .. }, InsertValue::PrimaryKeys(_)) => {
                Err("Attempted to insert primary keys into an internal node.")
            }
            (BPlusTreeNode::Leaf { .. }, InsertValue::Page(_)) => {
                Err("Attempted to insert a page ID into a leaf node.")
            }
        }
    }


    /// Splits a full node.
    /// Returns the median key (to be promoted for internal, copied for leaf) and the new sibling node.
    /// The original node (self) becomes the left node and is modified in place.
    /// The new sibling node becomes the right node.
    /// `order` is the maximum number of children for an internal node, or max items for a leaf.
    /// Max keys for an internal node = order - 1
    /// Max keys for a leaf node = order - 1 (can be different in some designs)
    pub fn split(&mut self, order: usize, new_page_id: PageId) -> Result<(KeyType, BPlusTreeNode), &'static str> {
        if !self.is_full(order) {
            // Technically, splits can happen before "full" in some strategies (e.g. to maintain a minimum fill factor proactively)
            // but for a basic implementation, we usually split when it's strictly full.
            // For now, let's assume we only call split on a node that needs it.
        }

        let mid_point = (order -1) / 2; // Index of the median key for promotion/copying

        match self {
            BPlusTreeNode::Internal { page_id, parent_page_id, keys, children } => {
                // A node is split when it has 'order' keys (i.e., it's overfull).
                // Max keys is order-1. So, an overfull node has order keys.
                // Or, if split is called pre-emptively on a "just full" node (order-1 keys)
                // before inserting the new element that would make it overfull.
                // Current tree.rs insert logic makes the node overfull, then calls split.
                // So, keys.len() here should be == order.
                if keys.len() < order {
                     return Err("Internal node not overfull enough to split (requires 'order' keys).");
                }

                let median_key = keys.remove(mid_point); // This key moves up to the parent

                let mut new_keys = keys.drain(mid_point..).collect::<Vec<KeyType>>();
                let mut new_children = children.drain(mid_point + 1..).collect::<Vec<PageId>>(); // Children after median

                let new_internal_node = BPlusTreeNode::Internal {
                    page_id: new_page_id,
                    parent_page_id: *parent_page_id, // New node shares the same parent initially
                    keys: new_keys,
                    children: new_children,
                };
                // `self` is now the left node, already modified by `drain`.
                Ok((median_key, new_internal_node))
            }
            BPlusTreeNode::Leaf { page_id, parent_page_id, keys, values, next_leaf } => {
                 // Similar to internal nodes, a leaf is split when it has 'order' keys/values.
                if keys.len() < order {
                     return Err("Leaf node not overfull enough to split (requires 'order' key-value pairs).");
                }
                // For leaf nodes, the median key is *copied* to the parent, not removed from a leaf.
                // It also becomes the first key in the new right sibling.
                let median_key_copy = keys[mid_point].clone();

                let mut new_keys = keys.drain(mid_point..).collect::<Vec<KeyType>>();
                let mut new_values = values.drain(mid_point..).collect::<Vec<Vec<PrimaryKey>>>();

                let original_next_leaf = *next_leaf; // Save original next_leaf for the new node
                *next_leaf = Some(new_page_id); // Current node points to the new sibling

                let new_leaf_node = BPlusTreeNode::Leaf {
                    page_id: new_page_id,
                    parent_page_id: *parent_page_id, // New node shares the same parent initially
                    keys: new_keys,
                    values: new_values,
                    next_leaf: original_next_leaf,
                };
                // `self` is now the left node, already modified by `drain`.
                Ok((median_key_copy, new_leaf_node))
            }
        }
    }

    // --- Serialization / Deserialization ---

    pub fn to_bytes(&self) -> Result<Vec<u8>, SerializationError> {
        let mut bytes = Vec::new();
        match self {
            BPlusTreeNode::Internal { page_id, parent_page_id, keys, children } => {
                bytes.push(0u8); // 0 for Internal Node
                bytes.extend_from_slice(&page_id.to_be_bytes());
                bytes.extend_from_slice(&parent_page_id.is_some().to_be_bytes());
                if let Some(pid) = parent_page_id {
                    bytes.extend_from_slice(&pid.to_be_bytes());
                }

                bytes.extend_from_slice(&(keys.len() as u32).to_be_bytes());
                for key in keys {
                    bytes.extend_from_slice(&(key.len() as u32).to_be_bytes());
                    bytes.extend_from_slice(key);
                }
                bytes.extend_from_slice(&(children.len() as u32).to_be_bytes());
                for child_id in children {
                    bytes.extend_from_slice(&child_id.to_be_bytes());
                }
            }
            BPlusTreeNode::Leaf { page_id, parent_page_id, keys, values, next_leaf } => {
                bytes.push(1u8); // 1 for Leaf Node
                bytes.extend_from_slice(&page_id.to_be_bytes());
                bytes.extend_from_slice(&parent_page_id.is_some().to_be_bytes());
                if let Some(pid) = parent_page_id {
                    bytes.extend_from_slice(&pid.to_be_bytes());
                }

                bytes.extend_from_slice(&(keys.len() as u32).to_be_bytes());
                for key in keys {
                    bytes.extend_from_slice(&(key.len() as u32).to_be_bytes());
                    bytes.extend_from_slice(key);
                }
                bytes.extend_from_slice(&(values.len() as u32).to_be_bytes());
                for pks in values {
                    bytes.extend_from_slice(&(pks.len() as u32).to_be_bytes()); // Number of PKs for this key
                    for pk in pks {
                        bytes.extend_from_slice(&(pk.len() as u32).to_be_bytes());
                        bytes.extend_from_slice(pk);
                    }
                }
                bytes.extend_from_slice(&next_leaf.is_some().to_be_bytes());
                if let Some(id) = next_leaf {
                    bytes.extend_from_slice(&id.to_be_bytes());
                }
            }
        }
        Ok(bytes)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, SerializationError> {
        let mut cursor = std::io::Cursor::new(bytes);
        let node_type = read_u8(&mut cursor)?;

        let page_id = read_u64(&mut cursor)?;
        let has_parent_page_id = read_bool(&mut cursor)?;
        let parent_page_id = if has_parent_page_id { Some(read_u64(&mut cursor)?) } else { None };


        match node_type {
            0 => { // Internal Node
                let num_keys = read_u32(&mut cursor)? as usize;
                let mut keys = Vec::with_capacity(num_keys);
                for _ in 0..num_keys {
                    let key_len = read_u32(&mut cursor)? as usize;
                    keys.push(read_vec_u8(&mut cursor, key_len)?);
                }
                let num_children = read_u32(&mut cursor)? as usize;
                let mut children = Vec::with_capacity(num_children);
                for _ in 0..num_children {
                    children.push(read_u64(&mut cursor)?);
                }
                Ok(BPlusTreeNode::Internal { page_id, parent_page_id, keys, children })
            }
            1 => { // Leaf Node
                let num_keys = read_u32(&mut cursor)? as usize;
                let mut keys = Vec::with_capacity(num_keys);
                for _ in 0..num_keys {
                    let key_len = read_u32(&mut cursor)? as usize;
                    keys.push(read_vec_u8(&mut cursor, key_len)?);
                }
                let num_values_vecs = read_u32(&mut cursor)? as usize;
                let mut values = Vec::with_capacity(num_values_vecs);
                for _ in 0..num_values_vecs {
                    let num_pks_for_key = read_u32(&mut cursor)? as usize;
                    let mut pks_for_key = Vec::with_capacity(num_pks_for_key);
                    for _ in 0..num_pks_for_key {
                         let pk_len = read_u32(&mut cursor)? as usize;
                         pks_for_key.push(read_vec_u8(&mut cursor, pk_len)?);
                    }
                    values.push(pks_for_key);
                }
                let has_next_leaf = read_bool(&mut cursor)?;
                let next_leaf = if has_next_leaf { Some(read_u64(&mut cursor)?) } else { None };
                Ok(BPlusTreeNode::Leaf { page_id, parent_page_id, keys, values, next_leaf })
            }
            _ => Err(SerializationError::UnknownNodeType(node_type)),
        }
    }
}

// Helper enum for insert_key_value
#[derive(Debug)]
pub enum InsertValue {
    Page(PageId),
    PrimaryKeys(Vec<PrimaryKey>),
}


// --- Serialization Helper Functions ---
fn read_u8(cursor: &mut std::io::Cursor<&[u8]>) -> Result<u8, SerializationError> {
    let mut buf = [0u8; 1];
    std::io::Read::read_exact(cursor, &mut buf)?;
    Ok(u8::from_be_bytes(buf))
}

fn read_u32(cursor: &mut std::io::Cursor<&[u8]>) -> Result<u32, SerializationError> {
    let mut buf = [0u8; 4];
    std::io::Read::read_exact(cursor, &mut buf)?;
    Ok(u32::from_be_bytes(buf))
}

fn read_u64(cursor: &mut std::io::Cursor<&[u8]>) -> Result<u64, SerializationError> {
    let mut buf = [0u8; 8];
    std::io::Read::read_exact(cursor, &mut buf)?;
    Ok(u64::from_be_bytes(buf))
}

fn read_bool(cursor: &mut std::io::Cursor<&[u8]>) -> Result<bool, SerializationError> {
    let val = read_u8(cursor)?;
    Ok(val != 0)
}

fn read_vec_u8(cursor: &mut std::io::Cursor<&[u8]>, len: usize) -> Result<Vec<u8>, SerializationError> {
    let mut vec = vec![0u8; len];
    std::io::Read::read_exact(cursor, &mut vec)?;
    Ok(vec)
}


// --- Unit Tests ---
#[cfg(test)]
mod tests {
    use super::*;

    const TEST_ORDER: usize = 4; // Max 3 keys, 4 children for internal; Max 3 key-value pairs for leaf.
    const TEST_PAGE_ID: PageId = 100;
    const TEST_NEW_PAGE_ID: PageId = 101;
    const TEST_PARENT_PAGE_ID: Option<PageId> = Some(99);

    fn K(s: &str) -> KeyType { s.as_bytes().to_vec() }
    fn PK(s: &str) -> PrimaryKey { s.as_bytes().to_vec() }

    #[test]
    fn test_internal_node_creation_and_props() {
        let node = BPlusTreeNode::Internal {
            page_id: TEST_PAGE_ID,
            parent_page_id: TEST_PARENT_PAGE_ID,
            keys: vec![K("apple"), K("banana")],
            children: vec![1, 2, 3],
        };
        assert!(!node.is_leaf());
        assert_eq!(node.get_page_id(), TEST_PAGE_ID);
        assert_eq!(node.get_parent_page_id(), TEST_PARENT_PAGE_ID);
        assert_eq!(node.get_keys().len(), 2);
        assert!(!node.is_full(TEST_ORDER));
    }

    #[test]
    fn test_leaf_node_creation_and_props() {
        let node = BPlusTreeNode::Leaf {
            page_id: TEST_PAGE_ID,
            parent_page_id: TEST_PARENT_PAGE_ID,
            keys: vec![K("cat"), K("dog")],
            values: vec![vec![PK("pk_cat1")], vec![PK("pk_dog1"), PK("pk_dog2")]],
            next_leaf: Some(200),
        };
        assert!(node.is_leaf());
        assert_eq!(node.get_page_id(), TEST_PAGE_ID);
        assert_eq!(node.get_parent_page_id(), TEST_PARENT_PAGE_ID);
        assert_eq!(node.get_keys().len(), 2);
        assert!(!node.is_full(TEST_ORDER));
        if let BPlusTreeNode::Leaf { next_leaf, .. } = node {
            assert_eq!(next_leaf, Some(200));
        }
    }

    #[test]
    fn test_node_is_full() {
        let mut internal_node = BPlusTreeNode::Internal {
            page_id: 1, parent_page_id: None, keys: vec![], children: vec![0]
        };
        let mut leaf_node = BPlusTreeNode::Leaf {
            page_id: 2, parent_page_id: None, keys: vec![], values: vec![], next_leaf: None
        };

        // Order = 4, so max keys = 3
        assert!(!internal_node.is_full(TEST_ORDER));
        assert!(!leaf_node.is_full(TEST_ORDER));

        internal_node.insert_key_value(K("a"), InsertValue::Page(10), TEST_ORDER).unwrap();
        internal_node.insert_key_value(K("c"), InsertValue::Page(12), TEST_ORDER).unwrap();
        assert!(!internal_node.is_full(TEST_ORDER)); // 2 keys
        internal_node.insert_key_value(K("b"), InsertValue::Page(11), TEST_ORDER).unwrap();
        assert!(internal_node.is_full(TEST_ORDER)); // 3 keys

        leaf_node.insert_key_value(K("a"), InsertValue::PrimaryKeys(vec![PK("1")]), TEST_ORDER).unwrap();
        leaf_node.insert_key_value(K("c"), InsertValue::PrimaryKeys(vec![PK("3")]), TEST_ORDER).unwrap();
        assert!(!leaf_node.is_full(TEST_ORDER)); // 2 keys
        leaf_node.insert_key_value(K("b"), InsertValue::PrimaryKeys(vec![PK("2")]), TEST_ORDER).unwrap();
        assert!(leaf_node.is_full(TEST_ORDER)); // 3 keys
    }


    #[test]
    fn test_find_child_index_internal_node() {
        let node = BPlusTreeNode::Internal {
            page_id: TEST_PAGE_ID,
            parent_page_id: None,
            keys: vec![K("b"), K("d"), K("f")], // Children: <b | b<= & <d | d<= & <f | f<=
            children: vec![1, 2, 3, 4],
        };

        assert_eq!(node.find_child_index(&K("a")), Ok(0)); // Should go to child 1
        assert_eq!(node.find_child_index(&K("b")), Ok(1)); // Should go to child 2
        assert_eq!(node.find_child_index(&K("c")), Ok(1)); // Should go to child 2
        assert_eq!(node.find_child_index(&K("d")), Ok(2)); // Should go to child 3
        assert_eq!(node.find_child_index(&K("e")), Ok(2)); // Should go to child 3
        assert_eq!(node.find_child_index(&K("f")), Ok(3)); // Should go to child 4
        assert_eq!(node.find_child_index(&K("g")), Ok(3)); // Should go to child 4 (last child pointer)
    }

    #[test]
    fn test_insert_key_value_internal_node_sorted() {
        let mut node = BPlusTreeNode::Internal {
            page_id: TEST_PAGE_ID,
            parent_page_id: None,
            keys: vec![],
            children: vec![0], // Initial child
        };
        // Order is 4, max keys = 3
        node.insert_key_value(K("mango"), InsertValue::Page(1), TEST_ORDER).unwrap();
        assert_eq!(node.get_keys(), &vec![K("mango")]);
        assert_eq!(node.children, vec![0, 1]);

        node.insert_key_value(K("apple"), InsertValue::Page(2), TEST_ORDER).unwrap();
        assert_eq!(node.get_keys(), &vec![K("apple"), K("mango")]);
        assert_eq!(node.children, vec![0, 2, 1]); // apple child | mango child

        node.insert_key_value(K("banana"), InsertValue::Page(3), TEST_ORDER).unwrap();
        assert_eq!(node.get_keys(), &vec![K("apple"), K("banana"), K("mango")]);
        assert_eq!(node.children, vec![0, 2, 3, 1]); // apple child | banana child | mango child

        assert!(node.is_full(TEST_ORDER));
        assert!(node.insert_key_value(K("orange"), InsertValue::Page(4), TEST_ORDER).is_err());
    }

    #[test]
    fn test_insert_key_value_leaf_node_sorted() {
        let mut node = BPlusTreeNode::Leaf {
            page_id: TEST_PAGE_ID,
            parent_page_id: None,
            keys: vec![],
            values: vec![],
            next_leaf: None,
        };
        // Order is 4, max key-value pairs = 3
        node.insert_key_value(K("mango"), InsertValue::PrimaryKeys(vec![PK("pk_mango")]), TEST_ORDER).unwrap();
        assert_eq!(node.get_keys(), &vec![K("mango")]);
        assert_eq!(node.values, vec![vec![PK("pk_mango")]]);

        node.insert_key_value(K("apple"), InsertValue::PrimaryKeys(vec![PK("pk_apple")]), TEST_ORDER).unwrap();
        assert_eq!(node.get_keys(), &vec![K("apple"), K("mango")]);
        assert_eq!(node.values, vec![vec![PK("pk_apple")], vec![PK("pk_mango")]]);

        node.insert_key_value(K("banana"), InsertValue::PrimaryKeys(vec![PK("pk_banana")]), TEST_ORDER).unwrap();
        assert_eq!(node.get_keys(), &vec![K("apple"), K("banana"), K("mango")]);
        assert_eq!(node.values, vec![vec![PK("pk_apple")], vec![PK("pk_banana")], vec![PK("pk_mango")]]);

        assert!(node.is_full(TEST_ORDER));
        assert!(node.insert_key_value(K("orange"), InsertValue::PrimaryKeys(vec![PK("pk_orange")]), TEST_ORDER).is_err());
    }

    #[test]
    fn test_split_internal_node_order4() {
        // Order 4: Max 3 keys, 4 children. Min 1 key, 2 children after split.
        // Node becomes full with keys [k1, k2, k3] and children [c0, c1, c2, c3]
        // Split promotes k2.
        // Left node: [k1], children [c0, c1]
        // Right node: [k3], children [c2, c3]
        let mut node = BPlusTreeNode::Internal {
            page_id: TEST_PAGE_ID,
            parent_page_id: TEST_PARENT_PAGE_ID,
            keys: vec![K("apple"), K("banana"), K("grape")],
            children: vec![10, 20, 30, 40],
        };
        assert!(node.is_full(TEST_ORDER));

        let (median_key, new_node) = node.split(TEST_ORDER, TEST_NEW_PAGE_ID).unwrap();

        // Check median key
        assert_eq!(median_key, K("banana"));

        // Check original (left) node
        assert_eq!(node.get_page_id(), TEST_PAGE_ID);
        assert_eq!(node.get_parent_page_id(), TEST_PARENT_PAGE_ID);
        assert_eq!(node.get_keys(), &vec![K("apple")]);
        if let BPlusTreeNode::Internal { children, .. } = &node {
            assert_eq!(children, &vec![10, 20]);
        } else { panic!("Node should be Internal"); }

        // Check new (right) node
        assert_eq!(new_node.get_page_id(), TEST_NEW_PAGE_ID);
        assert_eq!(new_node.get_parent_page_id(), TEST_PARENT_PAGE_ID); // Parent ID copied
        assert_eq!(new_node.get_keys(), &vec![K("grape")]);
        if let BPlusTreeNode::Internal { children, .. } = &new_node {
            assert_eq!(children, &vec![30, 40]);
        } else { panic!("New node should be Internal"); }
    }

    #[test]
    fn test_split_leaf_node_order4() {
        // Order 4: Max 3 key-value pairs. Min 1 key-value after split.
        // Node becomes full with keys [k1, k2, k3] and values [v1, v2, v3]
        // Split copies k2 (median) up.
        // Left node: [k1], values [v1]. next_leaf points to new_node.
        // Right node: [k2, k3], values [v2, v3]. next_leaf is original next_leaf.
        let original_next_leaf_id = Some(300 as PageId);
        let mut node = BPlusTreeNode::Leaf {
            page_id: TEST_PAGE_ID,
            parent_page_id: TEST_PARENT_PAGE_ID,
            keys: vec![K("apple"), K("banana"), K("cherry")],
            values: vec![vec![PK("v_apple")], vec![PK("v_banana")], vec![PK("v_cherry")]],
            next_leaf: original_next_leaf_id,
        };
        assert!(node.is_full(TEST_ORDER));

        let (copied_median_key, new_node) = node.split(TEST_ORDER, TEST_NEW_PAGE_ID).unwrap();

        // Check copied median key
        assert_eq!(copied_median_key, K("banana"));

        // Check original (left) node
        assert_eq!(node.get_page_id(), TEST_PAGE_ID);
        assert_eq!(node.get_parent_page_id(), TEST_PARENT_PAGE_ID);
        assert_eq!(node.get_keys(), &vec![K("apple")]);
        if let BPlusTreeNode::Leaf { values, next_leaf, .. } = &node {
            assert_eq!(values, &vec![vec![PK("v_apple")]]);
            assert_eq!(*next_leaf, Some(TEST_NEW_PAGE_ID)); // Points to new right sibling
        } else { panic!("Node should be Leaf"); }

        // Check new (right) node
        assert_eq!(new_node.get_page_id(), TEST_NEW_PAGE_ID);
        assert_eq!(new_node.get_parent_page_id(), TEST_PARENT_PAGE_ID);
        assert_eq!(new_node.get_keys(), &vec![K("banana"), K("cherry")]); // Median key is first key in new right leaf
        if let BPlusTreeNode::Leaf { values, next_leaf, .. } = &new_node {
            assert_eq!(values, &vec![vec![PK("v_banana")], vec![PK("v_cherry")]]);
            assert_eq!(*next_leaf, original_next_leaf_id); // New node inherits original next_leaf
        } else { panic!("New node should be Leaf"); }
    }

    #[test]
    fn test_split_internal_node_order5() {
        // Order 5: Max 4 keys, 5 children. Min 2 keys, 3 children after split.
        // Node full: [k1, k2, k3, k4], children [c0, c1, c2, c3, c4]
        // Median key (mid_point = (5-1)/2 = 2) is k3. k3 moves up.
        // Left: [k1, k2], children [c0, c1, c2]
        // Right: [k4], children [c3, c4]
        const ORDER_5: usize = 5;
        let mut node = BPlusTreeNode::Internal {
            page_id: TEST_PAGE_ID,
            parent_page_id: TEST_PARENT_PAGE_ID,
            keys: vec![K("a"), K("b"), K("c"), K("d")],
            children: vec![1, 2, 3, 4, 5],
        };
        assert!(node.is_full(ORDER_5));

        let (median_key, new_node) = node.split(ORDER_5, TEST_NEW_PAGE_ID).unwrap();
        assert_eq!(median_key, K("c")); // k3

        // Left node
        assert_eq!(node.get_keys(), &vec![K("a"), K("b")]);
        if let BPlusTreeNode::Internal { children, .. } = &node {
            assert_eq!(children, &vec![1, 2, 3]);
        } else { panic!("Node should be Internal"); }

        // Right node
        assert_eq!(new_node.get_keys(), &vec![K("d")]);
         if let BPlusTreeNode::Internal { children, .. } = &new_node {
            assert_eq!(children, &vec![4, 5]);
        } else { panic!("New node should be Internal"); }
    }

    #[test]
    fn test_split_leaf_node_order5() {
        // Order 5: Max 4 KV pairs. Min 2 KV pairs after split.
        // Node full: [k1, k2, k3, k4], values [v1, v2, v3, v4]
        // Median key (mid_point = (5-1)/2 = 2) is k3. k3 is copied up.
        // Left: [k1, k2], values [v1, v2]
        // Right: [k3, k4], values [v3, v4]
        const ORDER_5: usize = 5;
        let mut node = BPlusTreeNode::Leaf {
            page_id: TEST_PAGE_ID,
            parent_page_id: TEST_PARENT_PAGE_ID,
            keys: vec![K("a"), K("b"), K("c"), K("d")],
            values: vec![vec![PK("v_a")], vec![PK("v_b")], vec![PK("v_c")], vec![PK("v_d")]],
            next_leaf: None,
        };
        assert!(node.is_full(ORDER_5));
        let (copied_median_key, new_node) = node.split(ORDER_5, TEST_NEW_PAGE_ID).unwrap();
        assert_eq!(copied_median_key, K("c")); // k3

        // Left node
        assert_eq!(node.get_keys(), &vec![K("a"), K("b")]);
        if let BPlusTreeNode::Leaf { values, next_leaf, .. } = &node {
            assert_eq!(values, &vec![vec![PK("v_a")], vec![PK("v_b")]]);
            assert_eq!(*next_leaf, Some(TEST_NEW_PAGE_ID));
        } else { panic!("Node should be Leaf"); }

        // Right node
        assert_eq!(new_node.get_keys(), &vec![K("c"), K("d")]); // Median key is first key in new right leaf
        if let BPlusTreeNode::Leaf { values, next_leaf, .. } = &new_node {
            assert_eq!(values, &vec![vec![PK("v_c")], vec![PK("v_d")]]);
            assert_eq!(*next_leaf, None);
        } else { panic!("New node should be Leaf"); }
    }


    #[test]
    fn test_serialization_deserialization_internal_node() {
        let node = BPlusTreeNode::Internal {
            page_id: 123,
            parent_page_id: Some(456),
            keys: vec![K("key1"), K("key22")],
            children: vec![101, 202, 303],
        };
        let bytes = node.to_bytes().unwrap();
        let deserialized_node = BPlusTreeNode::from_bytes(&bytes).unwrap();
        assert_eq!(node, deserialized_node);
    }

    #[test]
    fn test_serialization_deserialization_internal_node_no_parent() {
        let node = BPlusTreeNode::Internal {
            page_id: 123,
            parent_page_id: None,
            keys: vec![K("key1"), K("key22")],
            children: vec![101, 202, 303],
        };
        let bytes = node.to_bytes().unwrap();
        let deserialized_node = BPlusTreeNode::from_bytes(&bytes).unwrap();
        assert_eq!(node, deserialized_node);
    }

    #[test]
    fn test_serialization_deserialization_leaf_node() {
        let node = BPlusTreeNode::Leaf {
            page_id: 789,
            parent_page_id: Some(101112),
            keys: vec![K("leaf_key1"), K("leaf_key222")],
            values: vec![vec![PK("pk1a"), PK("pk1b")], vec![PK("pk2")]],
            next_leaf: Some(999),
        };
        let bytes = node.to_bytes().unwrap();
        let deserialized_node = BPlusTreeNode::from_bytes(&bytes).unwrap();
        assert_eq!(node, deserialized_node);
    }

    #[test]
    fn test_serialization_deserialization_leaf_node_no_parent_no_next() {
        let node = BPlusTreeNode::Leaf {
            page_id: 789,
            parent_page_id: None,
            keys: vec![K("leaf_key1"), K("leaf_key222")],
            values: vec![vec![PK("pk1a"), PK("pk1b")], vec![PK("pk2")]],
            next_leaf: None,
        };
        let bytes = node.to_bytes().unwrap();
        let deserialized_node = BPlusTreeNode::from_bytes(&bytes).unwrap();
        assert_eq!(node, deserialized_node);
    }

    #[test]
    fn test_serialization_empty_internal_node() {
        let node = BPlusTreeNode::Internal {
            page_id: 1, parent_page_id: None, keys: vec![], children: vec![10] // Must have at least one child
        };
        let bytes = node.to_bytes().unwrap();
        let deserialized_node = BPlusTreeNode::from_bytes(&bytes).unwrap();
        assert_eq!(node, deserialized_node);
    }

    #[test]
    fn test_serialization_empty_leaf_node() {
        let node = BPlusTreeNode::Leaf {
            page_id: 1, parent_page_id: None, keys: vec![], values: vec![], next_leaf: None
        };
        let bytes = node.to_bytes().unwrap();
        let deserialized_node = BPlusTreeNode::from_bytes(&bytes).unwrap();
        assert_eq!(node, deserialized_node);
    }
}
