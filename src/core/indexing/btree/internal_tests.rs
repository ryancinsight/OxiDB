// src/core/indexing/btree/internal_tests.rs
#![cfg(test)]

use super::tree::*; // Access BPlusTreeIndex, OxidbError, etc.
use super::node::*; // Access BPlusTreeNode, KeyType, PageId, PrimaryKey, etc.
use crate::core::indexing::btree::node::BPlusTreeNode::{Internal, Leaf}; // For defining nodes

use std::fs;
use tempfile::{tempdir, TempDir};

// Helper function (from tree.rs tests) to create KeyType
fn k(s: &str) -> KeyType {
    s.as_bytes().to_vec()
}

// Helper function (from tree.rs tests) to create PrimaryKey
fn pk(s: &str) -> PrimaryKey {
    s.as_bytes().to_vec()
}

// Common setup_tree function (adapted from tree.rs tests)
const TEST_TREE_ORDER: usize = 4; // Or make it configurable

fn setup_tree(test_name: &str) -> (BPlusTreeIndex, std::path::PathBuf, TempDir) {
    let dir = tempdir().expect("Failed to create tempdir for test");
    let path = dir.path().join(format!("{}_internal.db", test_name)); // Ensure unique db name
    if path.exists() {
        fs::remove_file(&path).expect("Failed to remove existing test db file");
    }
    let tree = BPlusTreeIndex::new(test_name.to_string(), path.clone(), TEST_TREE_ORDER)
        .expect("Failed to create BPlusTreeIndex");
    (tree, path, dir)
}

fn construct_tree_with_nodes(
    tree: &mut BPlusTreeIndex,
    nodes: Vec<BPlusTreeNode>,
    root_page_id: PageId,
    next_available_page_id: PageId,
) -> Result<(), OxidbError> {
    if nodes.is_empty() {
        return Err(OxidbError::TreeLogicError("Cannot construct tree with empty node list".to_string()));
    }

    for node in &nodes { // Iterate by reference
        tree.write_node(node)?;
    }

    tree.root_page_id = root_page_id;
    tree.next_available_page_id = next_available_page_id;
    tree.free_list_head_page_id = SENTINEL_PAGE_ID; // Assuming fresh tree for tests
    tree.write_metadata()?;
    Ok(())
}

#[test]
    fn test_internal_borrow_from_right_sibling_new() -> Result<(), OxidbError> {
        let (mut tree, _path, _dir) = setup_tree("internal_borrow_right_new");

        const R_PID: PageId = 0;
        const IL0_PID: PageId = 1;
        const L0_PID: PageId = 2;
        const L1_PID: PageId = 3;
        const ML01_PID: PageId = L0_PID;

        const IL1_PID: PageId = 4;
        const L2_PID: PageId = 5;
        const L3_PID: PageId = 6;
        const L4_PID: PageId = 7;
        const NEXT_AVAILABLE_PAGE_ID: PageId = 8;

        let nodes_to_create = vec![
            BPlusTreeNode::Internal { page_id: R_PID, parent_page_id: None, keys: vec![k("03")], children: vec![IL0_PID, IL1_PID] },
            BPlusTreeNode::Internal { page_id: IL0_PID, parent_page_id: Some(R_PID), keys: vec![k("01")], children: vec![L0_PID, L1_PID] },
            BPlusTreeNode::Leaf { page_id: L0_PID, parent_page_id: Some(IL0_PID), keys: vec![k("00")], values: vec![vec![pk("v00")]], next_leaf: Some(L1_PID) },
            BPlusTreeNode::Leaf { page_id: L1_PID, parent_page_id: Some(IL0_PID), keys: vec![k("01")], values: vec![vec![pk("v01")]], next_leaf: Some(L2_PID) },
            BPlusTreeNode::Internal { page_id: IL1_PID, parent_page_id: Some(R_PID), keys: vec![k("05"), k("07")], children: vec![L2_PID, L3_PID, L4_PID] },
            BPlusTreeNode::Leaf { page_id: L2_PID, parent_page_id: Some(IL1_PID), keys: vec![k("03"), k("04")], values: vec![vec![pk("v03")], vec![pk("v04")]], next_leaf: Some(L3_PID) },
            BPlusTreeNode::Leaf { page_id: L3_PID, parent_page_id: Some(IL1_PID), keys: vec![k("05"), k("06")], values: vec![vec![pk("v05")], vec![pk("v06")]], next_leaf: Some(L4_PID) },
            BPlusTreeNode::Leaf { page_id: L4_PID, parent_page_id: Some(IL1_PID), keys: vec![k("07"), k("08"), k("09")], values: vec![vec![pk("v07")], vec![pk("v08")], vec![pk("v09")]], next_leaf: None },
        ];

        construct_tree_with_nodes(&mut tree, nodes_to_create, R_PID, NEXT_AVAILABLE_PAGE_ID)?;
        let _l1_node_before_del = tree.read_node(L1_PID)?;
        let deleted = tree.delete(&k("00"), None)?;
        assert!(deleted, "Deletion of k('00') should be successful");
        let root_node_after = tree.read_node(R_PID)?;
        assert_eq!(root_node_after.get_keys(), &vec![k("05")], "Root keys incorrect after borrow");
        match &root_node_after {
            BPlusTreeNode::Internal { children, .. } => {
                assert_eq!(children, &vec![IL0_PID, IL1_PID], "Root children incorrect");
            }
            _ => panic!("Root should be internal"),
        }
        let il0_node_after = tree.read_node(IL0_PID)?;
        assert_eq!(il0_node_after.get_keys(), &vec![k("03")], "IL0 keys incorrect after borrow");
        assert_eq!(il0_node_after.get_parent_page_id(), Some(R_PID), "IL0 parent incorrect");
        match &il0_node_after {
            BPlusTreeNode::Internal { children, .. } => {
                assert_eq!(children, &vec![ML01_PID, L2_PID], "IL0 children incorrect after borrow");
            }
            _ => panic!("IL0 should be internal"),
        }
        let il1_node_after = tree.read_node(IL1_PID)?;
        assert_eq!(il1_node_after.get_keys(), &vec![k("07")], "IL1 keys incorrect after borrow");
        assert_eq!(il1_node_after.get_parent_page_id(), Some(R_PID), "IL1 parent incorrect");
        match &il1_node_after {
            BPlusTreeNode::Internal { children, .. } => {
                assert_eq!(children, &vec![L3_PID, L4_PID], "IL1 children incorrect after borrow");
            }
            _ => panic!("IL1 should be internal"),
        }
        let ml01_node_after = tree.read_node(ML01_PID)?;
        assert_eq!(ml01_node_after.get_keys(), &vec![k("01")], "Merged leaf ML01 keys incorrect");
        assert_eq!(ml01_node_after.get_parent_page_id(), Some(IL0_PID), "Merged leaf ML01 parent incorrect");
        match &ml01_node_after {
            BPlusTreeNode::Leaf { next_leaf, .. } => {
                assert_eq!(*next_leaf, Some(L2_PID), "Merged leaf ML01 next_leaf incorrect (should point to L2, the new sibling)");
            }
            _ => panic!("ML01 should be a leaf node"),
        }
        let l2_node_after = tree.read_node(L2_PID)?;
        assert_eq!(l2_node_after.get_parent_page_id(), Some(IL0_PID), "L2 parent incorrect after move to IL0");
        match &l2_node_after {
            BPlusTreeNode::Leaf { next_leaf, .. } => {
                assert_eq!(*next_leaf, Some(L3_PID), "L2 next_leaf incorrect (should remain linked to L3)");
            }
            _ => panic!("L2 should be a leaf node"),
        }
        let l3_node_after = tree.read_node(L3_PID)?;
        assert_eq!(l3_node_after.get_parent_page_id(), Some(IL1_PID), "L3 parent incorrect (should still be IL1)");
         match &l3_node_after {
            BPlusTreeNode::Leaf { next_leaf, .. } => {
                assert_eq!(*next_leaf, Some(L4_PID), "L3 next_leaf incorrect");
            }
            _ => panic!("L3 should be a leaf node"),
        }
        assert_eq!(tree.free_list_head_page_id, L1_PID, "L1_PID was not deallocated and made head of free list.");
        let reallocated_page_id = tree.allocate_new_page_id()?;
        assert_eq!(reallocated_page_id, L1_PID, "allocate_new_page_id did not reuse the deallocated L1_PID.");
        Ok(())
    }

#[test]
    fn test_internal_borrow_from_left_sibling_new() -> Result<(), OxidbError> {
        let (mut tree, _path, _dir) = setup_tree("internal_borrow_left_new");
        assert_eq!(tree.order, 4, "Test assumes order 4");

        const R_PID: PageId = 0;
        const IL0_PID: PageId = 1;
        const L0_PID: PageId = 2;
        const L1_PID: PageId = 3;
        const L2_PID: PageId = 4;
        const IL1_PID: PageId = 5;
        const L3_PID: PageId = 6;
        const L4_PID: PageId = 7;
        const ML34_PID: PageId = L3_PID;
        const NEXT_AVAILABLE_PAGE_ID: PageId = 8;

        let nodes_to_construct = vec![
            BPlusTreeNode::Internal {
                page_id: R_PID, parent_page_id: None, keys: vec![k("05")], children: vec![IL0_PID, IL1_PID]
            },
            BPlusTreeNode::Internal {
                page_id: IL0_PID, parent_page_id: Some(R_PID), keys: vec![k("01"), k("03")], children: vec![L0_PID, L1_PID, L2_PID]
            },
            BPlusTreeNode::Leaf {
                page_id: L0_PID, parent_page_id: Some(IL0_PID), keys: vec![k("00")], values: vec![vec![pk("v00")]], next_leaf: Some(L1_PID)
            },
            BPlusTreeNode::Leaf {
                page_id: L1_PID, parent_page_id: Some(IL0_PID), keys: vec![k("01"), k("02")], values: vec![vec![pk("v01")], vec![pk("v02")]], next_leaf: Some(L2_PID)
            },
            BPlusTreeNode::Leaf {
                page_id: L2_PID, parent_page_id: Some(IL0_PID), keys: vec![k("03"), k("04")], values: vec![vec![pk("v03")], vec![pk("v04")]], next_leaf: Some(L3_PID)
            },
            BPlusTreeNode::Internal {
                page_id: IL1_PID, parent_page_id: Some(R_PID), keys: vec![k("07")], children: vec![L3_PID, L4_PID]
            },
            BPlusTreeNode::Leaf {
                page_id: L3_PID, parent_page_id: Some(IL1_PID), keys: vec![k("06")], values: vec![vec![pk("v06")]], next_leaf: Some(L4_PID)
            },
            BPlusTreeNode::Leaf {
                page_id: L4_PID, parent_page_id: Some(IL1_PID), keys: vec![k("07")], values: vec![vec![pk("v07")]], next_leaf: None
            },
        ];
        construct_tree_with_nodes(&mut tree, nodes_to_construct, R_PID, NEXT_AVAILABLE_PAGE_ID)?;
        let _l4_node_before_del = tree.read_node(L4_PID)?;
        let deleted = tree.delete(&k("06"), None)?;
        assert!(deleted, "Deletion of k('06') should be successful");
        let root_node_after = tree.read_node(R_PID)?;
        assert_eq!(root_node_after.get_keys(), &vec![k("03")], "Root key incorrect after borrow");
        match &root_node_after {
            BPlusTreeNode::Internal { children, .. } => {
                assert_eq!(children, &vec![IL0_PID, IL1_PID], "Root children incorrect");
            }
            _ => panic!("Root should be internal"),
        }
        let il0_node_after = tree.read_node(IL0_PID)?;
        assert_eq!(il0_node_after.get_keys(), &vec![k("01")], "IL0 keys incorrect after lending");
        assert_eq!(il0_node_after.get_parent_page_id(), Some(R_PID), "IL0 parent incorrect");
        match &il0_node_after {
            BPlusTreeNode::Internal { children, .. } => {
                assert_eq!(children, &vec![L0_PID, L1_PID], "IL0 children incorrect after lending");
            }
            _ => panic!("IL0 should be internal"),
        }
        let il1_node_after = tree.read_node(IL1_PID)?;
        assert_eq!(il1_node_after.get_keys(), &vec![k("05")], "IL1 keys incorrect after borrowing");
        assert_eq!(il1_node_after.get_parent_page_id(), Some(R_PID), "IL1 parent incorrect");
        match &il1_node_after {
            BPlusTreeNode::Internal { children, .. } => {
                assert_eq!(children, &vec![L2_PID, ML34_PID], "IL1 children incorrect after borrowing");
            }
            _ => panic!("IL1 should be internal"),
        }
        let l2_node_after_move = tree.read_node(L2_PID)?;
        assert_eq!(l2_node_after_move.get_parent_page_id(), Some(IL1_PID), "L2 parent incorrect after move to IL1");
        match &l2_node_after_move {
            BPlusTreeNode::Leaf { next_leaf, .. } => {
                assert_eq!(*next_leaf, Some(ML34_PID), "L2 next_leaf incorrect, should point to merged leaf ML34 (on L3_PID)");
            }
            _ => panic!("L2 should be a leaf node"),
        }
        let ml34_node_after = tree.read_node(ML34_PID)?;
        assert_eq!(ml34_node_after.get_keys(), &vec![k("07")], "Merged leaf ML34 keys incorrect");
        assert_eq!(ml34_node_after.get_parent_page_id(), Some(IL1_PID), "Merged leaf ML34 parent incorrect");
         match &ml34_node_after {
            BPlusTreeNode::Leaf { next_leaf, .. } => {
                assert_eq!(*next_leaf, None, "Merged leaf ML34 next_leaf incorrect (was L4, which was None)");
            }
            _ => panic!("ML34 should be a leaf node"),
        }
        assert_eq!(tree.free_list_head_page_id, L4_PID, "L4_PID was not deallocated and made head of free list.");
        let reallocated_page_id = tree.allocate_new_page_id()?;
        assert_eq!(reallocated_page_id, L4_PID, "allocate_new_page_id did not reuse the deallocated L4_PID.");
        Ok(())
    }

#[test]
    fn test_internal_merge_with_left_sibling_new() -> Result<(), OxidbError> {
        let (mut tree, _path, _dir) = setup_tree("internal_merge_left_new");
        assert_eq!(tree.order, 4, "Test assumes order 4 (min 1 key for internal/leaf)");

        // Define Page IDs
        const R_PID: PageId = 0;
        const IL0_PID: PageId = 1;  // Absorber, at min keys
        const L0_PID: PageId = 2;
        const L1_PID: PageId = 3;
        const IL1_PID: PageId = 4;  // Will underflow and merge into IL0
        const L2_PID: PageId = 5;   // Target for key deletion's leaf, will host merged L2+L3
        const L3_PID: PageId = 6;   // Will merge with L2, page deallocated
        const IL2_PID: PageId = 7;  // Rightmost internal, to ensure IL1 doesn't borrow right
        const L4_PID: PageId = 8;
        const L5_PID: PageId = 9;
        const NEXT_AVAILABLE_PAGE_ID: PageId = 10;

        // L2's page hosts the merged L2+L3 content. L3_PID is deallocated.
        const ML23_PID: PageId = L2_PID;

        // Initial Tree Structure:
        // Root (R_PID): keys [k("03"), k("07")] -> children [IL0, IL1, IL2]
        //   IL0 (Absorber): keys [k("01")] -> children [L0, L1] (at min keys)
        //     L0: keys [k("00")] -> next L1
        //     L1: keys [k("02")] -> next L2 (original L2, before it becomes ML23)
        //   IL1 (Will underflow): keys [k("05")] -> children [L2, L3]
        //     L2: keys [k("04")] (delete this) -> next L3
        //     L3: keys [k("06")] (min keys) -> next L4
        //   IL2 (Right Guard): keys [k("09")] -> children [L4, L5] (at min keys)
        //     L4: keys [k("08")] -> next L5
        //     L5: keys [k("10")] -> next None

        let nodes_to_construct = vec![
            BPlusTreeNode::Internal {
                page_id: R_PID, parent_page_id: None, keys: vec![k("03"), k("07")], children: vec![IL0_PID, IL1_PID, IL2_PID]
            },
            BPlusTreeNode::Internal {
                page_id: IL0_PID, parent_page_id: Some(R_PID), keys: vec![k("01")], children: vec![L0_PID, L1_PID]
            },
            BPlusTreeNode::Leaf {
                page_id: L0_PID, parent_page_id: Some(IL0_PID), keys: vec![k("00")], values: vec![vec![pk("v00")]], next_leaf: Some(L1_PID)
            },
            BPlusTreeNode::Leaf {
                page_id: L1_PID, parent_page_id: Some(IL0_PID), keys: vec![k("02")], values: vec![vec![pk("v02")]], next_leaf: Some(L2_PID)
            },
            BPlusTreeNode::Internal {
                page_id: IL1_PID, parent_page_id: Some(R_PID), keys: vec![k("05")], children: vec![L2_PID, L3_PID]
            },
            BPlusTreeNode::Leaf {
                page_id: L2_PID, parent_page_id: Some(IL1_PID), keys: vec![k("04")], values: vec![vec![pk("v04")]], next_leaf: Some(L3_PID)
            },
            BPlusTreeNode::Leaf {
                page_id: L3_PID, parent_page_id: Some(IL1_PID), keys: vec![k("06")], values: vec![vec![pk("v06")]], next_leaf: Some(L4_PID)
            },
            BPlusTreeNode::Internal {
                page_id: IL2_PID, parent_page_id: Some(R_PID), keys: vec![k("09")], children: vec![L4_PID, L5_PID]
            },
            BPlusTreeNode::Leaf {
                page_id: L4_PID, parent_page_id: Some(IL2_PID), keys: vec![k("08")], values: vec![vec![pk("v08")]], next_leaf: Some(L5_PID)
            },
            BPlusTreeNode::Leaf {
                page_id: L5_PID, parent_page_id: Some(IL2_PID), keys: vec![k("10")], values: vec![vec![pk("v10")]], next_leaf: None
            },
        ];
        construct_tree_with_nodes(&mut tree, nodes_to_construct, R_PID, NEXT_AVAILABLE_PAGE_ID)?;

        let _l3_node_before_del = tree.read_node(L3_PID)?; // Page L3_PID will be deallocated

        // --- Perform deletion ---
        let deleted = tree.delete(&k("04"), None)?;
        assert!(deleted, "Deletion of k('04') should be successful");

        // --- Assertions for final state ---
        let root_node_after = tree.read_node(R_PID)?;
        assert_eq!(root_node_after.get_keys(), &vec![k("07")], "Root key incorrect after merge");
        match &root_node_after {
            BPlusTreeNode::Internal { children, .. } => {
                assert_eq!(children, &vec![IL0_PID, IL2_PID], "Root children incorrect");
            }
            _ => panic!("Root should be internal"),
        }

        let il0_node_after = tree.read_node(IL0_PID)?;
        assert_eq!(il0_node_after.get_keys(), &vec![k("01"), k("03"), k("05")], "IL0 keys incorrect after merge");
        assert_eq!(il0_node_after.get_parent_page_id(), Some(R_PID), "IL0 parent incorrect");
        match &il0_node_after {
            BPlusTreeNode::Internal { children, .. } => {
                assert_eq!(children, &vec![L0_PID, L1_PID, ML23_PID], "IL0 children incorrect after merge");
            }
            _ => panic!("IL0 should be internal"),
        }

        let ml23_node = tree.read_node(ML23_PID)?;
        assert_eq!(ml23_node.get_keys(), &vec![k("06")], "Merged leaf ML23 (on L2_PID) keys incorrect");
        assert_eq!(ml23_node.get_parent_page_id(), Some(IL0_PID), "Merged leaf ML23 parent pointer incorrect");
        match ml23_node {
            Leaf { next_leaf, .. } => assert_eq!(*next_leaf, Some(L4_PID), "Merged leaf ML23 next pointer incorrect"),
            _ => panic!("ML23 node is not a leaf"),
        }

        let l0_node_after = tree.read_node(L0_PID)?;
        assert_eq!(l0_node_after.get_parent_page_id(), Some(IL0_PID));
        match l0_node_after { Leaf {next_leaf, ..} => assert_eq!(next_leaf, Some(L1_PID)), _ => panic!() };

        let l1_node_after = tree.read_node(L1_PID)?;
        assert_eq!(l1_node_after.get_parent_page_id(), Some(IL0_PID));
        match l1_node_after { Leaf {next_leaf, ..} => assert_eq!(next_leaf, Some(L2_PID)), _ => panic!() }; // L2_PID is ML23_PID

        let il2_node_after = tree.read_node(IL2_PID)?;
        assert_eq!(il2_node_after.get_parent_page_id(), Some(R_PID));
        assert_eq!(il2_node_after.get_keys(), &vec![k("09")]);
        match il2_node_after { Internal{children, ..} => assert_eq!(children, &vec![L4_PID, L5_PID]), _=>panic!()};

        let mut deallocated_pages_found = std::collections::HashSet::new();
        deallocated_pages_found.insert(tree.allocate_new_page_id()?);
        deallocated_pages_found.insert(tree.allocate_new_page_id()?);

        assert!(deallocated_pages_found.contains(&L3_PID), "L3_PID was not deallocated and reused.");
        assert!(deallocated_pages_found.contains(&IL1_PID), "IL1_PID was not deallocated and reused.");

        Ok(())
    }

// Placeholder for future tests.
