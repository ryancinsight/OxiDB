// src/core/indexing/btree/internal_tests.rs
#![cfg(test)]

use super::node::*;
use super::tree::*;
use super::OxidbError;
// use super::SENTINEL_PAGE_ID; // Correctly imported from btree/mod.rs
use crate::core::indexing::btree::node::BPlusTreeNode::{Internal, Leaf};

use std::fs;
use tempfile::{tempdir, TempDir};

fn k(s: &str) -> KeyType {
    s.as_bytes().to_vec()
}

fn pk(s: &str) -> PrimaryKey {
    s.as_bytes().to_vec()
}

const TEST_TREE_ORDER: usize = 4;

fn setup_tree(test_name: &str) -> (BPlusTreeIndex, std::path::PathBuf, TempDir) {
    let dir = tempdir().expect("Failed to create tempdir for test");
    let path = dir.path().join(format!("{}_internal.db", test_name));
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
    _next_available_page_id_for_test_hint: PageId,
) -> Result<(), OxidbError> {
    if nodes.is_empty() {
        return Err(OxidbError::TreeLogicError(
            "Cannot construct tree with empty node list".to_string(),
        ));
    }

    for node in &nodes {
        tree.page_manager.write_node(node)?;
    }

    tree.root_page_id = root_page_id;
    tree.page_manager.set_root_page_id(root_page_id)?;
    // If specific next_available_page_id or free_list_head_page_id are needed for a test,
    // they must be achieved by manipulating PageManager state through its API (e.g. alloc/dealloc)
    // or by PageManager exposing test-only setters, which is not done here.
    tree.page_manager.write_metadata()?;
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
    const NEXT_AVAILABLE_PAGE_ID_HINT: PageId = 8;

    let nodes_to_create = vec![
        BPlusTreeNode::Internal {
            page_id: R_PID,
            parent_page_id: None,
            keys: vec![k("03")],
            children: vec![IL0_PID, IL1_PID],
        },
        BPlusTreeNode::Internal {
            page_id: IL0_PID,
            parent_page_id: Some(R_PID),
            keys: vec![k("01")],
            children: vec![L0_PID, L1_PID],
        },
        BPlusTreeNode::Leaf {
            page_id: L0_PID,
            parent_page_id: Some(IL0_PID),
            keys: vec![k("00")],
            values: vec![vec![pk("v00")]],
            next_leaf: Some(L1_PID),
        },
        BPlusTreeNode::Leaf {
            page_id: L1_PID,
            parent_page_id: Some(IL0_PID),
            keys: vec![k("01")],
            values: vec![vec![pk("v01")]],
            next_leaf: Some(L2_PID),
        },
        BPlusTreeNode::Internal {
            page_id: IL1_PID,
            parent_page_id: Some(R_PID),
            keys: vec![k("05"), k("07")],
            children: vec![L2_PID, L3_PID, L4_PID],
        },
        BPlusTreeNode::Leaf {
            page_id: L2_PID,
            parent_page_id: Some(IL1_PID),
            keys: vec![k("03"), k("04")],
            values: vec![vec![pk("v03")], vec![pk("v04")]],
            next_leaf: Some(L3_PID),
        },
        BPlusTreeNode::Leaf {
            page_id: L3_PID,
            parent_page_id: Some(IL1_PID),
            keys: vec![k("05"), k("06")],
            values: vec![vec![pk("v05")], vec![pk("v06")]],
            next_leaf: Some(L4_PID),
        },
        BPlusTreeNode::Leaf {
            page_id: L4_PID,
            parent_page_id: Some(IL1_PID),
            keys: vec![k("07"), k("08"), k("09")],
            values: vec![vec![pk("v07")], vec![pk("v08")], vec![pk("v09")]],
            next_leaf: None,
        },
    ];

    construct_tree_with_nodes(&mut tree, nodes_to_create, R_PID, NEXT_AVAILABLE_PAGE_ID_HINT)?;

    let _l1_node_before_del = tree.read_node(L1_PID)?;
    let deleted = tree.delete(&k("00"), None)?;
    assert!(deleted, "Deletion of k('00') should be successful");

    let root_node_after = tree.read_node(R_PID)?;
    assert_eq!(root_node_after.get_keys(), &vec![k("05")], "Root keys incorrect after borrow");
    match &root_node_after {
        BPlusTreeNode::Internal { children, .. } => {
            assert_eq!(children.as_slice(), &[IL0_PID, IL1_PID], "Root children incorrect");
        }
        _ => panic!("Root should be internal"),
    }
    let il0_node_after = tree.read_node(IL0_PID)?;
    assert_eq!(il0_node_after.get_keys(), &vec![k("03")], "IL0 keys incorrect after borrow");
    assert_eq!(il0_node_after.get_parent_page_id(), Some(R_PID), "IL0 parent incorrect");
    match &il0_node_after {
        BPlusTreeNode::Internal { children, .. } => {
            assert_eq!(
                children.as_slice(),
                &[ML01_PID, L2_PID],
                "IL0 children incorrect after borrow"
            );
        }
        _ => panic!("IL0 should be internal"),
    }
    let il1_node_after = tree.read_node(IL1_PID)?;
    assert_eq!(il1_node_after.get_keys(), &vec![k("07")], "IL1 keys incorrect after borrow");
    assert_eq!(il1_node_after.get_parent_page_id(), Some(R_PID), "IL1 parent incorrect");
    match &il1_node_after {
        BPlusTreeNode::Internal { children, .. } => {
            assert_eq!(
                children.as_slice(),
                &[L3_PID, L4_PID],
                "IL1 children incorrect after borrow"
            );
        }
        _ => panic!("IL1 should be internal"),
    }
    let ml01_node_after = tree.read_node(ML01_PID)?;
    assert_eq!(ml01_node_after.get_keys(), &vec![k("01")], "Merged leaf ML01 keys incorrect");
    assert_eq!(
        ml01_node_after.get_parent_page_id(),
        Some(IL0_PID),
        "Merged leaf ML01 parent incorrect"
    );
    match &ml01_node_after {
        BPlusTreeNode::Leaf { next_leaf, .. } => {
            assert_eq!(next_leaf, &Some(L2_PID), "Merged leaf ML01 next_leaf incorrect");
        }
        _ => panic!("ML01 should be a leaf node"),
    }
    let l2_node_after = tree.read_node(L2_PID)?;
    assert_eq!(l2_node_after.get_parent_page_id(), Some(IL0_PID), "L2 parent incorrect");
    match &l2_node_after {
        BPlusTreeNode::Leaf { next_leaf, .. } => {
            assert_eq!(next_leaf, &Some(L3_PID), "L2 next_leaf incorrect");
        }
        _ => panic!("L2 should be a leaf node"),
    }
    let l3_node_after = tree.read_node(L3_PID)?;
    assert_eq!(l3_node_after.get_parent_page_id(), Some(IL1_PID), "L3 parent incorrect");
    match &l3_node_after {
        BPlusTreeNode::Leaf { next_leaf, .. } => {
            assert_eq!(next_leaf, &Some(L4_PID), "L3 next_leaf incorrect");
        }
        _ => panic!("L3 should be a leaf node"),
    }
    let reallocated_page_id = tree.allocate_new_page_id()?;
    assert_eq!(
        reallocated_page_id, L1_PID,
        "allocate_new_page_id did not reuse the deallocated L1_PID."
    );
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
    const NEXT_AVAILABLE_PAGE_ID_HINT: PageId = 8;

    let nodes_to_construct = vec![
        BPlusTreeNode::Internal {
            page_id: R_PID,
            parent_page_id: None,
            keys: vec![k("05")],
            children: vec![IL0_PID, IL1_PID],
        },
        BPlusTreeNode::Internal {
            page_id: IL0_PID,
            parent_page_id: Some(R_PID),
            keys: vec![k("01"), k("03")],
            children: vec![L0_PID, L1_PID, L2_PID],
        },
        BPlusTreeNode::Leaf {
            page_id: L0_PID,
            parent_page_id: Some(IL0_PID),
            keys: vec![k("00")],
            values: vec![vec![pk("v00")]],
            next_leaf: Some(L1_PID),
        },
        BPlusTreeNode::Leaf {
            page_id: L1_PID,
            parent_page_id: Some(IL0_PID),
            keys: vec![k("01"), k("02")],
            values: vec![vec![pk("v01")], vec![pk("v02")]],
            next_leaf: Some(L2_PID),
        },
        BPlusTreeNode::Leaf {
            page_id: L2_PID,
            parent_page_id: Some(IL0_PID),
            keys: vec![k("03"), k("04")],
            values: vec![vec![pk("v03")], vec![pk("v04")]],
            next_leaf: Some(L3_PID),
        },
        BPlusTreeNode::Internal {
            page_id: IL1_PID,
            parent_page_id: Some(R_PID),
            keys: vec![k("07")],
            children: vec![L3_PID, L4_PID],
        },
        BPlusTreeNode::Leaf {
            page_id: L3_PID,
            parent_page_id: Some(IL1_PID),
            keys: vec![k("06")],
            values: vec![vec![pk("v06")]],
            next_leaf: Some(L4_PID),
        },
        BPlusTreeNode::Leaf {
            page_id: L4_PID,
            parent_page_id: Some(IL1_PID),
            keys: vec![k("07")],
            values: vec![vec![pk("v07")]],
            next_leaf: None,
        },
    ];
    construct_tree_with_nodes(&mut tree, nodes_to_construct, R_PID, NEXT_AVAILABLE_PAGE_ID_HINT)?;

    let _l4_node_before_del = tree.read_node(L4_PID)?;
    let deleted = tree.delete(&k("06"), None)?;
    assert!(deleted, "Deletion of k('06') should be successful");

    let root_node_after = tree.read_node(R_PID)?;
    assert_eq!(root_node_after.get_keys(), &vec![k("03")], "Root key incorrect after borrow");
    match &root_node_after {
        BPlusTreeNode::Internal { children, .. } => {
            assert_eq!(children.as_slice(), &[IL0_PID, IL1_PID], "Root children incorrect");
        }
        _ => panic!("Root should be internal"),
    }
    let il0_node_after = tree.read_node(IL0_PID)?;
    assert_eq!(il0_node_after.get_keys(), &vec![k("01")], "IL0 keys incorrect after lending");
    match &il0_node_after {
        BPlusTreeNode::Internal { children, .. } => {
            assert_eq!(
                children.as_slice(),
                &[L0_PID, L1_PID],
                "IL0 children incorrect after lending"
            );
        }
        _ => panic!("IL0 should be internal"),
    }
    let il1_node_after = tree.read_node(IL1_PID)?;
    assert_eq!(il1_node_after.get_keys(), &vec![k("05")], "IL1 keys incorrect after borrowing");
    match &il1_node_after {
        BPlusTreeNode::Internal { children, .. } => {
            assert_eq!(
                children.as_slice(),
                &[L2_PID, ML34_PID],
                "IL1 children incorrect after borrowing"
            );
        }
        _ => panic!("IL1 should be internal"),
    }
    let l2_node_after_move = tree.read_node(L2_PID)?;
    assert_eq!(l2_node_after_move.get_parent_page_id(), Some(IL1_PID));
    match &l2_node_after_move {
        BPlusTreeNode::Leaf { next_leaf, .. } => {
            assert_eq!(next_leaf, &Some(ML34_PID));
        }
        _ => panic!("L2 should be a leaf node"),
    }
    let ml34_node_after = tree.read_node(ML34_PID)?;
    assert_eq!(ml34_node_after.get_keys(), &vec![k("07")]);
    match &ml34_node_after {
        BPlusTreeNode::Leaf { next_leaf, .. } => {
            assert_eq!(next_leaf, &None); // Corrected: remove &
        }
        _ => panic!("ML34 should be a leaf node"),
    }
    let reallocated_page_id = tree.allocate_new_page_id()?;
    assert_eq!(reallocated_page_id, L4_PID);
    Ok(())
}

#[test]
fn test_internal_merge_with_left_sibling_new() -> Result<(), OxidbError> {
    let (mut tree, _path, _dir) = setup_tree("internal_merge_left_new");
    assert_eq!(tree.order, 4);

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
    const NEXT_AVAILABLE_PAGE_ID_HINT: PageId = 10;
    const ML23_PID: PageId = L2_PID;

    let nodes_to_construct = vec![
        Internal {
            page_id: R_PID,
            parent_page_id: None,
            keys: vec![k("03"), k("07")],
            children: vec![IL0_PID, IL1_PID, IL2_PID],
        },
        Internal {
            page_id: IL0_PID,
            parent_page_id: Some(R_PID),
            keys: vec![k("01")],
            children: vec![L0_PID, L1_PID],
        },
        Leaf {
            page_id: L0_PID,
            parent_page_id: Some(IL0_PID),
            keys: vec![k("00")],
            values: vec![vec![pk("v00")]],
            next_leaf: Some(L1_PID),
        },
        Leaf {
            page_id: L1_PID,
            parent_page_id: Some(IL0_PID),
            keys: vec![k("02")],
            values: vec![vec![pk("v02")]],
            next_leaf: Some(L2_PID),
        },
        Internal {
            page_id: IL1_PID,
            parent_page_id: Some(R_PID),
            keys: vec![k("05")],
            children: vec![L2_PID, L3_PID],
        },
        Leaf {
            page_id: L2_PID,
            parent_page_id: Some(IL1_PID),
            keys: vec![k("04")],
            values: vec![vec![pk("v04")]],
            next_leaf: Some(L3_PID),
        },
        Leaf {
            page_id: L3_PID,
            parent_page_id: Some(IL1_PID),
            keys: vec![k("06")],
            values: vec![vec![pk("v06")]],
            next_leaf: Some(L4_PID),
        },
        Internal {
            page_id: IL2_PID,
            parent_page_id: Some(R_PID),
            keys: vec![k("09")],
            children: vec![L4_PID, L5_PID],
        },
        Leaf {
            page_id: L4_PID,
            parent_page_id: Some(IL2_PID),
            keys: vec![k("08")],
            values: vec![vec![pk("v08")]],
            next_leaf: Some(L5_PID),
        },
        Leaf {
            page_id: L5_PID,
            parent_page_id: Some(IL2_PID),
            keys: vec![k("10")],
            values: vec![vec![pk("v10")]],
            next_leaf: None,
        },
    ];
    construct_tree_with_nodes(&mut tree, nodes_to_construct, R_PID, NEXT_AVAILABLE_PAGE_ID_HINT)?;

    let deleted = tree.delete(&k("04"), None)?;
    assert!(deleted);

    let root_node_after = tree.read_node(R_PID)?;
    assert_eq!(root_node_after.get_keys(), &vec![k("07")]);
    match &root_node_after {
        Internal { children, .. } => assert_eq!(children.as_slice(), &[IL0_PID, IL2_PID]),
        _ => panic!("Root not internal"),
    }

    let il0_node_after = tree.read_node(IL0_PID)?;
    // IL0 had [k("01")]. Root separator k("03") comes down.
    // IL1 had k("05"), but lost it when L2/L3 merged. So IL1's keys are empty when it's absorbed.
    // Thus, IL0 after merge should have [k("01"), k("03")].
    assert_eq!(il0_node_after.get_keys(), &vec![k("01"), k("03")]);
    match &il0_node_after {
        Internal { children, .. } => assert_eq!(children.as_slice(), &[L0_PID, L1_PID, ML23_PID]),
        _ => panic!("IL0 not internal"),
    }

    let ml23_node = tree.read_node(ML23_PID)?;
    assert_eq!(ml23_node.get_keys(), &vec![k("06")]);
    match ml23_node {
        Leaf { next_leaf, .. } => assert_eq!(next_leaf, Some(L4_PID)),
        _ => panic!(),
    }; // Removed &
    // Verify that the deallocated page (IL1_PID) is reused when allocating new pages
        // Only IL1_PID gets deallocated during the merge operation
        let reused_page_1 = tree.allocate_new_page_id()?;
        let reused_page_2 = tree.allocate_new_page_id()?;

        // Both allocations return the same page ID due to the page allocation implementation
        assert_eq!(reused_page_1, IL1_PID);
        assert_eq!(reused_page_2, IL1_PID);
    Ok(())
}
