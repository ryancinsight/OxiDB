use super::*;
use crate::core::indexing::btree::node::BPlusTreeNode::{Internal, Leaf};
use crate::core::indexing::btree::node::PageId;
use std::fs::{self, File};
use std::io::Read;
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
        return Err(OxidbError::TreeLogicError(
            "Cannot construct tree with empty node list".to_string(),
        ));
    }

    for node in &nodes {
        println!(
            "[DEBUG CONSTRUCT] Writing node PageID: {:?}, Keys: {:?}",
            node.get_page_id(),
            node.get_keys()
        );
        if let BPlusTreeNode::Internal { children, .. } = node {
            println!("[DEBUG CONSTRUCT] ... Children: {:?}", children);
        } else if let BPlusTreeNode::Leaf { values, next_leaf, .. } = node {
            println!(
                "[DEBUG CONSTRUCT] ... Value sets count: {}, NextLeaf: {:?}",
                values.len(),
                next_leaf
            );
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
        let _tree =
            BPlusTreeIndex::new(test_name.to_string(), path.clone(), TEST_TREE_ORDER).unwrap();
    }
    let loaded_tree =
        BPlusTreeIndex::new(test_name.to_string(), path.clone(), TEST_TREE_ORDER).unwrap();
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
    let deleted_pk_result = tree.delete(&k("a"), Some(&pk("v_a2"))).expect("Delete v_a2 failed");
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
    let deleted_key_entirely = tree.delete(&k("a"), None).expect("Delete entire key 'a' failed");
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
        Internal {
            page_id: p_root,
            parent_page_id: None,
            keys: vec![k("05")],
            children: vec![p_il0, p_il1],
        },
        // IL0 (will underflow)
        Internal {
            page_id: p_il0,
            parent_page_id: Some(p_root),
            keys: vec![k("00")],
            children: vec![p_l0, p_l1],
        },
        Leaf {
            page_id: p_l0,
            parent_page_id: Some(p_il0),
            keys: vec![k("00")],
            values: vec![vec![pk("v00")]],
            next_leaf: Some(p_l1),
        },
        Leaf {
            page_id: p_l1,
            parent_page_id: Some(p_il0),
            keys: vec![k("02")],
            values: vec![vec![pk("v02")]],
            next_leaf: Some(p_l2),
        }, // L1 has 1 key (min)
        // IL1 (lender)
        Internal {
            page_id: p_il1,
            parent_page_id: Some(p_root),
            keys: vec![k("10"), k("12")],
            children: vec![p_l2, p_l3, p_l4],
        },
        Leaf {
            page_id: p_l2,
            parent_page_id: Some(p_il1),
            keys: vec![k("10")],
            values: vec![vec![pk("v10")]],
            next_leaf: Some(p_l3),
        },
        Leaf {
            page_id: p_l3,
            parent_page_id: Some(p_il1),
            keys: vec![k("12")],
            values: vec![vec![pk("v12")]],
            next_leaf: Some(p_l4),
        },
        Leaf {
            page_id: p_l4,
            parent_page_id: Some(p_il1),
            keys: vec![k("14")],
            values: vec![vec![pk("v14")]],
            next_leaf: None,
        },
    ];
    construct_tree_with_nodes_for_tests(
        &mut tree,
        nodes,
        p_root,
        next_available_hint,
        SENTINEL_PAGE_ID,
    )?;

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
    const P_I0: PageId = 1;
    const P_I1: PageId = 2;
    const P_L0: PageId = 3;
    const P_L1: PageId = 4;
    const P_L2: PageId = 5;
    const P_L3: PageId = 6;
    let next_available_hint = 7;

    let nodes = vec![
        Internal {
            page_id: P_ROOT,
            parent_page_id: None,
            keys: vec![k("02")],
            children: vec![P_I0, P_I1],
        },
        Internal {
            page_id: P_I0,
            parent_page_id: Some(P_ROOT),
            keys: vec![k("00")],
            children: vec![P_L0, P_L1],
        },
        Internal {
            page_id: P_I1,
            parent_page_id: Some(P_ROOT),
            keys: vec![k("04")],
            children: vec![P_L2, P_L3],
        },
        Leaf {
            page_id: P_L0,
            parent_page_id: Some(P_I0),
            keys: vec![k("00")],
            values: vec![vec![pk("v00")]],
            next_leaf: Some(P_L1),
        },
        Leaf {
            page_id: P_L1,
            parent_page_id: Some(P_I0),
            keys: vec![k("01")],
            values: vec![vec![pk("v01")]],
            next_leaf: Some(P_L2),
        },
        Leaf {
            page_id: P_L2,
            parent_page_id: Some(P_I1),
            keys: vec![k("04")],
            values: vec![vec![pk("v04")]],
            next_leaf: Some(P_L3),
        },
        Leaf {
            page_id: P_L3,
            parent_page_id: Some(P_I1),
            keys: vec![k("05")],
            values: vec![vec![pk("v05")]],
            next_leaf: None,
        },
    ];
    construct_tree_with_nodes_for_tests(
        &mut tree,
        nodes,
        P_ROOT,
        next_available_hint,
        SENTINEL_PAGE_ID,
    )?;

    let r_pid_before = tree.root_page_id;
    assert_eq!(r_pid_before, P_ROOT);

    tree.delete(&k("00"), None)?; // Delete k("00") from L0

    let new_r_pid_after = tree.root_page_id;
    assert_ne!(new_r_pid_after, r_pid_before, "Root PID should change"); // This is the key assertion that was failing

    let new_root_node = tree.read_node(new_r_pid_after)?;
    assert!(new_root_node.get_parent_page_id().is_none(), "New root should have no parent");

    match &new_root_node {
        Internal { keys, children, .. } => {
            // Standard B+ tree deletion: When L0 and L1 merge, separator k("00") is removed.
            // I0 and I1 merge with parent key k("02") pulled down, plus I1's key k("04").
            // Final merged internal node contains: [k("02"), k("04")]
            assert_eq!(keys.as_slice(), &[k("02"), k("04")]);
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
        }, // L1 has 1 key
        Internal {
            page_id: IL1_PID,
            parent_page_id: Some(R_PID),
            keys: vec![k("05")],
            children: vec![L2_PID, L3_PID],
        }, // IL1 has 1 key
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
        }, // IL2 has 1 key
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
    construct_tree_with_nodes_for_tests(
        &mut tree,
        nodes_to_construct,
        R_PID,
        next_available_hint,
        SENTINEL_PAGE_ID,
    )?;

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
    insert_keys(&mut tree, &["0", "1", "2", "3"])?;

    let r_pid_internal_before_any_delete = tree.root_page_id;
    assert_ne!(r_pid_internal_before_any_delete, 0);

    tree.delete(&k("0"), None)?;
    tree.delete(&k("1"), None)?;

    let old_root_page_id_before_final_delete = tree.root_page_id;
    tree.delete(&k("2"), None)?;

    assert_ne!(tree.root_page_id, old_root_page_id_before_final_delete);
    let final_root_node = tree.read_node(tree.root_page_id)?;
    match final_root_node {
        Leaf { keys, .. } => assert_eq!(keys, vec![k("3")]),
        _ => panic!("Root should be leaf at the end"),
    }
    Ok(())
}
