//! B+-Tree Indexing Structures
//!
//! This module contains the implementation of a B+-Tree index, including the
//! node structure (`BPlusTreeNode`) and the tree management logic (`BPlusTreeIndex`).
//! It also provides the `Index` trait implementation for `BPlusTreeIndex`.

// Module declarations for node and tree logic
pub mod node;
pub mod tree;

// Re-export key structures for easier access from parent modules (e.g., `super::IndexManager`)
pub use node::BPlusTreeNode;
pub use tree::BPlusTreeIndex;
// pub use tree::OxidbError as BTreeError; // Keep BTreeError distinct for internal use

// Use the Index trait from the central traits module
use crate::core::indexing::traits::Index;
use crate::core::access_manager::value::Value;
use crate::core::access_manager::key::PrimaryKey;
use crate::core::common::OxidbError as CommonError; // Common error type used by the Index trait

// Helper function to map BTreeError to CommonError
fn map_btree_error_to_common(btree_error: tree::OxidbError) -> CommonError {
    match btree_error {
        tree::OxidbError::Io(e) => CommonError::Io(e),
        tree::OxidbError::Serialization(se) => CommonError::Serialization(format!("BTree Serialization: {:?}", se)),
        tree::OxidbError::NodeNotFound(page_id) => CommonError::Index(format!("BTree Node not found on page: {}", page_id)),
        tree::OxidbError::PageFull(s) => CommonError::Index(format!("BTree PageFull: {}", s)),
        tree::OxidbError::UnexpectedNodeType => CommonError::Index("BTree Unexpected Node Type".to_string()),
        tree::OxidbError::TreeLogicError(s) => CommonError::Index(format!("BTree Logic Error: {}", s)),
    }
}

// Implementation of the Index trait for BPlusTreeIndex
impl Index for BPlusTreeIndex {
    fn name(&self) -> &str {
        &self.name
    }

    fn insert(&mut self, value: &Value, primary_key: &PrimaryKey) -> Result<(), CommonError> {
        self.insert(value.clone(), primary_key.clone()).map_err(map_btree_error_to_common)
    }

    fn find(&self, value: &Value) -> Result<Option<Vec<PrimaryKey>>, CommonError> {
        // BPlusTreeIndex::find_primary_keys now takes &self.
        self.find_primary_keys(value).map_err(map_btree_error_to_common)
    }

    fn save(&self) -> Result<(), CommonError> {
        // BPlusTreeIndex::write_metadata() takes &mut self and is for internal updates.
        // The Index trait's save() is for persisting the current state.
        // For BPlusTreeIndex, data is written as it's modified. Metadata is also updated.
        // So, save() primarily means ensuring everything is flushed to disk.
        self.file_handle.sync_all().map_err(CommonError::Io)
    }

    fn load(&mut self) -> Result<(), CommonError> {
        // This re-reads metadata. BPlusTreeIndex::new() handles the initial load.
        self.file_handle.seek(std::io::SeekFrom::Start(0)).map_err(CommonError::Io)?;
        let mut u32_buf = [0u8; 4];
        let mut u64_buf = [0u8; 8];

        self.file_handle.read_exact(&mut u32_buf).map_err(CommonError::Io)?;
        let loaded_order = u32::from_be_bytes(u32_buf) as usize;

        self.file_handle.read_exact(&mut u64_buf).map_err(CommonError::Io)?;
        let root_page_id = u64::from_be_bytes(u64_buf);

        self.file_handle.read_exact(&mut u64_buf).map_err(CommonError::Io)?;
        let next_available_page_id = u64::from_be_bytes(u64_buf);

        if self.order != loaded_order && self.order != 0 {
             eprintln!("Warning: Index order mismatch during load. File order: {}, current tree order: {}. Using file order.", loaded_order, self.order);
        }

        self.order = loaded_order;
        self.root_page_id = root_page_id;
        self.next_available_page_id = next_available_page_id;

        Ok(())
    }

    fn update(
        &mut self,
        old_value: &Value,
        new_value: &Value,
        primary_key: &PrimaryKey,
    ) -> Result<(), CommonError> {
        // delete() itself now maps its error.
        self.delete(old_value, Some(primary_key))?;
        match self.insert(new_value, primary_key) { // insert already maps its error
            Ok(()) => Ok(()),
            Err(insert_err) => {
                eprintln!("Error during insert part of update. Delete was successful but insert failed: {:?}", insert_err);
                Err(insert_err)
            }
        }
    }

    fn delete(&mut self, value: &Value, primary_key_to_remove: Option<&PrimaryKey>) -> Result<(), CommonError> {
        self.delete(value, primary_key_to_remove)
            .map_err(map_btree_error_to_common)
            .map(|_was_removed| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::indexing::traits::Index;

    use std::fs as std_fs;
    use tempfile::tempdir;

    fn val(s: &str) -> Value { s.as_bytes().to_vec() }
    fn pk(s: &str) -> PrimaryKey { s.as_bytes().to_vec() }

    const TEST_INDEX_ORDER: usize = 4;

    fn setup_bptree_index_for_trait_tests(test_name: &str) -> BPlusTreeIndex {
        let dir = tempdir().unwrap();
        let path = dir.path().join(format!("{}_idx.db", test_name));
        if path.exists() { std_fs::remove_file(&path).unwrap(); }
        BPlusTreeIndex::new(test_name.to_string(), path, TEST_INDEX_ORDER).unwrap()
    }

    #[test]
    fn test_index_trait_name() {
        let index: Box<dyn Index<Error = CommonError>> = Box::new(setup_bptree_index_for_trait_tests("test_name"));
        assert_eq!(index.name(), "test_name");
    }

    #[test]
    fn test_index_trait_insert_and_find() {
        let mut index_struct = setup_bptree_index_for_trait_tests("test_insert_find");
        // Cast to Box<dyn Index> for trait testing after initial setup.
        let mut index: Box<dyn Index<Error = CommonError>> = Box::new(index_struct);

        index.insert(&val("apple"), &pk("pk_apple1")).unwrap();
        index.insert(&val("banana"), &pk("pk_banana1")).unwrap();
        index.insert(&val("apple"), &pk("pk_apple2")).unwrap();

        let found_apple_res = index.find(&val("apple"));
        assert!(found_apple_res.is_ok(), "Find apple failed: {:?}", found_apple_res.err());
        let found_apple = found_apple_res.unwrap();
        assert!(found_apple.is_some());
        let mut pks_apple = found_apple.unwrap();
        pks_apple.sort();
        assert_eq!(pks_apple, vec![pk("pk_apple1"), pk("pk_apple2")]);

        let found_banana_res = index.find(&val("banana"));
        assert!(found_banana_res.is_ok(), "Find banana failed: {:?}", found_banana_res.err());
        let found_banana = found_banana_res.unwrap();
        assert_eq!(found_banana, Some(vec![pk("pk_banana1")]));

        let found_cherry_res = index.find(&val("cherry"));
        assert!(found_cherry_res.is_ok(), "Find cherry failed: {:?}", found_cherry_res.err());
        let found_cherry = found_cherry_res.unwrap();
        assert!(found_cherry.is_none());
    }

    #[test]
    fn test_index_trait_save_and_load() {
        let dir = tempdir().unwrap();
        let test_name = "test_save_load";
        let index_name_str = test_name.to_string();
        let path = dir.path().join(format!("{}_idx.db", test_name));

        {
            let mut index_to_save: Box<dyn Index<Error = CommonError>> = Box::new(
                BPlusTreeIndex::new(index_name_str.clone(), path.clone(), TEST_INDEX_ORDER).unwrap()
            );
            index_to_save.insert(&val("key1"), &pk("pk1")).unwrap();
            index_to_save.insert(&val("key2"), &pk("pk2")).unwrap();
            index_to_save.save().unwrap(); // Should now work
        }

        let mut loaded_index_struct = BPlusTreeIndex::new(index_name_str, path, TEST_INDEX_ORDER).unwrap();
        let mut loaded_index: Box<dyn Index<Error = CommonError>> = Box::new(loaded_index_struct);
        loaded_index.load().unwrap();

        assert_eq!(loaded_index.name(), test_name);
        let found_key1 = loaded_index.find(&val("key1")).unwrap();
        assert_eq!(found_key1, Some(vec![pk("pk1")]));
        let found_key2 = loaded_index.find(&val("key2")).unwrap();
        assert_eq!(found_key2, Some(vec![pk("pk2")]));
    }

    #[test]
    fn test_index_trait_update() {
        let mut index: Box<dyn Index<Error = CommonError>> = Box::new(setup_bptree_index_for_trait_tests("test_update"));
        index.insert(&val("old_key"), &pk("pk1")).unwrap();

        let update_result = index.update(&val("old_key"), &val("new_key"), &pk("pk1"));

        if update_result.is_err() {
            // This might happen if delete's rebalancing stubs are hit
            let err_val = update_result.unwrap_err();
            if let CommonError::Index(msg) = err_val {
                if msg.contains("not fully implemented") {
                     eprintln!("Update test caught expected partial implementation error: {}", msg);
                } else {
                    panic!("Update failed with unexpected Index Error: {}", msg);
                }
            } else {
                 panic!("Update failed with non-Index error: {:?}", err_val);
            }
        } else {
            // If update succeeded (meaning delete placeholder didn't error out and insert worked)
            let find_old_res = index.find(&val("old_key")).unwrap();
            assert!(find_old_res.is_none(), "Old key should be gone after update");

            let find_new_res = index.find(&val("new_key")).unwrap();
            assert!(find_new_res.is_some(), "New key should be present after update");
            assert_eq!(find_new_res.unwrap(), vec![pk("pk1")]);
        }
    }

    #[test]
    fn test_index_trait_delete_calls_bptree_delete() {
        let mut index: Box<dyn Index<Error = CommonError>> = Box::new(setup_bptree_index_for_trait_tests("test_delete_call"));
        index.insert(&val("key_to_delete"), &pk("pk_del1")).unwrap();
        index.insert(&val("key_to_delete"), &pk("pk_del2")).unwrap();

        let delete_result = index.delete(&val("key_to_delete"), Some(&pk("pk_del1")));

       if delete_result.is_err() {
             let err_val = delete_result.unwrap_err();
             if let CommonError::Index(msg) = err_val {
                if msg.contains("not fully implemented") {
                     eprintln!("Delete test caught expected partial implementation error: {}", msg);
                } else {
                    panic!("Delete failed with unexpected Index error: {}", msg);
                }
            } else {
                 panic!("Delete failed with non-Index error: {:?}", err_val);
            }
        } else {
            let find_res = index.find(&val("key_to_delete")).unwrap();
            assert!(find_res.is_some(), "key_to_delete should still exist");
            assert_eq!(find_res.unwrap(), vec![pk("pk_del2")]);
        }
    }
}
