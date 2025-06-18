use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use crate::core::common::OxidbError;
use crate::core::indexing::hash_index::HashIndex;
use crate::core::indexing::traits::Index; // Assumes Index trait uses common::OxidbError
use crate::core::indexing::btree::BPlusTreeIndex; // Import BPlusTreeIndex
use crate::core::query::commands::{Key as PrimaryKey, Value};

/// A type alias for a shared, thread-safe index.
/// It uses `Arc` for shared ownership and `RwLock` for interior mutability,
/// allowing multiple threads to read or one thread to write to the index.
/// The `dyn Index + Send + Sync` part means it's a trait object that can be
/// sent between threads and accessed from multiple threads safely.
type SharedIndex = Arc<RwLock<dyn Index + Send + Sync>>;

/// Manages all indexes within the database system.
/// It handles creation, retrieval, and data manipulation (insert, delete, update, find)
/// for various index types.
#[derive(Debug)]
pub struct IndexManager {
    /// A map storing the actual index instances, keyed by index name.
    indexes: HashMap<String, SharedIndex>,
    /// The base file system path where index data is stored.
    base_path: PathBuf,
}

impl IndexManager {
    pub fn new(base_path: PathBuf) -> Result<Self, OxidbError> {
        if !base_path.exists() {
            std::fs::create_dir_all(&base_path).map_err(OxidbError::Io)?;
        } else if !base_path.is_dir() {
            return Err(OxidbError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Base path for indexes must be a directory.",
            )));
        }
        Ok(IndexManager { indexes: HashMap::new(), base_path })
    }

    pub fn create_index(&mut self, index_name: String, index_type: &str) -> Result<(), OxidbError> {
        if self.indexes.contains_key(&index_name) {
            return Err(OxidbError::Index(format!(
                "Index with name '{}' already exists.",
                index_name
            )));
        }

        let index_path = self.base_path.join(format!("{}.{}", index_name, index_type));

        let index: SharedIndex = match index_type {
            "hash" => {
                // HashIndex::new expects base_path, not full file path. It forms filename inside.
                let hash_index = HashIndex::new(index_name.clone(), &self.base_path)?;
                Arc::new(RwLock::new(hash_index))
            }
            "btree" => {
                const DEFAULT_BTREE_ORDER: usize = 5;
                // BPlusTreeIndex::new expects the full path to its file.
                let btree_index = BPlusTreeIndex::new(
                    index_name.clone(),
                    index_path, // Pass the constructed path
                    DEFAULT_BTREE_ORDER
                ).map_err(|e| OxidbError::Index(format!("BTree creation error: {:?}",e)))?; // Map btree::OxidbError
                Arc::new(RwLock::new(btree_index))
            }
            _ => {
                return Err(OxidbError::Index(format!("Unsupported index type: {}", index_type)));
            }
        };

        self.indexes.insert(index_name, index);
        Ok(())
    }

    pub fn get_index(&self, index_name: &str) -> Option<SharedIndex> {
        self.indexes.get(index_name).cloned()
    }

    pub fn base_path(&self) -> PathBuf {
        self.base_path.clone()
    }

    // ... (other methods: insert_into_index, on_insert_data, delete_from_index, on_delete_data, on_update_data, find_by_index)
    // These methods should work fine if the Index trait methods correctly map their errors to common::OxidbError.

    pub fn insert_into_index(
        &self,
        index_name: &str,
        value: &Value,
        primary_key: &PrimaryKey,
    ) -> Result<(), OxidbError> {
        match self.indexes.get(index_name) {
            Some(index_arc) => {
                let mut index = index_arc.write().map_err(|_| {
                    OxidbError::Lock("Failed to acquire write lock on index".to_string())
                })?;
                index.insert(value, primary_key) // This now expects Result<(), common::OxidbError>
            }
            None => {
                Err(OxidbError::Index(format!("Index '{}' not found for insertion.", index_name)))
            }
        }
    }

    pub fn on_insert_data(
        &self,
        indexed_values: &HashMap<String, Value>,
        primary_key: &PrimaryKey,
    ) -> Result<(), OxidbError> {
        for (index_name, value) in indexed_values {
            if let Some(index_arc) = self.indexes.get(index_name) {
                let mut index = index_arc.write().map_err(|_| {
                    OxidbError::Lock("Failed to acquire write lock on index".to_string())
                })?;
                index.insert(value, primary_key)?;
            } else {
                eprintln!("Warning: Index '{}' not found during data insertion.", index_name);
            }
        }
        Ok(())
    }

    pub fn delete_from_index(
        &self,
        index_name: &str,
        value: &Value,
        primary_key: Option<&PrimaryKey>,
    ) -> Result<(), OxidbError> {
        match self.indexes.get(index_name) {
            Some(index_arc) => {
                let mut index = index_arc.write().map_err(|_| {
                    OxidbError::Lock("Failed to acquire write lock on index".to_string())
                })?;
                index.delete(value, primary_key)
            }
            None => {
                Err(OxidbError::Index(format!("Index '{}' not found for deletion.", index_name)))
            }
        }
    }

    pub fn on_delete_data(
        &self,
        indexed_values: &HashMap<String, Value>,
        primary_key: &PrimaryKey,
    ) -> Result<(), OxidbError> {
        for (index_name, value) in indexed_values {
            if let Some(index_arc) = self.indexes.get(index_name) {
                let mut index = index_arc.write().map_err(|_| {
                    OxidbError::Lock("Failed to acquire write lock on index".to_string())
                })?;
                index.delete(value, Some(primary_key))?;
            } else {
                eprintln!("Warning: Index '{}' not found during data deletion.", index_name);
            }
        }
        Ok(())
    }

    pub fn on_update_data(
        &self,
        old_values_map: &HashMap<String, Value>,
        new_values_map: &HashMap<String, Value>,
        primary_key: &PrimaryKey,
    ) -> Result<(), OxidbError> {
        for (index_name, index_arc) in &self.indexes {
            if let (Some(old_value), Some(new_value)) =
                (old_values_map.get(index_name), new_values_map.get(index_name))
            {
                let mut index = index_arc.write().map_err(|_| {
                    OxidbError::Lock(format!(
                        "Failed to acquire write lock on index '{}' for update.",
                        index_name
                    ))
                })?;
                index.update(old_value, new_value, primary_key)?;
            }
        }
        Ok(())
    }

    pub fn find_by_index(
        &self,
        index_name: &str,
        value: &Value,
    ) -> Result<Option<Vec<PrimaryKey>>, OxidbError> {
        match self.indexes.get(index_name) {
            Some(index_arc) => {
                let index = index_arc.read().map_err(|_| {
                    OxidbError::Lock("Failed to acquire read lock on index".to_string())
                })?;
                index.find(value)
            }
            None => Err(OxidbError::Index(format!(
                "Index '{}' not found for find operation.",
                index_name
            ))),
        }
    }


    pub fn save_all_indexes(&self) -> Result<(), OxidbError> {
        for index_arc in self.indexes.values() {
            let index = index_arc.read().map_err(|_| {
                OxidbError::Lock("Failed to acquire read lock for saving index".to_string())
            })?;
            index.save()?;
        }
        Ok(())
    }

    pub fn load_all_indexes(&mut self) -> Result<(), OxidbError> {
        for (name, index_arc) in &self.indexes {
            let mut index = index_arc.write().map_err(|_| {
                OxidbError::Lock(format!("Failed to lock index {} for loading", name))
            })?;
            index
                .load()
                .map_err(|e| OxidbError::Index(format!("Error loading index {}: {}", name, e)))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // use crate::core::indexing::hash_index::HashIndex; // Keep for existing tests if any
    // use crate::core::indexing::traits::Index; // Already imported
    use std::fs::{create_dir_all, File}; // `self` import removed, direct use of `create_dir_all`
    use tempfile::tempdir;

    fn val(s: &str) -> Value {
        s.as_bytes().to_vec()
    }

    fn pk(s: &str) -> PrimaryKey {
        s.as_bytes().to_vec()
    }

    #[test]
    fn test_new_index_manager() -> Result<(), OxidbError> {
        let temp_dir = tempdir().expect("test_new_index_manager: Failed to create temp dir");
        let base_path = temp_dir.path().join("test_db_indexes");

        assert!(!base_path.exists());
        let manager = IndexManager::new(base_path.clone())?;
        assert!(base_path.exists() && base_path.is_dir());
        assert_eq!(manager.base_path, base_path);

        let manager2 = IndexManager::new(base_path.clone())?; // Re-opening on same path is fine
        assert_eq!(manager2.base_path, base_path);

        Ok(())
    }

    #[test]
    fn test_new_index_manager_base_path_is_file() -> Result<(), OxidbError> {
        let temp_dir = tempdir().expect("test_new_index_manager_base_path_is_file: Failed to create temp dir");
        let file_path = temp_dir.path().join("file_not_dir.txt");
        File::create(&file_path).expect("test_new_index_manager_base_path_is_file: Failed to create test file");

        let result = IndexManager::new(file_path);
        assert!(result.is_err());
        if let Err(OxidbError::Io(io_err)) = result {
            assert_eq!(io_err.kind(), std::io::ErrorKind::InvalidInput);
        } else {
            panic!("Expected OxidbError::Io for base_path being a file, got {:?}", result);
        }
        Ok(())
    }

    #[test]
    fn test_create_hash_index() -> Result<(), OxidbError> {
        let temp_dir = tempdir().expect("test_create_hash_index: Failed to create temp dir");
        let mut manager = IndexManager::new(temp_dir.path().to_path_buf())?;

        manager.create_index("idx1_hash".to_string(), "hash")?;
        assert!(manager.indexes.contains_key("idx1_hash"), "Hash index should exist after creation");
        assert!(manager.get_index("idx1_hash").is_some(), "Hash index should be retrievable");

        let result_duplicate = manager.create_index("idx1_hash".to_string(), "hash");
        assert!(matches!(result_duplicate, Err(OxidbError::Index(_))), "Creating duplicate hash index should fail");

        let result_unsupported = manager.create_index("idx2_unsupported".to_string(), "weird_idx");
        assert!(matches!(result_unsupported, Err(OxidbError::Index(_))), "Creating unsupported index type should fail");
        Ok(())
    }

    // --- BPlusTreeIndex Integration Tests ---

    #[test]
    fn test_create_btree_index() -> Result<(), OxidbError> {
        let temp_dir = tempdir().expect("test_create_btree_index: Failed to create temp dir");
        let base_path = temp_dir.path().to_path_buf();
        let mut manager = IndexManager::new(base_path.clone())?;
        let index_name = "my_btree_idx".to_string();

        manager.create_index(index_name.clone(), "btree")?;
        assert!(manager.indexes.contains_key(&index_name), "BTree index should exist after creation");
        assert!(manager.get_index(&index_name).is_some(), "BTree index should be retrievable");

        // Check that the .btree file was created
        let btree_file_path = base_path.join(format!("{}.btree", index_name));
        assert!(btree_file_path.exists(), "BTree index file should be created at {:?}", btree_file_path);
        assert!(btree_file_path.is_file(), "BTree index path should be a file");

        Ok(())
    }

    #[test]
    fn test_insert_find_delete_via_manager_btree() -> Result<(), OxidbError> {
        let temp_dir = tempdir().expect("test_insert_find_delete_via_manager_btree: Failed to create temp dir");
        let mut manager = IndexManager::new(temp_dir.path().to_path_buf())?;
        let index_name = "crud_btree_idx".to_string();
        manager.create_index(index_name.clone(), "btree")?;

        let val1 = val("apple");
        let pk1 = pk("pk_apple1");
        let val2 = val("banana");
        let pk2 = pk("pk_banana2");
        let val1_pk2 = pk("pk_apple_also2");

        // Insert
        manager.insert_into_index(&index_name, &val1, &pk1)?;
        manager.insert_into_index(&index_name, &val2, &pk2)?;
        manager.insert_into_index(&index_name, &val1, &val1_pk2)?; // val1 now has two pks

        // Find
        let found_val1 = manager.find_by_index(&index_name, &val1)?.expect("val1 should be found in btree index");
        assert_eq!(found_val1.len(), 2, "val1 should have two primary keys");
        assert!(found_val1.contains(&pk1), "val1 should contain pk1");
        assert!(found_val1.contains(&val1_pk2), "val1 should contain val1_pk2");

        let found_val2 = manager.find_by_index(&index_name, &val2)?.expect("val2 should be found in btree index");
        assert_eq!(found_val2, vec![pk2.clone()], "val2 should have one primary key");

        assert!(manager.find_by_index(&index_name, &val("cherry"))?.is_none(), "cherry should not be found in btree index");

        // Delete specific PK
        manager.delete_from_index(&index_name, &val1, Some(&pk1))?;
        let found_val1_after_delete_pk = manager.find_by_index(&index_name, &val1)?.expect("val1 should still be found after deleting one pk");
        assert_eq!(found_val1_after_delete_pk, vec![val1_pk2.clone()], "val1 should only contain val1_pk2 after pk1 deletion");

        // Delete entire key entry
        manager.delete_from_index(&index_name, &val2, None)?;
        assert!(manager.find_by_index(&index_name, &val2)?.is_none(), "val2 should be deleted from btree index");

        // Delete last PK for val1
        manager.delete_from_index(&index_name, &val1, Some(&val1_pk2))?;
        assert!(manager.find_by_index(&index_name, &val1)?.is_none(), "val1 should be fully deleted from btree index");

        Ok(())
    }

    #[test]
    fn test_save_load_btree_via_manager() -> Result<(), OxidbError> {
        let temp_dir = tempdir().expect("test_save_load_btree_via_manager: Failed to create temp dir");
        let base_path = temp_dir.path().to_path_buf();
        let index_name = "saveload_btree".to_string();

        let val1 = val("persistent_apple");
        let pk1 = pk("pk_pa1");
        let val2 = val("persistent_banana");
        let pk2 = pk("pk_pb2");

        // Scope for first manager instance
        {
            let mut manager1 = IndexManager::new(base_path.clone())?;
            manager1.create_index(index_name.clone(), "btree")?;
            manager1.insert_into_index(&index_name, &val1, &pk1)?;
            manager1.insert_into_index(&index_name, &val2, &pk2)?;
            manager1.save_all_indexes()?; // Calls index.save() for BTree
        }

        // New manager instance, should load from disk
        let mut manager2 = IndexManager::new(base_path)?;
        // Re-creating the index by name should make it load its existing file.
        // BPlusTreeIndex::new handles loading if the file exists.
        manager2.create_index(index_name.clone(), "btree")?;

        // manager2.load_all_indexes()?; // This is redundant if create_index properly loads.

        assert_eq!(manager2.find_by_index(&index_name, &val1)?, Some(vec![pk1.clone()]), "val1 should be found after load");
        assert_eq!(manager2.find_by_index(&index_name, &val2)?, Some(vec![pk2.clone()]), "val2 should be found after load");

        // Test finding non-existent key
        assert!(manager2.find_by_index(&index_name, &val("persistent_cherry"))?.is_none(), "Non-existent key should not be found after load");

        Ok(())
    }

    // Existing hash index tests (should be kept and pass)
    #[test]
    fn test_create_index_loads_existing_hash_file() -> Result<(), OxidbError> {
        let temp_dir = tempdir().expect("test_create_index_loads_existing_hash_file: Failed to create temp dir");
        let index_name = "preexisting_hash_idx".to_string();
        let base_path_for_hash = temp_dir.path();

        let value1 = val("value_for_preload_hash");
        let pk1 = pk("pk_preload_hash");

        {
            // HashIndex::new expects base_path and constructs filename internally
            let mut pre_index = HashIndex::new(index_name.clone(), base_path_for_hash)?;
            pre_index.insert(&value1, &pk1)?;
            pre_index.save()?; // Ensure data is written to file
        }

        let mut manager = IndexManager::new(base_path_for_hash.to_path_buf())?;
        // This should load the existing hash index file
        manager.create_index(index_name.clone(), "hash")?;

        let loaded_pks = manager
            .find_by_index(&index_name, &value1)?
            .expect("Value should be found in preloaded hash index after manager creation");
        assert_eq!(loaded_pks.len(), 1, "There should be one PK for the preloaded value");
        assert_eq!(loaded_pks[0], pk1, "The PK should match the preloaded PK");

        Ok(())
    }
}
