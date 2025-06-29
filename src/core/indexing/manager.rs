use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use crate::core::common::OxidbError;
use crate::core::indexing::btree::BPlusTreeIndex;
use crate::core::indexing::hash_index::HashIndex;
use crate::core::indexing::traits::Index;
use crate::core::indexing::vector::{VectorIndex, kdtree::KdTreeIndex}; // Import VectorIndex and KdTreeIndex
use crate::core::query::commands::{Key as PrimaryKey, Value};
use crate::core::types::VectorData; // For VectorIndex operations

/// A type alias for a shared, thread-safe scalar index.
type SharedScalarIndex = Arc<RwLock<dyn Index + Send + Sync>>;

/// A type alias for a shared, thread-safe vector index.
type SharedVectorIndex = Arc<RwLock<dyn VectorIndex + Send + Sync>>;

/// Manages all indexes within the database system.
#[derive(Debug)]
pub struct IndexManager {
    /// A map storing scalar index instances (hash, btree), keyed by index name.
    scalar_indexes: HashMap<String, SharedScalarIndex>,
    /// A map storing vector index instances (kdtree), keyed by index name.
    vector_indexes: HashMap<String, SharedVectorIndex>,
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
        Ok(IndexManager {
            scalar_indexes: HashMap::new(),
            vector_indexes: HashMap::new(),
            base_path,
        })
    }

    /// Creates a scalar index (e.g., "hash", "btree").
    pub fn create_scalar_index(&mut self, index_name: String, index_type: &str) -> Result<(), OxidbError> {
        if self.scalar_indexes.contains_key(&index_name) || self.vector_indexes.contains_key(&index_name) {
            return Err(OxidbError::Index(format!(
                "Index with name '{}' already exists.",
                index_name
            )));
        }

        let index_path = self.base_path.join(format!("{}.{}", index_name, index_type));

        let index: SharedScalarIndex = match index_type {
            "hash" => {
                let hash_index = HashIndex::new(index_name.clone(), &self.base_path)?;
                Arc::new(RwLock::new(hash_index))
            }
            "btree" => {
                const DEFAULT_BTREE_ORDER: usize = 5;
                let btree_index = BPlusTreeIndex::new(
                    index_name.clone(),
                    index_path,
                    DEFAULT_BTREE_ORDER,
                )
                .map_err(|e| OxidbError::Index(format!("BTree creation error: {:?}", e)))?;
                Arc::new(RwLock::new(btree_index))
            }
            _ => {
                return Err(OxidbError::Index(format!("Unsupported scalar index type: {}", index_type)));
            }
        };

        self.scalar_indexes.insert(index_name, index);
        Ok(())
    }

    /// Creates a vector index (e.g., "kdtree").
    pub fn create_vector_index(&mut self, index_name: String, index_type: &str, dimension: u32) -> Result<(), OxidbError> {
        if self.scalar_indexes.contains_key(&index_name) || self.vector_indexes.contains_key(&index_name) {
            return Err(OxidbError::Index(format!(
                "Index with name '{}' already exists.",
                index_name
            )));
        }

        let index: SharedVectorIndex = match index_type {
            "kdtree" => {
                // KdTreeIndex::new expects base_path for its file construction.
                let kd_tree_index = KdTreeIndex::new(index_name.clone(), dimension, &self.base_path)?;
                Arc::new(RwLock::new(kd_tree_index))
            }
            _ => {
                return Err(OxidbError::Index(format!("Unsupported vector index type: {}", index_type)));
            }
        };

        self.vector_indexes.insert(index_name, index);
        Ok(())
    }

    // Renamed original create_index to create_scalar_index.
    // For backward compatibility or generic DDL, a dispatcher could be made.
    // pub fn create_index(&mut self, index_name: String, index_type: &str, options: IndexOptions) -> Result<(), OxidbError>
    // where IndexOptions could specify dimension for vector indexes etc.

    pub fn get_scalar_index(&self, index_name: &str) -> Option<SharedScalarIndex> {
        self.scalar_indexes.get(index_name).cloned()
    }

    pub fn get_vector_index(&self, index_name: &str) -> Option<SharedVectorIndex> {
        self.vector_indexes.get(index_name).cloned()
    }

    pub fn base_path(&self) -> PathBuf {
        self.base_path.clone()
    }

    pub fn insert_into_scalar_index(
        &self,
        index_name: &str,
        value: &Value,
        primary_key: &PrimaryKey,
    ) -> Result<(), OxidbError> {
        match self.scalar_indexes.get(index_name) {
            Some(index_arc) => {
                let mut index = index_arc.write().map_err(|_| {
                    OxidbError::Lock(format!("Failed to acquire write lock on scalar index '{}'", index_name))
                })?;
                index.insert(value, primary_key)
            }
            None => {
                Err(OxidbError::Index(format!("Scalar index '{}' not found for insertion.", index_name)))
            }
        }
    }

    /// Inserts a (vector, primary_key) pair into a named vector index.
    /// Note: This is for direct insertion. Vector indexes like KD-Tree often benefit from bulk `build`.
    pub fn insert_into_vector_index(
        &self,
        index_name: &str,
        vector: &VectorData,
        primary_key: &PrimaryKey,
    ) -> Result<(), OxidbError> {
        match self.vector_indexes.get(index_name) {
            Some(index_arc) => {
                let mut index = index_arc.write().map_err(|_| {
                    OxidbError::Lock(format!("Failed to acquire write lock on vector index '{}'", index_name))
                })?;
                index.insert(vector, primary_key)
            }
            None => Err(OxidbError::Index(format!(
                "Vector index '{}' not found for insertion.",
                index_name
            ))),
        }
    }


    pub fn on_insert_data(
        &self,
        indexed_values: &HashMap<String, Value>, // For scalar indexes
        // indexed_vectors: &HashMap<String, VectorData>, // TODO: For vector indexes
        primary_key: &PrimaryKey,
    ) -> Result<(), OxidbError> {
        for (index_name, value) in indexed_values {
            if let Some(index_arc) = self.scalar_indexes.get(index_name) {
                let mut index = index_arc.write().map_err(|_| {
                    OxidbError::Lock(format!("Failed to acquire write lock on scalar index '{}'", index_name))
                })?;
                index.insert(value, primary_key)?;
            } else {
                // Consider if it could be a vector index name; current indexed_values is Value not VectorData
                eprintln!("Warning: Scalar index '{}' not found during data insertion for PK {:?}.", index_name, primary_key);
            }
        }
        // TODO: Handle indexed_vectors for vector indexes similarly
        Ok(())
    }

    pub fn delete_from_scalar_index(
        &self,
        index_name: &str,
        value: &Value,
        primary_key: Option<&PrimaryKey>,
    ) -> Result<(), OxidbError> {
        match self.scalar_indexes.get(index_name) {
            Some(index_arc) => {
                let mut index = index_arc.write().map_err(|_| {
                    OxidbError::Lock(format!("Failed to acquire write lock on scalar index '{}'", index_name))
                })?;
                index.delete(value, primary_key)
            }
            None => {
                Err(OxidbError::Index(format!("Scalar index '{}' not found for deletion.", index_name)))
            }
        }
    }

    /// Deletes an entry from a named vector index using its primary key.
    pub fn delete_from_vector_index(
        &self,
        index_name: &str,
        primary_key: &PrimaryKey,
    ) -> Result<(), OxidbError> {
        match self.vector_indexes.get(index_name) {
            Some(index_arc) => {
                let mut index = index_arc.write().map_err(|_| {
                    OxidbError::Lock(format!("Failed to acquire write lock on vector index '{}'", index_name))
                })?;
                index.delete(primary_key)
            }
            None => Err(OxidbError::Index(format!(
                "Vector index '{}' not found for deletion.",
                index_name
            ))),
        }
    }


    pub fn on_delete_data(
        &self,
        indexed_values: &HashMap<String, Value>, // For scalar indexes
        // indexed_vectors: &HashMap<String, VectorData>, // TODO: For vector indexes
        primary_key: &PrimaryKey,
    ) -> Result<(), OxidbError> {
        for (index_name, value) in indexed_values {
            if let Some(index_arc) = self.scalar_indexes.get(index_name) {
                let mut index = index_arc.write().map_err(|_| {
                    OxidbError::Lock(format!("Failed to acquire write lock on scalar index '{}'", index_name))
                })?;
                index.delete(value, Some(primary_key))?;
            } else {
                eprintln!("Warning: Scalar index '{}' not found during data deletion for PK {:?}.", index_name, primary_key);
            }
        }
        // TODO: Handle indexed_vectors for vector indexes
        Ok(())
    }

    pub fn on_update_data(
        &self,
        old_values_map: &HashMap<String, Value>, // For scalar indexes
        new_values_map: &HashMap<String, Value>, // For scalar indexes
        // old_vectors_map: &HashMap<String, VectorData>, // TODO
        // new_vectors_map: &HashMap<String, VectorData>, // TODO
        primary_key: &PrimaryKey,
    ) -> Result<(), OxidbError> {
        for (index_name, index_arc) in &self.scalar_indexes {
            if let (Some(old_value), Some(new_value)) =
                (old_values_map.get(index_name), new_values_map.get(index_name))
            {
                let mut index = index_arc.write().map_err(|_| {
                    OxidbError::Lock(format!(
                        "Failed to acquire write lock on scalar index '{}' for update.",
                        index_name
                    ))
                })?;
                index.update(old_value, new_value, primary_key)?;
            }
        }
        // TODO: Handle vector index updates. This is more complex as it usually involves delete + insert + rebuild.
        Ok(())
    }

    pub fn find_by_scalar_index(
        &self,
        index_name: &str,
        value: &Value,
    ) -> Result<Option<Vec<PrimaryKey>>, OxidbError> {
        match self.scalar_indexes.get(index_name) {
            Some(index_arc) => {
                let index = index_arc.read().map_err(|_| {
                    OxidbError::Lock(format!("Failed to acquire read lock on scalar index '{}'", index_name))
                })?;
                index.find(value)
            }
            None => Err(OxidbError::Index(format!(
                "Scalar index '{}' not found for find operation.",
                index_name
            ))),
        }
    }

    /// Performs a K-Nearest Neighbor search on a named vector index.
    pub fn search_vector_index(
        &self,
        index_name: &str,
        query_vector: &VectorData,
        k: usize,
    ) -> Result<Vec<(PrimaryKey, f32)>, OxidbError> {
        match self.vector_indexes.get(index_name) {
            Some(index_arc) => {
                let index = index_arc.read().map_err(|_| {
                    OxidbError::Lock(format!("Failed to acquire read lock on vector index '{}'", index_name))
                })?;
                // Ensure index is built before search if necessary. Some VDBs do this implicitly.
                // Our current KdTreeIndex needs explicit build. The caller (e.g. query executor)
                // must ensure `build_vector_index` was called.
                index.search_knn(query_vector, k)
            }
            None => Err(OxidbError::Index(format!(
                "Vector index '{}' not found for KNN search.",
                index_name
            ))),
        }
    }

    /// Builds or rebuilds a named vector index with the provided data.
    pub fn build_vector_index(
        &self,
        index_name: &str,
        all_data: &[(PrimaryKey, VectorData)],
    ) -> Result<(), OxidbError> {
        match self.vector_indexes.get(index_name) {
            Some(index_arc) => {
                let mut index = index_arc.write().map_err(|_| {
                    OxidbError::Lock(format!("Failed to acquire write lock for building vector index '{}'", index_name))
                })?;
                index.build(all_data)
            }
            None => Err(OxidbError::Index(format!(
                "Vector index '{}' not found for building.",
                index_name
            ))),
        }
    }


    pub fn save_all_indexes(&self) -> Result<(), OxidbError> {
        for index_arc in self.scalar_indexes.values() {
            let index = index_arc.read().map_err(|_| {
                OxidbError::Lock("Failed to acquire read lock for saving scalar index".to_string())
            })?;
            index.save()?;
        }
        for index_arc in self.vector_indexes.values() {
            let index = index_arc.read().map_err(|_| {
                OxidbError::Lock("Failed to acquire read lock for saving vector index".to_string())
            })?;
            index.save()?;
        }
        Ok(())
    }

    pub fn load_all_indexes(&mut self) -> Result<(), OxidbError> {
        // Loading is typically handled at creation time by the index itself if its file exists.
        // This method could force a reload or be used if indexes are not loaded at creation.
        // For now, let's assume individual index `load` methods are called appropriately
        // (e.g. during `create_scalar_index` or `create_vector_index` if file exists).
        // This explicit `load_all_indexes` can be used to ensure all known indexes attempt to load.

        for (name, index_arc) in &self.scalar_indexes {
            let mut index = index_arc.write().map_err(|_| {
                OxidbError::Lock(format!("Failed to lock scalar index {} for loading", name))
            })?;
            index
                .load()
                .map_err(|e| OxidbError::Index(format!("Error loading scalar index {}: {}", name, e)))?;
        }
        for (name, index_arc) in &self.vector_indexes {
            let mut index = index_arc.write().map_err(|_| {
                OxidbError::Lock(format!("Failed to lock vector index {} for loading", name))
            })?;
            index
                .load()
                .map_err(|e| OxidbError::Index(format!("Error loading vector index {}: {}", name, e)))?;
            // After loading a vector index, it might need to be rebuilt.
            // The IndexManager or system needs a policy for this.
            // For now, loading just loads the data; `build_vector_index` is separate.
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // use crate::core::indexing::hash_index::HashIndex; // Keep for existing tests if any
    // use crate::core::indexing::traits::Index; // Already imported
    use std::fs::File;
    // Removed create_dir_all as it's unused in this test module's scope
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
        let temp_dir =
            tempdir().expect("test_new_index_manager_base_path_is_file: Failed to create temp dir");
        let file_path = temp_dir.path().join("file_not_dir.txt");
        File::create(&file_path)
            .expect("test_new_index_manager_base_path_is_file: Failed to create test file");

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
        assert!(
            manager.indexes.contains_key("idx1_hash"),
            "Hash index should exist after creation"
        );
        assert!(manager.get_index("idx1_hash").is_some(), "Hash index should be retrievable");

        let result_duplicate = manager.create_index("idx1_hash".to_string(), "hash");
        assert!(
            matches!(result_duplicate, Err(OxidbError::Index(_))),
            "Creating duplicate hash index should fail"
        );

        let result_unsupported = manager.create_index("idx2_unsupported".to_string(), "weird_idx");
        assert!(
            matches!(result_unsupported, Err(OxidbError::Index(_))),
            "Creating unsupported index type should fail"
        );
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
        assert!(
            manager.indexes.contains_key(&index_name),
            "BTree index should exist after creation"
        );
        assert!(manager.get_index(&index_name).is_some(), "BTree index should be retrievable");

        // Check that the .btree file was created
        let btree_file_path = base_path.join(format!("{}.btree", index_name));
        assert!(
            btree_file_path.exists(),
            "BTree index file should be created at {:?}",
            btree_file_path
        );
        assert!(btree_file_path.is_file(), "BTree index path should be a file");

        Ok(())
    }

    #[test]
    fn test_insert_find_delete_via_manager_btree() -> Result<(), OxidbError> {
        let temp_dir = tempdir()
            .expect("test_insert_find_delete_via_manager_btree: Failed to create temp dir");
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
        let found_val1 = manager
            .find_by_index(&index_name, &val1)?
            .expect("val1 should be found in btree index");
        assert_eq!(found_val1.len(), 2, "val1 should have two primary keys");
        assert!(found_val1.contains(&pk1), "val1 should contain pk1");
        assert!(found_val1.contains(&val1_pk2), "val1 should contain val1_pk2");

        let found_val2 = manager
            .find_by_index(&index_name, &val2)?
            .expect("val2 should be found in btree index");
        assert_eq!(found_val2, vec![pk2.clone()], "val2 should have one primary key");

        assert!(
            manager.find_by_index(&index_name, &val("cherry"))?.is_none(),
            "cherry should not be found in btree index"
        );

        // Delete specific PK
        manager.delete_from_index(&index_name, &val1, Some(&pk1))?;
        let found_val1_after_delete_pk = manager
            .find_by_index(&index_name, &val1)?
            .expect("val1 should still be found after deleting one pk");
        assert_eq!(
            found_val1_after_delete_pk,
            vec![val1_pk2.clone()],
            "val1 should only contain val1_pk2 after pk1 deletion"
        );

        // Delete entire key entry
        manager.delete_from_index(&index_name, &val2, None)?;
        assert!(
            manager.find_by_index(&index_name, &val2)?.is_none(),
            "val2 should be deleted from btree index"
        );

        // Delete last PK for val1
        manager.delete_from_index(&index_name, &val1, Some(&val1_pk2))?;
        assert!(
            manager.find_by_index(&index_name, &val1)?.is_none(),
            "val1 should be fully deleted from btree index"
        );

        Ok(())
    }

    #[test]
    fn test_save_load_btree_via_manager() -> Result<(), OxidbError> {
        let temp_dir =
            tempdir().expect("test_save_load_btree_via_manager: Failed to create temp dir");
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

        assert_eq!(
            manager2.find_by_index(&index_name, &val1)?,
            Some(vec![pk1.clone()]),
            "val1 should be found after load"
        );
        assert_eq!(
            manager2.find_by_index(&index_name, &val2)?,
            Some(vec![pk2.clone()]),
            "val2 should be found after load"
        );

        // Test finding non-existent key
        assert!(
            manager2.find_by_index(&index_name, &val("persistent_cherry"))?.is_none(),
            "Non-existent key should not be found after load"
        );

        Ok(())
    }

    // Existing hash index tests (should be kept and pass)
    #[test]
    fn test_create_index_loads_existing_hash_file() -> Result<(), OxidbError> {
        let temp_dir = tempdir()
            .expect("test_create_index_loads_existing_hash_file: Failed to create temp dir");
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
