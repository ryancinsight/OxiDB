use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock}; // For thread-safe access to indexes

use crate::core::common::error::DbError;
use crate::core::indexing::traits::Index;
use crate::core::indexing::hash_index::HashIndex; // Assuming HashIndex is the first type
use crate::core::query::commands::{Value, Key as PrimaryKey};

// Define a type alias for a shared, thread-safe index instance
type SharedIndex = Arc<RwLock<dyn Index + Send + Sync>>;

#[derive(Debug)] // Added Debug derive
pub struct IndexManager {
    indexes: HashMap<String, SharedIndex>,
    base_path: PathBuf, // Directory to store all index files
}

impl IndexManager {
    /// Creates a new `IndexManager`.
    ///
    /// # Arguments
    ///
    /// * `base_path` - The base directory where index files will be stored.
    ///                 This directory should exist.
    pub fn new(base_path: PathBuf) -> Result<Self, DbError> {
        if !base_path.exists() {
            std::fs::create_dir_all(&base_path)
                .map_err(|e| DbError::IoError(e))?;
        } else if !base_path.is_dir() {
            return Err(DbError::IoError(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Base path for indexes must be a directory.",
            )));
        }
        Ok(IndexManager {
            indexes: HashMap::new(),
            base_path,
        })
    }

    /// Creates a new index and adds it to the manager.
    /// Currently, only HashIndex is supported.
    ///
    /// # Arguments
    ///
    /// * `index_name` - The name for the new index.
    /// * `index_type` - A string specifying the type of index to create (e.g., "hash").
    ///
    /// # Errors
    ///
    /// Returns `DbError::IndexError` if an index with the same name already exists
    /// or if the index type is unsupported.
    pub fn create_index(&mut self, index_name: String, index_type: &str) -> Result<(), DbError> {
        if self.indexes.contains_key(&index_name) {
            return Err(DbError::IndexError(format!(
                "Index with name '{}' already exists.",
                index_name
            )));
        }

        let index: SharedIndex = match index_type {
            "hash" => {
                let hash_index = HashIndex::new(index_name.clone(), &self.base_path)?;
                Arc::new(RwLock::new(hash_index))
            }
            _ => {
                return Err(DbError::IndexError(format!(
                    "Unsupported index type: {}",
                    index_type
                )));
            }
        };

        self.indexes.insert(index_name, index);
        Ok(())
    }

    /// Retrieves an index by its name.
    pub fn get_index(&self, index_name: &str) -> Option<SharedIndex> {
        self.indexes.get(index_name).cloned()
    }

    /// Inserts a value into a specific index.
    ///
    /// Called by QueryExecutor when new data is inserted.
    pub fn insert_into_index(
        &self,
        index_name: &str,
        value: &Value,
        primary_key: &PrimaryKey,
    ) -> Result<(), DbError> {
        match self.indexes.get(index_name) {
            Some(index_arc) => {
                let mut index = index_arc.write().map_err(|_| DbError::LockError("Failed to acquire write lock on index".to_string()))?;
                index.insert(value, primary_key)
            }
            None => Err(DbError::IndexError(format!(
                "Index '{}' not found for insertion.",
                index_name
            ))),
        }
    }

    /// Inserts a value into all applicable indexes.
    /// This is a more generic method that might be used by QueryExecutor.
    /// The logic to determine which indexes are "applicable" would be based on schema or configuration,
    /// which is not yet implemented. For now, it iterates all indexes.
    /// This method needs to be refined once schema/table information is available.
    pub fn on_insert_data(
        &self,
        // table_name: &str, // Future: to know which indexes are relevant
        // column_name: &str, // Future: to know which value belongs to which index
        indexed_values: &HashMap<String, Value>, // Map of index_name to value to be indexed
        primary_key: &PrimaryKey,
    ) -> Result<(), DbError> {
        for (index_name, value) in indexed_values {
            if let Some(index_arc) = self.indexes.get(index_name) {
                let mut index = index_arc.write().map_err(|_| DbError::LockError("Failed to acquire write lock on index".to_string()))?;
                index.insert(value, primary_key)?;
            } else {
                // Optionally log a warning or error if an expected index is missing
                // For now, we'll assume it's okay if an index mentioned doesn't exist (e.g. dynamic schema)
                eprintln!("Warning: Index '{}' not found during data insertion.", index_name);
            }
        }
        Ok(())
    }


    /// Deletes a value from a specific index.
    ///
    /// Called by QueryExecutor when data is deleted.
    pub fn delete_from_index(
        &self,
        index_name: &str,
        value: &Value,
        primary_key: Option<&PrimaryKey>,
    ) -> Result<(), DbError> {
        match self.indexes.get(index_name) {
            Some(index_arc) => {
                let mut index = index_arc.write().map_err(|_| DbError::LockError("Failed to acquire write lock on index".to_string()))?;
                index.delete(value, primary_key)
            }
            None => Err(DbError::IndexError(format!(
                "Index '{}' not found for deletion.",
                index_name
            ))),
        }
    }

    /// Deletes a value from all applicable indexes.
    /// Similar to `on_insert_data`, needs refinement with schema information.
    pub fn on_delete_data(
        &self,
        indexed_values: &HashMap<String, Value>, // Map of index_name to value that was indexed
        primary_key: &PrimaryKey, // The PK of the row being deleted
    ) -> Result<(), DbError> {
        for (index_name, value) in indexed_values {
            if let Some(index_arc) = self.indexes.get(index_name) {
                let mut index = index_arc.write().map_err(|_| DbError::LockError("Failed to acquire write lock on index".to_string()))?;
                // When deleting a row, we want to remove this specific PK from the index entry.
                index.delete(value, Some(primary_key))?;
            } else {
                eprintln!("Warning: Index '{}' not found during data deletion.", index_name);
            }
        }
        Ok(())
    }

    /// Handles index updates when data is modified.
    ///
    /// For each managed index, if both old and new values for the data corresponding
    /// to that index are provided, it calls the `update` method on the index instance.
    ///
    /// # Arguments
    ///
    /// * `old_values_map` - A map where keys are index names and values are the old byte values
    ///                      that were indexed for the given primary key.
    /// * `new_values_map` - A map where keys are index names and values are the new byte values
    ///                      to be indexed for the given primary key.
    /// * `primary_key` - The primary key of the row being updated.
    ///
    /// # Errors
    ///
    /// Returns `DbError` if any index update fails.
    pub fn on_update_data(
        &self,
        old_values_map: &HashMap<String, Value>,
        new_values_map: &HashMap<String, Value>,
        primary_key: &PrimaryKey,
    ) -> Result<(), DbError> {
        for (index_name, index_arc) in &self.indexes {
            // Check if this index is affected by the update
            if let (Some(old_value), Some(new_value)) =
                (old_values_map.get(index_name), new_values_map.get(index_name))
            {
                // If old_value and new_value are the same, the index's update method
                // might short-circuit, but calling it is correct.
                let mut index = index_arc.write().map_err(|_| {
                    DbError::LockError(format!("Failed to acquire write lock on index '{}' for update.", index_name))
                })?;
                index.update(old_value, new_value, primary_key)?;
            }
            // If an index name is in one map but not the other, it implies
            // the field was either just added or just removed, or the maps are incomplete.
            // Current Index::update handles old_value == new_value.
            // If a field relevant to an index is added (was NULL, now has value),
            // new_value would be Some, old_value might be represented by a special NULL marker if indexed, or not present.
            // If a field relevant to an index is removed (set to NULL),
            // old_value would be Some, new_value might be NULL marker or not present.
            // This simplified on_update_data assumes the maps provide relevant old/new states for fields
            // that are *currently* indexed. More complex schema changes (add/remove indexed field)
            // would need more sophisticated handling.
        }
        Ok(())
    }


    /// Finds primary keys by querying a specific index.
    pub fn find_by_index(
        &self,
        index_name: &str,
        value: &Value,
    ) -> Result<Option<Vec<PrimaryKey>>, DbError> {
        match self.indexes.get(index_name) {
            Some(index_arc) => {
                let index = index_arc.read().map_err(|_| DbError::LockError("Failed to acquire read lock on index".to_string()))?;
                index.find(value)
            }
            None => Err(DbError::IndexError(format!(
                "Index '{}' not found for find operation.",
                index_name
            ))),
        }
    }

    /// Saves all managed indexes to their respective files.
    pub fn save_all_indexes(&self) -> Result<(), DbError> {
        for index_arc in self.indexes.values() {
            let index = index_arc.read().map_err(|_| DbError::LockError("Failed to acquire read lock for saving index".to_string()))?;
            index.save()?;
        }
        Ok(())
    }

    /// Loads all managed indexes from their respective files.
    /// This would typically be called at startup.
    /// Note: `HashIndex::new` already tries to load itself. This method could be used
    /// to explicitly re-load or discover indexes if they weren't created via `create_index`
    /// during the current session (e.g. loading from a config file).
    /// For simplicity, we assume indexes are created via `create_index` which handles initial load.
    /// This method could be expanded to scan `base_path` for index files and load them.
    pub fn load_all_indexes(&mut self) -> Result<(), DbError> {
        // Clear existing in-memory indexes before loading to avoid duplicates if called multiple times.
        // self.indexes.clear();
        //
        // This is a simplified version. A more robust version would:
        // 1. Scan the self.base_path for .idx files.
        // 2. For each file, derive the index name.
        // 3. Create a new HashIndex (or other type based on metadata) and call load on it.
        // For now, we rely on HashIndex::new loading itself. If IndexManager is created
        // and then create_index is called, loading happens there.
        // If IndexManager is created and we want to load all existing indexes from disk without
        // knowing their names beforehand, that's a more complex discovery process.

        // This is a placeholder if we need an explicit "load all" after manager creation.
        // Currently, `HashIndex::new` handles its own loading.
        // If we have a list of known indexes (e.g., from a config), we could iterate and load.
        for (name, index_arc) in &self.indexes {
            let mut index = index_arc.write().map_err(|_| DbError::LockError(format!("Failed to lock index {} for loading", name)))?;
            index.load().map_err(|e| DbError::IndexError(format!("Error loading index {}: {}", name, e.to_string())))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::indexing::hash_index::HashIndex; // For checking type later if needed & direct use
    use crate::core::indexing::traits::Index; // For direct index interaction
    use tempfile::tempdir;
    use std::fs::File;
    use std::io::Write; // Required for file operations like flush

    // Helper to create a Value (Vec<u8>) from a string literal
    fn val(s: &str) -> Value {
        s.as_bytes().to_vec()
    }

    // Helper to create a PrimaryKey (Vec<u8>) from a string literal
    fn pk(s: &str) -> PrimaryKey {
        s.as_bytes().to_vec()
    }

    #[test]
    fn test_new_index_manager() -> Result<(), DbError> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let base_path = temp_dir.path().join("test_db_indexes");

        // Test creation when base_path does not exist
        assert!(!base_path.exists());
        let manager = IndexManager::new(base_path.clone())?;
        assert!(base_path.exists() && base_path.is_dir());
        assert_eq!(manager.base_path, base_path);

        // Test creation when base_path already exists
        let manager2 = IndexManager::new(base_path.clone())?;
        assert_eq!(manager2.base_path, base_path);

        Ok(())
    }

    #[test]
    fn test_new_index_manager_base_path_is_file() -> Result<(), DbError> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("file_not_dir.txt");
        File::create(&file_path).expect("Failed to create test file");

        let result = IndexManager::new(file_path);
        assert!(result.is_err());
        if let Err(DbError::IoError(io_err)) = result {
            assert_eq!(io_err.kind(), std::io::ErrorKind::InvalidInput);
        } else {
            panic!("Expected IoError for base_path being a file, got {:?}", result);
        }
        Ok(())
    }

    #[test]
    fn test_create_index() -> Result<(), DbError> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let mut manager = IndexManager::new(temp_dir.path().to_path_buf())?;

        // Successfully create a "hash" index
        manager.create_index("idx1".to_string(), "hash")?;
        assert!(manager.indexes.contains_key("idx1"));
        assert!(manager.get_index("idx1").is_some());

        // Attempt to create an index with a name that already exists
        let result_duplicate = manager.create_index("idx1".to_string(), "hash");
        assert!(matches!(result_duplicate, Err(DbError::IndexError(_))));

        // Attempt to create an index with an unsupported type
        let result_unsupported = manager.create_index("idx2".to_string(), "btree");
        assert!(matches!(result_unsupported, Err(DbError::IndexError(_))));

        Ok(())
    }

    #[test]
    fn test_create_index_loads_existing_file() -> Result<(), DbError> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let index_name = "preexisting_idx".to_string();
        let base_path = temp_dir.path();

        let value1 = val("value_for_preload");
        let pk1 = pk("pk_preload");

        // Manually create a HashIndex and save it to simulate a pre-existing index file
        {
            let mut pre_index = HashIndex::new(index_name.clone(), base_path)?;
            pre_index.insert(&value1, &pk1)?;
            pre_index.save()?;
        }

        let mut manager = IndexManager::new(base_path.to_path_buf())?;
        // Now, creating the index should load the data from the file
        manager.create_index(index_name.clone(), "hash")?;

        let loaded_pks = manager.find_by_index(&index_name, &value1)?
            .expect("Value should be found in preloaded index");
        assert_eq!(loaded_pks.len(), 1);
        assert_eq!(loaded_pks[0], pk1);

        Ok(())
    }

    #[test]
    fn test_get_index() -> Result<(), DbError> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let mut manager = IndexManager::new(temp_dir.path().to_path_buf())?;
        manager.create_index("idx1".to_string(), "hash")?;

        assert!(manager.get_index("idx1").is_some());
        assert!(manager.get_index("non_existent_idx").is_none());
        Ok(())
    }

    #[test]
    fn test_insert_operations() -> Result<(), DbError> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let mut manager = IndexManager::new(temp_dir.path().to_path_buf())?;
        let index_name = "insert_op_idx".to_string();
        manager.create_index(index_name.clone(), "hash")?;

        let value1 = val("val1");
        let pk1 = pk("pk1");

        // Test insert_into_index
        manager.insert_into_index(&index_name, &value1, &pk1)?;
        let found_pks = manager.find_by_index(&index_name, &value1)?.expect("Should find val1");
        assert_eq!(found_pks, vec![pk1.clone()]);

        // Test insert_into_index for non-existent index
        let result_non_idx = manager.insert_into_index("no_such_idx", &value1, &pk1);
        assert!(matches!(result_non_idx, Err(DbError::IndexError(_))));

        // Test on_insert_data
        let value2 = val("val2");
        let pk2 = pk("pk2");
        let mut map_values = HashMap::new();
        map_values.insert(index_name.clone(), value2.clone());
        map_values.insert("no_idx_here".to_string(), val("other_val")); // This index doesn't exist

        manager.on_insert_data(&map_values, &pk2)?;
        let found_pks2 = manager.find_by_index(&index_name, &value2)?.expect("Should find val2");
        assert_eq!(found_pks2, vec![pk2.clone()]);
        // eprint! warning for "no_idx_here" is expected but not easily testable here.

        Ok(())
    }

    #[test]
    fn test_delete_operations() -> Result<(), DbError> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let mut manager = IndexManager::new(temp_dir.path().to_path_buf())?;
        let index_name = "delete_op_idx".to_string();
        manager.create_index(index_name.clone(), "hash")?;

        let value1 = val("del_val1");
        let pk1 = pk("del_pk1");
        let pk2 = pk("del_pk2");

        manager.insert_into_index(&index_name, &value1, &pk1)?;
        manager.insert_into_index(&index_name, &value1, &pk2)?;

        // Test delete_from_index (specific PK)
        manager.delete_from_index(&index_name, &value1, Some(&pk1))?;
        let found_pks = manager.find_by_index(&index_name, &value1)?.expect("Should still find val1");
        assert_eq!(found_pks, vec![pk2.clone()]);

        // Test delete_from_index for non-existent index
        let result_non_idx = manager.delete_from_index("no_such_idx", &value1, Some(&pk1));
        assert!(matches!(result_non_idx, Err(DbError::IndexError(_))));

        // Test on_delete_data
        let value2 = val("del_val2");
        let pk3 = pk("del_pk3");
        manager.insert_into_index(&index_name, &value2, &pk3)?;

        let mut map_delete_values = HashMap::new();
        map_delete_values.insert(index_name.clone(), value2.clone());
        map_delete_values.insert("no_idx_here_del".to_string(), val("other_val_del"));

        manager.on_delete_data(&map_delete_values, &pk3)?;
        assert!(manager.find_by_index(&index_name, &value2)?.is_none(), "val2 should be deleted");

        // Test deleting last PK for value1 using delete_from_index
        manager.delete_from_index(&index_name, &value1, Some(&pk2))?;
        assert!(manager.find_by_index(&index_name, &value1)?.is_none(), "val1 should be fully deleted");

        Ok(())
    }

    #[test]
    fn test_find_by_index_behavior() -> Result<(), DbError> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let mut manager = IndexManager::new(temp_dir.path().to_path_buf())?;
        let index_name = "find_test_idx".to_string();
        manager.create_index(index_name.clone(), "hash")?;

        let value_exists = val("existing_value");
        let pk_exists = pk("existing_pk");
        manager.insert_into_index(&index_name, &value_exists, &pk_exists)?;

        // Find existing value
        let result1 = manager.find_by_index(&index_name, &value_exists)?;
        assert_eq!(result1, Some(vec![pk_exists]));

        // Find non-existent value in existing index
        let value_not_in_index = val("value_not_here");
        let result2 = manager.find_by_index(&index_name, &value_not_in_index)?;
        assert_eq!(result2, None);

        // Attempt to find in non-existent index
        let index_not_exists = "non_existent_idx_for_find".to_string();
        let result3 = manager.find_by_index(&index_not_exists, &value_exists);
        assert!(matches!(result3, Err(DbError::IndexError(_))));

        Ok(())
    }

    #[test]
    fn test_save_and_load_all_indexes() -> Result<(), DbError> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let base_path = temp_dir.path().to_path_buf();

        let idx1_name = "multi_idx1".to_string();
        let idx2_name = "multi_idx2".to_string();
        let val1 = val("m_val1");
        let pk1 = pk("m_pk1");
        let val2 = val("m_val2");
        let pk2 = pk("m_pk2");

        // Create manager1, add indexes, insert data, save all
        {
            let mut manager1 = IndexManager::new(base_path.clone())?;
            manager1.create_index(idx1_name.clone(), "hash")?;
            manager1.create_index(idx2_name.clone(), "hash")?;
            manager1.insert_into_index(&idx1_name, &val1, &pk1)?;
            manager1.insert_into_index(&idx2_name, &val2, &pk2)?;
            manager1.save_all_indexes()?;
        }

        // Create manager2, re-create indexes (this should load them)
        let mut manager2 = IndexManager::new(base_path)?;
        manager2.create_index(idx1_name.clone(), "hash")?; // HashIndex::new loads data
        manager2.create_index(idx2_name.clone(), "hash")?; // HashIndex::new loads data

        assert_eq!(manager2.find_by_index(&idx1_name, &val1)?, Some(vec![pk1]));
        assert_eq!(manager2.find_by_index(&idx2_name, &val2)?, Some(vec![pk2]));

        // Test explicit load_all_indexes (currently reloads known indexes)
        // Modify one index in memory without saving, then load_all should revert it if it reloads from disk.
        let shared_idx1 = manager2.get_index(&idx1_name).unwrap();
        let val1_temp = val("m_val1_temp_in_memory");
        let pk1_temp = pk("m_pk1_temp");
        shared_idx1.write().unwrap().insert(&val1_temp, &pk1_temp)?; // Not saved to disk

        // Verify temp data is in memory
        assert_eq!(manager2.find_by_index(&idx1_name, &val1_temp)?, Some(vec![pk1_temp]));

        manager2.load_all_indexes()?; // This should reload idx1 from its file

        // Temp data should be gone, original data should be there
        assert_eq!(manager2.find_by_index(&idx1_name, &val1_temp)?, None);
        assert_eq!(manager2.find_by_index(&idx1_name, &val1)?, Some(vec![pk("m_pk1")]));


        Ok(())
    }

    #[test]
    fn test_on_update_data_calls_index_update() -> Result<(), DbError> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let mut manager = IndexManager::new(temp_dir.path().to_path_buf())?;
        let index_name = "update_test_idx".to_string();
        manager.create_index(index_name.clone(), "hash")?;

        let old_val = val("old_indexed_value");
        let new_val = val("new_indexed_value");
        let primary_key = pk("pk_updated");

        // Pre-populate the index with the old value
        manager.insert_into_index(&index_name, &old_val, &primary_key)?;
        assert_eq!(manager.find_by_index(&index_name, &old_val)?.unwrap(), vec![primary_key.clone()]);

        let mut old_values_map = HashMap::new();
        old_values_map.insert(index_name.clone(), old_val.clone());
        // Also test case where another index is not affected / not present in new_values_map
        old_values_map.insert("other_index_name".to_string(), val("other_old"));


        let mut new_values_map = HashMap::new();
        new_values_map.insert(index_name.clone(), new_val.clone());
        new_values_map.insert("other_index_name".to_string(), val("other_new"));


        manager.on_update_data(&old_values_map, &new_values_map, &primary_key)?;

        // Verify old value is gone for this PK
        assert!(manager.find_by_index(&index_name, &old_val)?.is_none());
        // Verify new value is present for this PK
        assert_eq!(manager.find_by_index(&index_name, &new_val)?.unwrap(), vec![primary_key.clone()]);

        Ok(())
    }

    #[test]
    fn test_on_update_data_value_unchanged_in_index() -> Result<(), DbError> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let mut manager = IndexManager::new(temp_dir.path().to_path_buf())?;
        let index_name = "update_unchanged_test_idx".to_string();
        manager.create_index(index_name.clone(), "hash")?;

        let current_val = val("current_indexed_value");
        let primary_key = pk("pk_unchanged_update");

        manager.insert_into_index(&index_name, &current_val, &primary_key)?;

        let mut old_values_map = HashMap::new();
        old_values_map.insert(index_name.clone(), current_val.clone());
        let mut new_values_map = HashMap::new();
        new_values_map.insert(index_name.clone(), current_val.clone()); // Same value

        manager.on_update_data(&old_values_map, &new_values_map, &primary_key)?;

        // Value should still be there, and PK count should be 1.
        let found_pks = manager.find_by_index(&index_name, &current_val)?.unwrap();
        assert_eq!(found_pks.len(), 1);
        assert_eq!(found_pks[0], primary_key);

        Ok(())
    }

    #[test]
    fn test_on_update_data_index_not_in_maps() -> Result<(), DbError> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let mut manager = IndexManager::new(temp_dir.path().to_path_buf())?;
        let index_name = "update_missing_maps_idx".to_string();
        manager.create_index(index_name.clone(), "hash")?;

        let original_val = val("original_val");
        let primary_key = pk("pk_missing");
        manager.insert_into_index(&index_name, &original_val, &primary_key)?;


        let old_values_map = HashMap::new(); // Index not mentioned
        let new_values_map = HashMap::new(); // Index not mentioned

        manager.on_update_data(&old_values_map, &new_values_map, &primary_key)?;

        // Index should be unchanged as it was not part of the update maps
        assert_eq!(manager.find_by_index(&index_name, &original_val)?.unwrap(), vec![primary_key.clone()]);

        Ok(())
    }
}

// Add `mod tests` block here later.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::indexing::hash_index::HashIndex; // For checking type later if needed & direct use
    use crate::core::indexing::traits::Index; // For direct index interaction
    use tempfile::tempdir;
    use std::fs::File;
    use std::io::Write; // Required for file operations like flush

    // Helper to create a Value (Vec<u8>) from a string literal
    fn val(s: &str) -> Value {
        s.as_bytes().to_vec()
    }

    // Helper to create a PrimaryKey (Vec<u8>) from a string literal
    fn pk(s: &str) -> PrimaryKey {
        s.as_bytes().to_vec()
    }

    #[test]
    fn test_new_index_manager() -> Result<(), DbError> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let base_path = temp_dir.path().join("test_db_indexes");

        // Test creation when base_path does not exist
        assert!(!base_path.exists());
        let manager = IndexManager::new(base_path.clone())?;
        assert!(base_path.exists() && base_path.is_dir());
        assert_eq!(manager.base_path, base_path);

        // Test creation when base_path already exists
        let manager2 = IndexManager::new(base_path.clone())?;
        assert_eq!(manager2.base_path, base_path);

        Ok(())
    }

    #[test]
    fn test_new_index_manager_base_path_is_file() -> Result<(), DbError> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("file_not_dir.txt");
        File::create(&file_path).expect("Failed to create test file");

        let result = IndexManager::new(file_path);
        assert!(result.is_err());
        if let Err(DbError::IoError(io_err)) = result {
            assert_eq!(io_err.kind(), std::io::ErrorKind::InvalidInput);
        } else {
            panic!("Expected IoError for base_path being a file, got {:?}", result);
        }
        Ok(())
    }

    #[test]
    fn test_create_index() -> Result<(), DbError> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let mut manager = IndexManager::new(temp_dir.path().to_path_buf())?;

        // Successfully create a "hash" index
        manager.create_index("idx1".to_string(), "hash")?;
        assert!(manager.indexes.contains_key("idx1"));
        assert!(manager.get_index("idx1").is_some());

        // Attempt to create an index with a name that already exists
        let result_duplicate = manager.create_index("idx1".to_string(), "hash");
        assert!(matches!(result_duplicate, Err(DbError::IndexError(_))));

        // Attempt to create an index with an unsupported type
        let result_unsupported = manager.create_index("idx2".to_string(), "btree");
        assert!(matches!(result_unsupported, Err(DbError::IndexError(_))));

        Ok(())
    }

    #[test]
    fn test_create_index_loads_existing_file() -> Result<(), DbError> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let index_name = "preexisting_idx".to_string();
        let base_path = temp_dir.path();

        let value1 = val("value_for_preload");
        let pk1 = pk("pk_preload");

        // Manually create a HashIndex and save it to simulate a pre-existing index file
        {
            let mut pre_index = HashIndex::new(index_name.clone(), base_path)?;
            pre_index.insert(&value1, &pk1)?;
            pre_index.save()?;
        }

        let mut manager = IndexManager::new(base_path.to_path_buf())?;
        // Now, creating the index should load the data from the file
        manager.create_index(index_name.clone(), "hash")?;

        let loaded_pks = manager.find_by_index(&index_name, &value1)?
            .expect("Value should be found in preloaded index");
        assert_eq!(loaded_pks.len(), 1);
        assert_eq!(loaded_pks[0], pk1);

        Ok(())
    }

    #[test]
    fn test_get_index() -> Result<(), DbError> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let mut manager = IndexManager::new(temp_dir.path().to_path_buf())?;
        manager.create_index("idx1".to_string(), "hash")?;

        assert!(manager.get_index("idx1").is_some());
        assert!(manager.get_index("non_existent_idx").is_none());
        Ok(())
    }

    #[test]
    fn test_insert_operations() -> Result<(), DbError> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let mut manager = IndexManager::new(temp_dir.path().to_path_buf())?;
        let index_name = "insert_op_idx".to_string();
        manager.create_index(index_name.clone(), "hash")?;

        let value1 = val("val1");
        let pk1 = pk("pk1");

        // Test insert_into_index
        manager.insert_into_index(&index_name, &value1, &pk1)?;
        let found_pks = manager.find_by_index(&index_name, &value1)?.expect("Should find val1");
        assert_eq!(found_pks, vec![pk1.clone()]);

        // Test insert_into_index for non-existent index
        let result_non_idx = manager.insert_into_index("no_such_idx", &value1, &pk1);
        assert!(matches!(result_non_idx, Err(DbError::IndexError(_))));

        // Test on_insert_data
        let value2 = val("val2");
        let pk2 = pk("pk2");
        let mut map_values = HashMap::new();
        map_values.insert(index_name.clone(), value2.clone());
        map_values.insert("no_idx_here".to_string(), val("other_val")); // This index doesn't exist

        manager.on_insert_data(&map_values, &pk2)?;
        let found_pks2 = manager.find_by_index(&index_name, &value2)?.expect("Should find val2");
        assert_eq!(found_pks2, vec![pk2.clone()]);
        // eprint! warning for "no_idx_here" is expected but not easily testable here.

        Ok(())
    }

    #[test]
    fn test_delete_operations() -> Result<(), DbError> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let mut manager = IndexManager::new(temp_dir.path().to_path_buf())?;
        let index_name = "delete_op_idx".to_string();
        manager.create_index(index_name.clone(), "hash")?;

        let value1 = val("del_val1");
        let pk1 = pk("del_pk1");
        let pk2 = pk("del_pk2");

        manager.insert_into_index(&index_name, &value1, &pk1)?;
        manager.insert_into_index(&index_name, &value1, &pk2)?;

        // Test delete_from_index (specific PK)
        manager.delete_from_index(&index_name, &value1, Some(&pk1))?;
        let found_pks = manager.find_by_index(&index_name, &value1)?.expect("Should still find val1");
        assert_eq!(found_pks, vec![pk2.clone()]);

        // Test delete_from_index for non-existent index
        let result_non_idx = manager.delete_from_index("no_such_idx", &value1, Some(&pk1));
        assert!(matches!(result_non_idx, Err(DbError::IndexError(_))));

        // Test on_delete_data
        let value2 = val("del_val2");
        let pk3 = pk("del_pk3");
        manager.insert_into_index(&index_name, &value2, &pk3)?;

        let mut map_delete_values = HashMap::new();
        map_delete_values.insert(index_name.clone(), value2.clone());
        map_delete_values.insert("no_idx_here_del".to_string(), val("other_val_del"));

        manager.on_delete_data(&map_delete_values, &pk3)?;
        assert!(manager.find_by_index(&index_name, &value2)?.is_none(), "val2 should be deleted");

        // Test deleting last PK for value1 using delete_from_index
        manager.delete_from_index(&index_name, &value1, Some(&pk2))?;
        assert!(manager.find_by_index(&index_name, &value1)?.is_none(), "val1 should be fully deleted");

        Ok(())
    }

    #[test]
    fn test_find_by_index_behavior() -> Result<(), DbError> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let mut manager = IndexManager::new(temp_dir.path().to_path_buf())?;
        let index_name = "find_test_idx".to_string();
        manager.create_index(index_name.clone(), "hash")?;

        let value_exists = val("existing_value");
        let pk_exists = pk("existing_pk");
        manager.insert_into_index(&index_name, &value_exists, &pk_exists)?;

        // Find existing value
        let result1 = manager.find_by_index(&index_name, &value_exists)?;
        assert_eq!(result1, Some(vec![pk_exists]));

        // Find non-existent value in existing index
        let value_not_in_index = val("value_not_here");
        let result2 = manager.find_by_index(&index_name, &value_not_in_index)?;
        assert_eq!(result2, None);

        // Attempt to find in non-existent index
        let index_not_exists = "non_existent_idx_for_find".to_string();
        let result3 = manager.find_by_index(&index_not_exists, &value_exists);
        assert!(matches!(result3, Err(DbError::IndexError(_))));

        Ok(())
    }

    #[test]
    fn test_save_and_load_all_indexes() -> Result<(), DbError> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let base_path = temp_dir.path().to_path_buf();

        let idx1_name = "multi_idx1".to_string();
        let idx2_name = "multi_idx2".to_string();
        let val1 = val("m_val1");
        let pk1 = pk("m_pk1");
        let val2 = val("m_val2");
        let pk2 = pk("m_pk2");

        // Create manager1, add indexes, insert data, save all
        {
            let mut manager1 = IndexManager::new(base_path.clone())?;
            manager1.create_index(idx1_name.clone(), "hash")?;
            manager1.create_index(idx2_name.clone(), "hash")?;
            manager1.insert_into_index(&idx1_name, &val1, &pk1)?;
            manager1.insert_into_index(&idx2_name, &val2, &pk2)?;
            manager1.save_all_indexes()?;
        }

        // Create manager2, re-create indexes (this should load them)
        let mut manager2 = IndexManager::new(base_path)?;
        manager2.create_index(idx1_name.clone(), "hash")?; // HashIndex::new loads data
        manager2.create_index(idx2_name.clone(), "hash")?; // HashIndex::new loads data

        assert_eq!(manager2.find_by_index(&idx1_name, &val1)?, Some(vec![pk1]));
        assert_eq!(manager2.find_by_index(&idx2_name, &val2)?, Some(vec![pk2]));

        // Test explicit load_all_indexes (currently reloads known indexes)
        // Modify one index in memory without saving, then load_all should revert it if it reloads from disk.
        let shared_idx1 = manager2.get_index(&idx1_name).unwrap();
        let val1_temp = val("m_val1_temp_in_memory");
        let pk1_temp = pk("m_pk1_temp");
        shared_idx1.write().unwrap().insert(&val1_temp, &pk1_temp)?; // Not saved to disk

        // Verify temp data is in memory
        assert_eq!(manager2.find_by_index(&idx1_name, &val1_temp)?, Some(vec![pk1_temp]));

        manager2.load_all_indexes()?; // This should reload idx1 from its file

        // Temp data should be gone, original data should be there
        assert_eq!(manager2.find_by_index(&idx1_name, &val1_temp)?, None);
        assert_eq!(manager2.find_by_index(&idx1_name, &val1)?, Some(vec![pk("m_pk1")]));


        Ok(())
    }
}
