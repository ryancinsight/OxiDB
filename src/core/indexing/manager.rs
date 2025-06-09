use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use crate::core::common::OxidbError; // Changed
use crate::core::indexing::hash_index::HashIndex;
use crate::core::indexing::traits::Index;
use crate::core::query::commands::{Key as PrimaryKey, Value};

type SharedIndex = Arc<RwLock<dyn Index + Send + Sync>>;

#[derive(Debug)]
pub struct IndexManager {
    indexes: HashMap<String, SharedIndex>,
    base_path: PathBuf,
}

impl IndexManager {
    pub fn new(base_path: PathBuf) -> Result<Self, OxidbError> { // Changed
        if !base_path.exists() {
            std::fs::create_dir_all(&base_path).map_err(OxidbError::Io)?; // Changed
        } else if !base_path.is_dir() {
            return Err(OxidbError::Io(std::io::Error::new( // Changed
                std::io::ErrorKind::InvalidInput,
                "Base path for indexes must be a directory.",
            )));
        }
        Ok(IndexManager { indexes: HashMap::new(), base_path })
    }

    pub fn create_index(&mut self, index_name: String, index_type: &str) -> Result<(), OxidbError> { // Changed
        if self.indexes.contains_key(&index_name) {
            return Err(OxidbError::Index(format!( // Changed
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
                return Err(OxidbError::Index(format!("Unsupported index type: {}", index_type))); // Changed
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

    pub fn insert_into_index(
        &self,
        index_name: &str,
        value: &Value,
        primary_key: &PrimaryKey,
    ) -> Result<(), OxidbError> { // Changed
        match self.indexes.get(index_name) {
            Some(index_arc) => {
                let mut index = index_arc.write().map_err(|_| {
                    OxidbError::Lock("Failed to acquire write lock on index".to_string()) // Changed
                })?;
                index.insert(value, primary_key)
            }
            None => {
                Err(OxidbError::Index(format!("Index '{}' not found for insertion.", index_name))) // Changed
            }
        }
    }

    pub fn on_insert_data(
        &self,
        indexed_values: &HashMap<String, Value>,
        primary_key: &PrimaryKey,
    ) -> Result<(), OxidbError> { // Changed
        for (index_name, value) in indexed_values {
            if let Some(index_arc) = self.indexes.get(index_name) {
                let mut index = index_arc.write().map_err(|_| {
                    OxidbError::Lock("Failed to acquire write lock on index".to_string()) // Changed
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
    ) -> Result<(), OxidbError> { // Changed
        match self.indexes.get(index_name) {
            Some(index_arc) => {
                let mut index = index_arc.write().map_err(|_| {
                    OxidbError::Lock("Failed to acquire write lock on index".to_string()) // Changed
                })?;
                index.delete(value, primary_key)
            }
            None => {
                Err(OxidbError::Index(format!("Index '{}' not found for deletion.", index_name))) // Changed
            }
        }
    }

    pub fn on_delete_data(
        &self,
        indexed_values: &HashMap<String, Value>,
        primary_key: &PrimaryKey,
    ) -> Result<(), OxidbError> { // Changed
        for (index_name, value) in indexed_values {
            if let Some(index_arc) = self.indexes.get(index_name) {
                let mut index = index_arc.write().map_err(|_| {
                    OxidbError::Lock("Failed to acquire write lock on index".to_string()) // Changed
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
    ) -> Result<(), OxidbError> { // Changed
        for (index_name, index_arc) in &self.indexes {
            if let (Some(old_value), Some(new_value)) =
                (old_values_map.get(index_name), new_values_map.get(index_name))
            {
                let mut index = index_arc.write().map_err(|_| {
                    OxidbError::Lock(format!( // Changed
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
    ) -> Result<Option<Vec<PrimaryKey>>, OxidbError> { // Changed
        match self.indexes.get(index_name) {
            Some(index_arc) => {
                let index = index_arc.read().map_err(|_| {
                    OxidbError::Lock("Failed to acquire read lock on index".to_string()) // Changed
                })?;
                index.find(value)
            }
            None => Err(OxidbError::Index(format!( // Changed
                "Index '{}' not found for find operation.",
                index_name
            ))),
        }
    }

    pub fn save_all_indexes(&self) -> Result<(), OxidbError> { // Changed
        for index_arc in self.indexes.values() {
            let index = index_arc.read().map_err(|_| {
                OxidbError::Lock("Failed to acquire read lock for saving index".to_string()) // Changed
            })?;
            index.save()?;
        }
        Ok(())
    }

    pub fn load_all_indexes(&mut self) -> Result<(), OxidbError> { // Changed
        for (name, index_arc) in &self.indexes {
            let mut index = index_arc.write().map_err(|_| {
                OxidbError::Lock(format!("Failed to lock index {} for loading", name)) // Changed
            })?;
            index
                .load()
                .map_err(|e| OxidbError::Index(format!("Error loading index {}: {}", name, e)))?; // Changed
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::indexing::hash_index::HashIndex;
    use crate::core::indexing::traits::Index;
    use std::fs::File;
    use tempfile::tempdir;
    // use std::io::Write; // No longer needed here as it's not directly used by tests. File::create is enough for test setup.

    fn val(s: &str) -> Value {
        s.as_bytes().to_vec()
    }

    fn pk(s: &str) -> PrimaryKey {
        s.as_bytes().to_vec()
    }

    #[test]
    fn test_new_index_manager() -> Result<(), OxidbError> { // Changed
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let base_path = temp_dir.path().join("test_db_indexes");

        assert!(!base_path.exists());
        let manager = IndexManager::new(base_path.clone())?;
        assert!(base_path.exists() && base_path.is_dir());
        assert_eq!(manager.base_path, base_path);

        let manager2 = IndexManager::new(base_path.clone())?;
        assert_eq!(manager2.base_path, base_path);

        Ok(())
    }

    #[test]
    fn test_new_index_manager_base_path_is_file() -> Result<(), OxidbError> { // Changed
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("file_not_dir.txt");
        File::create(&file_path).expect("Failed to create test file");

        let result = IndexManager::new(file_path);
        assert!(result.is_err());
        if let Err(OxidbError::Io(io_err)) = result { // Changed
            assert_eq!(io_err.kind(), std::io::ErrorKind::InvalidInput);
        } else {
            panic!("Expected OxidbError::Io for base_path being a file, got {:?}", result); // Changed
        }
        Ok(())
    }

    #[test]
    fn test_create_index() -> Result<(), OxidbError> { // Changed
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let mut manager = IndexManager::new(temp_dir.path().to_path_buf())?;

        manager.create_index("idx1".to_string(), "hash")?;
        assert!(manager.indexes.contains_key("idx1"));
        assert!(manager.get_index("idx1").is_some());

        let result_duplicate = manager.create_index("idx1".to_string(), "hash");
        assert!(matches!(result_duplicate, Err(OxidbError::Index(_)))); // Changed

        let result_unsupported = manager.create_index("idx2".to_string(), "btree");
        assert!(matches!(result_unsupported, Err(OxidbError::Index(_)))); // Changed

        Ok(())
    }

    #[test]
    fn test_create_index_loads_existing_file() -> Result<(), OxidbError> { // Changed
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let index_name = "preexisting_idx".to_string();
        let base_path = temp_dir.path();

        let value1 = val("value_for_preload");
        let pk1 = pk("pk_preload");

        {
            let mut pre_index = HashIndex::new(index_name.clone(), base_path)?;
            pre_index.insert(&value1, &pk1)?;
            pre_index.save()?;
        }

        let mut manager = IndexManager::new(base_path.to_path_buf())?;
        manager.create_index(index_name.clone(), "hash")?;

        let loaded_pks = manager
            .find_by_index(&index_name, &value1)?
            .expect("Value should be found in preloaded index");
        assert_eq!(loaded_pks.len(), 1);
        assert_eq!(loaded_pks[0], pk1);

        Ok(())
    }

    #[test]
    fn test_get_index() -> Result<(), OxidbError> { // Changed
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let mut manager = IndexManager::new(temp_dir.path().to_path_buf())?;
        manager.create_index("idx1".to_string(), "hash")?;

        assert!(manager.get_index("idx1").is_some());
        assert!(manager.get_index("non_existent_idx").is_none());
        Ok(())
    }

    #[test]
    fn test_insert_operations() -> Result<(), OxidbError> { // Changed
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let mut manager = IndexManager::new(temp_dir.path().to_path_buf())?;
        let index_name = "insert_op_idx".to_string();
        manager.create_index(index_name.clone(), "hash")?;

        let value1 = val("val1");
        let pk1 = pk("pk1");

        manager.insert_into_index(&index_name, &value1, &pk1)?;
        let found_pks = manager.find_by_index(&index_name, &value1)?.expect("Should find val1");
        assert_eq!(found_pks, vec![pk1.clone()]);

        let result_non_idx = manager.insert_into_index("no_such_idx", &value1, &pk1);
        assert!(matches!(result_non_idx, Err(OxidbError::Index(_)))); // Changed

        let value2 = val("val2");
        let pk2 = pk("pk2");
        let mut map_values = HashMap::new();
        map_values.insert(index_name.clone(), value2.clone());
        map_values.insert("no_idx_here".to_string(), val("other_val"));

        manager.on_insert_data(&map_values, &pk2)?;
        let found_pks2 = manager.find_by_index(&index_name, &value2)?.expect("Should find val2");
        assert_eq!(found_pks2, vec![pk2.clone()]);

        Ok(())
    }

    #[test]
    fn test_delete_operations() -> Result<(), OxidbError> { // Changed
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let mut manager = IndexManager::new(temp_dir.path().to_path_buf())?;
        let index_name = "delete_op_idx".to_string();
        manager.create_index(index_name.clone(), "hash")?;

        let value1 = val("del_val1");
        let pk1 = pk("del_pk1");
        let pk2 = pk("del_pk2");

        manager.insert_into_index(&index_name, &value1, &pk1)?;
        manager.insert_into_index(&index_name, &value1, &pk2)?;

        manager.delete_from_index(&index_name, &value1, Some(&pk1))?;
        let found_pks =
            manager.find_by_index(&index_name, &value1)?.expect("Should still find val1");
        assert_eq!(found_pks, vec![pk2.clone()]);

        let result_non_idx = manager.delete_from_index("no_such_idx", &value1, Some(&pk1));
        assert!(matches!(result_non_idx, Err(OxidbError::Index(_)))); // Changed

        let value2 = val("del_val2");
        let pk3 = pk("del_pk3");
        manager.insert_into_index(&index_name, &value2, &pk3)?;

        let mut map_delete_values = HashMap::new();
        map_delete_values.insert(index_name.clone(), value2.clone());
        map_delete_values.insert("no_idx_here_del".to_string(), val("other_val_del"));

        manager.on_delete_data(&map_delete_values, &pk3)?;
        assert!(manager.find_by_index(&index_name, &value2)?.is_none(), "val2 should be deleted");

        manager.delete_from_index(&index_name, &value1, Some(&pk2))?;
        assert!(
            manager.find_by_index(&index_name, &value1)?.is_none(),
            "val1 should be fully deleted"
        );

        Ok(())
    }

    #[test]
    fn test_find_by_index_behavior() -> Result<(), OxidbError> { // Changed
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let mut manager = IndexManager::new(temp_dir.path().to_path_buf())?;
        let index_name = "find_test_idx".to_string();
        manager.create_index(index_name.clone(), "hash")?;

        let value_exists = val("existing_value");
        let pk_exists = pk("existing_pk");
        manager.insert_into_index(&index_name, &value_exists, &pk_exists)?;

        let result1 = manager.find_by_index(&index_name, &value_exists)?;
        assert_eq!(result1, Some(vec![pk_exists]));

        let value_not_in_index = val("value_not_here");
        let result2 = manager.find_by_index(&index_name, &value_not_in_index)?;
        assert_eq!(result2, None);

        let index_not_exists = "non_existent_idx_for_find".to_string();
        let result3 = manager.find_by_index(&index_not_exists, &value_exists);
        assert!(matches!(result3, Err(OxidbError::Index(_)))); // Changed

        Ok(())
    }

    #[test]
    fn test_save_and_load_all_indexes() -> Result<(), OxidbError> { // Changed
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let base_path = temp_dir.path().to_path_buf();

        let idx1_name = "multi_idx1".to_string();
        let idx2_name = "multi_idx2".to_string();
        let val1 = val("m_val1");
        let pk1 = pk("m_pk1");
        let val2 = val("m_val2");
        let pk2 = pk("m_pk2");

        {
            let mut manager1 = IndexManager::new(base_path.clone())?;
            manager1.create_index(idx1_name.clone(), "hash")?;
            manager1.create_index(idx2_name.clone(), "hash")?;
            manager1.insert_into_index(&idx1_name, &val1, &pk1)?;
            manager1.insert_into_index(&idx2_name, &val2, &pk2)?;
            manager1.save_all_indexes()?;
        }

        let mut manager2 = IndexManager::new(base_path)?;
        manager2.create_index(idx1_name.clone(), "hash")?;
        manager2.create_index(idx2_name.clone(), "hash")?;

        assert_eq!(manager2.find_by_index(&idx1_name, &val1)?, Some(vec![pk1]));
        assert_eq!(manager2.find_by_index(&idx2_name, &val2)?, Some(vec![pk2]));

        let shared_idx1 = manager2.get_index(&idx1_name).unwrap();
        let val1_temp = val("m_val1_temp_in_memory");
        let pk1_temp = pk("m_pk1_temp");
        shared_idx1.write().unwrap().insert(&val1_temp, &pk1_temp)?;

        assert_eq!(manager2.find_by_index(&idx1_name, &val1_temp)?, Some(vec![pk1_temp]));

        manager2.load_all_indexes()?;

        assert_eq!(manager2.find_by_index(&idx1_name, &val1_temp)?, None);
        assert_eq!(manager2.find_by_index(&idx1_name, &val1)?, Some(vec![pk("m_pk1")]));

        Ok(())
    }

    #[test]
    fn test_on_update_data_calls_index_update() -> Result<(), OxidbError> { // Changed
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let mut manager = IndexManager::new(temp_dir.path().to_path_buf())?;
        let index_name = "update_test_idx".to_string();
        manager.create_index(index_name.clone(), "hash")?;

        let old_val = val("old_indexed_value");
        let new_val = val("new_indexed_value");
        let primary_key = pk("pk_updated");

        manager.insert_into_index(&index_name, &old_val, &primary_key)?;
        assert_eq!(
            manager.find_by_index(&index_name, &old_val)?.unwrap(),
            vec![primary_key.clone()]
        );

        let mut old_values_map = HashMap::new();
        old_values_map.insert(index_name.clone(), old_val.clone());
        old_values_map.insert("other_index_name".to_string(), val("other_old"));

        let mut new_values_map = HashMap::new();
        new_values_map.insert(index_name.clone(), new_val.clone());
        new_values_map.insert("other_index_name".to_string(), val("other_new"));

        manager.on_update_data(&old_values_map, &new_values_map, &primary_key)?;

        assert!(manager.find_by_index(&index_name, &old_val)?.is_none());
        assert_eq!(
            manager.find_by_index(&index_name, &new_val)?.unwrap(),
            vec![primary_key.clone()]
        );

        Ok(())
    }

    #[test]
    fn test_on_update_data_value_unchanged_in_index() -> Result<(), OxidbError> { // Changed
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
        new_values_map.insert(index_name.clone(), current_val.clone());

        manager.on_update_data(&old_values_map, &new_values_map, &primary_key)?;

        let found_pks = manager.find_by_index(&index_name, &current_val)?.unwrap();
        assert_eq!(found_pks.len(), 1);
        assert_eq!(found_pks[0], primary_key);

        Ok(())
    }

    #[test]
    fn test_on_update_data_index_not_in_maps() -> Result<(), OxidbError> { // Changed
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let mut manager = IndexManager::new(temp_dir.path().to_path_buf())?;
        let index_name = "update_missing_maps_idx".to_string();
        manager.create_index(index_name.clone(), "hash")?;

        let original_val = val("original_val");
        let primary_key = pk("pk_missing");
        manager.insert_into_index(&index_name, &original_val, &primary_key)?;

        let old_values_map = HashMap::new();
        let new_values_map = HashMap::new();

        manager.on_update_data(&old_values_map, &new_values_map, &primary_key)?;

        assert_eq!(
            manager.find_by_index(&index_name, &original_val)?.unwrap(),
            vec![primary_key.clone()]
        );

        Ok(())
    }
}
