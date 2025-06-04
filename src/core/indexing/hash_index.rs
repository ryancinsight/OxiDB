use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};
use serde::{Deserialize, Serialize}; // For persistence

use crate::core::common::error::DbError;
use crate::core::indexing::traits::Index;
use crate::core::query::commands::{Value, Key as PrimaryKey}; // Value is Vec<u8>

const DEFAULT_INDEX_FILE_EXTENSION: &str = "idx";

#[derive(Serialize, Deserialize, Debug)]
pub struct HashIndex {
    name: String,
    store: HashMap<Value, Vec<PrimaryKey>>,
    file_path: PathBuf, // Path for persistence
}

impl HashIndex {
    /// Creates a new `HashIndex`.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the index.
    /// * `base_path` - The directory where the index file will be stored.
    ///
    /// The actual index file will be named `[name].[DEFAULT_INDEX_FILE_EXTENSION]`
    /// within the `base_path`.
    pub fn new(name: String, base_path: &Path) -> Result<Self, DbError> {
        let mut file_path = base_path.to_path_buf();
        file_path.push(format!("{}.{}", name, DEFAULT_INDEX_FILE_EXTENSION));

        let mut index = HashIndex {
            name,
            store: HashMap::new(),
            file_path,
        };

        // Try to load existing index data if the file exists
        if index.file_path.exists() {
            index.load().map_err(|e| DbError::IndexError(format!("Failed to load index {}: {}", index.name, e)))?;
        }

        Ok(index)
    }
}

impl Index for HashIndex {
    fn name(&self) -> &str {
        &self.name
    }

    fn insert(&mut self, value: &Value, primary_key: &PrimaryKey) -> Result<(), DbError> {
        let mut primary_keys = self.store.entry(value.clone()).or_insert_with(Vec::new);
        if !primary_keys.contains(primary_key) {
            primary_keys.push(primary_key.clone());
        }
        // For now, persistence on every insert might be too slow.
        // Consider batching or explicit save calls.
        // self.save()
        Ok(())
    }

    fn delete(&mut self, value: &Value, primary_key: Option<&PrimaryKey>) -> Result<(), DbError> {
        if let Some(primary_keys) = self.store.get_mut(value) {
            if let Some(pk_to_delete) = primary_key {
                primary_keys.retain(|pk| pk != pk_to_delete);
                if primary_keys.is_empty() {
                    self.store.remove(value);
                }
            } else {
                // If no specific primary key is given, remove all entries for this value.
                self.store.remove(value);
            }
        }
        // self.save()
        Ok(())
    }

    fn find(&self, value: &Value) -> Result<Option<Vec<PrimaryKey>>, DbError> {
        Ok(self.store.get(value).cloned())
    }

    fn save(&self) -> Result<(), DbError> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true) // Overwrite existing file
            .open(&self.file_path)
            .map_err(|e| DbError::IoError(e))?;
        let writer = BufWriter::new(file);
        bincode::serialize_into(writer, &self.store)
            .map_err(|e| DbError::SerializationError(format!("Failed to serialize index data: {}", e)))
    }

    fn load(&mut self) -> Result<(), DbError> {
        if !self.file_path.exists() {
            // If the file doesn't exist, it's not an error; it just means no data to load.
            // Initialize with an empty store, which is already the case.
            self.store = HashMap::new();
            return Ok(());
        }
        let file = File::open(&self.file_path).map_err(|e| DbError::IoError(e))?;
        if file.metadata().map_err(|e| DbError::IoError(e))?.len() == 0 {
            // File is empty, treat as no data.
            self.store = HashMap::new();
            return Ok(());
        }
        let reader = BufReader::new(file);
        self.store = bincode::deserialize_from(reader)
            .map_err(|e| DbError::DeserializationError(format!("Failed to deserialize index data: {}", e)))?;
        Ok(())
    }

    fn update(&mut self, old_value_for_index: &Value, new_value_for_index: &Value, primary_key: &PrimaryKey) -> Result<(), DbError> {
        if old_value_for_index == new_value_for_index {
            // If the indexed value hasn't changed, no update to the index is needed for this specific key.
            // However, ensure the primary_key is associated with new_value_for_index if it wasn't before
            // (e.g. if this is part of a repair or if logic allows updating non-indexed fields only).
            // For most cases, if old == new, the PK should already be there.
            // A simple `insert` will ensure it's there without duplicating.
            return self.insert(new_value_for_index, primary_key);
        }

        // Value changed, so remove old index entry and add new one.
        self.delete(old_value_for_index, Some(primary_key))?;
        self.insert(new_value_for_index, primary_key)?;
        // self.save() // Consider persistence strategy
        Ok(())
    }
}

// Consider adding a `mod tests` block here later for unit tests.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::indexing::traits::Index;
    use tempfile::tempdir;
    use std::path::Path;
    use crate::core::query::commands::Value; // Ensure Value is in scope for tests.

    // Helper to create a Value (Vec<u8>) from a string literal
    fn val(s: &str) -> Value {
        s.as_bytes().to_vec()
    }

    // Helper to create a PrimaryKey (Vec<u8>) from a string literal
    fn pk(s: &str) -> PrimaryKey {
        s.as_bytes().to_vec()
    }

    #[test]
    fn test_new_empty_index() -> Result<(), DbError> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let index = HashIndex::new("test_idx".to_string(), temp_dir.path())?;

        assert_eq!(index.name(), "test_idx");
        assert!(index.store.is_empty());
        Ok(())
    }

    #[test]
    fn test_new_loads_existing() -> Result<(), DbError> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let index_name = "existing_idx".to_string();
        let value1 = val("value1");
        let pk1 = pk("pk1");

        // Create an index, insert data, and save it
        {
            let mut index1 = HashIndex::new(index_name.clone(), temp_dir.path())?;
            index1.insert(&value1, &pk1)?;
            index1.save()?;
        }

        // Create a new instance with the same name and path
        let index2 = HashIndex::new(index_name, temp_dir.path())?;

        // Check if data is loaded
        let found_pks = index2.find(&value1)?.expect("Value should be found");
        assert_eq!(found_pks.len(), 1);
        assert_eq!(found_pks[0], pk1);
        Ok(())
    }

    #[test]
    fn test_insert_and_find() -> Result<(), DbError> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let mut index = HashIndex::new("insert_idx".to_string(), temp_dir.path())?;

        let value1 = val("value1");
        let pk1 = pk("pk1");
        let pk2 = pk("pk2");

        let value2 = val("value2");
        let pk3 = pk("pk3");

        // Insert single pk
        index.insert(&value1, &pk1)?;
        let found1 = index.find(&value1)?.expect("Should find value1");
        assert_eq!(found1, vec![pk1.clone()]);

        // Insert another pk for the same value
        index.insert(&value1, &pk2)?;
        let found1_updated = index.find(&value1)?.expect("Should find value1 again");
        assert_eq!(found1_updated.len(), 2);
        assert!(found1_updated.contains(&pk1));
        assert!(found1_updated.contains(&pk2));

        // Insert different value
        index.insert(&value2, &pk3)?;
        let found2 = index.find(&value2)?.expect("Should find value2");
        assert_eq!(found2, vec![pk3.clone()]);

        // Find non-existent value
        assert!(index.find(&val("non_existent_value"))?.is_none());
        Ok(())
    }

    #[test]
    fn test_insert_duplicate_pk_for_same_value() -> Result<(), DbError> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let mut index = HashIndex::new("duplicate_pk_idx".to_string(), temp_dir.path())?;
        let value1 = val("value1");
        let pk1 = pk("pk1");

        index.insert(&value1, &pk1)?;
        index.insert(&value1, &pk1)?; // Insert the same PK again

        let found_pks = index.find(&value1)?.expect("Value should be found");
        assert_eq!(found_pks.len(), 1, "Duplicate PK should not be added");
        assert_eq!(found_pks[0], pk1);
        Ok(())
    }


    #[test]
    fn test_delete_specific_pk_from_multiple() -> Result<(), DbError> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let mut index = HashIndex::new("delete_specific_pk_idx".to_string(), temp_dir.path())?;
        let value1 = val("value1");
        let pk1 = pk("pk1");
        let pk2 = pk("pk2");

        index.insert(&value1, &pk1)?;
        index.insert(&value1, &pk2)?;

        index.delete(&value1, Some(&pk1))?;
        let found_pks = index.find(&value1)?.expect("Value should still be found");
        assert_eq!(found_pks.len(), 1);
        assert_eq!(found_pks[0], pk2);
        Ok(())
    }

    #[test]
    fn test_delete_last_pk_removes_value() -> Result<(), DbError> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let mut index = HashIndex::new("delete_last_pk_idx".to_string(), temp_dir.path())?;
        let value1 = val("value1");
        let pk1 = pk("pk1");

        index.insert(&value1, &pk1)?;
        index.delete(&value1, Some(&pk1))?;

        assert!(index.find(&value1)?.is_none(), "Value should be removed after deleting its last PK");
        Ok(())
    }

    #[test]
    fn test_delete_all_pks_for_value() -> Result<(), DbError> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let mut index = HashIndex::new("delete_all_pks_idx".to_string(), temp_dir.path())?;
        let value1 = val("value1");
        let pk1 = pk("pk1");
        let pk2 = pk("pk2");

        index.insert(&value1, &pk1)?;
        index.insert(&value1, &pk2)?;

        index.delete(&value1, None)?; // Delete all PKs for value1
        assert!(index.find(&value1)?.is_none(), "Value should be removed when primary_key is None in delete");
        Ok(())
    }

    #[test]
    fn test_delete_non_existent_value() -> Result<(), DbError> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let mut index = HashIndex::new("delete_non_value_idx".to_string(), temp_dir.path())?;
        let value1 = val("value1"); // Not inserted

        index.delete(&value1, None)?; // Attempt delete
        assert!(index.find(&value1)?.is_none()); // Still shouldn't exist
        Ok(())
    }

    #[test]
    fn test_delete_non_existent_pk() -> Result<(), DbError> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let mut index = HashIndex::new("delete_non_pk_idx".to_string(), temp_dir.path())?;
        let value1 = val("value1");
        let pk1 = pk("pk1");
        let pk_non_existent = pk("pk_other");

        index.insert(&value1, &pk1)?;
        index.delete(&value1, Some(&pk_non_existent))?; // Attempt delete non-existent PK

        let found_pks = index.find(&value1)?.expect("Value should still exist");
        assert_eq!(found_pks, vec![pk1]); // pk1 should still be there
        Ok(())
    }

    #[test]
    fn test_save_and_load_persistence() -> Result<(), DbError> {
        let temp_dir = tempdir().expect("Failed to create temp dir for persistence test");
        let index_name = "persistence_idx".to_string();
        let index_path = temp_dir.path();

        let value1 = val("persist_val1");
        let pk1 = pk("persist_pk1");
        let pk2 = pk("persist_pk2");
        let value2 = val("persist_val2");
        let pk3 = pk("persist_pk3");

        // Create first index, insert data, save
        {
            let mut index1 = HashIndex::new(index_name.clone(), index_path)?;
            index1.insert(&value1, &pk1)?;
            index1.insert(&value1, &pk2)?;
            index1.insert(&value2, &pk3)?;
            index1.save()?;
        }

        // Create a new index instance and load data
        let mut index2 = HashIndex::new(index_name, index_path)?;
        // New already loads, but we can call load explicitly if we want to test that part,
        // though `new` already covers the file-exists-load scenario.
        // For an explicit load test on an existing instance:
        // index2.store.clear(); // Clear to ensure data comes from load
        // index2.load()?; // This is implicitly tested by `new` if file exists.

        let found_v1 = index2.find(&value1)?.expect("value1 should be loaded");
        assert_eq!(found_v1.len(), 2);
        assert!(found_v1.contains(&pk1));
        assert!(found_v1.contains(&pk2));

        let found_v2 = index2.find(&value2)?.expect("value2 should be loaded");
        assert_eq!(found_v2, vec![pk3]);
        Ok(())
    }

    #[test]
    fn test_load_from_empty_file() -> Result<(), DbError> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let index_name = "empty_file_idx".to_string();
        let index_file_path = temp_dir.path().join(format!("{}.{}", index_name, DEFAULT_INDEX_FILE_EXTENSION));

        // Create an empty file
        File::create(&index_file_path).expect("Failed to create empty file");

        let index = HashIndex::new(index_name, temp_dir.path())?;
        assert!(index.store.is_empty(), "Index store should be empty after loading from an empty file");
        Ok(())
    }

    #[test]
    fn test_load_from_non_existent_file() -> Result<(), DbError> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        // HashIndex::new will attempt to load, but file won't exist.
        let index = HashIndex::new("non_existent_file_idx".to_string(), temp_dir.path())?;
        assert!(index.store.is_empty(), "Index store should be empty if no file exists");
        Ok(())
    }

    #[test]
    fn test_index_update_value_changed() -> Result<(), DbError> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let mut index = HashIndex::new("update_idx".to_string(), temp_dir.path())?;

        let old_val = val("old_value");
        let new_val = val("new_value");
        let pk1 = pk("pk1");

        // Insert initial value
        index.insert(&old_val, &pk1)?;
        assert_eq!(index.find(&old_val)?.unwrap(), vec![pk1.clone()]);
        assert!(index.find(&new_val)?.is_none());

        // Update to new value
        index.update(&old_val, &new_val, &pk1)?;
        assert!(index.find(&old_val)?.is_none(), "Old value should be removed after update");
        assert_eq!(index.find(&new_val)?.unwrap(), vec![pk1.clone()], "New value should be inserted after update");

        Ok(())
    }

    #[test]
    fn test_index_update_value_unchanged() -> Result<(), DbError> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let mut index = HashIndex::new("update_unchanged_idx".to_string(), temp_dir.path())?;

        let val1 = val("value1");
        let pk1 = pk("pk1");

        index.insert(&val1, &pk1)?;
        assert_eq!(index.find(&val1)?.unwrap(), vec![pk1.clone()]);

        // Update with the same value
        index.update(&val1, &val1, &pk1)?;
        assert_eq!(index.find(&val1)?.unwrap(), vec![pk1.clone()], "Value should still exist and be unchanged");
        // Ensure no duplicate PKs if insert is called internally
        let pks = index.store.get(&val1).unwrap();
        assert_eq!(pks.len(), 1, "PK list should not grow if value is unchanged and PK already exists.");

        Ok(())
    }

    #[test]
    fn test_index_update_multiple_pks_one_changes() -> Result<(), DbError> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let mut index = HashIndex::new("update_multi_pk_idx".to_string(), temp_dir.path())?;

        let old_val = val("shared_old_value");
        let new_val = val("shared_new_value");
        let pk1 = pk("pk1"); // This one will change
        let pk2 = pk("pk2"); // This one will remain associated with old_val

        index.insert(&old_val, &pk1)?;
        index.insert(&old_val, &pk2)?;
        assert_eq!(index.find(&old_val)?.unwrap().len(), 2);

        // Update pk1 from old_val to new_val
        index.update(&old_val, &new_val, &pk1)?;

        let old_val_pks = index.find(&old_val)?.expect("Old value should still exist for pk2");
        assert_eq!(old_val_pks.len(), 1);
        assert_eq!(old_val_pks[0], pk2); // pk2 should still be under old_val

        let new_val_pks = index.find(&new_val)?.expect("New value should exist for pk1");
        assert_eq!(new_val_pks.len(), 1);
        assert_eq!(new_val_pks[0], pk1); // pk1 should now be under new_val

        Ok(())
    }

    #[test]
    fn test_index_update_from_non_existent_old_value() -> Result<(), DbError> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let mut index = HashIndex::new("update_non_old_idx".to_string(), temp_dir.path())?;

        let old_val_non_existent = val("non_existent_old_value");
        let new_val = val("new_value_from_nothing");
        let pk1 = pk("pk1");

        // Attempt update where old_val was never inserted for pk1
        index.update(&old_val_non_existent, &new_val, &pk1)?;

        // The new value should be inserted
        assert_eq!(index.find(&new_val)?.unwrap(), vec![pk1.clone()]);
        // The non-existent old value should still not be found
        assert!(index.find(&old_val_non_existent)?.is_none());

        Ok(())
    }
}
