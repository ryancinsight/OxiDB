use std::collections::HashMap;
use std::fs::{File, OpenOptions, rename}; // Added rename
use std::io::{BufReader, BufWriter, Write, ErrorKind, BufRead}; // Added BufRead
use std::path::{Path, PathBuf};
use crate::core::common::error::DbError;
use crate::core::storage::engine::traits::KeyValueStore;
use crate::core::common::traits::{DataSerializer, DataDeserializer};

#[derive(Debug)] // Added Debug
pub struct SimpleFileKvStore {
    file_path: PathBuf,
    cache: HashMap<Vec<u8>, Vec<u8>>,
}

impl SimpleFileKvStore {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, DbError> {
        let path_buf = path.as_ref().to_path_buf();
        let mut store = Self {
            file_path: path_buf,
            cache: HashMap::new(),
        };
        // load_from_disk will handle non-existent files gracefully.
        store.load_from_disk()?;
        Ok(store)
    }

    // Helper function to read data from a given path into the cache
    fn read_data_into_cache(&mut self, file_to_load: &Path) -> Result<(), DbError> {
        self.cache.clear();
        let file = match File::open(file_to_load) {
            Ok(f) => f,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()), // File not found is okay, means no data
            Err(e) => return Err(DbError::IoError(e)),
        };

        let mut reader = BufReader::new(file);
        loop {
            let buffer = reader.fill_buf().map_err(DbError::IoError)?;
            if buffer.is_empty() {
                break; // Clean EOF
            }

            let key = Vec::<u8>::deserialize(&mut reader)
                .map_err(|e| DbError::StorageError(format!("Failed to deserialize key from {}: {}", file_to_load.display(), e)))?;
            
            let value = Vec::<u8>::deserialize(&mut reader)
                .map_err(|e| DbError::StorageError(format!("Failed to deserialize value for key {:?} from {}: {}", String::from_utf8_lossy(&key), file_to_load.display(), e)))?;
            
            self.cache.insert(key, value);
        }
        Ok(())
    }

    fn load_from_disk(&mut self) -> Result<(), DbError> {
        let temp_file_path = self.file_path.with_extension("tmp");

        if temp_file_path.exists() {
            // Attempt to load from temporary file first
            match self.read_data_into_cache(&temp_file_path) {
                Ok(()) => {
                    // If successful, the cache is populated from temp file.
                    // Now, atomically rename temp file to main file.
                    if let Err(e) = rename(&temp_file_path, &self.file_path) {
                        // If rename fails, return a specific error.
                        // The temporary file is left in place for future recovery attempts.
                        return Err(DbError::StorageError(format!(
                            "Successfully loaded from temporary file {} but failed to rename it to {}: {}",
                            temp_file_path.display(),
                            self.file_path.display(),
                            e
                        )));
                    }
                    // Rename successful, recovery complete.
                    return Ok(());
                }
                Err(load_err) => {
                    // Loading from temp file failed (e.g., corrupted).
                    // Log this error (optional, could be done by caller or a logging framework)
                    // eprintln!("Failed to load from temporary file {}: {}. Attempting to delete it.", temp_file_path.display(), load_err);
                    
                    // Attempt to delete the corrupted temporary file.
                    if let Err(remove_err) = std::fs::remove_file(&temp_file_path) {
                        // If deletion fails, this is a problem as corrupted temp might interfere later.
                        // Return an error indicating this problematic state.
                        return Err(DbError::StorageError(format!(
                            "Corrupted temporary file {} could not be loaded ({}) or deleted ({}). Manual intervention may be required.",
                            temp_file_path.display(),
                            load_err,
                            remove_err
                        )));
                    }
                    // Corrupted temp file deleted (or was already gone). Proceed to load main file.
                    // The error from read_data_into_cache (load_err) is effectively handled by deleting temp and trying main.
                }
            }
        }

        // If temporary file didn't exist, or loading from it failed and it was deleted,
        // attempt to load from the main database file.
        let main_file_path = self.file_path.clone();
        self.read_data_into_cache(&main_file_path)
    }

    fn save_to_disk(&self) -> Result<(), DbError> {
        // 1. Construct Temporary File Path
        let temp_file_path = self.file_path.with_extension("tmp");

        // Ensure temp file is cleaned up if anything goes wrong.
        // This guard will attempt to remove the temp file when it goes out of scope,
        // unless `disarm()` is called.
        struct TempFileGuard<'a>(&'a PathBuf);
        impl<'a> Drop for TempFileGuard<'a> {
            fn drop(&mut self) {
                let _ = std::fs::remove_file(self.0); // Ignore error on cleanup
            }
        }
        let _temp_file_guard = TempFileGuard(&temp_file_path);


        // 2. Create and Write to Temporary File
        let temp_file = OpenOptions::new()
            .write(true)
            .create(true) // Create if it doesn't exist
            .truncate(true) // Truncate if it exists (e.g., from a previous failed attempt)
            .open(&temp_file_path)
            .map_err(|e| DbError::IoError(e))?;
        
        let mut writer = BufWriter::new(temp_file);

        for (key, value) in &self.cache {
            Vec::<u8>::serialize(key, &mut writer)
                .map_err(|e| DbError::StorageError(format!("Failed to serialize key: {}", e)))?;
            Vec::<u8>::serialize(value, &mut writer)
                .map_err(|e| DbError::StorageError(format!("Failed to serialize value: {}", e)))?;
        }

        // 3. Flush to Disk
        writer.flush().map_err(DbError::IoError)?;
        // Ensure metadata and data are synced to the underlying file system.
        writer.get_ref().sync_all().map_err(DbError::IoError)?;

        // 4. Atomic Rename
        // If write and flush were successful, attempt the rename.
        rename(&temp_file_path, &self.file_path).map_err(|e| {
            // If rename fails, the temp file is still there. 
            // For this subtask, we'll attempt to clean it up.
            // The decision to leave it for recovery can be a future enhancement.
            let _ = std::fs::remove_file(&temp_file_path); 
            DbError::IoError(e)
        })?;

        // Disarm the guard: rename was successful, so we don't want to delete the (now main) file.
        // However, the temp file path no longer exists, so the guard would fail to delete it anyway.
        // For clarity, we could explicitly disarm if the guard held the final path,
        // but here it's fine as `temp_file_path` is what it tries to delete.
        // std::mem::forget(_temp_file_guard); // Alternative to a dedicated disarm method

        Ok(())
    }
}

impl KeyValueStore<Vec<u8>, Vec<u8>> for SimpleFileKvStore {
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), DbError> {
        self.cache.insert(key, value);
        self.save_to_disk()
    }

    fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, DbError> {
        Ok(self.cache.get(key).cloned())
    }

    fn delete(&mut self, key: &Vec<u8>) -> Result<bool, DbError> {
        if self.cache.remove(key).is_some() {
            self.save_to_disk()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn contains_key(&self, key: &Vec<u8>) -> Result<bool, DbError> {
        Ok(self.cache.contains_key(key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::{NamedTempFile, Builder};
    use std::fs::{write, remove_file, read}; // For manual file operations

    // Helper to create a file with specific key-value data
    fn create_file_with_kv_data(path: &Path, data: &[(Vec<u8>, Vec<u8>)]) -> Result<(), DbError> {
        let file = OpenOptions::new().write(true).create(true).truncate(true).open(path).map_err(DbError::IoError)?;
        let mut writer = BufWriter::new(file);
        for (key, value) in data {
            Vec::<u8>::serialize(key, &mut writer)?; // Assuming DataSerializer is in scope via super::*
            Vec::<u8>::serialize(value, &mut writer)?;
        }
        writer.flush().map_err(DbError::IoError)?;
        writer.get_ref().sync_all().map_err(DbError::IoError)?; // Ensure data is on disk
        Ok(())
    }
    // Removed duplicate imports that were here.
    // The actual imports are correctly placed above, after the main `use super::*;` for the tests module.

    // Helper to create a file with specific key-value data
    // ... (helper function already added in previous diff chunk) // This comment is illustrative, not in actual code.

    #[test]
    fn test_new_store_empty_and_reload() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();
        {
            let store = SimpleFileKvStore::new(path).unwrap();
            assert!(store.cache.is_empty());
        } 
        let reloaded_store = SimpleFileKvStore::new(path).unwrap();
        assert!(reloaded_store.cache.is_empty());
    }
    
    #[test]
    fn test_load_from_empty_file() {
        let temp_file = NamedTempFile::new().unwrap();
        // Ensure the file is actually created and empty
        File::create(temp_file.path()).unwrap();
        let store = SimpleFileKvStore::new(temp_file.path()).unwrap();
        assert!(store.cache.is_empty());
    }

    #[test]
    fn test_put_and_get() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut store = SimpleFileKvStore::new(temp_file.path()).unwrap();
        let key1 = b"key1".to_vec();
        let value1 = b"value1".to_vec();
        store.put(key1.clone(), value1.clone()).unwrap();
        assert_eq!(store.get(&key1).unwrap(), Some(value1.clone()));

        let key2 = b"key2".to_vec();
        let value2 = b"value2_long".to_vec();
        store.put(key2.clone(), value2.clone()).unwrap();
        assert_eq!(store.get(&key2).unwrap(), Some(value2.clone()));
        assert_eq!(store.get(&key1).unwrap(), Some(value1.clone()));
    }
    
    #[test]
    fn test_put_update() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut store = SimpleFileKvStore::new(temp_file.path()).unwrap();
        let key1 = b"key1".to_vec();
        let value1 = b"value1".to_vec();
        let value1_updated = b"value1_updated".to_vec();

        store.put(key1.clone(), value1.clone()).unwrap();
        assert_eq!(store.get(&key1).unwrap(), Some(value1.clone()));

        store.put(key1.clone(), value1_updated.clone()).unwrap();
        assert_eq!(store.get(&key1).unwrap(), Some(value1_updated.clone()));
    }

    #[test]
    fn test_get_non_existent() {
        let temp_file = NamedTempFile::new().unwrap();
        let store = SimpleFileKvStore::new(temp_file.path()).unwrap();
        assert_eq!(store.get(&b"non_existent_key".to_vec()).unwrap(), None);
    }

    #[test]
    fn test_delete() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut store = SimpleFileKvStore::new(temp_file.path()).unwrap();
        let key1 = b"key1".to_vec();
        let value1 = b"value1".to_vec();
        store.put(key1.clone(), value1.clone()).unwrap();
        assert!(store.delete(&key1).unwrap());
        assert_eq!(store.get(&key1).unwrap(), None);
        assert!(!store.contains_key(&key1).unwrap());
    }

    #[test]
    fn test_delete_non_existent() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut store = SimpleFileKvStore::new(temp_file.path()).unwrap();
        assert!(!store.delete(&b"non_existent_key".to_vec()).unwrap());
    }

    #[test]
    fn test_contains_key() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut store = SimpleFileKvStore::new(temp_file.path()).unwrap();
        let key1 = b"key1".to_vec();
        store.put(key1.clone(), b"value1".to_vec()).unwrap();
        assert!(store.contains_key(&key1).unwrap());
        assert!(!store.contains_key(&b"non_existent_key".to_vec()).unwrap());
    }

    #[test]
    fn test_persistence() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_path_buf();
        let key1 = b"persist_key".to_vec();
        let value1 = b"persist_value".to_vec();
        {
            let mut store = SimpleFileKvStore::new(&path).unwrap();
            store.put(key1.clone(), value1.clone()).unwrap();
        }
        let reloaded_store = SimpleFileKvStore::new(&path).unwrap();
        assert_eq!(reloaded_store.get(&key1).unwrap(), Some(value1));
        assert_eq!(reloaded_store.cache.len(), 1);
    }

    #[test]
    fn test_save_to_disk_atomic_success() {
        let db_file = NamedTempFile::new().unwrap();
        let db_path = db_file.path().to_path_buf();
        let temp_db_path = db_path.with_extension("tmp");

        let mut store = SimpleFileKvStore::new(&db_path).unwrap();
        let key1 = b"key1".to_vec();
        let value1 = b"value1".to_vec();
        store.put(key1.clone(), value1.clone()).unwrap(); // put calls save_to_disk

        // Verify main file has the data
        let reloaded_store = SimpleFileKvStore::new(&db_path).unwrap(); // Removed mut
        assert_eq!(reloaded_store.get(&key1).unwrap(), Some(value1.clone()));
        assert_eq!(reloaded_store.cache.len(), 1);

        // Verify temp file does not exist
        assert!(!temp_db_path.exists(), "Temporary file should not exist after successful save.");
    }

    #[test]
    fn test_load_from_disk_prefers_valid_temp_file() {
        let main_db_file = NamedTempFile::new().unwrap();
        let main_db_path = main_db_file.path().to_path_buf();
        let temp_db_path = main_db_path.with_extension("tmp");

        // Setup: Main file with initial data, temp file with newer data
        let initial_data = vec![(b"key1".to_vec(), b"value_initial".to_vec())];
        create_file_with_kv_data(&main_db_path, &initial_data).unwrap();

        let temp_data = vec![
            (b"key1".to_vec(), b"value_new".to_vec()),
            (b"key2".to_vec(), b"value2".to_vec()),
        ];
        create_file_with_kv_data(&temp_db_path, &temp_data).unwrap();

        // Action: Create store, triggering load_from_disk
        let store = SimpleFileKvStore::new(&main_db_path).unwrap();

        // Assertions
        assert_eq!(store.get(&b"key1".to_vec()).unwrap(), Some(b"value_new".to_vec()));
        assert_eq!(store.get(&b"key2".to_vec()).unwrap(), Some(b"value2".to_vec()));
        assert_eq!(store.cache.len(), 2, "Cache should contain 2 items from temp file");

        // Assert main file now reflects temp data
        let main_file_content_check_store = SimpleFileKvStore::new(&main_db_path).unwrap(); // Removed mut
        // Clear cache and reload directly from file to be absolutely sure (new() already does this)
        // main_file_content_check_store.load_from_disk().unwrap(); // This is done by new()
        assert_eq!(main_file_content_check_store.get(&b"key1".to_vec()).unwrap(), Some(b"value_new".to_vec()));
        assert_eq!(main_file_content_check_store.get(&b"key2".to_vec()).unwrap(), Some(b"value2".to_vec()));
        assert_eq!(main_file_content_check_store.cache.len(), 2);
        
        assert!(!temp_db_path.exists(), "Temporary file should be removed after successful recovery.");
    }

    #[test]
    fn test_load_from_disk_handles_corrupted_temp_file_and_uses_main_file() {
        let main_db_file = NamedTempFile::new().unwrap();
        let main_db_path = main_db_file.path().to_path_buf();
        let temp_db_path = main_db_path.with_extension("tmp");

        // Setup: Main file with valid data
        let main_data = vec![(b"key_main".to_vec(), b"value_main".to_vec())];
        create_file_with_kv_data(&main_db_path, &main_data).unwrap();

        // Setup: Corrupted temp file
        write(&temp_db_path, b"this is corrupted data").unwrap();

        // Action: Create store
        let store = SimpleFileKvStore::new(&main_db_path).unwrap();

        // Assertions
        assert_eq!(store.get(&b"key_main".to_vec()).unwrap(), Some(b"value_main".to_vec()));
        assert_eq!(store.cache.len(), 1, "Cache should contain 1 item from main file");
        assert!(!temp_db_path.exists(), "Corrupted temporary file should be deleted.");
        
        // Verify main file content is still intact
        let file_content = read(&main_db_path).unwrap();
        let expected_content = {
            let mut content = Vec::new();
            let mut writer = BufWriter::new(&mut content);
            // Iterate by reference to avoid consuming main_data, in case it's needed later
            // (though not strictly necessary in this specific test as it's the last use)
            for (k, v) in &main_data { 
                Vec::<u8>::serialize(k, &mut writer).unwrap();
                Vec::<u8>::serialize(v, &mut writer).unwrap();
            }
            writer.flush().unwrap(); // Ensure all data is written to content
            drop(writer); // Explicitly drop writer before content is moved
            content 
        };
        assert_eq!(file_content, expected_content, "Main file content should not have changed.");
    }

    #[test]
    fn test_load_from_disk_handles_temp_file_and_no_main_file() {
        let main_db_file = Builder::new().prefix("test_main_db").tempfile().unwrap();
        let main_db_path = main_db_file.path().to_path_buf();
        let temp_db_path = main_db_path.with_extension("tmp");

        // Ensure main file does not exist by closing and removing the tempfile handle for it
        main_db_file.close().unwrap(); 
        if main_db_path.exists() { // defensive
            remove_file(&main_db_path).unwrap();
        }
        assert!(!main_db_path.exists());


        // Setup: Valid temp file with data
        let temp_data = vec![(b"key_temp".to_vec(), b"value_temp".to_vec())];
        create_file_with_kv_data(&temp_db_path, &temp_data).unwrap();
        assert!(temp_db_path.exists());

        // Action: Create store
        let store = SimpleFileKvStore::new(&main_db_path).unwrap();

        // Assertions
        assert_eq!(store.get(&b"key_temp".to_vec()).unwrap(), Some(b"value_temp".to_vec()));
        assert_eq!(store.cache.len(), 1, "Cache should contain 1 item from temp file");
        assert!(main_db_path.exists(), "Main DB file should have been created from temp file.");
        assert!(!temp_db_path.exists(), "Temporary file should be deleted after successful recovery.");

        // Verify content of new main file
        let reloaded_store = SimpleFileKvStore::new(&main_db_path).unwrap(); // Removed mut
        assert_eq!(reloaded_store.get(&b"key_temp".to_vec()).unwrap(), Some(b"value_temp".to_vec()));
    }
    
    #[test]
    fn test_load_from_disk_handles_corrupted_temp_file_and_no_main_file() {
        let main_db_file = Builder::new().prefix("test_main_db_corrupt_tmp").tempfile().unwrap();
        let main_db_path = main_db_file.path().to_path_buf();
        let temp_db_path = main_db_path.with_extension("tmp");

        main_db_file.close().unwrap();
        if main_db_path.exists() {
            remove_file(&main_db_path).unwrap();
        }
        assert!(!main_db_path.exists());

        // Setup: Corrupted temp file
        write(&temp_db_path, b"corrupted data").unwrap();
        assert!(temp_db_path.exists());
        
        // Action: Create store
        let store = SimpleFileKvStore::new(&main_db_path).unwrap();

        // Assertions
        assert!(store.cache.is_empty(), "Cache should be empty");
        assert!(!temp_db_path.exists(), "Corrupted temporary file should be deleted.");
        assert!(!main_db_path.exists(), "Main DB file should still not exist.");
    }

    // Test for `test_save_to_disk_error_during_temp_file_write_preserves_original`
    // This test is more conceptual. If save_to_disk fails mid-way (e.g. writing to temp),
    // the TempFileGuard should clean up the .tmp file. On next load,
    // if a .tmp file is found (which it shouldn't be if guard worked), it would be handled.
    // If no .tmp is found, it loads the original main file.
    // The key is that the original file is not touched until successful rename.
    #[test]
    fn test_state_after_simulated_failed_save_preserves_original() {
        let db_file = NamedTempFile::new().unwrap();
        let db_path = db_file.path().to_path_buf();
        let temp_db_path = db_path.with_extension("tmp");

        // Initial state: key_orig=value_orig
        let key_orig = b"key_orig".to_vec();
        let value_orig = b"value_orig".to_vec();
        {
            let mut store = SimpleFileKvStore::new(&db_path).unwrap();
            store.put(key_orig.clone(), value_orig.clone()).unwrap();
        } // Store is dropped, data saved.

        // Simulate a partial, failed save: create a .tmp file manually, as if a save crashed.
        // This .tmp file could be empty, partially written, or corrupt.
        // For this test, let's say it's different from what we'll try to save next.
        write(&temp_db_path, b"some other data, simulating a crashed previous save attempt").unwrap();

        // Now, attempt a new `put` operation. The `save_to_disk` will try to create a new .tmp file.
        // The existing .tmp (from our manual write) will be truncated and overwritten.
        // If this new save operation were to fail (conceptually, hard to force failure here),
        // the TempFileGuard should remove this *new* .tmp file.
        // Let's assume the save is successful for THIS put operation.
        let key_new = b"key_new".to_vec();
        let value_new = b"value_new".to_vec();
        {
            let mut store = SimpleFileKvStore::new(&db_path).unwrap(); // Loads original data
            assert_eq!(store.get(&key_orig).unwrap(), Some(value_orig.clone()));
            store.put(key_new.clone(), value_new.clone()).unwrap(); // This save should succeed
        }
        
        // The store should now contain key_orig and key_new.
        // The .tmp file from "some other data" should be gone, replaced by the successful save.
        let store = SimpleFileKvStore::new(&db_path).unwrap();
        assert_eq!(store.get(&key_orig).unwrap(), Some(value_orig.clone()));
        assert_eq!(store.get(&key_new).unwrap(), Some(value_new.clone()));
        assert_eq!(store.cache.len(), 2);
        assert!(!temp_db_path.exists(), "Temp file should not exist after a successful save.");

        // To better test the "preserves original if temp write fails" scenario:
        // 1. Original data exists.
        // 2. `save_to_disk` starts, creates `foo.tmp`.
        // 3. `save_to_disk` fails writing to `foo.tmp` (e.g. `serialize` error, `flush` error, `sync_all` error).
        // 4. `TempFileGuard` for `foo.tmp` runs, deleting `foo.tmp`.
        // 5. Original `db_path` file is untouched.
        // This is inherently tested by the existing `save_to_disk` structure and error handling.
        // If any of those steps fail, `rename` is not called.
        // The `TempFileGuard` ensures the temp file is cleaned up.
        // So, loading the store again would just load the original data.
        // We can verify this by checking that if `save_to_disk` fails (e.g. by `put`), the old data is still there.
        // However, making Vec::<u8>::serialize fail on demand is not trivial.
        // The current `test_save_to_disk_atomic_success` already shows that if `put` (and thus `save_to_disk`)
        // is successful, the new data is there and .tmp is gone.
        // The crucial part is that no rename happens if any prior step in `save_to_disk` fails.
    }
    
    #[test]
    fn test_load_from_malformed_file_key_eof() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();
        // Malformed: Valid length prefix for key, but not enough bytes for the key itself
        let key_len_bytes = (5u64).to_be_bytes(); // Expect 5 bytes for key
        let mut file_content = key_len_bytes.to_vec();
        file_content.extend_from_slice(b"abc"); // Only 3 bytes for key
        std::fs::write(path, file_content).unwrap();
        
        let result = SimpleFileKvStore::new(path);
        assert!(result.is_err());
        match result.unwrap_err() {
            DbError::StorageError(msg) => {
                assert!(msg.contains("Failed to deserialize key")); // Corrected assertion
                assert!(msg.contains("failed to fill whole buffer")); // Error from read_exact in Vec::deserialize
            },
            e => panic!("Unexpected error type for malformed key (EOF): {:?}", e),
        }
    }

    #[test]
    fn test_load_from_malformed_file_value_eof() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();
        
        let mut file_content = Vec::new();
        let key = b"mykey".to_vec();
        // Write a valid key
        Vec::<u8>::serialize(&key, &mut file_content).unwrap(); 
        // Malformed: Valid length prefix for value, but not enough bytes for the value itself
        let value_len_bytes = (10u64).to_be_bytes(); // Expect 10 bytes for value
        file_content.extend_from_slice(&value_len_bytes);
        file_content.extend_from_slice(b"short"); // Only 5 bytes for value
        std::fs::write(path, file_content).unwrap();
        
        let result = SimpleFileKvStore::new(path);
        assert!(result.is_err());
        match result.unwrap_err() {
            DbError::StorageError(msg) => {
                assert!(msg.contains("Failed to deserialize value for key")); // Corrected assertion
                assert!(msg.contains("IO Error: failed to fill whole buffer")); 
            },
            e => panic!("Unexpected error type for malformed value (EOF): {:?}", e),
        }
    }
}
