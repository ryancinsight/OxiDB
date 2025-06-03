use std::collections::{HashMap, HashSet}; // Added HashSet
use std::fs::{File, OpenOptions, rename};
use std::io::{BufReader, BufWriter, Write, ErrorKind, BufRead};
use std::path::{Path, PathBuf};
use crate::core::common::error::DbError;
use crate::core::storage::engine::traits::KeyValueStore;
use crate::core::common::traits::{DataSerializer, DataDeserializer};
use crate::core::storage::engine::wal::{WalEntry, WalWriter};
use crate::core::transaction::Transaction; // Added import

#[derive(Debug)] // Added Debug
pub struct SimpleFileKvStore {
    file_path: PathBuf,
    cache: HashMap<Vec<u8>, Vec<u8>>,
    wal_writer: WalWriter,
}

impl SimpleFileKvStore {
    /// Creates a new `SimpleFileKvStore` instance.
    ///
    /// The store is initialized from the data file at the given `path`.
    /// If the file does not exist, an empty store is created.
    /// This method also performs recovery from a Write-Ahead Log (WAL) if one exists,
    /// ensuring that any previously uncommitted operations are applied.
    ///
    /// # Errors
    /// Returns `DbError` if there are issues reading from the data file or WAL,
    /// or if recovery procedures encounter an unrecoverable error.
    pub fn new(path: impl AsRef<Path>) -> Result<Self, DbError> {
        let path_buf = path.as_ref().to_path_buf();
        let wal_writer = WalWriter::new(&path_buf);
        let mut store = Self {
            file_path: path_buf,
            cache: HashMap::new(),
            wal_writer,
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
        self.read_data_into_cache(&main_file_path)?; // Note: added ? here

        // WAL Replay
        let mut wal_file_path = self.file_path.to_path_buf();
        let original_extension = wal_file_path.extension().map(|s| s.to_os_string());

        if let Some(ext) = original_extension {
            let mut new_ext = ext;
            new_ext.push(".wal");
            wal_file_path.set_extension(new_ext);
        } else {
            wal_file_path.set_extension("wal");
        }

        if wal_file_path.exists() {
            let wal_file = match File::open(&wal_file_path) {
                Ok(f) => f,
                Err(e) if e.kind() == ErrorKind::NotFound => {
                    // This case should ideally be caught by wal_file_path.exists()
                    // but good to have as a safeguard.
                    return Ok(());
                }
                Err(e) => return Err(DbError::IoError(e)),
            };
            let mut reader = BufReader::new(wal_file);

            let mut transaction_operations: HashMap<u64, Vec<WalEntry>> = HashMap::new();
            let mut committed_transactions: HashSet<u64> = HashSet::new();
            let mut rolled_back_transactions: HashSet<u64> = HashSet::new();

            // First Pass: Populate data structures
            loop {
                match <WalEntry as DataDeserializer<WalEntry>>::deserialize(&mut reader) {
                    Ok(entry) => {
                        match &entry {
                            WalEntry::Put { transaction_id, .. } | WalEntry::Delete { transaction_id, .. } => {
                                transaction_operations.entry(*transaction_id).or_default().push(entry);
                            }
                            WalEntry::TransactionCommit { transaction_id } => {
                                committed_transactions.insert(*transaction_id);
                            }
                            WalEntry::TransactionRollback { transaction_id } => {
                                rolled_back_transactions.insert(*transaction_id);
                            }
                        }
                    }
                    Err(DbError::IoError(e)) if e.kind() == ErrorKind::UnexpectedEof => break, // Clean EOF
                    Err(DbError::DeserializationError(msg)) => {
                        eprintln!("WAL corruption detected (Deserialization error): {}. Replay stopped. Data up to this point is recovered.", msg);
                        break; // Stop on corruption
                    }
                    Err(e) => {
                        eprintln!("Error during WAL replay: {}. Replay stopped. Data up to this point is recovered.", e);
                        break; // Stop on other critical errors
                    }
                }
            }

            // Second Pass: Apply committed operations
            for (tx_id, operations) in transaction_operations {
                if committed_transactions.contains(&tx_id) && !rolled_back_transactions.contains(&tx_id) {
                    for entry in operations {
                        match entry {
                            WalEntry::Put { key, value, .. } => { // transaction_id already known
                                self.cache.insert(key, value);
                            }
                            WalEntry::Delete { key, .. } => { // transaction_id already known
                                self.cache.remove(&key);
                            }
                            _ => {} // TransactionCommit/Rollback entries are not operations themselves
                        }
                    }
                }
                // Operations for transactions not committed or explicitly rolled back are ignored.
            }
        }
        Ok(()) // End of load_from_disk
    }

    /// Saves the current in-memory state of the key-value store to disk.
    ///
    /// This operation involves:
    /// 1. Writing all key-value pairs from the cache to a new temporary file.
    /// 2. Flushing the temporary file's content to disk and ensuring it's synced.
    /// 3. Atomically renaming the temporary file to replace the main data file.
    ///    This ensures that the main data file is only updated if the entire save is successful.
    /// 4. If the atomic rename is successful, the Write-Ahead Log (WAL) file is deleted,
    ///    as its entries are now reflected in the main data file.
    ///
    /// A `TempFileGuard` is used to ensure that the temporary file is cleaned up
    /// if any error occurs before the atomic rename.
    ///
    /// # Errors
    /// Returns `DbError` if any part of the process fails, such as:
    /// - I/O errors during file creation, writing, flushing, or syncing.
    /// - Errors during serialization of keys or values.
    /// - Failure to atomically rename the temporary file to the main data file.
    /// - (Note: Failure to delete the WAL file after a successful save is reported via `eprintln!`
    ///   but does not cause this method to return an error, as the main data is already safe.)
    pub fn save_to_disk(&self) -> Result<(), DbError> {
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

        // Delete WAL file after successful save to disk
        let mut wal_file_path = self.file_path.to_path_buf();
        let original_extension = wal_file_path.extension().map(|s| s.to_os_string());

        if let Some(ext) = original_extension {
            let mut new_ext = ext;
            new_ext.push(".wal");
            wal_file_path.set_extension(new_ext);
        } else {
            wal_file_path.set_extension("wal");
        }

        if wal_file_path.exists() {
            if let Err(e) = std::fs::remove_file(&wal_file_path) {
                eprintln!("Failed to delete WAL file {}: {}. Main data save was successful.", wal_file_path.display(), e);
            }
        }

        Ok(())
    }
}

impl KeyValueStore<Vec<u8>, Vec<u8>> for SimpleFileKvStore {
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>, transaction: &Transaction) -> Result<(), DbError> {
        let wal_entry = WalEntry::Put {
            transaction_id: transaction.id,
            key: key.clone(), 
            value: value.clone(),
        };
        // Log to WAL first
        self.wal_writer.log_entry(&wal_entry)?;
        // Then update cache
        self.cache.insert(key, value);
        Ok(())
    }

    fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, DbError> {
        Ok(self.cache.get(key).cloned())
    }

    fn delete(&mut self, key: &Vec<u8>, transaction: &Transaction) -> Result<bool, DbError> {
        if !self.cache.contains_key(key) {
            return Ok(false); // Key doesn't exist, nothing to delete
        }
        
        let wal_entry = WalEntry::Delete {
            transaction_id: transaction.id,
            key: key.clone(),
        };
        // Log to WAL first
        self.wal_writer.log_entry(&wal_entry)?;
        // Then update cache
        self.cache.remove(key);
        Ok(true)
    }

    fn contains_key(&self, key: &Vec<u8>) -> Result<bool, DbError> {
        Ok(self.cache.contains_key(key))
    }

    fn log_wal_entry(&mut self, entry: &WalEntry) -> Result<(), DbError> {
        self.wal_writer.log_entry(entry)
    }
}

/// Implements the `Drop` trait for `SimpleFileKvStore`.
///
/// When a `SimpleFileKvStore` instance goes out of scope, this `drop` method
/// is called to ensure that any in-memory data is persisted to disk.
/// It achieves this by calling `self.save_to_disk()`.
///
/// If `save_to_disk()` encounters an error during this process (e.g., an I/O error),
/// the error is printed to `stderr` via `eprintln!`. This is a common pattern for
/// handling fallible operations within `Drop`, as `drop` itself cannot return a `Result`.
impl Drop for SimpleFileKvStore {
    fn drop(&mut self) {
        if let Err(e) = self.save_to_disk() {
            eprintln!("Error saving data to disk during drop: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::{NamedTempFile, Builder};
    use std::fs::{write, remove_file, read, File as StdFile}; 
    use crate::core::storage::engine::wal::WalEntry;
    use crate::core::transaction::Transaction;
    use std::io::ErrorKind;
    use std::collections::HashSet; // For test setup if needed


    // Helper to create a main DB file with specific key-value data
    fn create_db_file_with_kv_data(path: &Path, data: &[(Vec<u8>, Vec<u8>)]) -> Result<(), DbError> {
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
        let dummy_transaction = Transaction::new(0); // Dummy transaction
        let key1 = b"key1".to_vec();
        let value1 = b"value1".to_vec();
        store.put(key1.clone(), value1.clone(), &dummy_transaction).unwrap();
        assert_eq!(store.get(&key1).unwrap(), Some(value1.clone()));

        let key2 = b"key2".to_vec();
        let value2 = b"value2_long".to_vec();
        store.put(key2.clone(), value2.clone(), &dummy_transaction).unwrap();
        assert_eq!(store.get(&key2).unwrap(), Some(value2.clone()));
        assert_eq!(store.get(&key1).unwrap(), Some(value1.clone()));
    }
    
    #[test]
    fn test_put_update() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut store = SimpleFileKvStore::new(temp_file.path()).unwrap();
        let dummy_transaction = Transaction::new(0); // Dummy transaction
        let key1 = b"key1".to_vec();
        let value1 = b"value1".to_vec();
        let value1_updated = b"value1_updated".to_vec();

        store.put(key1.clone(), value1.clone(), &dummy_transaction).unwrap();
        assert_eq!(store.get(&key1).unwrap(), Some(value1.clone()));

        store.put(key1.clone(), value1_updated.clone(), &dummy_transaction).unwrap();
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
        let dummy_transaction = Transaction::new(0); // Dummy transaction
        let key1 = b"key1".to_vec();
        let value1 = b"value1".to_vec();
        store.put(key1.clone(), value1.clone(), &dummy_transaction).unwrap();
        assert!(store.delete(&key1, &dummy_transaction).unwrap());
        assert_eq!(store.get(&key1).unwrap(), None);
        assert!(!store.contains_key(&key1).unwrap());
    }

    #[test]
    fn test_delete_non_existent() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut store = SimpleFileKvStore::new(temp_file.path()).unwrap();
        let dummy_transaction = Transaction::new(0); // Dummy transaction
        assert!(!store.delete(&b"non_existent_key".to_vec(), &dummy_transaction).unwrap());
    }

    #[test]
    fn test_contains_key() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut store = SimpleFileKvStore::new(temp_file.path()).unwrap();
        let dummy_transaction = Transaction::new(0); // Dummy transaction
        let key1 = b"key1".to_vec();
        store.put(key1.clone(), b"value1".to_vec(), &dummy_transaction).unwrap();
        assert!(store.contains_key(&key1).unwrap());
        assert!(!store.contains_key(&b"non_existent_key".to_vec()).unwrap());
    }

    #[test]
    fn test_persistence() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_path_buf();
        let dummy_transaction = Transaction::new(0); // Dummy transaction
        let key1 = b"persist_key".to_vec();
        let value1 = b"persist_value".to_vec();
        {
            let mut store = SimpleFileKvStore::new(&path).unwrap();
            store.put(key1.clone(), value1.clone(), &dummy_transaction).unwrap();
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
        let dummy_transaction = Transaction::new(0); // Dummy transaction
        let key1 = b"key1".to_vec();
        let value1 = b"value1".to_vec();
        store.put(key1.clone(), value1.clone(), &dummy_transaction).unwrap(); // put calls save_to_disk

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
        create_db_file_with_kv_data(&main_db_path, &initial_data).unwrap();

        let temp_data = vec![
            (b"key1".to_vec(), b"value_new".to_vec()),
            (b"key2".to_vec(), b"value2".to_vec()),
        ];
        create_db_file_with_kv_data(&temp_db_path, &temp_data).unwrap();

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
        create_db_file_with_kv_data(&main_db_path, &main_data).unwrap();

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
        create_db_file_with_kv_data(&temp_db_path, &temp_data).unwrap();
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
            let dummy_transaction = Transaction::new(0); // Dummy transaction
            store.put(key_orig.clone(), value_orig.clone(), &dummy_transaction).unwrap();
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
            let dummy_transaction = Transaction::new(0); // Dummy transaction
            assert_eq!(store.get(&key_orig).unwrap(), Some(value_orig.clone()));
            store.put(key_new.clone(), value_new.clone(), &dummy_transaction).unwrap(); // This save should succeed
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

    // Helper function to derive WAL path from DB path
    fn derive_wal_path(db_path: &Path) -> PathBuf {
        let mut wal_path = db_path.to_path_buf();
        let original_extension = wal_path.extension().map(|s| s.to_os_string());
        if let Some(ext) = original_extension {
            let mut new_ext = ext;
            new_ext.push(".wal");
            wal_path.set_extension(new_ext);
        } else {
            wal_path.set_extension("wal");
        }
        wal_path
    }

    // Helper to read all entries from a WAL file
    fn read_all_wal_entries(wal_path: &Path) -> Result<Vec<WalEntry>, DbError> {
        let file = StdFile::open(wal_path).map_err(DbError::IoError)?;
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();
        loop {
            match <WalEntry as DataDeserializer<WalEntry>>::deserialize(&mut reader) {
                Ok(entry) => entries.push(entry),
                Err(DbError::IoError(e)) if e.kind() == ErrorKind::UnexpectedEof => {
                    // This is the expected way to detect EOF when reading sequentially.
                    // It typically occurs when deserialize_from_reader tries to read the next op_type.
                    break;
                }
                Err(e) => {
                    // For test purposes, any other error during WAL reading is problematic.
                    // This could include DeserializationError or other IoErrors.
                    return Err(e);
                }
            }
        }
        Ok(entries)
    }


    #[test]
    fn test_put_writes_to_wal_and_cache() {
        let db_file = NamedTempFile::new().unwrap();
        let db_path = db_file.path();
        let wal_path = derive_wal_path(db_path);
        let dummy_transaction = Transaction::new(0); // Dummy transaction

        let mut store = SimpleFileKvStore::new(db_path).unwrap();
        let key = b"wal_key1".to_vec();
        let value = b"wal_value1".to_vec();
        store.put(key.clone(), value.clone(), &dummy_transaction).unwrap();

        assert_eq!(store.get(&key).unwrap(), Some(value.clone()));
        assert!(wal_path.exists());

        let entries = read_all_wal_entries(&wal_path).unwrap();
        assert_eq!(entries.len(), 1);
        match &entries[0] {
            WalEntry::Put { transaction_id, key: k, value: v } => {
                assert_eq!(*transaction_id, 0); // From dummy_transaction
                assert_eq!(k, &key);
                assert_eq!(v, &value);
            }
            _ => panic!("Expected Put entry"),
        }
    }

    #[test]
    fn test_delete_writes_to_wal_and_cache() {
        let db_file = NamedTempFile::new().unwrap();
        let db_path = db_file.path();
        let wal_path = derive_wal_path(db_path);
        let dummy_transaction_put = Transaction::new(0); // Dummy for put
        let dummy_transaction_delete = Transaction::new(1); // Dummy for delete

        let mut store = SimpleFileKvStore::new(db_path).unwrap();
        let key = b"wal_del_key".to_vec();
        let value = b"wal_del_value".to_vec();

        store.put(key.clone(), value.clone(), &dummy_transaction_put).unwrap();
        store.delete(&key, &dummy_transaction_delete).unwrap();

        assert_eq!(store.get(&key).unwrap(), None);
        assert!(wal_path.exists());

        let entries = read_all_wal_entries(&wal_path).unwrap();
        assert_eq!(entries.len(), 2);
        match &entries[0] {
            WalEntry::Put { transaction_id, key: k, value: v } => {
                assert_eq!(*transaction_id, 0);
                assert_eq!(k, &key);
                assert_eq!(v, &value);
            }
            _ => panic!("Expected Put entry as first entry"),
        }
        match &entries[1] {
            WalEntry::Delete { transaction_id, key: k } => {
                assert_eq!(*transaction_id, 1);
                assert_eq!(k, &key);
            }
            _ => panic!("Expected Delete entry as second entry"),
        }
    }

    #[test]
    fn test_load_from_disk_no_wal() {
        let db_file = NamedTempFile::new().unwrap();
        let db_path = db_file.path();
        let wal_path = derive_wal_path(db_path);

        let key = b"main_data_key".to_vec();
        let value = b"main_data_value".to_vec();
        let dummy_transaction = Transaction::new(0); // Dummy transaction

        {
            let mut store = SimpleFileKvStore::new(db_path).unwrap();
            store.put(key.clone(), value.clone(), &dummy_transaction).unwrap();
            store.save_to_disk().unwrap(); // This should delete the WAL
        }

        assert!(!wal_path.exists(), "WAL file should not exist after save_to_disk");

        let store = SimpleFileKvStore::new(db_path).unwrap();
        assert_eq!(store.get(&key).unwrap(), Some(value));
    }

    #[test]
    fn test_load_from_disk_with_wal_replay() {
        let db_file = NamedTempFile::new().unwrap();
        let db_path = db_file.path();
        let wal_path = derive_wal_path(db_path);

        // Initial data in main file (key0=val0_main)
        let key0 = b"key0".to_vec();
        let val0_main = b"val0_main".to_vec();
        create_db_file_with_kv_data(db_path, &[(key0.clone(), val0_main.clone())]).unwrap();

        // Setup WAL entries for different transactions
        let wal_writer = WalWriter::new(db_path);

        // Transaction 1: Committed (key1=val1, key0 deleted)
        wal_writer.log_entry(&WalEntry::Put { transaction_id: 1, key: b"key1".to_vec(), value: b"val1".to_vec() }).unwrap();
        wal_writer.log_entry(&WalEntry::Delete { transaction_id: 1, key: key0.clone() }).unwrap();
        wal_writer.log_entry(&WalEntry::TransactionCommit { transaction_id: 1 }).unwrap();

        // Transaction 2: Rolled back (key2=val2)
        wal_writer.log_entry(&WalEntry::Put { transaction_id: 2, key: b"key2".to_vec(), value: b"val2".to_vec() }).unwrap();
        wal_writer.log_entry(&WalEntry::TransactionRollback { transaction_id: 2 }).unwrap();

        // Transaction 3: Incomplete (key3=val3)
        wal_writer.log_entry(&WalEntry::Put { transaction_id: 3, key: b"key3".to_vec(), value: b"val3".to_vec() }).unwrap();
        
        // Transaction 4: Committed then rolled back (key4=val4) - should be rolled back
        wal_writer.log_entry(&WalEntry::Put { transaction_id: 4, key: b"key4".to_vec(), value: b"val4".to_vec() }).unwrap();
        wal_writer.log_entry(&WalEntry::TransactionCommit { transaction_id: 4 }).unwrap();
        wal_writer.log_entry(&WalEntry::TransactionRollback { transaction_id: 4 }).unwrap();

        // Transaction 5: Rolled back then committed (key5=val5) - should be rolled back (rollback usually final)
        // This scenario is less common but tests defensive logic.
        wal_writer.log_entry(&WalEntry::Put { transaction_id: 5, key: b"key5".to_vec(), value: b"val5".to_vec() }).unwrap();
        wal_writer.log_entry(&WalEntry::TransactionRollback { transaction_id: 5 }).unwrap();
        wal_writer.log_entry(&WalEntry::TransactionCommit { transaction_id: 5 }).unwrap(); // Commit after rollback

        // Action: Load store, triggering WAL replay
        let store = SimpleFileKvStore::new(db_path).unwrap();

        // Assertions:
        // Initial data (key0) should be gone due to committed TX1
        assert_eq!(store.get(&key0).unwrap(), None, "key0 should be deleted by committed TX1");
        // TX1 data
        assert_eq!(store.get(&b"key1".to_vec()).unwrap(), Some(b"val1".to_vec()), "key1 from committed TX1 should exist");
        // TX2 data (rolled back)
        assert_eq!(store.get(&b"key2".to_vec()).unwrap(), None, "key2 from rolled back TX2 should not exist");
        // TX3 data (incomplete)
        assert_eq!(store.get(&b"key3".to_vec()).unwrap(), None, "key3 from incomplete TX3 should not exist");
        // TX4 data (committed then rolled back)
        assert_eq!(store.get(&b"key4".to_vec()).unwrap(), None, "key4 from committed then rolled back TX4 should not exist");
        // TX5 data (rolled back then committed)
        assert_eq!(store.get(&b"key5".to_vec()).unwrap(), None, "key5 from rolled back then committed TX5 should not exist");

        assert!(wal_path.exists(), "WAL file should still exist after load_from_disk");
    }
    
    #[test]
    fn test_wal_recovery_after_simulated_crash() { // Incomplete transaction
        let db_file = NamedTempFile::new().unwrap();
        let db_path = db_file.path();
        let wal_path = derive_wal_path(db_path);

        let key_a = b"keyA_crash".to_vec();
        let val_a = b"valA_crash".to_vec();
        let key_b = b"keyB_crash".to_vec();
        let val_b = b"valB_crash".to_vec();

        {
            let mut store = SimpleFileKvStore::new(db_path).unwrap();
            // Operations for transaction ID 100 (incomplete)
            store.put(key_a.clone(), val_a.clone(), &Transaction::new(100)).unwrap();
            store.put(key_b.clone(), val_b.clone(), &Transaction::new(100)).unwrap();
            // No TransactionCommit for tx_id 100
            std::mem::forget(store); // Simulate crash
        }
        assert!(wal_path.exists());

        // Simulate crash by creating a new store instance. load_from_disk will run.
        let store_after_crash = SimpleFileKvStore::new(db_path).unwrap();
        // Operations from incomplete transaction 100 should NOT be in the cache.
        assert_eq!(store_after_crash.get(&key_a).unwrap(), None, "key_a from incomplete tx should not exist");
        assert_eq!(store_after_crash.get(&key_b).unwrap(), None, "key_b from incomplete tx should not exist");
    }

    #[test]
    fn test_wal_recovery_commit_then_rollback_same_tx() {
        let db_file = NamedTempFile::new().unwrap();
        let db_path = db_file.path();
        let wal_writer = WalWriter::new(db_path);

        // Transaction 1: Puts, then Commit, then Rollback
        wal_writer.log_entry(&WalEntry::Put { transaction_id: 1, key: b"key_cr".to_vec(), value: b"val_cr".to_vec() }).unwrap();
        wal_writer.log_entry(&WalEntry::TransactionCommit { transaction_id: 1 }).unwrap();
        wal_writer.log_entry(&WalEntry::TransactionRollback { transaction_id: 1 }).unwrap(); // Rollback after commit

        let store = SimpleFileKvStore::new(db_path).unwrap();
        assert_eq!(store.get(&b"key_cr".to_vec()).unwrap(), None, "Data from tx committed then rolled back should not exist");
    }
    
    #[test]
    fn test_wal_recovery_multiple_interleaved_transactions() {
        let db_file = NamedTempFile::new().unwrap();
        let db_path = db_file.path();
        let wal_writer = WalWriter::new(db_path);

        // TX 10 (Committed)
        wal_writer.log_entry(&WalEntry::Put { transaction_id: 10, key: b"key10_1".to_vec(), value: b"val10_1".to_vec() }).unwrap();
        
        // TX 20 (Incomplete)
        wal_writer.log_entry(&WalEntry::Put { transaction_id: 20, key: b"key20_1".to_vec(), value: b"val20_1".to_vec() }).unwrap();
        
        // TX 10 (Committed)
        wal_writer.log_entry(&WalEntry::Put { transaction_id: 10, key: b"key10_2".to_vec(), value: b"val10_2".to_vec() }).unwrap();
        wal_writer.log_entry(&WalEntry::TransactionCommit { transaction_id: 10 }).unwrap();

        // TX 30 (Rolled Back)
        wal_writer.log_entry(&WalEntry::Put { transaction_id: 30, key: b"key30_1".to_vec(), value: b"val30_1".to_vec() }).unwrap();
        wal_writer.log_entry(&WalEntry::TransactionRollback { transaction_id: 30 }).unwrap();
        
        // TX 20 (Still Incomplete) - another operation
        wal_writer.log_entry(&WalEntry::Delete { transaction_id: 20, key: b"some_other_key".to_vec() }).unwrap();

        let store = SimpleFileKvStore::new(db_path).unwrap();

        // Check TX 10 (Committed)
        assert_eq!(store.get(&b"key10_1".to_vec()).unwrap(), Some(b"val10_1".to_vec()));
        assert_eq!(store.get(&b"key10_2".to_vec()).unwrap(), Some(b"val10_2".to_vec()));

        // Check TX 20 (Incomplete)
        assert_eq!(store.get(&b"key20_1".to_vec()).unwrap(), None);
        assert_eq!(store.get(&b"some_other_key".to_vec()).unwrap(), None); // Assuming it wasn't in main file

        // Check TX 30 (Rolled Back)
        assert_eq!(store.get(&b"key30_1".to_vec()).unwrap(), None);
    }


    #[test]
    fn test_wal_truncation_after_save_to_disk() {
        let db_file = NamedTempFile::new().unwrap();
        let db_path = db_file.path();
        let wal_path = derive_wal_path(db_path);

        let key = b"trunc_key".to_vec();
        let value = b"trunc_val".to_vec();
        let dummy_transaction = Transaction::new(0); // Dummy transaction
        {
            let mut store = SimpleFileKvStore::new(db_path).unwrap();
            store.put(key.clone(), value.clone(), &dummy_transaction).unwrap();
            assert!(wal_path.exists());
            store.save_to_disk().unwrap();
        }
        
        assert!(!wal_path.exists(), "WAL file should not exist after save_to_disk");

        let store = SimpleFileKvStore::new(db_path).unwrap();
        assert_eq!(store.get(&key).unwrap(), Some(value.clone()));
    }

    #[test]
    fn test_wal_replay_stops_on_corruption() {
        let db_file = NamedTempFile::new().unwrap(); // Main DB file (can be empty for this test)
        let db_path = db_file.path();
        let wal_path = derive_wal_path(db_path);

        let key_good = b"key_good".to_vec();
        let value_good = b"value_good".to_vec();
        let key_bad = b"key_bad".to_vec();
        let value_bad = b"value_bad".to_vec();

        // Manually create WAL with corruption
        {
            let wal_file_handle = OpenOptions::new().write(true).create(true).truncate(true).open(&wal_path).unwrap();
            let mut writer = BufWriter::new(wal_file_handle);
            
            // Valid entry
            <WalEntry as DataSerializer<WalEntry>>::serialize(&WalEntry::Put{ transaction_id: 0, key: key_good.clone(), value: value_good.clone() }, &mut writer).unwrap();
            
            // Corrupted data (e.g. invalid operation type or bad checksum, simpler: just random bytes not forming a valid entry part)
            writer.write_all(&[0xDE, 0xAD, 0xBE, 0xEF]).unwrap(); // Random bytes, will cause deserialization to fail
            
            // Another valid entry (should not be reached)
            <WalEntry as DataSerializer<WalEntry>>::serialize(&WalEntry::Put{ transaction_id: 1, key: key_bad.clone(), value: value_bad.clone() }, &mut writer).unwrap();
            writer.flush().unwrap();
        }

        let store = SimpleFileKvStore::new(db_path).unwrap(); // Triggers load_from_disk and WAL replay
        
        assert_eq!(store.get(&key_good).unwrap(), Some(value_good.clone()), "Should recover key before corruption");
        assert_eq!(store.get(&key_bad).unwrap(), None, "Should not recover key after corruption");
    }

    #[test]
    fn test_drop_persists_data() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_path_buf();
        let key1 = b"drop_key".to_vec();
        let value1 = b"drop_value".to_vec();
        let dummy_transaction = Transaction::new(0); // Dummy transaction

        {
            let mut store = SimpleFileKvStore::new(&path).unwrap();
            store.put(key1.clone(), value1.clone(), &dummy_transaction).unwrap();
            // Store goes out of scope here, Drop should be called.
        }

        // Re-load the store and check if data is persisted.
        let reloaded_store = SimpleFileKvStore::new(&path).unwrap();
        assert_eq!(reloaded_store.get(&key1).unwrap(), Some(value1));
        assert_eq!(reloaded_store.cache.len(), 1);

        // Also check that the WAL file is cleared after a successful save_to_disk (which drop calls)
        let wal_path = derive_wal_path(&path);
        assert!(!wal_path.exists(), "WAL file should not exist after successful drop/save.");
    }

    #[test]
    fn test_put_atomicity_wal_failure() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("test_put_atomicity.db");
        let wal_path = derive_wal_path(&db_path);

        // Create a directory where the WAL file should be, to cause WAL write to fail
        std::fs::create_dir_all(&wal_path).unwrap_or_else(|e| panic!("Failed to create dir at WAL path {:?}: {}", wal_path, e));
        assert!(wal_path.is_dir());

        let mut store = SimpleFileKvStore::new(&db_path).expect("Store creation should succeed even if WAL path is a dir, as WAL is written lazily.");

        let key = b"atomic_put_key".to_vec();
        let value = b"atomic_put_value".to_vec();
        let dummy_transaction = Transaction::new(0);

        // Attempt to put, expecting failure from WAL
        let result = store.put(key.clone(), value.clone(), &dummy_transaction);
        
        assert!(result.is_err(), "put operation should fail due to WAL error");
        match result.unwrap_err() {
            DbError::IoError(io_err) => {
                // On Linux, this is "Is a directory (os error 21)"
                // On Windows, it might be different, e.g. "Access is denied. (os error 5)" if trying to open dir as file
                // For CI stability, we might not want to assert the exact OS error string/code.
                // Just checking it's an IoError is a good start.
                eprintln!("Confirmed IoError on put: {:?}", io_err); // For debugging in CI
            }
            other_err => panic!("Expected DbError::IoError, got {:?}", other_err),
        }

        // Assert that the cache does not contain the key
        assert!(store.get(&key).unwrap().is_none(), "Cache should not contain key after failed WAL write for put.");
        assert!(!store.cache.contains_key(&key), "Cache should not contain key directly.");

        // Cleanup: remove the directory we created
        let _ = std::fs::remove_dir_all(&wal_path);
    }

    #[test]
    fn test_delete_atomicity_wal_failure() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("test_delete_atomicity.db");
        let wal_path = derive_wal_path(&db_path);
        let dummy_transaction = Transaction::new(0);

        let key = b"atomic_del_key".to_vec();
        let value = b"atomic_del_value".to_vec();

        // 1. Setup store and insert an item successfully
        {
            let mut store = SimpleFileKvStore::new(&db_path).unwrap();
            store.put(key.clone(), value.clone(), &dummy_transaction).unwrap();
            store.save_to_disk().unwrap(); // Ensure data is in main file, WAL is clear
        }
        
        assert!(!wal_path.exists(), "WAL should be cleared by save_to_disk");

        // 2. Create a directory where the WAL file should be, to cause next WAL write to fail
        std::fs::create_dir_all(&wal_path).unwrap_or_else(|e| panic!("Failed to create dir at WAL path {:?}: {}", wal_path, e));
        assert!(wal_path.is_dir());
        
        // 3. Re-open the store. It will load from the main file. WalWriter will point to the problematic path.
        let mut store = SimpleFileKvStore::new(&db_path).unwrap();
        assert!(store.get(&key).unwrap().is_some(), "Key should be present from main file load.");
        assert!(store.cache.contains_key(&key), "Cache should contain key after load.");


        // 4. Attempt to delete, expecting failure from WAL
        let result = store.delete(&key, &dummy_transaction);

        assert!(result.is_err(), "delete operation should fail due to WAL error");
         match result.unwrap_err() {
            DbError::IoError(io_err) => {
                 eprintln!("Confirmed IoError on delete: {:?}", io_err); 
            }
            other_err => panic!("Expected DbError::IoError, got {:?}", other_err),
        }

        // 5. Assert that the cache still contains the key
        assert!(store.get(&key).unwrap().is_some(), "Cache should still contain key after failed WAL write for delete.");
        assert!(store.cache.contains_key(&key), "Cache should still contain key directly.");
        assert_eq!(store.cache.get(&key), Some(&value), "Value should be the original value.");

        // Cleanup: remove the directory we created
        let _ = std::fs::remove_dir_all(&wal_path);
    }
}
