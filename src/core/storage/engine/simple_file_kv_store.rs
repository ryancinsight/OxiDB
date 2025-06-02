use std::collections::HashMap;
use std::fs::{File, OpenOptions};
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

    fn load_from_disk(&mut self) -> Result<(), DbError> {
        self.cache.clear();
        let file = match File::open(&self.file_path) {
            Ok(f) => f,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(DbError::IoError(e)),
        };

        let mut reader = BufReader::new(file);

        loop {
            // Check for EOF *before* trying to read the length of the key.
            let buffer = reader.fill_buf().map_err(DbError::IoError)?;
            if buffer.is_empty() {
                break; // Clean EOF
            }

            let key = Vec::<u8>::deserialize(&mut reader)
                .map_err(|e| DbError::StorageError(format!("Failed to deserialize key: {}", e)))?;
            
            // If key deserialization succeeded, a value must follow.
            // Any error here (including UnexpectedEof) means a corrupted/truncated file.
            let value = Vec::<u8>::deserialize(&mut reader)
                .map_err(|e| DbError::StorageError(format!("Failed to deserialize value for key {:?}: {}", String::from_utf8_lossy(&key), e)))?;
            
            self.cache.insert(key, value);
        }
        Ok(())
    }

    fn save_to_disk(&self) -> Result<(), DbError> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.file_path)
            .map_err(DbError::IoError)?;
        let mut writer = BufWriter::new(file);

        for (key, value) in &self.cache {
            Vec::<u8>::serialize(key, &mut writer)?;
            Vec::<u8>::serialize(value, &mut writer)?;
        }
        writer.flush().map_err(DbError::IoError)?;
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
    use tempfile::NamedTempFile;

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
