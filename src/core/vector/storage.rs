//! Vector storage module for oxidb
//!
//! This module provides persistent storage for vector data following SOLID principles:
//! - Single Responsibility: Focused on vector storage operations
//! - Open/Closed: Extensible for different storage backends
//! - Liskov Substitution: Storage implementations can be substituted
//! - Interface Segregation: Clean, focused interfaces
//! - Dependency Inversion: Depends on abstractions, not concrete implementations

use crate::core::common::OxidbError;
use crate::core::types::VectorData;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Vector entry with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorEntry {
    pub id: String,
    pub vector: VectorData,
    pub metadata: HashMap<String, String>,
    pub timestamp: u64,
}

impl VectorEntry {
    /// Create a new vector entry
    #[must_use]
    pub fn new(id: String, vector: VectorData) -> Self {
        Self {
            id,
            vector,
            metadata: HashMap::new(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }

    /// Create a vector entry with metadata
    #[must_use]
    pub fn with_metadata(
        id: String,
        vector: VectorData,
        metadata: HashMap<String, String>,
    ) -> Self {
        Self {
            id,
            vector,
            metadata,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }

    /// Add metadata to the entry
    pub fn add_metadata(&mut self, key: String, value: String) {
        self.metadata.insert(key, value);
    }

    /// Get metadata value
    #[must_use]
    pub fn get_metadata(&self, key: &str) -> Option<&String> {
        self.metadata.get(key)
    }
}

/// Trait for vector storage operations (Interface Segregation Principle)
pub trait VectorStore {
    /// Store a vector entry
    fn store(&mut self, entry: VectorEntry) -> Result<(), OxidbError>;

    /// Retrieve a vector entry by ID
    fn retrieve(&self, id: &str) -> Result<Option<VectorEntry>, OxidbError>;

    /// Delete a vector entry
    fn delete(&mut self, id: &str) -> Result<bool, OxidbError>;

    /// List all vector IDs
    fn list_ids(&self) -> Result<Vec<String>, OxidbError>;

    /// Get the number of stored vectors
    fn count(&self) -> Result<usize, OxidbError>;

    /// Check if a vector exists
    fn exists(&self, id: &str) -> Result<bool, OxidbError>;
}

/// In-memory vector store implementation
/// Follows the Single Responsibility Principle - only handles in-memory storage
#[derive(Debug, Default)]
pub struct InMemoryVectorStore {
    vectors: HashMap<String, VectorEntry>,
}

impl InMemoryVectorStore {
    /// Create a new in-memory vector store
    #[must_use]
    pub fn new() -> Self {
        Self { vectors: HashMap::new() }
    }

    /// Clear all vectors
    pub fn clear(&mut self) {
        self.vectors.clear();
    }

    /// Get all vectors (for testing/debugging)
    #[must_use]
    pub const fn get_all(&self) -> &HashMap<String, VectorEntry> {
        &self.vectors
    }
}

impl VectorStore for InMemoryVectorStore {
    fn store(&mut self, entry: VectorEntry) -> Result<(), OxidbError> {
        let id = entry.id.clone();
        self.vectors.insert(id, entry);
        Ok(())
    }

    fn retrieve(&self, id: &str) -> Result<Option<VectorEntry>, OxidbError> {
        Ok(self.vectors.get(id).cloned())
    }

    fn delete(&mut self, id: &str) -> Result<bool, OxidbError> {
        Ok(self.vectors.remove(id).is_some())
    }

    fn list_ids(&self) -> Result<Vec<String>, OxidbError> {
        Ok(self.vectors.keys().cloned().collect())
    }

    fn count(&self) -> Result<usize, OxidbError> {
        Ok(self.vectors.len())
    }

    fn exists(&self, id: &str) -> Result<bool, OxidbError> {
        Ok(self.vectors.contains_key(id))
    }
}

/// Vector store factory following the Factory pattern
pub struct VectorStoreFactory;

impl VectorStoreFactory {
    /// Create an in-memory vector store
    #[must_use]
    pub fn create_in_memory_store() -> Box<dyn VectorStore> {
        Box::new(InMemoryVectorStore::new())
    }

    /// Create a vector store based on configuration (extensible for future backends)
    pub fn create_store(store_type: &str) -> Result<Box<dyn VectorStore>, OxidbError> {
        match store_type {
            "memory" => Ok(Self::create_in_memory_store()),
            _ => Err(OxidbError::InvalidInput {
                message: format!("Unsupported vector store type: {store_type}"),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::vector::VectorFactory;

    #[test]
    fn test_vector_entry_creation() {
        let vector = VectorFactory::create_vector(3, vec![1.0, 2.0, 3.0]).unwrap();
        let entry = VectorEntry::new("test_id".to_string(), vector);

        assert_eq!(entry.id, "test_id");
        assert_eq!(entry.vector.dimension, 3);
        assert!(entry.metadata.is_empty());
        assert!(entry.timestamp > 0);
    }

    #[test]
    fn test_vector_entry_with_metadata() {
        let vector = VectorFactory::create_vector(2, vec![1.0, 2.0]).unwrap();
        let mut metadata = HashMap::new();
        metadata.insert("type".to_string(), "embedding".to_string());

        let mut entry = VectorEntry::with_metadata("test_id".to_string(), vector, metadata);
        entry.add_metadata("source".to_string(), "document_1".to_string());

        assert_eq!(entry.get_metadata("type"), Some(&"embedding".to_string()));
        assert_eq!(entry.get_metadata("source"), Some(&"document_1".to_string()));
        assert_eq!(entry.get_metadata("nonexistent"), None);
    }

    #[test]
    fn test_in_memory_vector_store() {
        let mut store = InMemoryVectorStore::new();
        let vector = VectorFactory::create_vector(3, vec![1.0, 2.0, 3.0]).unwrap();
        let entry = VectorEntry::new("test_id".to_string(), vector);

        // Test store
        assert!(store.store(entry.clone()).is_ok());
        assert_eq!(store.count().unwrap(), 1);
        assert!(store.exists("test_id").unwrap());

        // Test retrieve
        let retrieved = store.retrieve("test_id").unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().id, "test_id");

        // Test list_ids
        let ids = store.list_ids().unwrap();
        assert_eq!(ids.len(), 1);
        assert!(ids.contains(&"test_id".to_string()));

        // Test delete
        assert!(store.delete("test_id").unwrap());
        assert_eq!(store.count().unwrap(), 0);
        assert!(!store.exists("test_id").unwrap());
        assert!(!store.delete("test_id").unwrap()); // Already deleted
    }

    #[test]
    fn test_vector_store_factory() {
        let store = VectorStoreFactory::create_in_memory_store();
        assert_eq!(store.count().unwrap(), 0);

        let store2 = VectorStoreFactory::create_store("memory").unwrap();
        assert_eq!(store2.count().unwrap(), 0);

        let result = VectorStoreFactory::create_store("invalid");
        assert!(result.is_err());
    }
}
