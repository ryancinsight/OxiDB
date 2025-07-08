// src/core/rag/core_components.rs

use crate::core::common::types::Value;
use std::collections::HashMap;

/// Represents a piece of text content, often a document or a chunk of a document.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Document {
    /// Unique identifier for the document.
    pub id: String,
    /// The actual text content.
    pub content: String,
    /// Optional metadata associated with the document.
    pub metadata: Option<HashMap<String, Value>>,
    /// Optional embedding of the document's content.
    pub embedding: Option<Embedding>,
}

impl Document {
    /// Creates a new document.
    pub fn new(id: String, content: String) -> Self {
        Self { id, content, metadata: None, embedding: None }
    }

    /// Adds metadata to the document.
    pub fn with_metadata(mut self, metadata: HashMap<String, Value>) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Adds an embedding to the document.
    pub fn with_embedding(mut self, embedding: Embedding) -> Self {
        self.embedding = Some(embedding);
        self
    }
}

/// Represents a vector embedding.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Embedding {
    pub vector: Vec<f32>,
    // Could include model name or other metadata about the embedding
}

impl From<Vec<f32>> for Embedding {
    fn from(vector: Vec<f32>) -> Self {
        Self { vector }
    }
}

impl Embedding {
    pub fn as_slice(&self) -> &[f32] {
        &self.vector
    }
}

// Basic tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_document_creation() {
        let doc_id = "doc1".to_string();
        let content = "This is a test document.".to_string();
        let doc = Document::new(doc_id.clone(), content.clone());

        assert_eq!(doc.id, doc_id);
        assert_eq!(doc.content, content);
        assert!(doc.metadata.is_none());
        assert!(doc.embedding.is_none());
    }

    #[test]
    fn test_document_with_metadata() {
        let mut metadata = HashMap::new();
        metadata.insert("source".to_string(), Value::Text("website".to_string()));
        let doc = Document::new("doc2".to_string(), "Another doc.".to_string())
            .with_metadata(metadata.clone());

        assert_eq!(
            doc.metadata.unwrap().get("source").unwrap(),
            &Value::Text("website".to_string())
        );
    }

    #[test]
    fn test_document_with_embedding() {
        let embedding_vec = vec![0.1, 0.2, 0.3];
        let embedding = Embedding::from(embedding_vec.clone());
        let doc = Document::new("doc3".to_string(), "Content with embedding.".to_string())
            .with_embedding(embedding.clone());

        assert_eq!(doc.embedding.unwrap().vector, embedding_vec);
    }

    #[test]
    fn test_embedding_creation_and_as_slice() {
        let vector = vec![1.0, 2.0, 3.0, 4.0];
        let embedding = Embedding::from(vector.clone());
        assert_eq!(embedding.vector, vector);
        assert_eq!(embedding.as_slice(), vector.as_slice());
    }
}
