// src/core/rag/embedder.rs

use super::core_components::{Document, Embedding};
use crate::core::common::OxidbError;
use async_trait::async_trait;

/// Trait for models that can generate embeddings for documents.
#[async_trait]
pub trait EmbeddingModel: Send + Sync {
    /// Generates an embedding for a single document.
    async fn embed_document(&self, document: &Document) -> Result<Embedding, OxidbError>;

    /// Generates embeddings for a batch of documents.
    /// Default implementation calls `embed_document` for each document.
    /// Implementers can override this for batch-optimized embedding generation.
    async fn embed_documents(&self, documents: &[Document]) -> Result<Vec<Embedding>, OxidbError> {
        let mut embeddings = Vec::with_capacity(documents.len());
        for doc in documents {
            embeddings.push(self.embed_document(doc).await?);
        }
        Ok(embeddings)
    }
}

/// A simple mock embedding model for testing purposes.
#[cfg(test)]
pub(crate) struct MockEmbeddingModel {
    pub(crate) dimension: usize,
    pub(crate) fixed_embedding_value: Option<f32>, // If set, all dimensions will have this value
}

#[cfg(test)]
#[async_trait]
#[cfg(test)]
impl EmbeddingModel for MockEmbeddingModel {
    async fn embed_document(&self, document: &Document) -> Result<Embedding, OxidbError> {
        let value_to_fill = self.fixed_embedding_value.unwrap_or_else(|| {
            // Create a pseudo-random value based on document content length for some variation
            (document.content.len() % 100) as f32 / 100.0
        });
        let vec = vec![value_to_fill; self.dimension];
        Ok(Embedding::from(vec))
    }

    async fn embed_documents(&self, documents: &[Document]) -> Result<Vec<Embedding>, OxidbError> {
        let mut embeddings = Vec::new();
        for doc in documents {
            let embedding = self.embed_document(doc).await?;
            embeddings.push(embedding);
        }
        Ok(embeddings)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::rag::core_components::Document; // Ensure Document is in scope

    #[tokio::test]
    async fn test_mock_embedding_model_single_document() {
        let model = MockEmbeddingModel { dimension: 3, fixed_embedding_value: Some(0.5) };
        let doc = Document::new("id1".to_string(), "Test content".to_string());
        let embedding = model.embed_document(&doc).await.unwrap();
        assert_eq!(embedding.vector, vec![0.5, 0.5, 0.5]);
        assert_eq!(embedding.vector.len(), 3);
    }

    #[tokio::test]
    async fn test_mock_embedding_model_batch_documents() {
        let model = MockEmbeddingModel { dimension: 2, fixed_embedding_value: None };
        let docs = vec![
            Document::new("id1".to_string(), "Short".to_string()), // len 5 -> 0.05
            Document::new("id2".to_string(), "Longer content".to_string()), // len 14 -> 0.14
        ];
        let embeddings = model.embed_documents(&docs).await.unwrap();
        assert_eq!(embeddings.len(), 2);
        assert_eq!(embeddings[0].vector, vec![0.05, 0.05]);
        assert_eq!(embeddings[1].vector, vec![0.14, 0.14]);
    }

    #[tokio::test]
    async fn test_mock_embedding_model_default_batch_via_single() {
        struct TestModel {
            dimension: usize,
        }
        #[async_trait]
        impl EmbeddingModel for TestModel {
            async fn embed_document(&self, document: &Document) -> Result<Embedding, OxidbError> {
                Ok(Embedding::from(vec![
                    (document.id.chars().last().unwrap_or('0').to_digit(10).unwrap_or(0) % 10)
                        as f32;
                    self.dimension
                ]))
            }
        }

        let model = TestModel { dimension: 1 };
        let docs = vec![
            Document::new("doc1".to_string(), "".to_string()),
            Document::new("doc2".to_string(), "".to_string()),
        ];
        // This test relies on the default implementation of embed_documents
        let embeddings = model.embed_documents(&docs).await.unwrap();
        assert_eq!(embeddings.len(), 2);
        assert_eq!(embeddings[0].vector, vec![1.0]);
        assert_eq!(embeddings[1].vector, vec![2.0]);
    }
}
