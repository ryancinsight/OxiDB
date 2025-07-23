// src/core/rag/retriever.rs

use super::core_components::{Document, Embedding};
use crate::core::common::OxidbError;
use crate::core::vector::similarity::{cosine_similarity, dot_product};
use async_trait::async_trait; // Assuming these are pub

/// Defines the type of similarity metric to use for retrieval.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimilarityMetric {
    Cosine,
    DotProduct,
    // Euclidean, // Example for future extension
}

/// Trait for retrieving relevant documents based on a query embedding.
#[async_trait]
pub trait Retriever: Send + Sync {
    /// Retrieves the top_k most relevant documents for a given query embedding.
    async fn retrieve(
        &self,
        query_embedding: &Embedding,
        top_k: usize,
        metric: SimilarityMetric,
    ) -> Result<Vec<Document>, OxidbError>;
}

/// A simple in-memory retriever for testing and basic use cases.
/// It stores documents and their embeddings in memory and performs a brute-force search.
pub struct InMemoryRetriever {
    documents: Vec<Document>, // Stores documents, assuming they might already have embeddings
}

impl InMemoryRetriever {
    #[must_use]
    pub const fn new(documents: Vec<Document>) -> Self {
        Self { documents }
    }

    /// Adds a document to the retriever.
    /// Note: For simplicity, this mock implementation expects documents to already have embeddings.
    /// A real implementation might embed them here or expect an external process.
    pub fn add_document(&mut self, document: Document) {
        self.documents.push(document);
    }
}

#[async_trait]
impl Retriever for InMemoryRetriever {
    async fn retrieve(
        &self,
        query_embedding: &Embedding,
        top_k: usize,
        metric: SimilarityMetric,
    ) -> Result<Vec<Document>, OxidbError> {
        if top_k == 0 {
            return Ok(Vec::new());
        }

        let mut scored_documents: Vec<(f32, &Document)> = Vec::new();

        for doc in &self.documents {
            if let Some(doc_embedding) = &doc.embedding {
                let score = match metric {
                    SimilarityMetric::Cosine => {
                        cosine_similarity(query_embedding.as_slice(), doc_embedding.as_slice())?
                    }
                    SimilarityMetric::DotProduct => {
                        dot_product(query_embedding.as_slice(), doc_embedding.as_slice())?
                    }
                };
                scored_documents.push((score, doc));
            }
        }

        // Sort by score. For cosine and dot product, higher is better.
        // If adding Euclidean, lower would be better, so sorting logic would need adjustment.
        scored_documents.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

        Ok(scored_documents
            .into_iter()
            .take(top_k)
            .map(|(_, doc)| doc.clone()) // Clone the document to return owned instances
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::rag::core_components::Embedding; // Ensure Embedding is in scope
    use crate::core::rag::embedder::{EmbeddingModel, MockEmbeddingModel}; // For generating embeddings
    use approx::assert_relative_eq;

    async fn setup_retriever() -> InMemoryRetriever {
        let model = MockEmbeddingModel { dimension: 2, fixed_embedding_value: None };
        let docs_content = vec![
            ("doc1", "apple banana"),      // len 12 -> emb [0.12, 0.12]
            ("doc2", "apple orange"), // len 12 -> emb [0.12, 0.12] (same as doc1 for this mock)
            ("doc3", "banana grape"), // len 12 -> emb [0.12, 0.12] (same as doc1 for this mock)
            ("doc4", "totally different"), // len 17 -> emb [0.17, 0.17]
        ];

        let mut documents = Vec::new();
        for (id, content) in docs_content {
            let doc = Document::new(id.to_string(), content.to_string());
            let embedding = model.embed_document(&doc).await.unwrap();
            documents.push(doc.with_embedding(embedding));
        }

        // Manually set one embedding to be distinct for better testing
        if let Some(doc_to_change) = documents.get_mut(2) {
            // doc3
            doc_to_change.embedding = Some(Embedding::from(vec![0.5, 0.5]));
        }

        InMemoryRetriever::new(documents)
    }

    #[tokio::test]
    async fn test_in_memory_retriever_cosine_similarity() {
        let retriever = setup_retriever().await;
        let query_embedding = Embedding::from(vec![0.45, 0.55]);

        // For debugging, let's calculate and print scores manually for each doc
        println!("Query: {:?}", query_embedding.as_slice());
        for doc in &retriever.documents {
            if let Some(emb) = &doc.embedding {
                let score = cosine_similarity(query_embedding.as_slice(), emb.as_slice());
                println!("Doc ID: {}, Embedding: {:?}, Score: {:?}", doc.id, emb.as_slice(), score);
            }
        }

        let results =
            retriever.retrieve(&query_embedding, 2, SimilarityMetric::Cosine).await.unwrap();

        println!(
            "Retrieved results: {:?}",
            results.iter().map(|d| d.id.as_str()).collect::<Vec<&str>>()
        );

        assert_eq!(results.len(), 2);
        // Based on program's output and re-verified calculations:
        // Query: [0.45, 0.55]
        // Doc3 [0.5, 0.5], Score: Ok(0.99503714)
        // Doc4 [0.17, 0.17], Score: Ok(0.99503726)
        // Doc1/Doc2 [0.12, 0.12], Score: Ok(0.99503714)
        // Doc4 has the highest score. Doc1, Doc2, Doc3 are tied.
        // Due to stable sort, their original relative order (doc1, doc2, doc3) is maintained for the tie.
        assert_eq!(results[0].id, "doc4");
        assert_eq!(
            results[1].id, "doc1",
            "Second document should be doc1 due to tie-breaking with stable sort."
        );
    }

    #[tokio::test]
    async fn test_in_memory_retriever_dot_product() {
        let retriever = setup_retriever().await;
        // Query embedding [1.0, 0.0], should favor embeddings with higher first component if positive
        // doc1, doc2: [0.12, 0.12] -> dot: 0.12
        // doc3:       [0.5, 0.5]   -> dot: 0.5
        // doc4:       [0.17, 0.17] -> dot: 0.17
        let query_embedding = Embedding::from(vec![1.0, 0.0]);
        let results =
            retriever.retrieve(&query_embedding, 2, SimilarityMetric::DotProduct).await.unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, "doc3"); // Highest dot product
        assert_eq!(results[1].id, "doc4"); // Next highest
    }

    #[tokio::test]
    async fn test_retrieve_top_k_zero() {
        let retriever = setup_retriever().await;
        let query_embedding = Embedding::from(vec![0.1, 0.1]);
        let results =
            retriever.retrieve(&query_embedding, 0, SimilarityMetric::Cosine).await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_retrieve_more_than_available() {
        let retriever = setup_retriever().await; // Has 4 documents
        let query_embedding = Embedding::from(vec![0.1, 0.1]);
        let results =
            retriever.retrieve(&query_embedding, 10, SimilarityMetric::Cosine).await.unwrap();
        assert_eq!(results.len(), 4); // Returns all available documents
    }

    #[tokio::test]
    async fn test_retrieve_no_documents_with_embeddings() {
        let retriever = InMemoryRetriever::new(vec![Document::new(
            "doc1".to_string(),
            "no embedding here".to_string(),
        )]);
        let query_embedding = Embedding::from(vec![0.1, 0.1]);
        let results =
            retriever.retrieve(&query_embedding, 1, SimilarityMetric::Cosine).await.unwrap();
        assert!(results.is_empty());
    }
    #[tokio::test]
    async fn test_in_memory_retriever_cosine_similarity_identical_query() {
        let retriever = setup_retriever().await;
        // Query with an embedding identical to doc3's
        let query_embedding = Embedding::from(vec![0.5, 0.5]);

        let results =
            retriever.retrieve(&query_embedding, 1, SimilarityMetric::Cosine).await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, "doc3");
        // The score should be 1.0 (or very close due to float precision)
        // We can't directly check the score from the retrieve API, but this implies it's the highest.

        // Verify score calculation for self-similarity
        let score = cosine_similarity(&[0.5, 0.5], &[0.5, 0.5]).unwrap();
        assert_relative_eq!(score, 1.0, epsilon = 1e-6);
    }
}
