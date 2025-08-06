// src/core/vector/search.rs
//! Vector Search Implementation following SOLID, CUPID, GRASP, DRY, YAGNI, and ACID principles
//!
//! This module provides efficient vector similarity search capabilities for RAG applications.
//! It implements various search strategies and indexing methods for high-performance vector retrieval.

use crate::core::common::OxidbError;
use crate::core::rag::document::{Document, Embedding};
use crate::core::vector::similarity::{cosine_similarity, dot_product};
use std::collections::{BinaryHeap, HashMap};
use std::cmp::Ordering;

/// Search result with similarity score
#[derive(Debug, Clone, PartialEq)]
pub struct SearchResult {
    pub document: Document,
    pub similarity_score: f32,
    pub rank: usize,
}

impl SearchResult {
    pub fn new(document: Document, similarity_score: f32, rank: usize) -> Self {
        Self {
            document,
            similarity_score,
            rank,
        }
    }
}

impl Eq for SearchResult {}

impl PartialOrd for SearchResult {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // Normal ordering: smaller scores are less than larger scores
        self.similarity_score.partial_cmp(&other.similarity_score)
    }
}

impl Ord for SearchResult {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

/// Search parameters for vector queries
#[derive(Debug, Clone)]
pub struct SearchParams {
    pub k: usize,                    // Number of results to return
    pub similarity_threshold: f32,   // Minimum similarity score
    pub include_metadata: bool,      // Whether to include document metadata
    pub max_distance: Option<f32>,   // Maximum distance for filtering
}

impl Default for SearchParams {
    fn default() -> Self {
        Self {
            k: 10,
            similarity_threshold: 0.0,
            include_metadata: true,
            max_distance: None,
        }
    }
}

/// Vector search strategy trait (Strategy Pattern - SOLID Open/Closed Principle)
pub trait VectorSearchStrategy: Send + Sync {
    /// Search for similar vectors
    fn search(
        &self,
        query_embedding: &Embedding,
        documents: &[Document],
        params: &SearchParams,
    ) -> Result<Vec<SearchResult>, OxidbError>;

    /// Get the name of this search strategy
    fn name(&self) -> &'static str;
}

/// Brute force linear search implementation
/// Follows Single Responsibility Principle - only does linear search
pub struct LinearSearchStrategy;

impl VectorSearchStrategy for LinearSearchStrategy {
    fn search(
        &self,
        query_embedding: &Embedding,
        documents: &[Document],
        params: &SearchParams,
    ) -> Result<Vec<SearchResult>, OxidbError> {
        let mut heap = BinaryHeap::new();
        
        for (idx, document) in documents.iter().enumerate() {
            if let Some(doc_embedding) = &document.embedding {
                let similarity = cosine_similarity(
                    query_embedding.as_slice(),
                    doc_embedding.as_slice(),
                )?;

                if similarity >= params.similarity_threshold {
                    if let Some(max_dist) = params.max_distance {
                        let distance = 1.0 - similarity;
                        if distance > max_dist {
                            continue;
                        }
                    }

                    let result = SearchResult::new(
                        document.clone(),
                        similarity,
                        idx,
                    );

                    if heap.len() < params.k {
                        heap.push(result);
                    } else if let Some(min_result) = heap.peek() {
                        if similarity > min_result.similarity_score {
                            heap.pop();
                            heap.push(result);
                        }
                    }
                }
            }
        }

        let mut results: Vec<SearchResult> = heap.into_sorted_vec();
        results.reverse(); // Convert to descending order

        // Update ranks
        for (rank, result) in results.iter_mut().enumerate() {
            result.rank = rank;
        }

        Ok(results)
    }

    fn name(&self) -> &'static str {
        "linear_search"
    }
}

/// Approximate Nearest Neighbor search using LSH (Locality Sensitive Hashing)
/// Follows Single Responsibility Principle - only does LSH-based search
pub struct LSHSearchStrategy {
    hash_tables: Vec<LSHHashTable>,
    #[allow(dead_code)] // Used for configuration but not in current implementation
    num_hash_functions: usize,
}

impl LSHSearchStrategy {
    pub fn new(num_tables: usize, num_hash_functions: usize, dimension: usize) -> Result<Self, OxidbError> {
        if num_tables == 0 || num_hash_functions == 0 {
            return Err(OxidbError::InvalidInput { field: "Unknown".to_string(), message: "Number of tables and hash functions must be positive".to_string(),
             });
        }

        let mut hash_tables = Vec::with_capacity(num_tables);
        for _ in 0..num_tables {
            hash_tables.push(LSHHashTable::new(num_hash_functions, dimension)?);
        }

        Ok(Self {
            hash_tables,
            num_hash_functions,
        })
    }

    pub fn index_documents(&mut self, documents: &[Document]) -> Result<(), OxidbError> {
        for document in documents {
            if let Some(embedding) = &document.embedding {
                for table in &mut self.hash_tables {
                    table.insert(document.id.clone(), embedding)?;
                }
            }
        }
        Ok(())
    }
}

impl VectorSearchStrategy for LSHSearchStrategy {
    fn search(
        &self,
        query_embedding: &Embedding,
        documents: &[Document],
        params: &SearchParams,
    ) -> Result<Vec<SearchResult>, OxidbError> {
        let mut candidate_ids = std::collections::HashSet::new();

        // Get candidates from all hash tables
        for table in &self.hash_tables {
            let candidates = table.get_candidates(query_embedding)?;
            candidate_ids.extend(candidates);
        }

        // Create document lookup
        let doc_map: HashMap<String, &Document> = documents
            .iter()
            .map(|doc| (doc.id.clone(), doc))
            .collect();

        // Compute similarities for candidates only
        let mut heap = BinaryHeap::new();
        
        for doc_id in candidate_ids {
            if let Some(&document) = doc_map.get(&doc_id) {
                if let Some(doc_embedding) = &document.embedding {
                    let similarity = cosine_similarity(
                        query_embedding.as_slice(),
                        doc_embedding.as_slice(),
                    )?;

                    if similarity >= params.similarity_threshold {
                        let result = SearchResult::new(
                            document.clone(),
                            similarity,
                            0, // Will be updated later
                        );

                        if heap.len() < params.k {
                            heap.push(result);
                        } else if let Some(min_result) = heap.peek() {
                            if similarity > min_result.similarity_score {
                                heap.pop();
                                heap.push(result);
                            }
                        }
                    }
                }
            }
        }

        let mut results: Vec<SearchResult> = heap.into_sorted_vec();
        results.reverse();

        // Update ranks
        for (rank, result) in results.iter_mut().enumerate() {
            result.rank = rank;
        }

        Ok(results)
    }

    fn name(&self) -> &'static str {
        "lsh_search"
    }
}

/// LSH Hash Table implementation
struct LSHHashTable {
    hash_functions: Vec<RandomProjection>,
    buckets: HashMap<Vec<i32>, Vec<String>>, // hash -> document_ids
}

impl LSHHashTable {
    fn new(num_hash_functions: usize, dimension: usize) -> Result<Self, OxidbError> {
        let mut hash_functions = Vec::with_capacity(num_hash_functions);
        
        for _ in 0..num_hash_functions {
            hash_functions.push(RandomProjection::new(dimension)?);
        }

        Ok(Self {
            hash_functions,
            buckets: HashMap::new(),
        })
    }

    fn insert(&mut self, doc_id: String, embedding: &Embedding) -> Result<(), OxidbError> {
        let hash = self.compute_hash(embedding)?;
        self.buckets.entry(hash).or_insert_with(Vec::new).push(doc_id);
        Ok(())
    }

    fn get_candidates(&self, query_embedding: &Embedding) -> Result<Vec<String>, OxidbError> {
        let hash = self.compute_hash(query_embedding)?;
        Ok(self.buckets.get(&hash).cloned().unwrap_or_default())
    }

    fn compute_hash(&self, embedding: &Embedding) -> Result<Vec<i32>, OxidbError> {
        let mut hash = Vec::with_capacity(self.hash_functions.len());
        
        for hash_fn in &self.hash_functions {
            hash.push(hash_fn.hash(embedding)?);
        }
        
        Ok(hash)
    }
}

/// Random projection hash function for LSH
struct RandomProjection {
    projection_vector: Vec<f32>,
}

impl RandomProjection {
    fn new(dimension: usize) -> Result<Self, OxidbError> {
        use rand::Rng;
        
        let mut rng = rand::thread_rng();
        let projection_vector: Vec<f32> = (0..dimension)
            .map(|_| rng.gen_range(-1.0..1.0))
            .collect();

        Ok(Self { projection_vector })
    }

    fn hash(&self, embedding: &Embedding) -> Result<i32, OxidbError> {
        let dot_product = dot_product(embedding.as_slice(), &self.projection_vector)?;
        Ok(if dot_product >= 0.0 { 1 } else { 0 })
    }
}

/// Vector search engine that manages different search strategies
/// Follows the Strategy Pattern and Dependency Inversion Principle
pub struct VectorSearchEngine {
    strategy: Box<dyn VectorSearchStrategy>,
    documents: Vec<Document>,
}

impl VectorSearchEngine {
    /// Create a new search engine with the specified strategy
    pub fn new(strategy: Box<dyn VectorSearchStrategy>) -> Self {
        Self {
            strategy,
            documents: Vec::new(),
        }
    }

    /// Add documents to the search index
    pub fn add_documents(&mut self, documents: Vec<Document>) -> Result<(), OxidbError> {
        // Validate all documents have embeddings
        for document in &documents {
            if document.embedding.is_none() {
                return Err(OxidbError::InvalidInput { field: "Unknown".to_string(), message: format!("Document '{ }' has no embedding", document.id),
                });
            }
        }

        self.documents.extend(documents);
        Ok(())
    }

    /// Search for similar documents
    pub fn search(
        &self,
        query_embedding: &Embedding,
        params: &SearchParams,
    ) -> Result<Vec<SearchResult>, OxidbError> {
        if self.documents.is_empty() {
            return Ok(Vec::new());
        }

        self.strategy.search(query_embedding, &self.documents, params)
    }

    /// Get the number of indexed documents
    pub fn document_count(&self) -> usize {
        self.documents.len()
    }

    /// Get strategy name
    pub fn strategy_name(&self) -> &'static str {
        self.strategy.name()
    }

    /// Clear all documents
    pub fn clear(&mut self) {
        self.documents.clear();
    }
}

/// Factory for creating search engines (Factory Pattern - GRASP Creator)
pub struct VectorSearchEngineFactory;

impl VectorSearchEngineFactory {
    /// Create a linear search engine
    pub fn create_linear_search() -> VectorSearchEngine {
        VectorSearchEngine::new(Box::new(LinearSearchStrategy))
    }

    /// Create an LSH-based search engine
    pub fn create_lsh_search(
        num_tables: usize,
        num_hash_functions: usize,
        dimension: usize,
    ) -> Result<VectorSearchEngine, OxidbError> {
        let strategy = LSHSearchStrategy::new(num_tables, num_hash_functions, dimension)?;
        Ok(VectorSearchEngine::new(Box::new(strategy)))
    }

    /// Create a search engine based on configuration
    pub fn create_from_config(
        strategy_name: &str,
        dimension: usize,
    ) -> Result<VectorSearchEngine, OxidbError> {
        match strategy_name.to_lowercase().as_str() {
            "linear" => Ok(Self::create_linear_search()),
            "lsh" => Self::create_lsh_search(4, 10, dimension), // Default LSH parameters
            _ => Err(OxidbError::InvalidInput { field: "Unknown".to_string(), message: format!("Unknown search strategy: { }", strategy_name),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::rag::document::{Document, Embedding};

    fn create_test_documents() -> Result<Vec<Document>, OxidbError> {
        let mut documents = Vec::new();
        
        // Create documents with different embeddings
        let doc1 = Document::new("doc1".to_string(), "First document".to_string())?
            .with_embedding(Embedding::new(vec![1.0, 0.0, 0.0])?)?;
        
        let doc2 = Document::new("doc2".to_string(), "Second document".to_string())?
            .with_embedding(Embedding::new(vec![0.0, 1.0, 0.0])?)?;
        
        let doc3 = Document::new("doc3".to_string(), "Third document".to_string())?
            .with_embedding(Embedding::new(vec![0.5, 0.5, 0.0])?)?;

        documents.push(doc1);
        documents.push(doc2);
        documents.push(doc3);

        Ok(documents)
    }

    #[test]
    fn test_linear_search() -> Result<(), OxidbError> {
        let documents = create_test_documents()?;
        let mut engine = VectorSearchEngineFactory::create_linear_search();
        engine.add_documents(documents)?;

        let query_embedding = Embedding::new(vec![1.0, 0.1, 0.0])?;
        let params = SearchParams::default();

        let results = engine.search(&query_embedding, &params)?;

        assert!(!results.is_empty());
        assert_eq!(results[0].document.id, "doc1"); // Should be most similar
        assert!(results[0].similarity_score > 0.9); // High similarity

        Ok(())
    }

    #[test]
    fn test_search_with_threshold() -> Result<(), OxidbError> {
        let documents = create_test_documents()?;
        let mut engine = VectorSearchEngineFactory::create_linear_search();
        engine.add_documents(documents)?;

        let query_embedding = Embedding::new(vec![1.0, 0.0, 0.0])?;
        let params = SearchParams {
            k: 10,
            similarity_threshold: 0.8, // High threshold
            include_metadata: true,
            max_distance: None,
        };

        let results = engine.search(&query_embedding, &params)?;

        // Only doc1 should meet the high similarity threshold
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].document.id, "doc1");

        Ok(())
    }

    #[test]
    fn test_lsh_search_creation() -> Result<(), OxidbError> {
        let engine = VectorSearchEngineFactory::create_lsh_search(4, 10, 3)?;
        assert_eq!(engine.strategy_name(), "lsh_search");
        Ok(())
    }

    #[test]
    fn test_factory_from_config() -> Result<(), OxidbError> {
        let linear_engine = VectorSearchEngineFactory::create_from_config("linear", 3)?;
        assert_eq!(linear_engine.strategy_name(), "linear_search");

        let lsh_engine = VectorSearchEngineFactory::create_from_config("lsh", 3)?;
        assert_eq!(lsh_engine.strategy_name(), "lsh_search");

        let invalid_result = VectorSearchEngineFactory::create_from_config("invalid", 3);
        assert!(invalid_result.is_err());

        Ok(())
    }

    #[test]
    fn test_search_result_ordering() {
        let doc1 = Document::new("doc1".to_string(), "content1".to_string()).unwrap();
        let doc2 = Document::new("doc2".to_string(), "content2".to_string()).unwrap();
        
        let result1 = SearchResult::new(doc1, 0.9, 0);
        let result2 = SearchResult::new(doc2, 0.8, 1);

        // Higher similarity scores should be greater than lower scores
        assert!(result1 > result2);
    }
}