// src/core/rag/hybrid.rs
//! Hybrid RAG implementation that combines vector search and GraphRAG

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::core::common::OxidbError;
use crate::core::common::types::Value;
use crate::core::rag::{
    document::{Document, Embedding},
    embedder::EmbeddingModel,
    graphrag::{GraphRAGContext, GraphRAGEngine, GraphRAGResult, KnowledgeNode},
    retriever::{Retriever, SimilarityMetric},
};

/// Configuration for hybrid RAG
#[derive(Debug, Clone)]
pub struct HybridRAGConfig {
    /// Weight for vector similarity score (0.0 to 1.0)
    pub vector_weight: f32,
    /// Weight for graph-based score (0.0 to 1.0)
    pub graph_weight: f32,
    /// Maximum number of vector results to consider
    pub max_vector_results: usize,
    /// Maximum graph traversal depth
    pub max_graph_depth: usize,
    /// Minimum similarity threshold for vector search
    pub min_similarity: f32,
    /// Whether to use graph relationships to expand vector results
    pub enable_graph_expansion: bool,
    /// Whether to use vector similarity to filter graph results
    pub enable_vector_filtering: bool,
}

impl Default for HybridRAGConfig {
    fn default() -> Self {
        Self {
            vector_weight: 0.5,
            graph_weight: 0.5,
            max_vector_results: 20,
            max_graph_depth: 3,
            min_similarity: 0.5,
            enable_graph_expansion: true,
            enable_vector_filtering: true,
        }
    }
}

/// Result from hybrid RAG query
#[derive(Debug, Clone)]
pub struct HybridRAGResult {
    /// The retrieved document
    pub document: Document,
    /// Combined score from vector and graph
    pub hybrid_score: f32,
    /// Vector similarity score
    pub vector_score: Option<f32>,
    /// Graph-based relevance score
    pub graph_score: Option<f32>,
    /// Path from query to this document in the graph
    pub graph_path: Option<Vec<String>>,
    /// Related entities from the graph
    pub related_entities: Vec<String>,
}

/// Hybrid RAG engine combining vector and graph approaches
pub struct HybridRAGEngine<E: EmbeddingModel + Send + Sync> {
    /// Vector retriever for similarity search
    vector_retriever: Arc<dyn Retriever>,
    /// Graph RAG engine for relationship-based retrieval
    graph_engine: Arc<dyn GraphRAGEngine>,
    /// Embedding model for query processing
    embedding_model: Arc<E>,
    /// Configuration
    config: HybridRAGConfig,
}

impl<E: EmbeddingModel + Send + Sync> HybridRAGEngine<E> {
    /// Create a new hybrid RAG engine
    pub fn new(
        vector_retriever: Arc<dyn Retriever>,
        graph_engine: Arc<dyn GraphRAGEngine>,
        embedding_model: Arc<E>,
        config: HybridRAGConfig,
    ) -> Self {
        Self {
            vector_retriever,
            graph_engine,
            embedding_model,
            config,
        }
    }

    /// Query using hybrid approach
    pub async fn query(&self, query: &str, context: Option<&GraphRAGContext>) -> Result<Vec<HybridRAGResult>, OxidbError> {
        // Get query embedding
        let query_embedding = self.embedding_model.as_ref().embed(query).await
            .map_err(|e| OxidbError::Internal(format!("Failed to embed query: {}", e)))?;

        // Perform vector search
        let vector_results = self.vector_retriever
            .retrieve(&query_embedding, self.config.max_vector_results, SimilarityMetric::Cosine)
            .await?;

        // Perform graph-based retrieval
        let graph_context = GraphRAGContext {
            query: query.to_string(),
            max_results: self.config.max_graph_depth, // Changed from max_graph_results to max_graph_depth
            similarity_threshold: self.config.min_similarity as f64, // Changed from similarity_threshold to min_similarity
            max_depth: self.config.max_graph_depth, // Changed from max_depth to max_graph_depth
            parameters: context.map(|c| c.parameters.clone()).unwrap_or_default(),
        };
        let graph_results = self.graph_engine
            .query(&graph_context)
            .await?;

        // Combine results
        self.combine_results(vector_results, graph_results, query_embedding.as_slice()).await
    }

    /// Query with specific entities as starting points
    pub async fn query_with_entities(
        &self,
        query: &str,
        entity_ids: &[String],
        _context: Option<&GraphRAGContext>,
    ) -> Result<Vec<HybridRAGResult>, OxidbError> {
        // Get query embedding
        let query_embedding = self.embedding_model.as_ref().embed(query).await
            .map_err(|e| OxidbError::Internal(format!("Failed to embed query: {}", e)))?;

        // Perform vector search
        let mut vector_results = self.vector_retriever
            .retrieve(&query_embedding, self.config.max_vector_results, SimilarityMetric::Cosine)
            .await?;

        // Filter vector results by entities if specified
        if !entity_ids.is_empty() {
            let entity_set: HashSet<_> = entity_ids.iter().cloned().collect();
            vector_results.retain(|doc| entity_set.contains(&doc.id));
        }

        // Perform graph traversal from specified entities
        let graph_context = GraphRAGContext {
            query: query.to_string(),
            max_results: self.config.max_graph_depth,
            similarity_threshold: self.config.min_similarity as f64,
            max_depth: self.config.max_graph_depth,
            parameters: {
                let mut params = HashMap::new();
                params.insert(
                    "entity_ids".to_string(),
                    Value::Text(entity_ids.join(","))
                );
                params
            },
        };
        
        let graph_result = self.graph_engine
            .query(&graph_context)
            .await?;
        self.combine_results(vector_results, graph_result, query_embedding.as_slice()).await
    }

    /// Combine vector and graph results
    async fn combine_results(
        &self,
        vector_results: Vec<Document>,
        graph_result: GraphRAGResult,
        _query_embedding: &[f32],
    ) -> Result<Vec<HybridRAGResult>, OxidbError> {
        let mut hybrid_results: HashMap<String, HybridRAGResult> = HashMap::new();

        // Process vector results
        for (idx, doc) in vector_results.into_iter().enumerate() {
            let vector_score = 1.0 / (idx as f32 + 1.0); // Simple ranking score
            hybrid_results.insert(doc.id.clone(), HybridRAGResult {
                document: doc,
                hybrid_score: vector_score * self.config.vector_weight,
                vector_score: Some(vector_score),
                graph_score: None,
                graph_path: None,
                related_entities: Vec::new(),
            });
        }

        // Process graph results - convert KnowledgeNode to Document
        for (idx, node) in graph_result.documents.iter().enumerate() {
            let doc = Document {
                id: node.id.to_string(),
                content: node.content.clone(),
                metadata: Some(node.metadata.clone()),
                embedding: node.embedding.as_ref().map(|e| Embedding {
                    vector: e.vector.clone(),
                }),
            };
            
            let graph_score = if idx < graph_result.scores.len() {
                graph_result.scores[idx] as f32
            } else {
                1.0 / (idx as f32 + 1.0)
            };

            if let Some(existing) = hybrid_results.get_mut(&doc.id) {
                // Document found in both vector and graph results
                existing.graph_score = Some(graph_score);
                existing.graph_path = graph_result.reasoning_paths.first().map(|p| {
                    p.nodes.iter().map(|n| n.to_string()).collect()
                });
                existing.related_entities = graph_result.documents.iter()
                    .map(|n| n.content.clone())
                    .collect();
                existing.hybrid_score = self.calculate_hybrid_score(
                    existing.vector_score,
                    Some(graph_score),
                );
            } else {
                // Document only in graph results
                hybrid_results.insert(doc.id.clone(), HybridRAGResult {
                    document: doc,
                    hybrid_score: graph_score * self.config.graph_weight,
                    vector_score: None,
                    graph_score: Some(graph_score),
                    graph_path: graph_result.reasoning_paths.first().map(|p| {
                        p.nodes.iter().map(|n| n.to_string()).collect()
                    }),
                    related_entities: graph_result.documents.iter()
                        .map(|n| n.content.clone())
                        .collect(),
                });
            }
        }

        // Sort results by hybrid score
        let mut results: Vec<_> = hybrid_results.into_values().collect();
        results.sort_by(|a, b| b.hybrid_score.partial_cmp(&a.hybrid_score).unwrap_or(std::cmp::Ordering::Equal));

        // Limit results
        results.truncate(self.config.max_vector_results);

        Ok(results)
    }

    /// Calculate similarity between two embeddings
    #[allow(dead_code)]
    fn calculate_similarity(&self, embedding1: &Embedding, embedding2: &Embedding) -> f32 {
        let vec1 = embedding1.as_slice();
        let vec2 = embedding2.as_slice();

        if vec1.len() != vec2.len() {
            return 0.0;
        }

        // Cosine similarity
        let dot_product: f32 = vec1.iter().zip(vec2.iter()).map(|(a, b)| a * b).sum();
        let norm1: f32 = vec1.iter().map(|x| x * x).sum::<f32>().sqrt();
        let norm2: f32 = vec2.iter().map(|x| x * x).sum::<f32>().sqrt();

        if norm1 == 0.0 || norm2 == 0.0 {
            0.0
        } else {
            dot_product / (norm1 * norm2)
        }
    }

    /// Calculate graph-based score
    #[allow(dead_code)]
    fn calculate_graph_score(&self, result: &GraphRAGResult) -> f32 {
        let path_penalty = if let Some(path) = result.reasoning_paths.first() {
            // Shorter paths get higher scores
            1.0 / (1.0 + path.nodes.len() as f32 * 0.1)
        } else {
            0.5
        };

        let entity_boost = 1.0 + (result.documents.len() as f32 * 0.05).min(0.5);

        // Use the average of all scores if available
        let avg_score = if !result.scores.is_empty() {
            (result.scores.iter().sum::<f64>() / result.scores.len() as f64) as f32
        } else {
            0.5
        };

        avg_score * path_penalty * entity_boost
    }

    /// Calculate combined hybrid score
    fn calculate_hybrid_score(&self, vector_score: Option<f32>, graph_score: Option<f32>) -> f32 {
        match (vector_score, graph_score) {
            (Some(v), Some(g)) => v * self.config.vector_weight + g * self.config.graph_weight,
            (Some(v), None) => v * self.config.vector_weight,
            (None, Some(g)) => g * self.config.graph_weight,
            (None, None) => 0.0,
        }
    }

    /// Convert KnowledgeNode to Document
    #[allow(dead_code)]
    async fn entity_to_document(&self, node: KnowledgeNode) -> Option<Document> {
        let content = format!("{}: {}", node.node_type, node.content);
        let mut metadata = node.metadata.clone();
        metadata.insert("node_type".to_string(), Value::Text(node.node_type));
        metadata.insert("node_id".to_string(), Value::Integer(node.id as i64));

        Some(Document {
            id: node.id.to_string(),
            content,
            metadata: Some(metadata),
            embedding: node.embedding.as_ref().map(|e| Embedding {
                vector: e.vector.clone(),
            }),
        })
    }

    /// Update configuration
    pub fn set_config(&mut self, config: HybridRAGConfig) {
        self.config = config;
    }

    /// Get current configuration
    pub fn config(&self) -> &HybridRAGConfig {
        &self.config
    }
}

/// Builder for HybridRAGEngine
pub struct HybridRAGEngineBuilder<E: EmbeddingModel> {
    vector_retriever: Option<Arc<dyn Retriever>>,
    graph_engine: Option<Arc<dyn GraphRAGEngine>>,
    embedding_model: Option<Arc<E>>,
    config: HybridRAGConfig,
}

impl<E: EmbeddingModel> Default for HybridRAGEngineBuilder<E> {
    fn default() -> Self {
        Self {
            vector_retriever: None,
            graph_engine: None,
            embedding_model: None,
            config: HybridRAGConfig::default(),
        }
    }
}

impl<E: EmbeddingModel> HybridRAGEngineBuilder<E> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_vector_retriever(mut self, retriever: Arc<dyn Retriever>) -> Self {
        self.vector_retriever = Some(retriever);
        self
    }

    pub fn with_graph_engine(mut self, engine: Arc<dyn GraphRAGEngine>) -> Self {
        self.graph_engine = Some(engine);
        self
    }

    pub fn with_embedding_model(mut self, model: Arc<E>) -> Self {
        self.embedding_model = Some(model);
        self
    }

    pub fn with_config(mut self, config: HybridRAGConfig) -> Self {
        self.config = config;
        self
    }

    pub fn with_vector_weight(mut self, weight: f32) -> Self {
        self.config.vector_weight = weight.clamp(0.0, 1.0);
        self.config.graph_weight = (1.0 - weight).clamp(0.0, 1.0);
        self
    }

    pub fn build(self) -> Result<HybridRAGEngine<E>, OxidbError> {
        let vector_retriever = self.vector_retriever
            .ok_or_else(|| OxidbError::Configuration("Vector retriever not set".to_string()))?;
        let graph_engine = self.graph_engine
            .ok_or_else(|| OxidbError::Configuration("Graph engine not set".to_string()))?;
        let embedding_model = self.embedding_model
            .ok_or_else(|| OxidbError::Configuration("Embedding model not set".to_string()))?;

        Ok(HybridRAGEngine::new(
            vector_retriever,
            graph_engine,
            embedding_model,
            self.config,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::rag::embedder::SemanticEmbedder;
    use crate::core::rag::graphrag::GraphRAGEngineImpl;
    use crate::core::rag::retriever::InMemoryRetriever;
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    async fn test_hybrid_config() {
        let config = HybridRAGConfig::default();
        assert_eq!(config.vector_weight, 0.5);
        assert_eq!(config.graph_weight, 0.5);
        assert_eq!(config.max_vector_results, 20);
    }

    #[tokio::test]
    async fn test_hybrid_score_calculation() {
        let vector_retriever = Arc::new(InMemoryRetriever::new(Vec::new()));
        let graph_store = crate::core::graph::InMemoryGraphStore::new();
        let embedder = Arc::new(SemanticEmbedder::new(128));
        let config = crate::core::rag::graphrag::GraphRAGConfig::default();
        let graph_engine = Arc::new(GraphRAGEngineImpl::new(Arc::new(Mutex::new(Box::new(graph_store))), embedder.clone(), config));
        
        let engine = HybridRAGEngine::new(
            vector_retriever,
            graph_engine,
            embedder,
            HybridRAGConfig::default(),
        );

        let score = engine.calculate_hybrid_score(Some(0.8), Some(0.6));
        assert_eq!(score, 0.8 * 0.5 + 0.6 * 0.5); // 0.4 + 0.3 = 0.7
    }

    #[tokio::test]
    async fn test_builder() {
        let vector_retriever = Arc::new(InMemoryRetriever::new(Vec::new()));
        let graph_store = crate::core::graph::InMemoryGraphStore::new();
        let embedder = Arc::new(SemanticEmbedder::new(128));
        let config = crate::core::rag::graphrag::GraphRAGConfig::default();
        let graph_engine = Arc::new(GraphRAGEngineImpl::new(Arc::new(Mutex::new(Box::new(graph_store))), embedder.clone(), config));

        let engine = HybridRAGEngineBuilder::new()
            .with_vector_retriever(vector_retriever)
            .with_graph_engine(graph_engine)
            .with_embedding_model(embedder)
            .with_vector_weight(0.7)
            .build()
            .unwrap();

        assert_eq!(engine.config().vector_weight, 0.7);
        assert_eq!(engine.config().graph_weight, 0.3);
    }
}