//! Builder pattern for GraphRAG engine construction
//!
//! Provides a fluent API for configuring and building GraphRAG engines.

use super::engine::GraphRAGEngineImpl;
use super::types::GraphRAGConfig;
use crate::core::common::OxidbError;
use crate::core::graph::GraphStore;
use std::sync::Arc;

/// Builder for GraphRAG engines
pub struct GraphRAGEngineBuilder {
    graph_store: Option<Arc<dyn GraphStore>>,
    embedder: Option<Arc<dyn crate::core::rag::embedder::EmbeddingModel + Send + Sync>>,
    config: GraphRAGConfig,
}

impl Default for GraphRAGEngineBuilder {
    fn default() -> Self {
        Self {
            graph_store: None,
            embedder: None,
            config: GraphRAGConfig::default(),
        }
    }
}

impl GraphRAGEngineBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the graph store
    pub fn with_graph_store(
        mut self,
        store: Arc<dyn GraphStore>,
    ) -> Self {
        self.graph_store = Some(store);
        self
    }

    /// Set the embedding model
    pub fn with_embedder(
        mut self,
        embedder: Arc<dyn crate::core::rag::embedder::EmbeddingModel + Send + Sync>,
    ) -> Self {
        self.embedder = Some(embedder);
        self
    }

    /// Set the similarity threshold
    pub fn with_similarity_threshold(mut self, threshold: f64) -> Self {
        self.config.default_similarity_threshold = threshold;
        self
    }

    /// Set the maximum traversal depth
    pub fn with_max_depth(mut self, depth: usize) -> Self {
        self.config.max_traversal_depth = depth;
        self
    }

    /// Enable or disable caching
    pub fn with_caching(mut self, enable: bool) -> Self {
        self.config.enable_caching = enable;
        self
    }

    /// Build the GraphRAG engine
    pub fn build(self) -> Result<GraphRAGEngineImpl, OxidbError> {
        let graph_store = self.graph_store.ok_or_else(|| OxidbError::Configuration(
            "Graph store not configured".to_string()
        ))?;

        let embedder = self.embedder.ok_or_else(|| OxidbError::Configuration(
            "Embedding model not configured".to_string()
        ))?;

        Ok(GraphRAGEngineImpl::new(graph_store, embedder, self.config))
    }
}