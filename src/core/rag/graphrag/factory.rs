//! Factory for creating GraphRAG engines
//!
//! Provides convenient methods for creating pre-configured engines.

use super::builder::GraphRAGEngineBuilder;
use super::engine::GraphRAGEngineImpl;
use crate::core::common::OxidbError;
use crate::core::graph::InMemoryGraphStore;
use crate::core::rag::embedder::SemanticEmbedder;
use std::sync::Arc;

/// Factory for creating GraphRAG engines
pub struct GraphRAGFactory;

impl GraphRAGFactory {
    /// Create a default in-memory GraphRAG engine
    pub fn create_default() -> Result<GraphRAGEngineImpl, OxidbError> {
        let graph_store = Arc::new(InMemoryGraphStore::new());
        let embedder = Arc::new(SemanticEmbedder::new());

        GraphRAGEngineBuilder::new()
            .with_graph_store(graph_store)
            .with_embedder(embedder)
            .build()
    }

    /// Create a GraphRAG engine with custom configuration
    pub fn create_with_config(
        similarity_threshold: f64,
        max_depth: usize,
    ) -> Result<GraphRAGEngineImpl, OxidbError> {
        let graph_store = Arc::new(InMemoryGraphStore::new());
        let embedder = Arc::new(SemanticEmbedder::new());

        GraphRAGEngineBuilder::new()
            .with_graph_store(graph_store)
            .with_embedder(embedder)
            .with_similarity_threshold(similarity_threshold)
            .with_max_depth(max_depth)
            .build()
    }
}