//! Factory methods for creating GraphRAG engines with common configurations

use std::sync::{Arc, Mutex};
use crate::core::rag::embedder::SemanticEmbedder;
use super::{GraphRAGEngineImpl, GraphRAGConfig};
use crate::core::graph::GraphStore;

/// Factory for creating GraphRAG engines
pub struct GraphRAGFactory;

impl GraphRAGFactory {
    /// Create a default GraphRAG engine
    pub fn create_default<T: GraphStore + 'static>(graph_store: T) -> GraphRAGEngineImpl {
        let config = GraphRAGConfig {
            default_similarity_threshold: 0.7,
            max_traversal_depth: 3,
            enable_caching: true,
        };
        
        let embedder = Arc::new(SemanticEmbedder::new(384)); // Default dimension
        
        GraphRAGEngineImpl::new(Arc::new(Mutex::new(graph_store)), embedder, config)
    }
    
    /// Create a high-precision GraphRAG engine
    pub fn create_high_precision<T: GraphStore + 'static>(graph_store: T) -> GraphRAGEngineImpl {
        let config = GraphRAGConfig {
            default_similarity_threshold: 0.85,
            max_traversal_depth: 5,
            enable_caching: true,
        };
        
        let embedder = Arc::new(SemanticEmbedder::new(768)); // Higher dimension for precision
        
        GraphRAGEngineImpl::new(Arc::new(Mutex::new(graph_store)), embedder, config)
    }
    
    /// Create a fast GraphRAG engine for real-time queries
    pub fn create_fast<T: GraphStore + 'static>(graph_store: T) -> GraphRAGEngineImpl {
        let config = GraphRAGConfig {
            default_similarity_threshold: 0.6,
            max_traversal_depth: 2,
            enable_caching: false, // Disable caching for real-time
        };
        
        let embedder = Arc::new(SemanticEmbedder::new(128)); // Lower dimension for speed
        
        GraphRAGEngineImpl::new(Arc::new(Mutex::new(graph_store)), embedder, config)
    }
}