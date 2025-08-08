#![cfg(feature = "rag_examples")]
// examples/graphrag_config_demo.rs
//! Demonstrates configurable GraphRAG engine setup

use oxidb::core::rag::{Document};
use oxidb::core::rag::graphrag::{GraphRAGConfig, GraphRAGEngineImpl};
use oxidb::core::rag::embedder::{EmbeddingModel, TfIdfEmbedder, SemanticEmbedder};
use oxidb::core::graph::{GraphFactory, GraphStore};
use std::sync::{Arc, Mutex};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== GraphRAG Configuration Demo ===\n");

    // Create an in-memory graph store
    let graph_store: Arc<Mutex<Box<dyn GraphStore>>> = Arc::new(Mutex::new(GraphFactory::create_memory_graph()?));

    // Example 1: Using default configuration
    println!("1. Default Configuration:");
    let default_embedder: Arc<dyn EmbeddingModel + Send + Sync> = Arc::new(SemanticEmbedder::new(384));
    let _engine1 = GraphRAGEngineImpl::new(graph_store.clone(), default_embedder.clone(), GraphRAGConfig::default());
    println!("   Default similarity threshold: {}", GraphRAGConfig::default().default_similarity_threshold);

    // Example 2: Custom configuration
    println!("\n2. Custom Configuration:");
    let config = GraphRAGConfig { default_similarity_threshold: 0.8, max_traversal_depth: 4, enable_caching: true };
    let _engine2 = GraphRAGEngineImpl::new(graph_store.clone(), default_embedder.clone(), config.clone());
    println!("   Custom similarity threshold: {}", config.default_similarity_threshold);
    println!("   Max traversal depth: {}", config.max_traversal_depth);

    // Example 3: Using TF-IDF embedder
    println!("\n3. Using TF-IDF Embedder:");
    let documents = vec![
        Document::new("doc1".to_string(), "Sample document for TF-IDF".to_string()),
        Document::new("doc2".to_string(), "Another document with different content".to_string()),
    ];
    let tfidf_embedder: Arc<dyn EmbeddingModel + Send + Sync> = Arc::new(TfIdfEmbedder::new(&documents));
    let _engine3 = GraphRAGEngineImpl::new(graph_store.clone(), tfidf_embedder, GraphRAGConfig::default());
    println!("   Engine configured with TF-IDF embedder");

    // Example 4: Using SemanticEmbedder with 1024 dimensions
    println!("\n4. SemanticEmbedder with 1024 dimensions:");
    let semantic_embedder: Arc<dyn EmbeddingModel + Send + Sync> = Arc::new(SemanticEmbedder::new(1024));
    let _engine4 = GraphRAGEngineImpl::new(graph_store, semantic_embedder, GraphRAGConfig::default());
    println!("   SemanticEmbedder configured with 1024 dims");

    println!("\n=== Notes ===");
    println!("- GraphRAGEngineImpl::new accepts a graph store, an embedder, and a config");
    println!("- Configure similarity threshold and traversal depth via GraphRAGConfig");

    Ok(())
}