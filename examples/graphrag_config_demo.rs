// examples/graphrag_config_demo.rs
//! Demonstrates configurable embedding dimensions in GraphRAG

use oxidb::core::rag::graphrag::{GraphRAGConfig};
use oxidb::core::rag::embedder::{SemanticEmbedder};
use oxidb::core::rag::graphrag::builder::GraphRAGEngineBuilder;
use oxidb::core::rag::retriever::InMemoryRetriever;
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== GraphRAG Configurable Embedding Dimension Demo ===\n");

    // Example 1: Using default configuration
    println!("1. Default Configuration:");
    let _default_config = GraphRAGConfig::default();
    println!("   Default similarity threshold: {:.2}", _default_config.default_similarity_threshold);

    // Example 2: Custom configuration via builder with SemanticEmbedder(512)
    println!("\n2. Custom Configuration (512-dim SemanticEmbedder):");
    let engine2 = GraphRAGEngineBuilder::new()
        .with_graph_store(oxidb::core::graph::InMemoryGraphStore::default())
        .with_embedder(Arc::new(SemanticEmbedder::new(512)))
        .build()?;
    let _ = engine2; // suppress unused warning
    println!("   Configured engine with 512-dim SemanticEmbedder");

    // Example 3: Custom SemanticEmbedder(768)
    println!("\n3. Builder Pattern with 768 dimensions:");
    let engine3 = GraphRAGEngineBuilder::new()
        .with_graph_store(oxidb::core::graph::InMemoryGraphStore::default())
        .with_embedder(Arc::new(SemanticEmbedder::new(768)))
        .build()?;
    let _ = engine3;
    println!("   Configured engine with 768-dim SemanticEmbedder");

    println!("\n=== Benefits of Configurable Dimensions ===");
    println!("- Match dimensions to your specific embedding model");
    println!("- Avoid hardcoded values that can cause inconsistencies");
    println!("- Support different models with different dimensions");
    println!("- Easy to experiment with different embedding sizes");
    println!("- Maintain consistency between embedding model and storage");

    Ok(())
}