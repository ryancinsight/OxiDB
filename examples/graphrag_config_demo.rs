// examples/graphrag_config_demo.rs
//! Demonstrates configurable embedding dimensions in GraphRAG

use oxidb::core::rag::{
    Document, GraphRAGConfig, GraphRAGEngineBuilder, GraphRAGEngineImpl,
    SemanticEmbedder, TfIdfEmbedder,
};
use oxidb::core::rag::retriever::InMemoryRetriever;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== GraphRAG Configurable Embedding Dimension Demo ===\n");

    // Example 1: Using default configuration (384 dimensions)
    println!("1. Default Configuration:");
    let retriever1 = Box::new(InMemoryRetriever::new(vec![]));
    let engine1 = GraphRAGEngineImpl::new(retriever1);
    println!("   Default embedding dimension: 384");

    // Example 2: Using custom configuration with different dimension
    println!("\n2. Custom Configuration:");
    let config = GraphRAGConfig {
        default_embedding_dimension: 512,
        confidence_threshold: 0.7,
    };
    let retriever2 = Box::new(InMemoryRetriever::new(vec![]));
    let engine2 = GraphRAGEngineImpl::with_config(retriever2, config);
    println!("   Custom embedding dimension: 512");
    println!("   Custom confidence threshold: 0.7");

    // Example 3: Using builder pattern with specific dimension
    println!("\n3. Builder Pattern with 768 dimensions:");
    let retriever3 = Box::new(InMemoryRetriever::new(vec![]));
    let engine3 = GraphRAGEngineBuilder::new()
        .with_document_retriever(retriever3)
        .with_embedding_dimension(768)
        .with_confidence_threshold(0.6)
        .build()?;
    println!("   Embedding dimension: 768");
    println!("   Confidence threshold: 0.6");

    // Example 4: Using custom embedding model with its own dimension
    println!("\n4. Custom Embedding Model:");
    let documents = vec![
        Document::new("doc1".to_string(), "Sample document for TF-IDF".to_string()),
        Document::new("doc2".to_string(), "Another document with different content".to_string()),
    ];
    let tfidf_embedder = Box::new(TfIdfEmbedder::new(&documents));
    let tfidf_dimension = tfidf_embedder.embedding_dimension();
    println!("   TF-IDF embedding dimension: {}", tfidf_dimension);
    
    let retriever4 = Box::new(InMemoryRetriever::new(vec![]));
    let engine4 = GraphRAGEngineBuilder::new()
        .with_document_retriever(retriever4)
        .with_embedding_model(tfidf_embedder)
        .build()?;
    println!("   Engine configured with TF-IDF embedder");

    // Example 5: Using builder with custom SemanticEmbedder
    println!("\n5. Custom SemanticEmbedder with 1024 dimensions:");
    let semantic_embedder = Box::new(SemanticEmbedder::new(1024));
    let retriever5 = Box::new(InMemoryRetriever::new(vec![]));
    let engine5 = GraphRAGEngineBuilder::new()
        .with_document_retriever(retriever5)
        .with_embedding_model(semantic_embedder)
        .build()?;
    println!("   Semantic embedding dimension: 1024");

    println!("\n=== Benefits of Configurable Dimensions ===");
    println!("- Match dimensions to your specific embedding model");
    println!("- Avoid hardcoded values that can cause inconsistencies");
    println!("- Support different models with different dimensions");
    println!("- Easy to experiment with different embedding sizes");
    println!("- Maintain consistency between embedding model and storage");

    Ok(())
}