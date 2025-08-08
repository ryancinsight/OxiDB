use oxidb::core::graph::InMemoryGraphStore;
use oxidb::core::rag::document::Document;
use oxidb::core::rag::embedder::SemanticEmbedder;
use oxidb::core::rag::graphrag::{GraphRAGConfig, GraphRAGContext, GraphRAGEngine, GraphRAGEngineBuilder};
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Oxidb GraphRAG Demo (Modern API)");
    println!("===============================\n");

    // 1) Build a GraphRAG engine (in-memory graph store + semantic embedder)
    let engine_config = GraphRAGConfig::default();
    let embedder = Arc::new(SemanticEmbedder::new(128));

    let mut engine = GraphRAGEngineBuilder::new()
        .with_graph_store(InMemoryGraphStore::new())
        .with_embedder(embedder)
        .build()?;

    // 2) Create and add a few documents as knowledge nodes
    let mut docs = Vec::new();
    let mut meta = HashMap::new();
    meta.insert("category".to_string(), oxidb::Value::Text("tech".to_string()));

    docs.push(Document::new(
        "doc-1".to_string(),
        "TechCorp releases a new AI platform for analytics and NLP".to_string(),
    ).with_metadata(meta.clone()));

    docs.push(Document::new(
        "doc-2".to_string(),
        "The SmartAnalytics product by TechCorp helps enterprises gain insights".to_string(),
    ).with_metadata(meta.clone()));

    docs.push(Document::new(
        "doc-3".to_string(),
        "CEO Jane Smith announces record growth driven by AI strategy".to_string(),
    ).with_metadata(meta));

    let ids: Vec<u64> = {
        let mut added = Vec::new();
        for d in &docs {
            let id = engine.add_document(d).await?;
            added.push(id);
        }
        added
    };

    // 3) Add a simple relationship between two nodes
    if ids.len() >= 2 {
        engine
            .add_relationship(ids[0], ids[1], "related_to", 0.9)
            .await?;
    }

    // 4) Query the knowledge graph
    let ctx = GraphRAGContext {
        query: "analytics AI platform".to_string(),
        max_results: 5,
        similarity_threshold: engine_config.default_similarity_threshold,
        max_depth: engine_config.max_traversal_depth,
        parameters: HashMap::new(),
    };

    let result = engine.query(&ctx).await?;

    println!("Top documents: {}\n", result.documents.len());
    for (i, node) in result.documents.iter().enumerate() {
        println!("{}. [{}] {}", i + 1, node.node_type, node.content);
    }

    if !result.scores.is_empty() {
        println!("\nScores: {:?}", result.scores);
    }

    println!("\n✅ GraphRAG demo completed successfully!");
    Ok(())
}
