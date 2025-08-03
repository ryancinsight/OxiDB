// examples/hybrid_rag_demo.rs
//! Demonstrates the hybrid RAG system that combines vector search and GraphRAG

use oxidb::{Connection, OxidbError, QueryResult};
use oxidb::core::rag::{
    Document, Embedding,
    HybridRAGConfig, HybridRAGEngineBuilder,
    GraphRAGEngineBuilder, KnowledgeNode, KnowledgeEdge,
    SemanticEmbedder,
};
use oxidb::core::rag::retriever::InMemoryRetriever;
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Oxidb Hybrid RAG Demo ===\n");

    // Initialize database
    let mut conn = Connection::open_in_memory()?;
    
    // Create tables for storing documents and knowledge graph
    conn.execute("CREATE TABLE documents (id TEXT PRIMARY KEY, content TEXT, embedding VECTOR(384))")?;
    conn.execute("CREATE TABLE entities (id TEXT PRIMARY KEY, entity_type TEXT, name TEXT, description TEXT)")?;
    conn.execute("CREATE TABLE relationships (id TEXT PRIMARY KEY, from_entity TEXT, to_entity TEXT, relationship_type TEXT)")?;

    // Sample documents about a tech company
    let documents = vec![
        Document::new(
            "doc1".to_string(),
            "TechCorp was founded in 2010 by Jane Smith and John Doe. The company specializes in AI and machine learning solutions.".to_string()
        ),
        Document::new(
            "doc2".to_string(),
            "Jane Smith, CEO of TechCorp, has over 20 years of experience in artificial intelligence. She previously worked at BigTech Inc.".to_string()
        ),
        Document::new(
            "doc3".to_string(),
            "TechCorp's flagship product is SmartAnalytics, an AI-powered data analysis platform used by Fortune 500 companies.".to_string()
        ),
        Document::new(
            "doc4".to_string(),
            "In 2023, TechCorp acquired DataViz Solutions for $50 million to expand their visualization capabilities.".to_string()
        ),
        Document::new(
            "doc5".to_string(),
            "John Doe, CTO of TechCorp, leads the engineering team and oversees product development. He holds 15 patents in machine learning.".to_string()
        ),
    ];

    // Initialize embedding model
    let embedding_model = Arc::new(SemanticEmbedder::new());

    // Embed documents
    let mut embedded_docs = Vec::new();
    for doc in documents {
        let embedding = embedding_model.embed(&doc.content).await?;
        let embedded_doc = doc.with_embedding(embedding);
        embedded_docs.push(embedded_doc);
    }

    // Store documents in vector retriever
    let vector_retriever = Arc::new(InMemoryRetriever::new(embedded_docs.clone()));

    // Create knowledge graph
    let graph_retriever = Arc::new(InMemoryRetriever::new(embedded_docs));
    let graph_engine = Arc::new(
        GraphRAGEngineBuilder::new()
            .with_retriever(Box::new(graph_retriever.clone()))
            .build()
    );

    // Add entities to knowledge graph
    let entities = vec![
        KnowledgeNode {
            id: "entity_techcorp".to_string(),
            entity_type: "Company".to_string(),
            name: "TechCorp".to_string(),
            description: Some("AI and machine learning company founded in 2010".to_string()),
            embedding: None,
            properties: HashMap::new(),
            confidence_score: 0.9,
        },
        KnowledgeNode {
            id: "entity_jane".to_string(),
            entity_type: "Person".to_string(),
            name: "Jane Smith".to_string(),
            description: Some("CEO of TechCorp with 20+ years in AI".to_string()),
            embedding: None,
            properties: HashMap::new(),
            confidence_score: 0.9,
        },
        KnowledgeNode {
            id: "entity_john".to_string(),
            entity_type: "Person".to_string(),
            name: "John Doe".to_string(),
            description: Some("CTO of TechCorp with 15 patents".to_string()),
            embedding: None,
            properties: HashMap::new(),
            confidence_score: 0.9,
        },
        KnowledgeNode {
            id: "entity_smartanalytics".to_string(),
            entity_type: "Product".to_string(),
            name: "SmartAnalytics".to_string(),
            description: Some("AI-powered data analysis platform".to_string()),
            embedding: None,
            properties: HashMap::new(),
            confidence_score: 0.85,
        },
        KnowledgeNode {
            id: "entity_dataviz".to_string(),
            entity_type: "Company".to_string(),
            name: "DataViz Solutions".to_string(),
            description: Some("Visualization company acquired by TechCorp".to_string()),
            embedding: None,
            properties: HashMap::new(),
            confidence_score: 0.8,
        },
    ];

    // Add entities (would normally use mutable reference)
    println!("Building knowledge graph...");
    // Note: In a real implementation, we'd need to make graph_engine mutable
    // For this demo, we'll show the concept

    // Create relationships
    let relationships = vec![
        ("entity_jane", "entity_techcorp", "CEO_OF"),
        ("entity_john", "entity_techcorp", "CTO_OF"),
        ("entity_jane", "entity_techcorp", "FOUNDED"),
        ("entity_john", "entity_techcorp", "FOUNDED"),
        ("entity_techcorp", "entity_smartanalytics", "DEVELOPS"),
        ("entity_techcorp", "entity_dataviz", "ACQUIRED"),
    ];

    // Build hybrid RAG engine
    println!("\nInitializing Hybrid RAG Engine...");
    let hybrid_config = HybridRAGConfig {
        vector_weight: 0.6,
        graph_weight: 0.4,
        max_vector_results: 10,
        max_graph_depth: 3,
        min_similarity: 0.5,
        enable_graph_expansion: true,
        enable_vector_filtering: true,
    };

    let hybrid_engine = HybridRAGEngineBuilder::new()
        .with_vector_retriever(vector_retriever)
        .with_graph_engine(graph_engine)
        .with_embedding_model(embedding_model)
        .with_config(hybrid_config)
        .build()?;

    // Test queries
    println!("\n=== Testing Hybrid RAG Queries ===\n");

    let queries = vec![
        "Who founded TechCorp?",
        "What products does TechCorp make?",
        "Tell me about the company's leadership",
        "What acquisitions has TechCorp made?",
        "What is Jane Smith's background?",
    ];

    for query in queries {
        println!("Query: {}", query);
        println!("{}", "-".repeat(50));

        let results = hybrid_engine.query(query, None).await?;

        for (i, result) in results.iter().take(3).enumerate() {
            println!("\nResult {}:", i + 1);
            println!("  Document: {}", result.document.id);
            println!("  Content: {}...", &result.document.content[..80.min(result.document.content.len())]);
            println!("  Hybrid Score: {:.3}", result.hybrid_score);
            
            if let Some(vector_score) = result.vector_score {
                println!("  Vector Score: {:.3}", vector_score);
            }
            
            if let Some(graph_score) = result.graph_score {
                println!("  Graph Score: {:.3}", graph_score);
            }
            
            if !result.related_entities.is_empty() {
                println!("  Related Entities: {:?}", result.related_entities);
            }
            
            if let Some(ref path) = result.graph_path {
                println!("  Graph Path: {:?}", path);
            }
        }
        println!("\n");
    }

    // Test entity-specific queries
    println!("\n=== Testing Entity-Specific Queries ===\n");
    
    let entity_query = "What are the key achievements?";
    let entity_ids = vec!["entity_jane".to_string()];
    
    println!("Query: {} (starting from Jane Smith)", entity_query);
    println!("{}", "-".repeat(50));
    
    let entity_results = hybrid_engine.query_with_entities(
        entity_query,
        &entity_ids,
        None
    ).await?;

    for (i, result) in entity_results.iter().take(2).enumerate() {
        println!("\nResult {}:", i + 1);
        println!("  Document: {}", result.document.id);
        println!("  Content: {}...", &result.document.content[..80.min(result.document.content.len())]);
        println!("  Hybrid Score: {:.3}", result.hybrid_score);
    }

    // Demonstrate configuration changes
    println!("\n=== Testing Different Weight Configurations ===\n");
    
    let configs = vec![
        ("Vector-Heavy (80/20)", 0.8),
        ("Balanced (50/50)", 0.5),
        ("Graph-Heavy (20/80)", 0.2),
    ];

    let test_query = "Who leads TechCorp's engineering?";
    
    for (name, vector_weight) in configs {
        let mut custom_engine = HybridRAGEngineBuilder::new()
            .with_vector_retriever(vector_retriever.clone())
            .with_graph_engine(graph_engine.clone())
            .with_embedding_model(embedding_model.clone())
            .with_vector_weight(vector_weight)
            .build()?;
        
        println!("\nConfiguration: {}", name);
        let results = custom_engine.query(test_query, None).await?;
        
        if let Some(top_result) = results.first() {
            println!("  Top Result: {}", top_result.document.id);
            println!("  Hybrid Score: {:.3}", top_result.hybrid_score);
            if let Some(v) = top_result.vector_score {
                println!("  Vector Component: {:.3}", v * vector_weight);
            }
            if let Some(g) = top_result.graph_score {
                println!("  Graph Component: {:.3}", g * (1.0 - vector_weight));
            }
        }
    }

    println!("\n=== Demo Complete ===");
    Ok(())
}