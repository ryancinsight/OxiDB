//! Hybrid RAG Demo
//! 
//! This example demonstrates how to use both traditional retrieval and graph-based
//! knowledge retrieval in a hybrid approach.

use oxidb::Connection;
use oxidb::core::rag::{Document};
use oxidb::core::rag::retriever::InMemoryRetriever;
use std::collections::HashMap;
use std::sync::Arc;
use oxidb::Value;

// Simple synchronous embedding function for the example
fn generate_embedding(text: &str, dimension: usize) -> Vec<f32> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut embedding = vec![0.0; dimension];
    let words: Vec<&str> = text.split_whitespace().collect();
    for (i, word) in words.iter().enumerate() {
        let mut hasher = DefaultHasher::new();
        word.hash(&mut hasher);
        let hash = hasher.finish();
        for j in 0..dimension {
            let idx = (i + j) % dimension;
            embedding[idx] += ((hash >> j) & 0xFF) as f32 / 255.0;
        }
    }
    let magnitude: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
    if magnitude > 0.0 {
        for value in &mut embedding { *value /= magnitude; }
    }
    embedding
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("GraphRAG Demo - Knowledge Graph-based Retrieval");
    println!("==============================================\n");

    // Initialize database
    let mut conn = Connection::open_in_memory()?;
    
    // Create tables for storing knowledge graph
    conn.execute("CREATE TABLE IF NOT EXISTS nodes (
        id INTEGER PRIMARY KEY,
        node_type TEXT NOT NULL,
        content TEXT NOT NULL,
        confidence REAL
    )")?;
    
    conn.execute("CREATE TABLE IF NOT EXISTS edges (
        id INTEGER PRIMARY KEY,
        source_id INTEGER NOT NULL,
        target_id INTEGER NOT NULL,
        relationship_type TEXT NOT NULL,
        confidence REAL
    )")?;

    // Create sample documents
    let documents = vec![
        Document::new(
            "doc1".to_string(),
            "TechCorp announces revolutionary AI breakthrough in natural language processing".to_string(),
        ).with_metadata(HashMap::from([("category".to_string(), Value::Text("tech".to_string()))])),
        Document::new(
            "doc2".to_string(),
            "TechCorp's new SmartAnalytics platform leverages machine learning for business insights".to_string(),
        ).with_metadata(HashMap::from([("category".to_string(), Value::Text("product".to_string()))])),
        Document::new(
            "doc3".to_string(),
            "CEO Jane Smith leads TechCorp to record profits with innovative AI strategy".to_string(),
        ).with_metadata(HashMap::from([("category".to_string(), Value::Text("business".to_string()))])),
        Document::new(
            "doc4".to_string(),
            "TechCorp acquires DataViz Solutions to expand visualization capabilities".to_string(),
        ).with_metadata(HashMap::from([("category".to_string(), Value::Text("acquisition".to_string()))])),
    ];
    
    // Add documents with embeddings
    let mut embedded_docs = Vec::new();
    for doc in documents {
        let embedding_dimension = 384;
        let embedding = generate_embedding(&doc.content, embedding_dimension);
        let embedded_doc = doc.with_embedding(oxidb::core::rag::Embedding { vector: embedding });
        embedded_docs.push(embedded_doc);
    }

    // Store documents in retriever (not used further in this simple demo)
    let _retriever = Arc::new(InMemoryRetriever::new(embedded_docs));

    // Insert sample nodes into database
    let nodes = vec![
        (1_i64, "Company", "TechCorp", 0.9_f64),
        (2_i64, "Product", "SmartAnalytics", 0.85_f64),
        (3_i64, "Company", "DataViz Solutions", 0.8_f64),
    ];
    for (id, node_type, content, confidence) in &nodes {
        conn.execute_with_params(
            "INSERT INTO nodes (id, node_type, content, confidence) VALUES (?, ?, ?, ?)",
            &[
                Value::Integer(*id),
                Value::Text((*node_type).to_string()),
                Value::Text((*content).to_string()),
                Value::Float(*confidence),
            ],
        )?;
    }

    // Create relationships
    conn.execute_with_params(
        "INSERT INTO edges (source_id, target_id, relationship_type, confidence) VALUES (?, ?, ?, ?)",
        &[
            Value::Integer(1),
            Value::Integer(2),
            Value::Text("develops".to_string()),
            Value::Float(0.9),
        ],
    )?;
    conn.execute_with_params(
        "INSERT INTO edges (source_id, target_id, relationship_type, confidence) VALUES (?, ?, ?, ?)",
        &[
            Value::Integer(1),
            Value::Integer(3),
            Value::Text("acquired".to_string()),
            Value::Float(0.95),
        ],
    )?;

    println!("Knowledge Graph created with {} nodes", nodes.len());
    println!("\nNodes:");
    for (_, node_type, content, _) in &nodes {
        println!("  - {} ({})", content, node_type);
    }

    // Query the knowledge graph
    println!("\n\nQuerying Knowledge Graph:");
    println!("========================\n");

    // Find all products developed by TechCorp
    let result = conn.execute_with_params(
        "SELECT n2.content, n2.node_type \
         FROM nodes n1 \
         JOIN edges e ON n1.id = e.source_id \
         JOIN nodes n2 ON e.target_id = n2.id \
         WHERE n1.content = ? AND e.relationship_type = ?",
        &[
            Value::Text("TechCorp".to_string()),
            Value::Text("develops".to_string()),
        ],
    )?;

    println!("Products developed by TechCorp:");
    if result.rows.is_empty() {
        println!("  No products found");
    } else {
        for row in &result.rows {
            let name = match row.get(0).unwrap_or(&Value::Null) {
                Value::Text(s) => s.clone(),
                _ => "Unknown".to_string(),
            };
            let node_type = match row.get(1).unwrap_or(&Value::Null) {
                Value::Text(s) => s.clone(),
                _ => "Unknown".to_string(),
            };
            println!("  - {} ({})", name, node_type);
        }
    }

    // Find all acquisitions
    let result = conn.execute_with_params(
        "SELECT n1.content, n2.content, e.relationship_type \
         FROM nodes n1 \
         JOIN edges e ON n1.id = e.source_id \
         JOIN nodes n2 ON e.target_id = n2.id \
         WHERE e.relationship_type = ?",
        &[Value::Text("acquired".to_string())],
    )?;

    println!("\nAcquisitions:");
    if result.rows.is_empty() {
        println!("  No acquisitions found");
    } else {
        for row in &result.rows {
            let acquirer = match row.get(0).unwrap_or(&Value::Null) {
                Value::Text(s) => s.clone(),
                _ => "Unknown".to_string(),
            };
            let acquired = match row.get(1).unwrap_or(&Value::Null) {
                Value::Text(s) => s.clone(),
                _ => "Unknown".to_string(),
            };
            println!("  - {} acquired {}", acquirer, acquired);
        }
    }

    println!("\n✅ GraphRAG demo completed successfully!");
    Ok(())
}