//! Hybrid RAG Demo
//! 
//! This example demonstrates how to use both traditional retrieval and graph-based
//! knowledge retrieval in a hybrid approach.

use oxidb::Connection;
use oxidb::core::rag::{Document, KnowledgeNode};
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
        
        // Distribute word influence across embedding dimensions
        for j in 0..dimension {
            let idx = (i + j) % dimension;
            embedding[idx] += ((hash >> j) & 0xFF) as f32 / 255.0;
        }
    }
    
    // Normalize the embedding
    let magnitude: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
    if magnitude > 0.0 {
        for value in &mut embedding {
            *value /= magnitude;
        }
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
        entity_type TEXT NOT NULL,
        name TEXT NOT NULL,
        description TEXT,
        confidence REAL
    )")?;
    
    conn.execute("CREATE TABLE IF NOT EXISTS edges (
        id INTEGER PRIMARY KEY,
        source_id INTEGER NOT NULL,
        target_id INTEGER NOT NULL,
        relationship_type TEXT NOT NULL,
        confidence REAL,
        FOREIGN KEY (source_id) REFERENCES nodes(id),
        FOREIGN KEY (target_id) REFERENCES nodes(id)
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
        // Generate embedding for the document
        let embedding_dimension = 384;
        let embedding = generate_embedding(&doc.content, embedding_dimension);
        let embedded_doc = doc.with_embedding(oxidb::core::rag::Embedding { vector: embedding });
        embedded_docs.push(embedded_doc);
    }

    // Store documents in retriever
    let _retriever = Arc::new(InMemoryRetriever::new(embedded_docs));

    // Create sample nodes for the knowledge graph
    let nodes = vec![
        KnowledgeNode {
            id: 1, // Changed from string to u64
            entity_type: "Company".to_string(),
            name: "TechCorp".to_string(),
            description: Some("Leading technology company in AI and data solutions".to_string()),
            embedding: None,
            properties: HashMap::new(),
            confidence_score: 0.9,
        },
        KnowledgeNode {
            id: 2, // Changed from string to u64
            entity_type: "Product".to_string(),
            name: "SmartAnalytics".to_string(),
            description: Some("AI-powered data analysis platform".to_string()),
            embedding: None,
            properties: HashMap::new(),
            confidence_score: 0.85,
        },
        KnowledgeNode {
            id: 3, // Changed from string to u64
            entity_type: "Company".to_string(),
            name: "DataViz Solutions".to_string(),
            description: Some("Visualization company acquired by TechCorp".to_string()),
            embedding: None,
            properties: HashMap::new(),
            confidence_score: 0.8,
        },
    ];

    // Insert nodes into database
    for node in &nodes {
        conn.execute_with_params(
            "INSERT INTO nodes (id, entity_type, name, description, confidence) VALUES (?, ?, ?, ?, ?)",
            &[
                Value::Integer(node.id as i64),
                Value::Text(node.entity_type.clone()),
                Value::Text(node.name.clone()),
                Value::Text(node.description.as_ref().unwrap_or(&String::new()).clone()),
                Value::Float(node.confidence_score as f64),
            ]
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
        ]
    )?;
    
    conn.execute_with_params(
        "INSERT INTO edges (source_id, target_id, relationship_type, confidence) VALUES (?, ?, ?, ?)",
        &[
            Value::Integer(1),
            Value::Integer(3),
            Value::Text("acquired".to_string()),
            Value::Float(0.95),
        ]
    )?;

    println!("Knowledge Graph created with {} nodes", nodes.len());
    println!("\nNodes:");
    for node in &nodes {
        println!("  - {} ({}): {}", node.name, node.entity_type, 
                 node.description.as_ref().unwrap_or(&"No description".to_string()));
    }

    // Query the knowledge graph
    println!("\n\nQuerying Knowledge Graph:");
    println!("========================\n");

    // Find all products developed by TechCorp
    let result = conn.execute_with_params(
        "SELECT n2.name, n2.description 
         FROM nodes n1 
         JOIN edges e ON n1.id = e.source_id 
         JOIN nodes n2 ON e.target_id = n2.id 
         WHERE n1.name = ? AND e.relationship_type = ?",
        &[
            Value::Text("TechCorp".to_string()),
            Value::Text("develops".to_string()),
        ]
    )?;

    println!("Products developed by TechCorp:");
    match result {
        oxidb::QueryResult::Data(data) => {
            for row in &data.rows {
                let name = match row.get(0).unwrap_or(&oxidb::Value::Null) {
                    oxidb::Value::Text(s) => s,
                    _ => "Unknown",
                };
                let desc = match row.get(1).unwrap_or(&oxidb::Value::Null) {
                    oxidb::Value::Text(s) => s,
                    _ => "No description",
                };
                println!("  - {}: {}", name, desc);
            }
        }
        _ => println!("  No products found"),
    }

    // Find all acquisitions
    let result = conn.execute_with_params(
        "SELECT n1.name, n2.name, e.relationship_type 
         FROM nodes n1 
         JOIN edges e ON n1.id = e.source_id 
         JOIN nodes n2 ON e.target_id = n2.id 
         WHERE e.relationship_type = ?",
        &[Value::Text("acquired".to_string())]
    )?;

    println!("\nAcquisitions:");
    match result {
        oxidb::QueryResult::Data(data) => {
            for row in &data.rows {
                let acquirer = match row.get(0).unwrap_or(&oxidb::Value::Null) {
                    oxidb::Value::Text(s) => s,
                    _ => "Unknown",
                };
                let acquired = match row.get(1).unwrap_or(&oxidb::Value::Null) {
                    oxidb::Value::Text(s) => s,
                    _ => "Unknown",
                };
                println!("  - {} acquired {}", acquirer, acquired);
            }
        }
        _ => println!("  No acquisitions found"),
    }

    println!("\n✅ GraphRAG demo completed successfully!");

    Ok(())
}