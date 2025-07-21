use oxidb::core::graph::{GraphFactory, GraphData, Relationship};
use oxidb::core::rag::{Document, GraphRAGEngine, GraphRAGContext, KnowledgeNode, KnowledgeEdge};
use oxidb::core::rag::retriever::InMemoryRetriever;
use oxidb::core::rag::graphrag::GraphRAGEngineImpl;
use oxidb::core::types::DataType;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Oxidb GraphRAG Demo");
    println!("======================");
    
    // Step 1: Create sample documents for RAG
    println!("\n📄 Creating sample documents...");
    let documents = create_sample_documents();
    
    // Step 2: Set up GraphRAG engine
    println!("\n🧠 Setting up GraphRAG engine...");
    let retriever = Box::new(InMemoryRetriever::new(documents.clone()));
    let mut graphrag_engine = GraphRAGEngineImpl::new(retriever);
    
    // Step 3: Build knowledge graph from documents
    println!("\n🕸️  Building knowledge graph...");
    graphrag_engine.build_knowledge_graph(&documents).await?;
    
    // Step 4: Add custom entities and relationships
    println!("\n➕ Adding custom entities and relationships...");
    let alice_entity = create_person_entity("Alice", "Software Engineer", vec![0.1, 0.2, 0.3]);
    let bob_entity = create_person_entity("Bob", "Data Scientist", vec![0.2, 0.3, 0.4]);
    let company_entity = create_organization_entity("TechCorp", "Technology Company", vec![0.15, 0.25, 0.35]);
    
    let alice_id = graphrag_engine.add_entity(alice_entity).await?;
    let bob_id = graphrag_engine.add_entity(bob_entity).await?;
    let company_id = graphrag_engine.add_entity(company_entity).await?;
    
    // Add relationships
    let works_at_rel = create_relationship(alice_id, company_id, "WORKS_AT", 0.9);
    let colleague_rel = create_relationship(alice_id, bob_id, "COLLEAGUE", 0.8);
    
    graphrag_engine.add_relationship(works_at_rel).await?;
    graphrag_engine.add_relationship(colleague_rel).await?;
    
    // Step 5: Demonstrate GraphRAG queries
    println!("\n🔍 Performing GraphRAG queries...");
    
    // Query 1: Find related entities
    let related_entities = graphrag_engine.find_related_entities(alice_id, 2).await?;
    println!("Entities related to Alice (within 2 hops): {}", related_entities.len());
    for entity in &related_entities {
        println!("  - {} ({})", entity.name, entity.entity_type);
    }
    
    // Query 2: Get reasoning paths
    let reasoning_paths = graphrag_engine.get_reasoning_paths(alice_id, bob_id, 3).await?;
    println!("\nReasoning paths from Alice to Bob:");
    for (i, path) in reasoning_paths.iter().enumerate() {
        println!("  Path {}: {} (score: {:.2})", i + 1, path.explanation, path.reasoning_score);
    }
    
    // Query 3: Enhanced retrieval with graph context
    let query_embedding = vec![0.12, 0.22, 0.32].into(); // Similar to Alice's embedding
    let graph_context = GraphRAGContext {
        query_embedding,
        max_hops: 2,
        min_confidence: 0.5,
        include_relationships: vec!["WORKS_AT".to_string(), "COLLEAGUE".to_string()],
        exclude_relationships: vec![],
        entity_types: vec!["PERSON".to_string(), "ORGANIZATION".to_string()],
    };
    
    let graphrag_result = graphrag_engine.retrieve_with_graph(graph_context).await?;
    
    println!("\n📊 GraphRAG Query Results:");
    println!("Documents found: {}", graphrag_result.documents.len());
    println!("Relevant entities: {}", graphrag_result.relevant_entities.len());
    println!("Reasoning paths: {}", graphrag_result.reasoning_paths.len());
    println!("Overall confidence: {:.2}", graphrag_result.confidence_score);
    
    // Step 6: Demonstrate pure graph operations
    println!("\n🔗 Demonstrating pure graph operations...");
    let mut graph = GraphFactory::create_memory_graph()?;
    
    // Add nodes
    let node1_data = GraphData::new("Person".to_string())
        .with_property("name".to_string(), DataType::String("Charlie".to_string()))
        .with_property("age".to_string(), DataType::Integer(30));
    let node1_id = graph.add_node(node1_data)?;
    
    let node2_data = GraphData::new("Person".to_string())
        .with_property("name".to_string(), DataType::String("Diana".to_string()))
        .with_property("age".to_string(), DataType::Integer(28));
    let node2_id = graph.add_node(node2_data)?;
    
    // Add relationship
    let friendship = Relationship::bidirectional("FRIENDS".to_string());
    let edge_id = graph.add_edge(node1_id, node2_id, friendship, None)?;
    
    println!("Added nodes: {} and {}", node1_id, node2_id);
    println!("Added edge: {}", edge_id);
    
    // Query graph
    let neighbors = graph.get_neighbors(node1_id, oxidb::core::graph::traversal::TraversalDirection::Both)?;
    println!("Charlie's neighbors: {:?}", neighbors);
    
    // Note: find_shortest_path is part of GraphQuery trait, not GraphOperations
    // For this demo, we'll skip the shortest path since we're using the basic GraphOperations trait
    println!("Graph operations completed successfully!");
    
    println!("\n✅ GraphRAG demo completed successfully!");
    println!("\nKey features demonstrated:");
    println!("  ✓ Document-based knowledge graph construction");
    println!("  ✓ Custom entity and relationship management");
    println!("  ✓ Graph-enhanced retrieval with reasoning paths");
    println!("  ✓ Pure graph database operations");
    println!("  ✓ SOLID design principles throughout");
    
    Ok(())
}

fn create_sample_documents() -> Vec<Document> {
    vec![
        Document {
            id: "doc1".to_string(),
            content: "Alice is a software engineer at TechCorp. She specializes in database systems and works closely with the data science team.".to_string(),
            embedding: Some(vec![0.1, 0.2, 0.3].into()),
            metadata: Some({
                let mut meta = HashMap::new();
                meta.insert("source".to_string(), oxidb::core::common::types::Value::Text("hr_system".to_string()));
                meta.insert("department".to_string(), oxidb::core::common::types::Value::Text("engineering".to_string()));
                meta
            }),
        },
        Document {
            id: "doc2".to_string(),
            content: "Bob is a data scientist who joined TechCorp last year. He works on machine learning models and collaborates with Alice on database optimization.".to_string(),
            embedding: Some(vec![0.2, 0.3, 0.4].into()),
            metadata: Some({
                let mut meta = HashMap::new();
                meta.insert("source".to_string(), oxidb::core::common::types::Value::Text("hr_system".to_string()));
                meta.insert("department".to_string(), oxidb::core::common::types::Value::Text("data_science".to_string()));
                meta
            }),
        },
        Document {
            id: "doc3".to_string(),
            content: "TechCorp is a leading technology company focused on database solutions and artificial intelligence. The company has a strong engineering culture.".to_string(),
            embedding: Some(vec![0.15, 0.25, 0.35].into()),
            metadata: Some({
                let mut meta = HashMap::new();
                meta.insert("source".to_string(), oxidb::core::common::types::Value::Text("company_info".to_string()));
                meta.insert("type".to_string(), oxidb::core::common::types::Value::Text("organization".to_string()));
                meta
            }),
        },
    ]
}

fn create_person_entity(name: &str, role: &str, embedding: Vec<f32>) -> KnowledgeNode {
    let mut properties = HashMap::new();
    properties.insert("role".to_string(), DataType::String(role.to_string()));
    properties.insert("type".to_string(), DataType::String("person".to_string()));
    
    KnowledgeNode {
        id: 0, // Will be assigned by the engine
        entity_type: "PERSON".to_string(),
        name: name.to_string(),
        description: Some(format!("{} - {}", name, role)),
        embedding: Some(embedding.into()),
        properties,
        confidence_score: 0.9,
    }
}

fn create_organization_entity(name: &str, description: &str, embedding: Vec<f32>) -> KnowledgeNode {
    let mut properties = HashMap::new();
    properties.insert("industry".to_string(), DataType::String("technology".to_string()));
    properties.insert("type".to_string(), DataType::String("organization".to_string()));
    
    KnowledgeNode {
        id: 0, // Will be assigned by the engine
        entity_type: "ORGANIZATION".to_string(),
        name: name.to_string(),
        description: Some(description.to_string()),
        embedding: Some(embedding.into()),
        properties,
        confidence_score: 0.95,
    }
}

fn create_relationship(from: u64, to: u64, relationship_type: &str, confidence: f64) -> KnowledgeEdge {
    KnowledgeEdge {
        id: 0, // Will be assigned by the engine
        from_entity: from,
        to_entity: to,
        relationship_type: relationship_type.to_string(),
        description: Some(format!("{} relationship", relationship_type)),
        confidence_score: confidence,
        weight: Some(1.0),
    }
}