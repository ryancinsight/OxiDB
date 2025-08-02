//! Knowledge Graph RAG Example
//! 
//! This example demonstrates using Oxidb for GraphRAG (Graph-based Retrieval-Augmented Generation).
//! It shows how to build a knowledge graph with entities and relationships, then perform
//! graph-based queries to retrieve connected information.

use oxidb::{Oxidb, OxidbError};
use oxidb::core::types::{DataType, OrderedFloat, HashableVectorData, VectorData};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Entity {
    id: String,
    entity_type: EntityType,
    name: String,
    description: String,
    properties: HashMap<String, String>,
    embedding: Vec<f32>, // Vector embedding for semantic similarity
    created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum EntityType {
    Person,
    Organization,
    Technology,
    Concept,
    Event,
    Location,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Relationship {
    id: String,
    source_id: String,
    target_id: String,
    relationship_type: RelationshipType,
    properties: HashMap<String, String>,
    weight: f32, // Strength of the relationship
    created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum RelationshipType {
    WorksFor,
    Develops,
    Uses,
    RelatedTo,
    LocatedIn,
    PartOf,
    Influences,
    CompetesWith,
    CollaboratesWith,
}

#[derive(Debug, Clone)]
struct GraphQuery {
    start_entity: String,
    max_depth: usize,
    relationship_types: Option<Vec<RelationshipType>>,
    entity_types: Option<Vec<EntityType>>,
}

#[derive(Debug, Clone)]
struct GraphPath {
    entities: Vec<Entity>,
    relationships: Vec<Relationship>,
    total_weight: f32,
}

struct KnowledgeGraphDB {
    db: Oxidb,
    embedding_dimension: usize,
}

impl KnowledgeGraphDB {
    fn new(db_path: &str, embedding_dimension: usize) -> Result<Self, OxidbError> {
        let db = Oxidb::open(db_path)?;
        
        // Create tables for entities and relationships
        db.execute_sql("CREATE TABLE IF NOT EXISTS entities (
            id TEXT PRIMARY KEY,
            entity_type TEXT NOT NULL,
            name TEXT NOT NULL,
            description TEXT,
            properties TEXT,
            embedding VECTOR[128],
            created_at TEXT
        )")?;
        
        db.execute_sql("CREATE TABLE IF NOT EXISTS relationships (
            id TEXT PRIMARY KEY,
            source_id TEXT NOT NULL,
            target_id TEXT NOT NULL,
            relationship_type TEXT NOT NULL,
            properties TEXT,
            weight FLOAT DEFAULT 1.0,
            created_at TEXT,
            FOREIGN KEY (source_id) REFERENCES entities(id),
            FOREIGN KEY (target_id) REFERENCES entities(id)
        )")?;
        
        // Create indexes for better performance
        db.execute_sql("CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(entity_type)")?;
        db.execute_sql("CREATE INDEX IF NOT EXISTS idx_relationships_source ON relationships(source_id)")?;
        db.execute_sql("CREATE INDEX IF NOT EXISTS idx_relationships_target ON relationships(target_id)")?;
        db.execute_sql("CREATE INDEX IF NOT EXISTS idx_relationships_type ON relationships(relationship_type)")?;
        
        Ok(KnowledgeGraphDB { db, embedding_dimension })
    }
    
    // Entity management
    fn add_entity(&self, entity: &Entity) -> Result<(), OxidbError> {
        let properties_json = serde_json::to_string(&entity.properties).unwrap();
        let entity_type_str = serde_json::to_string(&entity.entity_type).unwrap();
        let embedding_str = format!("[{}]", entity.embedding.iter().map(|f| f.to_string()).collect::<Vec<_>>().join(","));
        
        let sql = format!(
            "INSERT INTO entities (id, entity_type, name, description, properties, embedding, created_at) 
             VALUES ('{}', {}, '{}', '{}', '{}', {}, '{}')",
            entity.id,
            entity_type_str,
            entity.name.replace("'", "''"),
            entity.description.replace("'", "''"),
            properties_json.replace("'", "''"),
            embedding_str,
            entity.created_at.to_rfc3339()
        );
        
        self.db.execute_sql(&sql)?;
        Ok(())
    }
    
    fn get_entity(&self, entity_id: &str) -> Result<Option<Entity>, OxidbError> {
        let sql = format!("SELECT * FROM entities WHERE id = '{}'", entity_id);
        let result = self.db.execute_sql(&sql)?;
        
        if let Some(row) = result.rows.first() {
            Ok(Some(self.row_to_entity(row)?))
        } else {
            Ok(None)
        }
    }
    
    // Relationship management
    fn add_relationship(&self, relationship: &Relationship) -> Result<(), OxidbError> {
        let properties_json = serde_json::to_string(&relationship.properties).unwrap();
        let rel_type_str = serde_json::to_string(&relationship.relationship_type).unwrap();
        
        let sql = format!(
            "INSERT INTO relationships (id, source_id, target_id, relationship_type, properties, weight, created_at) 
             VALUES ('{}', '{}', '{}', {}, '{}', {}, '{}')",
            relationship.id,
            relationship.source_id,
            relationship.target_id,
            rel_type_str,
            properties_json.replace("'", "''"),
            relationship.weight,
            relationship.created_at.to_rfc3339()
        );
        
        self.db.execute_sql(&sql)?;
        Ok(())
    }
    
    fn get_relationships(&self, entity_id: &str, direction: &str) -> Result<Vec<Relationship>, OxidbError> {
        let sql = match direction {
            "outgoing" => format!("SELECT * FROM relationships WHERE source_id = '{}'", entity_id),
            "incoming" => format!("SELECT * FROM relationships WHERE target_id = '{}'", entity_id),
            _ => format!("SELECT * FROM relationships WHERE source_id = '{}' OR target_id = '{}'", entity_id, entity_id),
        };
        
        let result = self.db.execute_sql(&sql)?;
        result.rows.iter()
            .map(|row| self.row_to_relationship(row))
            .collect()
    }
    
    // Graph traversal
    fn traverse_graph(&self, query: &GraphQuery) -> Result<Vec<GraphPath>, OxidbError> {
        let mut paths = Vec::new();
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        
        // Start with the initial entity
        if let Some(start_entity) = self.get_entity(&query.start_entity)? {
            let initial_path = GraphPath {
                entities: vec![start_entity.clone()],
                relationships: vec![],
                total_weight: 0.0,
            };
            queue.push_back((initial_path, 0));
            visited.insert(start_entity.id.clone());
        }
        
        while let Some((current_path, depth)) = queue.pop_front() {
            if depth >= query.max_depth {
                paths.push(current_path);
                continue;
            }
            
            let current_entity_id = &current_path.entities.last().unwrap().id;
            let relationships = self.get_relationships(current_entity_id, "outgoing")?;
            
            let mut extended_path = false;
            for rel in relationships {
                // Filter by relationship type if specified
                if let Some(ref rel_types) = query.relationship_types {
                    if !rel_types.contains(&rel.relationship_type) {
                        continue;
                    }
                }
                
                if !visited.contains(&rel.target_id) {
                    if let Some(target_entity) = self.get_entity(&rel.target_id)? {
                        // Filter by entity type if specified
                        if let Some(ref entity_types) = query.entity_types {
                            if !entity_types.contains(&target_entity.entity_type) {
                                continue;
                            }
                        }
                        
                        visited.insert(rel.target_id.clone());
                        
                        let mut new_path = current_path.clone();
                        new_path.entities.push(target_entity);
                        new_path.relationships.push(rel.clone());
                        new_path.total_weight += rel.weight;
                        
                        queue.push_back((new_path, depth + 1));
                        extended_path = true;
                    }
                }
            }
            
            // If no extensions were made, this is a terminal path
            if !extended_path && depth > 0 {
                paths.push(current_path);
            }
        }
        
        // Sort paths by total weight (descending)
        paths.sort_by(|a, b| b.total_weight.partial_cmp(&a.total_weight).unwrap());
        
        Ok(paths)
    }
    
    // Find similar entities using vector embeddings
    fn find_similar_entities(&self, entity_id: &str, limit: usize) -> Result<Vec<Entity>, OxidbError> {
        if let Some(entity) = self.get_entity(entity_id)? {
            let embedding_str = format!("[{}]", entity.embedding.iter().map(|f| f.to_string()).collect::<Vec<_>>().join(","));
            
            let sql = format!(
                "SELECT *, vector_distance(embedding, {}) as distance 
                 FROM entities 
                 WHERE id != '{}' 
                 ORDER BY distance ASC 
                 LIMIT {}",
                embedding_str, entity_id, limit
            );
            
            let result = self.db.execute_sql(&sql)?;
            result.rows.iter()
                .map(|row| self.row_to_entity(row))
                .collect()
        } else {
            Ok(vec![])
        }
    }
    
    // Find shortest path between two entities
    fn find_shortest_path(&self, start_id: &str, end_id: &str) -> Result<Option<GraphPath>, OxidbError> {
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        
        if let Some(start_entity) = self.get_entity(start_id)? {
            let initial_path = GraphPath {
                entities: vec![start_entity],
                relationships: vec![],
                total_weight: 0.0,
            };
            queue.push_back(initial_path);
            visited.insert(start_id.to_string());
        }
        
        while let Some(current_path) = queue.pop_front() {
            let current_entity_id = &current_path.entities.last().unwrap().id;
            
            if current_entity_id == end_id {
                return Ok(Some(current_path));
            }
            
            let relationships = self.get_relationships(current_entity_id, "outgoing")?;
            
            for rel in relationships {
                if !visited.contains(&rel.target_id) {
                    visited.insert(rel.target_id.clone());
                    
                    if let Some(target_entity) = self.get_entity(&rel.target_id)? {
                        let mut new_path = current_path.clone();
                        new_path.entities.push(target_entity);
                        new_path.relationships.push(rel.clone());
                        new_path.total_weight += rel.weight;
                        
                        queue.push_back(new_path);
                    }
                }
            }
        }
        
        Ok(None)
    }
    
    // Helper methods
    fn row_to_entity(&self, row: &[DataType]) -> Result<Entity, OxidbError> {
        Ok(Entity {
            id: self.get_string(&row[0])?,
            entity_type: serde_json::from_str(&self.get_string(&row[1])?).unwrap(),
            name: self.get_string(&row[2])?,
            description: self.get_string(&row[3])?,
            properties: serde_json::from_str(&self.get_string(&row[4])?).unwrap_or_default(),
            embedding: self.get_vector(&row[5])?.unwrap_or_default(),
            created_at: DateTime::parse_from_rfc3339(&self.get_string(&row[6])?)
                .unwrap()
                .with_timezone(&Utc),
        })
    }
    
    fn row_to_relationship(&self, row: &[DataType]) -> Result<Relationship, OxidbError> {
        Ok(Relationship {
            id: self.get_string(&row[0])?,
            source_id: self.get_string(&row[1])?,
            target_id: self.get_string(&row[2])?,
            relationship_type: serde_json::from_str(&self.get_string(&row[3])?).unwrap(),
            properties: serde_json::from_str(&self.get_string(&row[4])?).unwrap_or_default(),
            weight: self.get_float(&row[5])?,
            created_at: DateTime::parse_from_rfc3339(&self.get_string(&row[6])?)
                .unwrap()
                .with_timezone(&Utc),
        })
    }
    
    fn get_string(&self, data: &DataType) -> Result<String, OxidbError> {
        match data {
            DataType::String(s) => Ok(s.clone()),
            DataType::Null => Ok(String::new()),
            _ => Err(OxidbError::TypeMismatch),
        }
    }
    
    fn get_float(&self, data: &DataType) -> Result<f32, OxidbError> {
        match data {
            DataType::Float(f) => Ok(f.0 as f32),
            DataType::Integer(i) => Ok(*i as f32),
            _ => Err(OxidbError::TypeMismatch),
        }
    }
    
    fn get_vector(&self, data: &DataType) -> Result<Option<Vec<f32>>, OxidbError> {
        match data {
            DataType::Vector(v) => Ok(Some(v.0.data.clone())),
            DataType::Null => Ok(None),
            _ => Err(OxidbError::TypeMismatch),
        }
    }
}

// Simple embedding generator for demonstration
fn generate_embedding(text: &str, dimension: usize) -> Vec<f32> {
    let mut embedding = vec![0.0; dimension];
    let words: Vec<&str> = text.split_whitespace().collect();
    
    for (i, word) in words.iter().enumerate() {
        let hash = word.chars().map(|c| c as u32).sum::<u32>();
        let index = (hash as usize + i) % dimension;
        embedding[index] = ((hash % 100) as f32 / 100.0) * 2.0 - 1.0;
    }
    
    // Normalize
    let magnitude = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
    if magnitude > 0.0 {
        for x in &mut embedding {
            *x /= magnitude;
        }
    }
    
    embedding
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Knowledge Graph RAG Example ===\n");
    
    let embedding_dim = 128;
    let db = KnowledgeGraphDB::new("knowledge_graph.db", embedding_dim)?;
    
    // Create entities
    let entities = vec![
        Entity {
            id: "openai".to_string(),
            entity_type: EntityType::Organization,
            name: "OpenAI".to_string(),
            description: "AI research laboratory".to_string(),
            properties: HashMap::from([
                ("founded".to_string(), "2015".to_string()),
                ("type".to_string(), "Research".to_string()),
            ]),
            embedding: generate_embedding("openai artificial intelligence research gpt chatgpt", embedding_dim),
            created_at: Utc::now(),
        },
        Entity {
            id: "gpt4".to_string(),
            entity_type: EntityType::Technology,
            name: "GPT-4".to_string(),
            description: "Large language model developed by OpenAI".to_string(),
            properties: HashMap::from([
                ("release_year".to_string(), "2023".to_string()),
                ("parameters".to_string(), "1.76 trillion".to_string()),
            ]),
            embedding: generate_embedding("gpt4 language model transformer neural network", embedding_dim),
            created_at: Utc::now(),
        },
        Entity {
            id: "anthropic".to_string(),
            entity_type: EntityType::Organization,
            name: "Anthropic".to_string(),
            description: "AI safety company".to_string(),
            properties: HashMap::from([
                ("founded".to_string(), "2021".to_string()),
                ("focus".to_string(), "AI Safety".to_string()),
            ]),
            embedding: generate_embedding("anthropic ai safety claude constitutional", embedding_dim),
            created_at: Utc::now(),
        },
        Entity {
            id: "claude".to_string(),
            entity_type: EntityType::Technology,
            name: "Claude".to_string(),
            description: "AI assistant developed by Anthropic".to_string(),
            properties: HashMap::from([
                ("type".to_string(), "Conversational AI".to_string()),
                ("approach".to_string(), "Constitutional AI".to_string()),
            ]),
            embedding: generate_embedding("claude ai assistant anthropic constitutional helpful harmless", embedding_dim),
            created_at: Utc::now(),
        },
        Entity {
            id: "rag".to_string(),
            entity_type: EntityType::Concept,
            name: "Retrieval-Augmented Generation".to_string(),
            description: "Technique combining retrieval and generation for AI systems".to_string(),
            properties: HashMap::from([
                ("abbreviation".to_string(), "RAG".to_string()),
                ("use_case".to_string(), "Knowledge-grounded AI".to_string()),
            ]),
            embedding: generate_embedding("rag retrieval augmented generation vector search knowledge", embedding_dim),
            created_at: Utc::now(),
        },
        Entity {
            id: "vector_db".to_string(),
            entity_type: EntityType::Technology,
            name: "Vector Database".to_string(),
            description: "Database optimized for storing and querying vector embeddings".to_string(),
            properties: HashMap::from([
                ("examples".to_string(), "Pinecone, Weaviate, Qdrant".to_string()),
                ("use_case".to_string(), "Similarity search".to_string()),
            ]),
            embedding: generate_embedding("vector database embeddings similarity search hnsw faiss", embedding_dim),
            created_at: Utc::now(),
        },
    ];
    
    // Add entities to the graph
    println!("Building knowledge graph...");
    for entity in &entities {
        db.add_entity(entity)?;
        println!("Added entity: {} ({})", entity.name, match entity.entity_type {
            EntityType::Organization => "Organization",
            EntityType::Technology => "Technology",
            EntityType::Concept => "Concept",
            _ => "Other",
        });
    }
    
    // Create relationships
    let relationships = vec![
        Relationship {
            id: "rel_001".to_string(),
            source_id: "openai".to_string(),
            target_id: "gpt4".to_string(),
            relationship_type: RelationshipType::Develops,
            properties: HashMap::new(),
            weight: 1.0,
            created_at: Utc::now(),
        },
        Relationship {
            id: "rel_002".to_string(),
            source_id: "anthropic".to_string(),
            target_id: "claude".to_string(),
            relationship_type: RelationshipType::Develops,
            properties: HashMap::new(),
            weight: 1.0,
            created_at: Utc::now(),
        },
        Relationship {
            id: "rel_003".to_string(),
            source_id: "gpt4".to_string(),
            target_id: "rag".to_string(),
            relationship_type: RelationshipType::Uses,
            properties: HashMap::from([
                ("context".to_string(), "Knowledge grounding".to_string()),
            ]),
            weight: 0.8,
            created_at: Utc::now(),
        },
        Relationship {
            id: "rel_004".to_string(),
            source_id: "claude".to_string(),
            target_id: "rag".to_string(),
            relationship_type: RelationshipType::Uses,
            properties: HashMap::new(),
            weight: 0.8,
            created_at: Utc::now(),
        },
        Relationship {
            id: "rel_005".to_string(),
            source_id: "rag".to_string(),
            target_id: "vector_db".to_string(),
            relationship_type: RelationshipType::Uses,
            properties: HashMap::from([
                ("purpose".to_string(), "Embedding storage and retrieval".to_string()),
            ]),
            weight: 0.9,
            created_at: Utc::now(),
        },
        Relationship {
            id: "rel_006".to_string(),
            source_id: "openai".to_string(),
            target_id: "anthropic".to_string(),
            relationship_type: RelationshipType::CompetesWith,
            properties: HashMap::from([
                ("market".to_string(), "AI assistants".to_string()),
            ]),
            weight: 0.7,
            created_at: Utc::now(),
        },
    ];
    
    // Add relationships
    println!("\nAdding relationships...");
    for rel in &relationships {
        db.add_relationship(rel)?;
        println!("Added: {} -> {} ({})", 
            rel.source_id, 
            rel.target_id,
            match rel.relationship_type {
                RelationshipType::Develops => "develops",
                RelationshipType::Uses => "uses",
                RelationshipType::CompetesWith => "competes with",
                _ => "related to",
            }
        );
    }
    
    // Example 1: Graph traversal from OpenAI
    println!("\n--- Graph Traversal from OpenAI ---");
    let query1 = GraphQuery {
        start_entity: "openai".to_string(),
        max_depth: 3,
        relationship_types: None,
        entity_types: None,
    };
    
    let paths = db.traverse_graph(&query1)?;
    println!("Found {} paths from OpenAI (max depth: {})", paths.len(), query1.max_depth);
    
    for (i, path) in paths.iter().take(3).enumerate() {
        println!("\nPath {}: (weight: {:.2})", i + 1, path.total_weight);
        for (j, entity) in path.entities.iter().enumerate() {
            if j > 0 {
                let rel = &path.relationships[j - 1];
                println!("  --[{:?}]--> ", rel.relationship_type);
            }
            println!("  {}: {}", entity.entity_type as i32, entity.name);
        }
    }
    
    // Example 2: Find similar entities to RAG
    println!("\n--- Similar Entities to RAG ---");
    let similar = db.find_similar_entities("rag", 3)?;
    for entity in &similar {
        println!("- {} ({})", entity.name, match entity.entity_type {
            EntityType::Technology => "Technology",
            EntityType::Concept => "Concept",
            _ => "Other",
        });
    }
    
    // Example 3: Shortest path between entities
    println!("\n--- Shortest Path: OpenAI to Vector Database ---");
    if let Some(path) = db.find_shortest_path("openai", "vector_db")? {
        println!("Path found (total weight: {:.2}):", path.total_weight);
        for (i, entity) in path.entities.iter().enumerate() {
            if i > 0 {
                let rel = &path.relationships[i - 1];
                println!("  --[{:?}]--> ", rel.relationship_type);
            }
            println!("  {}", entity.name);
        }
    } else {
        println!("No path found");
    }
    
    // Example 4: Technology-focused traversal
    println!("\n--- Technology-Focused Graph Traversal ---");
    let query2 = GraphQuery {
        start_entity: "gpt4".to_string(),
        max_depth: 2,
        relationship_types: Some(vec![RelationshipType::Uses, RelationshipType::RelatedTo]),
        entity_types: Some(vec![EntityType::Technology, EntityType::Concept]),
    };
    
    let tech_paths = db.traverse_graph(&query2)?;
    println!("Found {} technology-related paths from GPT-4", tech_paths.len());
    
    for path in &tech_paths {
        let entity_names: Vec<String> = path.entities.iter()
            .map(|e| e.name.clone())
            .collect();
        println!("- {}", entity_names.join(" -> "));
    }
    
    Ok(())
}