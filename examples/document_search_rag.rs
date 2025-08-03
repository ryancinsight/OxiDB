//! Document Search RAG (Retrieval-Augmented Generation) Example
//! 
//! This example demonstrates using Oxidb for semantic document search with vector embeddings.
//! It simulates a knowledge base system where documents are stored with embeddings
//! and can be searched using natural language queries.

use oxidb::{Oxidb, OxidbError};
use oxidb::core::types::{DataType, OrderedFloat, HashableVectorData, VectorData};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Document {
    id: String,
    title: String,
    content: String,
    category: String,
    author: String,
    embedding: Vec<f32>, // Vector embedding of the document content
    metadata: HashMap<String, String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SearchQuery {
    text: String,
    embedding: Vec<f32>, // Vector embedding of the query
    category_filter: Option<String>,
    limit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SearchResult {
    document: Document,
    score: f32, // Similarity score
    snippet: String, // Relevant snippet from the document
}

struct DocumentSearchDB {
    db: Oxidb,
    embedding_dimension: usize,
}

impl DocumentSearchDB {
    fn new(db_path: &str, embedding_dimension: usize) -> Result<Self, OxidbError> {
        let db = Oxidb::open(db_path)?;
        
        // Create table for documents with vector embeddings
        let create_table_sql = format!(
            "CREATE TABLE IF NOT EXISTS documents (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                content TEXT NOT NULL,
                category TEXT,
                author TEXT,
                embedding VECTOR[{}] NOT NULL,
                metadata TEXT,
                created_at TEXT,
                updated_at TEXT
            )",
            embedding_dimension
        );
        
        db.execute_sql(&create_table_sql)?;
        
        // Create indexes for better performance
        db.execute_sql("CREATE INDEX IF NOT EXISTS idx_documents_category ON documents(category)")?;
        db.execute_sql("CREATE INDEX IF NOT EXISTS idx_documents_author ON documents(author)")?;
        
        // Create a vector index for similarity search (HNSW)
        db.execute_sql("CREATE INDEX IF NOT EXISTS idx_documents_embedding ON documents USING hnsw(embedding)")?;
        
        Ok(DocumentSearchDB { db, embedding_dimension })
    }
    
    // Document management
    fn add_document(&self, doc: &Document) -> Result<(), OxidbError> {
        let metadata_json = serde_json::to_string(&doc.metadata).unwrap();
        let embedding_str = format!("[{}]", doc.embedding.iter().map(|f| f.to_string()).collect::<Vec<_>>().join(","));
        
        let sql = format!(
            "INSERT INTO documents (id, title, content, category, author, embedding, metadata, created_at, updated_at) 
             VALUES ('{}', '{}', '{}', '{}', '{}', {}, '{}', '{}', '{}')",
            doc.id,
            doc.title.replace("'", "''"),
            doc.content.replace("'", "''"),
            doc.category,
            doc.author,
            embedding_str,
            metadata_json.replace("'", "''"),
            doc.created_at.to_rfc3339(),
            doc.updated_at.to_rfc3339()
        );
        
        self.db.execute_sql(&sql)?;
        Ok(())
    }
    
    fn update_document_embedding(&self, doc_id: &str, embedding: &[f32]) -> Result<(), OxidbError> {
        let embedding_str = format!("[{}]", embedding.iter().map(|f| f.to_string()).collect::<Vec<_>>().join(","));
        
        let sql = format!(
            "UPDATE documents SET embedding = {}, updated_at = '{}' WHERE id = '{}'",
            embedding_str,
            Utc::now().to_rfc3339(),
            doc_id
        );
        
        self.db.execute_sql(&sql)?;
        Ok(())
    }
    
    // Semantic search using vector similarity
    fn semantic_search(&self, query: &SearchQuery) -> Result<Vec<SearchResult>, OxidbError> {
        let embedding_str = format!("[{}]", query.embedding.iter().map(|f| f.to_string()).collect::<Vec<_>>().join(","));
        
        let mut sql = format!(
            "SELECT *, vector_distance(embedding, {}) as distance 
             FROM documents",
            embedding_str
        );
        
        // Add category filter if specified
        if let Some(category) = &query.category_filter {
            sql.push_str(&format!(" WHERE category = '{}'", category));
        }
        
        // Order by similarity (lower distance = higher similarity)
        sql.push_str(&format!(" ORDER BY distance ASC LIMIT {}", query.limit));
        
        let result = self.db.execute_sql(&sql)?;
        
        let mut search_results = Vec::new();
        for row in result.rows {
            let doc = self.row_to_document(&row)?;
            let distance = self.get_float(&row[row.len() - 1])?;
            
            // Convert distance to similarity score (1 / (1 + distance))
            let score = 1.0 / (1.0 + distance);
            
            // Extract relevant snippet
            let snippet = self.extract_snippet(&doc.content, &query.text, 150);
            
            search_results.push(SearchResult {
                document: doc,
                score,
                snippet,
            });
        }
        
        Ok(search_results)
    }
    
    // Hybrid search combining keyword and semantic search
    fn hybrid_search(&self, query: &SearchQuery, keyword_weight: f32) -> Result<Vec<SearchResult>, OxidbError> {
        // Semantic search results
        let semantic_results = self.semantic_search(query)?;
        
        // Keyword search
        let keywords = query.text.split_whitespace()
            .map(|k| k.to_lowercase())
            .collect::<Vec<_>>();
        
        let mut keyword_conditions = Vec::new();
        for keyword in &keywords {
            keyword_conditions.push(format!(
                "(LOWER(title) LIKE '%{}%' OR LOWER(content) LIKE '%{}%')",
                keyword, keyword
            ));
        }
        
        let keyword_sql = format!(
            "SELECT * FROM documents WHERE {}",
            keyword_conditions.join(" OR ")
        );
        
        if let Some(category) = &query.category_filter {
            let keyword_sql = format!("{} AND category = '{}'", keyword_sql, category);
        }
        
        let keyword_result = self.db.execute_sql(&keyword_sql)?;
        
        // Combine results with weighted scoring
        let mut combined_results = HashMap::new();
        
        // Add semantic results
        for result in semantic_results {
            combined_results.insert(
                result.document.id.clone(),
                (result, 1.0 - keyword_weight)
            );
        }
        
        // Add keyword results
        for row in keyword_result.rows {
            let doc = self.row_to_document(&row)?;
            let doc_id = doc.id.clone();
            
            // Calculate keyword score based on match count
            let mut keyword_score = 0.0;
            let content_lower = doc.content.to_lowercase();
            let title_lower = doc.title.to_lowercase();
            
            for keyword in &keywords {
                if title_lower.contains(keyword) {
                    keyword_score += 2.0; // Title matches are weighted higher
                }
                if content_lower.contains(keyword) {
                    keyword_score += 1.0;
                }
            }
            
            let normalized_score = (keyword_score / keywords.len() as f32).min(1.0);
            let snippet = self.extract_snippet(&doc.content, &query.text, 150);
            
            if let Some((existing_result, semantic_weight)) = combined_results.get_mut(&doc_id) {
                // Document found in both searches - combine scores
                existing_result.score = existing_result.score * semantic_weight + normalized_score * keyword_weight;
            } else {
                // Document only found in keyword search
                combined_results.insert(
                    doc_id,
                    (SearchResult {
                        document: doc,
                        score: normalized_score * keyword_weight,
                        snippet,
                    }, keyword_weight)
                );
            }
        }
        
        // Sort by combined score
        let mut final_results: Vec<SearchResult> = combined_results.into_values()
            .map(|(result, _)| result)
            .collect();
        
        final_results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
        final_results.truncate(query.limit);
        
        Ok(final_results)
    }
    
    // Get documents by category
    fn get_documents_by_category(&self, category: &str) -> Result<Vec<Document>, OxidbError> {
        let sql = format!("SELECT * FROM documents WHERE category = '{}'", category);
        let result = self.db.execute_sql(&sql)?;
        
        result.rows.iter()
            .map(|row| self.row_to_document(row))
            .collect()
    }
    
    // Helper methods
    fn extract_snippet(&self, content: &str, query: &str, max_length: usize) -> String {
        // Find the most relevant part of the content
        let query_words: Vec<&str> = query.split_whitespace().collect();
        let content_lower = content.to_lowercase();
        
        let mut best_start = 0;
        let mut best_score = 0;
        
        // Sliding window to find the best snippet
        let words: Vec<&str> = content.split_whitespace().collect();
        for i in 0..words.len() {
            let mut score = 0;
            let window_text = words[i..].join(" ").to_lowercase();
            
            for query_word in &query_words {
                if window_text.starts_with(&query_word.to_lowercase()) {
                    score += 2;
                } else if window_text.contains(&query_word.to_lowercase()) {
                    score += 1;
                }
            }
            
            if score > best_score {
                best_score = score;
                best_start = content.len() - words[i..].join(" ").len();
            }
        }
        
        // Extract snippet around the best match
        let start = best_start.saturating_sub(50);
        let end = (best_start + max_length).min(content.len());
        
        let mut snippet = content[start..end].to_string();
        if start > 0 {
            snippet = format!("...{}", snippet);
        }
        if end < content.len() {
            snippet = format!("{}...", snippet);
        }
        
        snippet
    }
    
    fn row_to_document(&self, row: &[DataType]) -> Result<Document, OxidbError> {
        Ok(Document {
            id: self.get_string(&row[0])?,
            title: self.get_string(&row[1])?,
            content: self.get_string(&row[2])?,
            category: self.get_string(&row[3])?,
            author: self.get_string(&row[4])?,
            embedding: self.get_vector(&row[5])?.unwrap_or_default(),
            metadata: serde_json::from_str(&self.get_string(&row[6])?).unwrap_or_default(),
            created_at: DateTime::parse_from_rfc3339(&self.get_string(&row[7])?)
                .unwrap()
                .with_timezone(&Utc),
            updated_at: DateTime::parse_from_rfc3339(&self.get_string(&row[8])?)
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

// Simulated embedding function (in real use, this would use a model like BERT or OpenAI embeddings)
fn generate_embedding(text: &str, dimension: usize) -> Vec<f32> {
    // This is a simple hash-based embedding for demonstration
    // In production, use proper embedding models
    let mut embedding = vec![0.0; dimension];
    let words: Vec<&str> = text.split_whitespace().collect();
    
    for (i, word) in words.iter().enumerate() {
        let hash = word.chars().map(|c| c as u32).sum::<u32>();
        let index = (hash as usize + i) % dimension;
        embedding[index] = ((hash % 100) as f32 / 100.0) * 2.0 - 1.0;
    }
    
    // Normalize the embedding
    let magnitude = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
    if magnitude > 0.0 {
        for x in &mut embedding {
            *x /= magnitude;
        }
    }
    
    embedding
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Document Search RAG Example ===\n");
    
    let embedding_dim = 128;
    let db = DocumentSearchDB::new("document_search.db", embedding_dim)?;
    
    // Create sample documents
    let documents = vec![
        Document {
            id: "doc_001".to_string(),
            title: "Introduction to Machine Learning".to_string(),
            content: "Machine learning is a subset of artificial intelligence that focuses on the development of algorithms and statistical models that enable computer systems to improve their performance on a specific task through experience. The field encompasses various approaches including supervised learning, unsupervised learning, and reinforcement learning.".to_string(),
            category: "AI/ML".to_string(),
            author: "Dr. Smith".to_string(),
            embedding: generate_embedding("machine learning artificial intelligence algorithms models supervised unsupervised reinforcement", embedding_dim),
            metadata: HashMap::from([
                ("difficulty".to_string(), "beginner".to_string()),
                ("pages".to_string(), "15".to_string()),
            ]),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        },
        Document {
            id: "doc_002".to_string(),
            title: "Deep Learning Fundamentals".to_string(),
            content: "Deep learning is a specialized subset of machine learning that uses artificial neural networks with multiple layers. These networks are inspired by the human brain and can learn complex patterns in large amounts of data. Applications include computer vision, natural language processing, and speech recognition.".to_string(),
            category: "AI/ML".to_string(),
            author: "Prof. Johnson".to_string(),
            embedding: generate_embedding("deep learning neural networks layers brain patterns computer vision nlp speech", embedding_dim),
            metadata: HashMap::from([
                ("difficulty".to_string(), "intermediate".to_string()),
                ("pages".to_string(), "25".to_string()),
            ]),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        },
        Document {
            id: "doc_003".to_string(),
            title: "Database Systems Overview".to_string(),
            content: "Database systems are organized collections of data that can be easily accessed, managed, and updated. They include relational databases like PostgreSQL and MySQL, NoSQL databases like MongoDB, and newer vector databases designed for AI applications. ACID properties ensure data consistency and reliability.".to_string(),
            category: "Databases".to_string(),
            author: "Dr. Chen".to_string(),
            embedding: generate_embedding("database systems relational nosql vector postgresql mysql mongodb acid consistency", embedding_dim),
            metadata: HashMap::from([
                ("difficulty".to_string(), "beginner".to_string()),
                ("pages".to_string(), "20".to_string()),
            ]),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        },
        Document {
            id: "doc_004".to_string(),
            title: "Vector Databases for AI".to_string(),
            content: "Vector databases are specialized database systems designed to store and query high-dimensional vector embeddings. They are essential for modern AI applications like semantic search, recommendation systems, and retrieval-augmented generation (RAG). Popular vector databases include Pinecone, Weaviate, and Qdrant.".to_string(),
            category: "Databases".to_string(),
            author: "Dr. Lee".to_string(),
            embedding: generate_embedding("vector databases embeddings semantic search recommendations rag pinecone weaviate qdrant", embedding_dim),
            metadata: HashMap::from([
                ("difficulty".to_string(), "advanced".to_string()),
                ("pages".to_string(), "18".to_string()),
            ]),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        },
        Document {
            id: "doc_005".to_string(),
            title: "Natural Language Processing Basics".to_string(),
            content: "Natural Language Processing (NLP) is a branch of AI that helps computers understand, interpret, and manipulate human language. It combines computational linguistics with machine learning and deep learning models. Common applications include text classification, sentiment analysis, machine translation, and chatbots.".to_string(),
            category: "AI/ML".to_string(),
            author: "Dr. Williams".to_string(),
            embedding: generate_embedding("nlp natural language processing linguistics text classification sentiment analysis translation chatbots", embedding_dim),
            metadata: HashMap::from([
                ("difficulty".to_string(), "intermediate".to_string()),
                ("pages".to_string(), "22".to_string()),
            ]),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        },
    ];
    
    // Add documents to database
    println!("Adding documents to database...");
    for doc in &documents {
        db.add_document(doc)?;
        println!("Added: {} by {}", doc.title, doc.author);
    }
    
    // Example 1: Semantic search
    println!("\n--- Semantic Search Example ---");
    let query1 = SearchQuery {
        text: "How do neural networks work in AI?".to_string(),
        embedding: generate_embedding("neural networks ai deep learning layers", embedding_dim),
        category_filter: None,
        limit: 3,
    };
    
    println!("Query: {}", query1.text);
    let results = db.semantic_search(&query1)?;
    
    for (i, result) in results.iter().enumerate() {
        println!("\n{}. {} (Score: {:.3})", i + 1, result.document.title, result.score);
        println!("   Author: {}, Category: {}", result.document.author, result.document.category);
        println!("   Snippet: {}", result.snippet);
    }
    
    // Example 2: Category-filtered search
    println!("\n--- Category-Filtered Search ---");
    let query2 = SearchQuery {
        text: "vector embeddings for search".to_string(),
        embedding: generate_embedding("vector embeddings search database", embedding_dim),
        category_filter: Some("Databases".to_string()),
        limit: 2,
    };
    
    println!("Query: {} (Category: Databases)", query2.text);
    let results = db.semantic_search(&query2)?;
    
    for (i, result) in results.iter().enumerate() {
        println!("\n{}. {} (Score: {:.3})", i + 1, result.document.title, result.score);
        println!("   Snippet: {}", result.snippet);
    }
    
    // Example 3: Hybrid search
    println!("\n--- Hybrid Search Example ---");
    let query3 = SearchQuery {
        text: "machine learning database".to_string(),
        embedding: generate_embedding("machine learning database ai", embedding_dim),
        category_filter: None,
        limit: 4,
    };
    
    println!("Query: {} (Hybrid with 0.3 keyword weight)", query3.text);
    let results = db.hybrid_search(&query3, 0.3)?;
    
    for (i, result) in results.iter().enumerate() {
        println!("\n{}. {} (Score: {:.3})", i + 1, result.document.title, result.score);
        println!("   Category: {}", result.document.category);
        println!("   Snippet: {}", result.snippet);
    }
    
    // Example 4: Update document embedding (simulating re-indexing)
    println!("\n--- Updating Document Embedding ---");
    let new_embedding = generate_embedding("database vector ai machine learning embeddings", embedding_dim);
    db.update_document_embedding("doc_003", &new_embedding)?;
    println!("Updated embedding for 'Database Systems Overview'");
    
    // Example 5: Get all documents in a category
    println!("\n--- Documents in AI/ML Category ---");
    let ai_docs = db.get_documents_by_category("AI/ML")?;
    for doc in &ai_docs {
        println!("- {} ({})", doc.title, doc.metadata.get("difficulty").unwrap_or(&"unknown".to_string()));
    }
    
    Ok(())
}