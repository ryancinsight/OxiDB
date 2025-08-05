//! Document Search RAG (Retrieval-Augmented Generation) Example
//! 
//! This example demonstrates using Oxidb for semantic document search with vector embeddings.
//! It simulates a knowledge base system where documents are stored with embeddings
//! and can be searched using natural language queries.

use oxidb::{Connection, Value, OxidbError};
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
    conn: Connection,
    embedding_dimension: usize,
}

impl DocumentSearchDB {
    fn new(db_path: &str, embedding_dimension: usize) -> Result<Self, OxidbError> {
        let mut conn = if db_path == ":memory:" {
            Connection::open_in_memory()?
        } else {
            Connection::open(db_path)?
        };
        
        // Create table for documents with vector embeddings
        let create_table_sql = format!(
            "CREATE TABLE IF NOT EXISTS documents (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                content TEXT NOT NULL,
                embedding VECTOR[{}] NOT NULL,
                metadata TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )",
            embedding_dimension
        );
        
        conn.execute(&create_table_sql)?;
        
        // Create index on embeddings for similarity search
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_doc_embeddings ON documents(embedding)"
        )?;
        
        Ok(Self {
            conn,
            embedding_dimension,
        })
    }
    
    fn add_document(&mut self, doc: Document) -> Result<(), OxidbError> {
        // Generate embedding for the document
        let embedding = generate_embedding(&doc.content, self.embedding_dimension);
        
        // Store document in database using parameterized query
        let sql = "INSERT INTO documents (id, title, content, embedding, metadata) 
                   VALUES (?, ?, ?, ?, ?)";
        
        self.conn.execute_with_params(
            sql,
            &[
                Value::Text(doc.id),
                Value::Text(doc.title),
                Value::Text(doc.content),
                Value::Vector(embedding),
                Value::Text(serde_json::to_string(&doc.metadata).unwrap_or_default()),
            ]
        )?;
        
        Ok(())
    }
    
    fn update_document_embedding(&mut self, doc_id: &str, embedding: &[f32]) -> Result<(), OxidbError> {
        let sql = "UPDATE documents SET embedding = ?, updated_at = ? WHERE id = ?";
        
        self.conn.execute_with_params(
            sql,
            &[
                Value::Vector(embedding.to_vec()),
                Value::Text(Utc::now().to_rfc3339()),
                Value::Text(doc_id.to_string()),
            ]
        )?;
        
        Ok(())
    }
    
    // Semantic search using vector similarity
    fn semantic_search(&mut self, query: SearchQuery) -> Result<Vec<SearchResult>, OxidbError> {
        // Build SQL with conditional WHERE clause
        let (sql, params) = if let Some(category) = &query.category_filter {
            (
                "SELECT *, vector_distance(embedding, ?) as distance 
                 FROM documents 
                 WHERE category = ? 
                 ORDER BY distance ASC 
                 LIMIT ?",
                vec![
                    Value::Vector(query.embedding.clone()),
                    Value::Text(category.clone()),
                    Value::Integer(query.limit as i64),
                ]
            )
        } else {
            (
                "SELECT *, vector_distance(embedding, ?) as distance 
                 FROM documents 
                 ORDER BY distance ASC 
                 LIMIT ?",
                vec![
                    Value::Vector(query.embedding.clone()),
                    Value::Integer(query.limit as i64),
                ]
            )
        };
        
        let result = self.conn.execute_with_params(sql, &params)?;
        
        let mut search_results = Vec::new();
        match result {
            oxidb::QueryResult::Data(data) => {
                for row in &data.rows {
                    let doc = self.row_to_document(row)?;
                    let score = match row.get(10) { // Assuming distance is the 11th column
                        Some(Value::Float(f)) => 1.0 - (*f as f32).min(1.0), // Convert distance to similarity
                        _ => 0.0,
                    };
                    
                    let snippet = self.generate_snippet(&doc.content, &query.text);
                    
                    search_results.push(SearchResult {
                        document: doc,
                        score,
                        snippet,
                    });
                }
            }
            _ => {}
        }
        
        Ok(search_results)
    }
    
    // Hybrid search combining keyword and semantic search
    fn hybrid_search(&mut self, query: &SearchQuery, keyword_weight: f32) -> Result<Vec<SearchResult>, OxidbError> {
        // Semantic search results
        let semantic_results = self.semantic_search(query.clone())?;
        
        // Keyword search with parameterized queries
        let keywords = query.text.split_whitespace()
            .map(|k| k.to_lowercase())
            .collect::<Vec<_>>();
        
        // Build parameterized query for keyword search
        let mut conditions = Vec::new();
        let mut params = Vec::new();
        
        for keyword in &keywords {
            conditions.push("(LOWER(title) LIKE ? OR LOWER(content) LIKE ?)");
            let pattern = format!("%{}%", keyword);
            params.push(Value::Text(pattern.clone()));
            params.push(Value::Text(pattern));
        }
        
        let (sql, final_params) = if let Some(category) = &query.category_filter {
            let sql = format!(
                "SELECT * FROM documents WHERE ({}) AND category = ?",
                conditions.join(" OR ")
            );
            params.push(Value::Text(category.clone()));
            (sql, params)
        } else {
            let sql = format!(
                "SELECT * FROM documents WHERE {}",
                conditions.join(" OR ")
            );
            (sql, params)
        };
        
        let keyword_result = self.conn.execute_with_params(&sql, &final_params)?;
        
        // Combine results with weighted scoring
        let mut combined_results: HashMap<String, SearchResult> = HashMap::new();
        
        // Add semantic results
        for result in semantic_results {
            combined_results.insert(result.document.id.clone(), result);
        }
        
        // Add keyword results
        match keyword_result {
            oxidb::QueryResult::Data(data) => {
                for row in &data.rows {
                    let doc = self.row_to_document(row)?;
                    let doc_id = doc.id.clone();
                    
                    let snippet = self.generate_snippet(&doc.content, &query.text);
                    let keyword_score = keyword_weight;
                    
                    if let Some(existing) = combined_results.get_mut(&doc_id) {
                        // Document found in both searches - combine scores
                        existing.score = existing.score * (1.0 - keyword_weight) + keyword_score;
                    } else {
                        // Document only found in keyword search
                        combined_results.insert(doc_id, SearchResult {
                            document: doc,
                            score: keyword_score,
                            snippet,
                        });
                    }
                }
            }
            _ => {}
        }
        
        // Sort by combined score
        let mut results: Vec<_> = combined_results.into_values().collect();
        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        
        Ok(results.into_iter().take(query.limit).collect())
    }
    
    // Get documents by category
    fn get_documents_by_category(&mut self, category: &str) -> Result<Vec<Document>, OxidbError> {
        let sql = "SELECT * FROM documents WHERE category = ?";
        let result = self.conn.execute_with_params(sql, &[Value::Text(category.to_string())])?;
        
        match result {
            oxidb::QueryResult::Data(data) => {
                data.rows.iter()
                    .map(|row| self.row_to_document(row))
                    .collect()
            }
            _ => Ok(Vec::new()),
        }
    }
    
    // Helper methods
    fn extract_snippet(&self, content: &str, query: &str, max_length: usize) -> String {
        // Find the most relevant part of the content
        let query_words: Vec<&str> = query.split_whitespace().collect();
        
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
    
    // Helper method to generate a snippet from content
    fn generate_snippet(&self, content: &str, query: &str) -> String {
        let query_lower = query.to_lowercase();
        let content_lower = content.to_lowercase();
        
        // Find the first occurrence of any query word
        let words: Vec<&str> = query_lower.split_whitespace().collect();
        let mut best_pos = None;
        
        for word in &words {
            if let Some(pos) = content_lower.find(word) {
                best_pos = Some(best_pos.map_or(pos, |p: usize| p.min(pos)));
            }
        }
        
        let snippet_start = best_pos.unwrap_or(0);
        let snippet_length = 200;
        
        // Extract snippet around the match
        let start = snippet_start.saturating_sub(50);
        let end = (start + snippet_length).min(content.len());
        
        let mut snippet = content[start..end].to_string();
        
        // Add ellipsis if needed
        if start > 0 {
            snippet = format!("...{}", snippet);
        }
        if end < content.len() {
            snippet = format!("{}...", snippet);
        }
        
        snippet
    }
    
    fn row_to_document(&self, row: &oxidb::Row) -> Result<Document, OxidbError> {
        Ok(Document {
            id: match row.get(0).ok_or(OxidbError::NotFound("Column 0 not found".to_string()))? {
                Value::Text(s) => s.clone(),
                _ => String::new(),
            },
            title: match row.get(1).ok_or(OxidbError::NotFound("Column 1 not found".to_string()))? {
                Value::Text(s) => s.clone(),
                _ => String::new(),
            },
            content: match row.get(2).ok_or(OxidbError::NotFound("Column 2 not found".to_string()))? {
                Value::Text(s) => s.clone(),
                _ => String::new(),
            },
            category: match row.get(3).ok_or(OxidbError::NotFound("Column 3 not found".to_string()))? {
                Value::Text(s) => s.clone(),
                _ => String::new(),
            },
            author: match row.get(4).ok_or(OxidbError::NotFound("Column 4 not found".to_string()))? {
                Value::Text(s) => s.clone(),
                _ => String::new(),
            },
            embedding: match row.get(5).ok_or(OxidbError::NotFound("Column 5 not found".to_string()))? {
                Value::Vector(v) => v.clone(),
                _ => vec![],
            },
            metadata: match row.get(6).ok_or(OxidbError::NotFound("Column 6 not found".to_string()))? {
                Value::Text(s) => serde_json::from_str(&s).unwrap_or_default(),
                _ => HashMap::new(),
            },
            created_at: match row.get(7).ok_or(OxidbError::NotFound("Column 7 not found".to_string()))? {
                Value::Text(s) => DateTime::parse_from_rfc3339(&s)
                    .unwrap_or_else(|_| DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z").unwrap())
                    .with_timezone(&Utc),
                _ => Utc::now(),
            },
            updated_at: match row.get(8).ok_or(OxidbError::NotFound("Column 8 not found".to_string()))? {
                Value::Text(s) => DateTime::parse_from_rfc3339(&s)
                    .unwrap_or_else(|_| DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z").unwrap())
                    .with_timezone(&Utc),
                _ => Utc::now(),
            },
        })
    }

}

// Simulated embedding function (in real use, this would use a model like BERT or OpenAI embeddings)
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
    println!("=== Document Search RAG Example ===\n");
    
    let embedding_dim = 384;
    let mut db = DocumentSearchDB::new(":memory:", embedding_dim)?;
    
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
        db.add_document(doc.clone())?;
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
    let results = db.semantic_search(query1)?;
    
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
    let results = db.semantic_search(query2)?;
    
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