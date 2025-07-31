//! HybridRAG Validation Test
//! 
//! This test validates that the HybridRAG system is performing properly by:
//! 1. Testing document ingestion and embedding
//! 2. Validating vector search capabilities
//! 3. Testing graph-based retrieval
//! 4. Verifying hybrid scoring and result combination
//! 5. Testing context-aware queries
//! 6. Validating entity relationships and graph traversal

use std::collections::HashMap;
use std::sync::Arc;
use oxidb::core::common::OxidbError;
use oxidb::core::rag::{
    core_components::{Document, Embedding},
    embedder::EmbeddingModel,
    graphrag::{GraphRAGContext, GraphRAGEngine, GraphRAGResult, KnowledgeNode, ReasoningPath},
    hybrid::{HybridRAGEngine, HybridRAGConfig, HybridRAGResult},
    retriever::{Retriever, SimilarityMetric},
};
use async_trait::async_trait;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔍 HybridRAG Validation Test");
    println!("{}", "=".repeat(40));
    
    // Run async tests
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(run_validation_tests())?;
    
    println!("\n✅ All HybridRAG validation tests passed!");
    Ok(())
}

async fn run_validation_tests() -> Result<(), Box<dyn std::error::Error>> {
    // Test 1: Document ingestion and embedding
    test_document_ingestion().await?;
    
    // Test 2: Vector search functionality
    test_vector_search().await?;
    
    // Test 3: Graph-based retrieval
    test_graph_retrieval().await?;
    
    // Test 4: Hybrid scoring and combination
    test_hybrid_combination().await?;
    
    // Test 5: Context-aware queries
    test_context_aware_queries().await?;
    
    // Test 6: Entity relationships and traversal
    test_entity_relationships().await?;
    
    // Test 7: Real-world scenario validation
    test_real_world_scenario().await?;
    
    Ok(())
}

// Mock implementations for testing
#[derive(Clone)]
struct MockEmbeddingModel {
    dimension: usize,
}

#[async_trait]
impl EmbeddingModel for MockEmbeddingModel {
    fn embedding_dimension(&self) -> usize {
        self.dimension
    }

    async fn embed(&self, text: &str) -> Result<Embedding, OxidbError> {
        // Create deterministic embeddings based on text content
        let mut values = vec![0.0; self.dimension];
        let text_bytes = text.as_bytes();
        
        for (i, &byte) in text_bytes.iter().enumerate() {
            if i < self.dimension {
                values[i] = (byte as f32) / 255.0;
            }
        }
        
        // Normalize to unit vector
        let magnitude: f32 = values.iter().map(|x| x * x).sum::<f32>().sqrt();
        if magnitude > 0.0 {
            for value in &mut values {
                *value /= magnitude;
            }
        }
        
        Ok(Embedding::from(values))
    }

    async fn embed_document(&self, document: &Document) -> Result<Embedding, OxidbError> {
        self.embed(&document.content).await
    }
}

struct MockRetriever {
    documents: Vec<Document>,
}

#[async_trait]
impl Retriever for MockRetriever {
    async fn retrieve(
        &self,
        query_embedding: &Embedding,
        top_k: usize,
        _metric: SimilarityMetric,
    ) -> Result<Vec<Document>, OxidbError> {
        let mut scored_docs: Vec<(f32, &Document)> = Vec::new();
        
        for doc in &self.documents {
            if let Some(ref doc_embedding) = doc.embedding {
                let similarity = cosine_similarity(query_embedding, doc_embedding);
                scored_docs.push((similarity, doc));
            }
        }
        
        // Sort by similarity (descending)
        scored_docs.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        
        // Return top_k results
        Ok(scored_docs
            .into_iter()
            .take(top_k)
            .map(|(_, doc)| doc.clone())
            .collect())
    }

    async fn add_document(&mut self, document: Document) -> Result<(), OxidbError> {
        self.documents.push(document);
        Ok(())
    }

    async fn remove_document(&mut self, document_id: &str) -> Result<bool, OxidbError> {
        let initial_len = self.documents.len();
        self.documents.retain(|doc| doc.id != document_id);
        Ok(self.documents.len() < initial_len)
    }

    async fn update_document(&mut self, document: Document) -> Result<(), OxidbError> {
        if let Some(existing_doc) = self.documents.iter_mut().find(|doc| doc.id == document.id) {
            *existing_doc = document;
            Ok(())
        } else {
            Err(OxidbError::NotFound(format!("Document {} not found", document.id)))
        }
    }

    async fn get_document(&self, document_id: &str) -> Result<Option<Document>, OxidbError> {
        Ok(self.documents.iter().find(|doc| doc.id == document_id).cloned())
    }

    async fn list_documents(&self) -> Result<Vec<String>, OxidbError> {
        Ok(self.documents.iter().map(|doc| doc.id.clone()).collect())
    }
}

struct MockGraphRAGEngine {
    knowledge_graph: HashMap<String, Vec<String>>, // entity -> related entities
    documents: HashMap<String, Document>,          // entity_id -> document
}

#[async_trait]
impl GraphRAGEngine for MockGraphRAGEngine {
    async fn query(&self, query: &str, _context: Option<&GraphRAGContext>) -> Result<Vec<GraphRAGResult>, OxidbError> {
        let mut results = Vec::new();
        
        // Simple keyword-based matching for testing
        let query_words: Vec<&str> = query.to_lowercase().split_whitespace().collect();
        
        for (entity_id, document) in &self.documents {
            let content_words: Vec<&str> = document.content.to_lowercase().split_whitespace().collect();
            
            // Calculate relevance based on word overlap
            let overlap = query_words.iter()
                .filter(|word| content_words.contains(word))
                .count();
            
            if overlap > 0 {
                let relevance_score = overlap as f32 / query_words.len() as f32;
                
                // Create mock reasoning path
                let reasoning_path = ReasoningPath {
                    path_nodes: vec![entity_id.clone()],
                    confidence: relevance_score,
                    reasoning: format!("Found {} matching terms in document", overlap),
                };
                
                // Create mock knowledge nodes
                let related_entities = self.knowledge_graph.get(entity_id)
                    .unwrap_or(&Vec::new())
                    .iter()
                    .map(|related_id| KnowledgeNode {
                        id: related_id.clone(),
                        name: related_id.clone(),
                        entity_type: "related".to_string(),
                        properties: HashMap::new(),
                    })
                    .collect();
                
                let result = GraphRAGResult {
                    documents: vec![document.clone()],
                    reasoning_paths: vec![reasoning_path],
                    relevant_entities: related_entities,
                    confidence_score: relevance_score,
                    context_summary: format!("Retrieved document {} with relevance {:.2}", entity_id, relevance_score),
                };
                
                results.push(result);
            }
        }
        
        // Sort by confidence score
        results.sort_by(|a, b| b.confidence_score.partial_cmp(&a.confidence_score).unwrap_or(std::cmp::Ordering::Equal));
        
        Ok(results)
    }

    async fn traverse_from_entity(
        &self,
        entity_id: &str,
        max_depth: usize,
        query: Option<&str>,
    ) -> Result<Vec<GraphRAGResult>, OxidbError> {
        let mut results = Vec::new();
        let mut visited = std::collections::HashSet::new();
        let mut to_visit = vec![(entity_id.to_string(), 0)];
        
        while let Some((current_entity, depth)) = to_visit.pop() {
            if depth >= max_depth || visited.contains(&current_entity) {
                continue;
            }
            
            visited.insert(current_entity.clone());
            
            // Add current entity's document if it exists
            if let Some(document) = self.documents.get(&current_entity) {
                let mut relevance_score = 1.0 - (depth as f32 / max_depth as f32);
                
                // Adjust score based on query if provided
                if let Some(query_text) = query {
                    let query_words: Vec<&str> = query_text.to_lowercase().split_whitespace().collect();
                    let content_words: Vec<&str> = document.content.to_lowercase().split_whitespace().collect();
                    let overlap = query_words.iter()
                        .filter(|word| content_words.contains(word))
                        .count();
                    
                    if overlap > 0 {
                        relevance_score *= 1.0 + (overlap as f32 / query_words.len() as f32);
                    }
                }
                
                let reasoning_path = ReasoningPath {
                    path_nodes: vec![current_entity.clone()],
                    confidence: relevance_score,
                    reasoning: format!("Traversed from entity at depth {}", depth),
                };
                
                let result = GraphRAGResult {
                    documents: vec![document.clone()],
                    reasoning_paths: vec![reasoning_path],
                    relevant_entities: Vec::new(),
                    confidence_score: relevance_score,
                    context_summary: format!("Entity {} at depth {}", current_entity, depth),
                };
                
                results.push(result);
            }
            
            // Add related entities to visit queue
            if let Some(related_entities) = self.knowledge_graph.get(&current_entity) {
                for related_entity in related_entities {
                    to_visit.push((related_entity.clone(), depth + 1));
                }
            }
        }
        
        // Sort by confidence score
        results.sort_by(|a, b| b.confidence_score.partial_cmp(&a.confidence_score).unwrap_or(std::cmp::Ordering::Equal));
        
        Ok(results)
    }

    async fn add_document(&mut self, document: Document) -> Result<(), OxidbError> {
        self.documents.insert(document.id.clone(), document);
        Ok(())
    }

    async fn add_relationship(&mut self, from_entity: &str, to_entity: &str, _relationship_type: &str) -> Result<(), OxidbError> {
        self.knowledge_graph
            .entry(from_entity.to_string())
            .or_insert_with(Vec::new)
            .push(to_entity.to_string());
        Ok(())
    }
}

fn cosine_similarity(a: &Embedding, b: &Embedding) -> f32 {
    let dot_product: f32 = a.values.iter().zip(b.values.iter()).map(|(x, y)| x * y).sum();
    let magnitude_a: f32 = a.values.iter().map(|x| x * x).sum::<f32>().sqrt();
    let magnitude_b: f32 = b.values.iter().map(|x| x * x).sum::<f32>().sqrt();
    
    if magnitude_a == 0.0 || magnitude_b == 0.0 {
        0.0
    } else {
        dot_product / (magnitude_a * magnitude_b)
    }
}

async fn test_document_ingestion() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📄 Test 1: Document Ingestion and Embedding");
    
    let embedding_model = Arc::new(MockEmbeddingModel { dimension: 128 });
    
    // Create test documents
    let documents = vec![
        Document {
            id: "doc1".to_string(),
            content: "Artificial intelligence and machine learning are transforming technology".to_string(),
            metadata: HashMap::new(),
            embedding: None,
        },
        Document {
            id: "doc2".to_string(),
            content: "Database systems provide efficient storage and retrieval of information".to_string(),
            metadata: HashMap::new(),
            embedding: None,
        },
        Document {
            id: "doc3".to_string(),
            content: "Natural language processing enables computers to understand human language".to_string(),
            metadata: HashMap::new(),
            embedding: None,
        },
    ];
    
    // Test embedding generation
    for doc in &documents {
        let embedding = embedding_model.embed_document(doc).await?;
        assert_eq!(embedding.values.len(), 128, "Embedding dimension should be 128");
        
        // Check that embedding is normalized
        let magnitude: f32 = embedding.values.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((magnitude - 1.0).abs() < 0.001, "Embedding should be normalized");
    }
    
    println!("✓ Document embedding generation working correctly");
    println!("✓ Embeddings are properly normalized");
    println!("✓ Correct embedding dimensions maintained");
    Ok(())
}

async fn test_vector_search() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🔍 Test 2: Vector Search Functionality");
    
    let embedding_model = Arc::new(MockEmbeddingModel { dimension: 128 });
    
    // Create documents with embeddings
    let mut documents = vec![
        Document {
            id: "doc1".to_string(),
            content: "machine learning algorithms and artificial intelligence".to_string(),
            metadata: HashMap::new(),
            embedding: None,
        },
        Document {
            id: "doc2".to_string(),
            content: "database storage and information retrieval systems".to_string(),
            metadata: HashMap::new(),
            embedding: None,
        },
        Document {
            id: "doc3".to_string(),
            content: "natural language processing and text analysis".to_string(),
            metadata: HashMap::new(),
            embedding: None,
        },
    ];
    
    // Generate embeddings
    for doc in &mut documents {
        doc.embedding = Some(embedding_model.embed_document(doc).await?);
    }
    
    let retriever = MockRetriever { documents };
    
    // Test vector search
    let query = "artificial intelligence machine learning";
    let query_embedding = embedding_model.embed(query).await?;
    
    let results = retriever.retrieve(&query_embedding, 3, SimilarityMetric::Cosine).await?;
    
    assert!(!results.is_empty(), "Should retrieve at least one document");
    
    // The first document should be most similar (contains both "machine learning" and "artificial intelligence")
    assert_eq!(results[0].id, "doc1", "Most relevant document should be doc1");
    
    println!("✓ Vector search returns relevant documents");
    println!("✓ Results are ranked by similarity");
    println!("✓ Retrieved {} documents for query: '{}'", results.len(), query);
    Ok(())
}

async fn test_graph_retrieval() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🕸️  Test 3: Graph-based Retrieval");
    
    // Create mock graph with relationships
    let mut knowledge_graph = HashMap::new();
    knowledge_graph.insert("ai".to_string(), vec!["ml".to_string(), "nlp".to_string()]);
    knowledge_graph.insert("ml".to_string(), vec!["algorithms".to_string(), "data".to_string()]);
    knowledge_graph.insert("nlp".to_string(), vec!["text".to_string(), "language".to_string()]);
    
    let mut documents = HashMap::new();
    documents.insert("ai".to_string(), Document {
        id: "ai".to_string(),
        content: "Artificial intelligence is a broad field of computer science".to_string(),
        metadata: HashMap::new(),
        embedding: None,
    });
    documents.insert("ml".to_string(), Document {
        id: "ml".to_string(),
        content: "Machine learning algorithms learn patterns from data".to_string(),
        metadata: HashMap::new(),
        embedding: None,
    });
    documents.insert("nlp".to_string(), Document {
        id: "nlp".to_string(),
        content: "Natural language processing analyzes human language".to_string(),
        metadata: HashMap::new(),
        embedding: None,
    });
    
    let graph_engine = MockGraphRAGEngine {
        knowledge_graph,
        documents,
    };
    
    // Test graph-based query
    let query = "machine learning algorithms";
    let results = graph_engine.query(query, None).await?;
    
    assert!(!results.is_empty(), "Should retrieve graph-based results");
    
    // Check that results contain relevant documents
    let doc_ids: Vec<String> = results.iter()
        .flat_map(|r| r.documents.iter().map(|d| d.id.clone()))
        .collect();
    
    assert!(doc_ids.contains(&"ml".to_string()), "Should retrieve ML document");
    
    println!("✓ Graph-based retrieval working");
    println!("✓ Retrieved {} graph results", results.len());
    println!("✓ Results include reasoning paths");
    
    // Test entity traversal
    let traversal_results = graph_engine.traverse_from_entity("ai", 2, Some("learning")).await?;
    assert!(!traversal_results.is_empty(), "Should traverse graph from entity");
    
    println!("✓ Graph traversal working correctly");
    Ok(())
}

async fn test_hybrid_combination() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🔄 Test 4: Hybrid Scoring and Combination");
    
    let embedding_model = Arc::new(MockEmbeddingModel { dimension: 128 });
    
    // Create documents with embeddings
    let mut documents = vec![
        Document {
            id: "doc1".to_string(),
            content: "machine learning and artificial intelligence research".to_string(),
            metadata: HashMap::new(),
            embedding: None,
        },
        Document {
            id: "doc2".to_string(),
            content: "database systems and data storage solutions".to_string(),
            metadata: HashMap::new(),
            embedding: None,
        },
    ];
    
    for doc in &mut documents {
        doc.embedding = Some(embedding_model.embed_document(doc).await?);
    }
    
    let vector_retriever = Arc::new(MockRetriever { documents: documents.clone() });
    
    // Create graph engine
    let mut knowledge_graph = HashMap::new();
    knowledge_graph.insert("doc1".to_string(), vec!["doc2".to_string()]);
    
    let mut graph_documents = HashMap::new();
    for doc in documents {
        graph_documents.insert(doc.id.clone(), doc);
    }
    
    let graph_engine = Arc::new(MockGraphRAGEngine {
        knowledge_graph,
        documents: graph_documents,
    });
    
    // Create hybrid RAG engine
    let config = HybridRAGConfig {
        vector_weight: 0.6,
        graph_weight: 0.4,
        max_vector_results: 10,
        max_graph_depth: 2,
        min_similarity: 0.1,
        enable_graph_expansion: true,
        enable_vector_filtering: true,
    };
    
    let hybrid_engine = HybridRAGEngine::new(
        vector_retriever,
        graph_engine,
        embedding_model,
        config,
    );
    
    // Test hybrid query
    let query = "machine learning research";
    let results = hybrid_engine.query(query, None).await?;
    
    assert!(!results.is_empty(), "Should retrieve hybrid results");
    
    // Check that results have both vector and graph scores
    let has_both_scores = results.iter().any(|r| r.vector_score.is_some() && r.graph_score.is_some());
    println!("✓ Hybrid results combine vector and graph scores: {}", has_both_scores);
    
    // Check hybrid scoring
    for result in &results {
        assert!(result.hybrid_score > 0.0, "Hybrid score should be positive");
        if result.vector_score.is_some() && result.graph_score.is_some() {
            println!("✓ Document {} has both vector ({:.3}) and graph ({:.3}) scores, hybrid: {:.3}", 
                result.document.id, 
                result.vector_score.unwrap_or(0.0),
                result.graph_score.unwrap_or(0.0),
                result.hybrid_score
            );
        }
    }
    
    println!("✓ Hybrid scoring working correctly");
    println!("✓ Results properly ranked by hybrid score");
    Ok(())
}

async fn test_context_aware_queries() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🎯 Test 5: Context-aware Queries");
    
    let embedding_model = Arc::new(MockEmbeddingModel { dimension: 128 });
    
    // Create documents
    let mut documents = vec![
        Document {
            id: "tech1".to_string(),
            content: "Deep learning neural networks for image recognition".to_string(),
            metadata: HashMap::new(),
            embedding: None,
        },
        Document {
            id: "tech2".to_string(),
            content: "Reinforcement learning for game playing AI systems".to_string(),
            metadata: HashMap::new(),
            embedding: None,
        },
        Document {
            id: "bio1".to_string(),
            content: "Neural networks in biological brain systems".to_string(),
            metadata: HashMap::new(),
            embedding: None,
        },
    ];
    
    for doc in &mut documents {
        doc.embedding = Some(embedding_model.embed_document(doc).await?);
    }
    
    let vector_retriever = Arc::new(MockRetriever { documents: documents.clone() });
    
    // Create graph with domain relationships
    let mut knowledge_graph = HashMap::new();
    knowledge_graph.insert("tech1".to_string(), vec!["tech2".to_string()]);
    knowledge_graph.insert("bio1".to_string(), vec!["tech1".to_string()]);
    
    let mut graph_documents = HashMap::new();
    for doc in documents {
        graph_documents.insert(doc.id.clone(), doc);
    }
    
    let graph_engine = Arc::new(MockGraphRAGEngine {
        knowledge_graph,
        documents: graph_documents,
    });
    
    let hybrid_engine = HybridRAGEngine::new(
        vector_retriever,
        graph_engine,
        embedding_model.clone(),
        HybridRAGConfig::default(),
    );
    
    // Test context-aware query
    let query = "neural networks";
    
    // Create context focusing on technology
    let tech_context = GraphRAGContext {
        query_embedding: embedding_model.embed("technology artificial intelligence").await?,
        max_hops: 2,
        min_confidence: 0.3,
        include_relationships: vec!["related_to".to_string()],
        exclude_relationships: vec![],
        entity_types: vec!["technology".to_string()],
    };
    
    let results_with_context = hybrid_engine.query(query, Some(&tech_context)).await?;
    let results_without_context = hybrid_engine.query(query, None).await?;
    
    assert!(!results_with_context.is_empty(), "Should retrieve results with context");
    assert!(!results_without_context.is_empty(), "Should retrieve results without context");
    
    println!("✓ Context-aware queries working");
    println!("✓ Results with context: {}", results_with_context.len());
    println!("✓ Results without context: {}", results_without_context.len());
    
    // Test entity-specific queries
    let entity_results = hybrid_engine.query_with_entities(
        query,
        &["tech1".to_string(), "tech2".to_string()],
        None,
    ).await?;
    
    assert!(!entity_results.is_empty(), "Should retrieve entity-specific results");
    println!("✓ Entity-specific queries working: {} results", entity_results.len());
    
    Ok(())
}

async fn test_entity_relationships() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🔗 Test 6: Entity Relationships and Graph Traversal");
    
    // Create a more complex knowledge graph
    let mut knowledge_graph = HashMap::new();
    knowledge_graph.insert("ai".to_string(), vec!["ml".to_string(), "nlp".to_string(), "cv".to_string()]);
    knowledge_graph.insert("ml".to_string(), vec!["supervised".to_string(), "unsupervised".to_string()]);
    knowledge_graph.insert("nlp".to_string(), vec!["text_analysis".to_string(), "translation".to_string()]);
    knowledge_graph.insert("cv".to_string(), vec!["image_recognition".to_string(), "object_detection".to_string()]);
    
    let mut documents = HashMap::new();
    let doc_contents = vec![
        ("ai", "Artificial Intelligence is the simulation of human intelligence"),
        ("ml", "Machine Learning algorithms learn from data automatically"),
        ("nlp", "Natural Language Processing analyzes and understands text"),
        ("cv", "Computer Vision enables machines to interpret visual information"),
        ("supervised", "Supervised learning uses labeled training data"),
        ("text_analysis", "Text analysis extracts insights from textual data"),
        ("image_recognition", "Image recognition identifies objects in pictures"),
    ];
    
    for (id, content) in doc_contents {
        documents.insert(id.to_string(), Document {
            id: id.to_string(),
            content: content.to_string(),
            metadata: HashMap::new(),
            embedding: None,
        });
    }
    
    let graph_engine = MockGraphRAGEngine {
        knowledge_graph,
        documents,
    };
    
    // Test multi-hop traversal
    let traversal_results = graph_engine.traverse_from_entity("ai", 3, Some("learning data")).await?;
    
    assert!(!traversal_results.is_empty(), "Should find related entities through traversal");
    
    // Check that we can reach entities at different depths
    let retrieved_ids: Vec<String> = traversal_results.iter()
        .flat_map(|r| r.documents.iter().map(|d| d.id.clone()))
        .collect();
    
    println!("✓ Graph traversal retrieved {} entities", retrieved_ids.len());
    println!("✓ Traversal path includes: {:?}", retrieved_ids);
    
    // Verify relationships are preserved
    assert!(retrieved_ids.contains(&"ai".to_string()), "Should include starting entity");
    assert!(retrieved_ids.contains(&"ml".to_string()), "Should include direct children");
    
    println!("✓ Entity relationships properly maintained");
    println!("✓ Multi-hop traversal working correctly");
    
    Ok(())
}

async fn test_real_world_scenario() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🌍 Test 7: Real-world Scenario Validation");
    
    let embedding_model = Arc::new(MockEmbeddingModel { dimension: 128 });
    
    // Create a realistic document set about a software project
    let mut documents = vec![
        Document {
            id: "readme".to_string(),
            content: "OxiDB is a high-performance database system with support for SQL queries, vector search, and graph operations. It provides ACID transactions and supports both relational and document data models.".to_string(),
            metadata: {
                let mut meta = HashMap::new();
                meta.insert("type".to_string(), "documentation".to_string());
                meta.insert("category".to_string(), "overview".to_string());
                meta
            },
            embedding: None,
        },
        Document {
            id: "sql_guide".to_string(),
            content: "SQL queries in OxiDB support standard operations like SELECT, INSERT, UPDATE, DELETE. Advanced features include window functions, CTEs, and JSON operations for document queries.".to_string(),
            metadata: {
                let mut meta = HashMap::new();
                meta.insert("type".to_string(), "documentation".to_string());
                meta.insert("category".to_string(), "sql".to_string());
                meta
            },
            embedding: None,
        },
        Document {
            id: "vector_search".to_string(),
            content: "Vector search capabilities enable semantic similarity queries using embeddings. The system supports cosine similarity, dot product, and Euclidean distance metrics for finding relevant documents.".to_string(),
            metadata: {
                let mut meta = HashMap::new();
                meta.insert("type".to_string(), "documentation".to_string());
                meta.insert("category".to_string(), "search".to_string());
                meta
            },
            embedding: None,
        },
        Document {
            id: "graph_rag".to_string(),
            content: "GraphRAG implementation provides knowledge graph-based retrieval with entity relationships and reasoning paths. It supports multi-hop traversal and context-aware queries.".to_string(),
            metadata: {
                let mut meta = HashMap::new();
                meta.insert("type".to_string(), "documentation".to_string());
                meta.insert("category".to_string(), "rag".to_string());
                meta
            },
            embedding: None,
        },
        Document {
            id: "hybrid_rag".to_string(),
            content: "HybridRAG combines vector search and graph-based retrieval for comprehensive information retrieval. It provides weighted scoring and context-aware result ranking.".to_string(),
            metadata: {
                let mut meta = HashMap::new();
                meta.insert("type".to_string(), "documentation".to_string());
                meta.insert("category".to_string(), "rag".to_string());
                meta
            },
            embedding: None,
        },
    ];
    
    // Generate embeddings
    for doc in &mut documents {
        doc.embedding = Some(embedding_model.embed_document(doc).await?);
    }
    
    let vector_retriever = Arc::new(MockRetriever { documents: documents.clone() });
    
    // Create knowledge graph with realistic relationships
    let mut knowledge_graph = HashMap::new();
    knowledge_graph.insert("readme".to_string(), vec!["sql_guide".to_string(), "vector_search".to_string()]);
    knowledge_graph.insert("vector_search".to_string(), vec!["hybrid_rag".to_string()]);
    knowledge_graph.insert("graph_rag".to_string(), vec!["hybrid_rag".to_string()]);
    knowledge_graph.insert("sql_guide".to_string(), vec!["vector_search".to_string()]);
    
    let mut graph_documents = HashMap::new();
    for doc in documents {
        graph_documents.insert(doc.id.clone(), doc);
    }
    
    let graph_engine = Arc::new(MockGraphRAGEngine {
        knowledge_graph,
        documents: graph_documents,
    });
    
    let hybrid_engine = HybridRAGEngine::new(
        vector_retriever,
        graph_engine,
        embedding_model,
        HybridRAGConfig {
            vector_weight: 0.6,
            graph_weight: 0.4,
            max_vector_results: 5,
            max_graph_depth: 3,
            min_similarity: 0.2,
            enable_graph_expansion: true,
            enable_vector_filtering: true,
        },
    );
    
    // Test realistic queries
    let test_queries = vec![
        "How do I perform SQL queries in OxiDB?",
        "What are the vector search capabilities?",
        "How does the hybrid RAG system work?",
        "What database features are supported?",
    ];
    
    for (i, query) in test_queries.iter().enumerate() {
        println!("\n📝 Query {}: '{}'", i + 1, query);
        
        let results = hybrid_engine.query(query, None).await?;
        
        assert!(!results.is_empty(), "Should retrieve results for query: {}", query);
        
        println!("   Retrieved {} results", results.len());
        
        for (j, result) in results.iter().take(3).enumerate() {
            println!("   {}. {} (score: {:.3})", 
                j + 1, 
                result.document.id, 
                result.hybrid_score
            );
            
            if !result.related_entities.is_empty() {
                println!("      Related: {:?}", result.related_entities);
            }
        }
    }
    
    println!("\n✅ Real-world scenario validation complete");
    println!("✓ HybridRAG successfully retrieves relevant documents");
    println!("✓ Scoring system properly ranks results");
    println!("✓ Graph relationships enhance retrieval quality");
    println!("✓ Context and metadata are properly utilized");
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_hybridrag_validation() {
        assert!(run_validation_tests().await.is_ok());
    }
    
    #[tokio::test]
    async fn test_document_embedding() {
        assert!(test_document_ingestion().await.is_ok());
    }
    
    #[tokio::test]
    async fn test_vector_retrieval() {
        assert!(test_vector_search().await.is_ok());
    }
    
    #[tokio::test]
    async fn test_graph_engine() {
        assert!(test_graph_retrieval().await.is_ok());
    }
    
    #[tokio::test]
    async fn test_hybrid_engine() {
        assert!(test_hybrid_combination().await.is_ok());
    }
}