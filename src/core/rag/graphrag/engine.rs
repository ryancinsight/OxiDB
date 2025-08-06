//! GraphRAG engine implementation
//!
//! Core engine for GraphRAG operations following SOLID principles.

use super::types::{GraphRAGContext, GraphRAGResult, KnowledgeNode, KnowledgeEdge, ReasoningPath, GraphRAGConfig};
use crate::core::common::OxidbError;
use crate::core::graph::{GraphStore, NodeId};
use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use std::collections::HashMap;

/// Trait for GraphRAG engines following Interface Segregation Principle
#[async_trait]
pub trait GraphRAGEngine: Send + Sync {
    /// Query the knowledge graph with RAG
    async fn query(&self, context: &GraphRAGContext) -> Result<GraphRAGResult, OxidbError>;

    /// Add a document to the knowledge graph
    async fn add_document(
        &mut self,
        document: &crate::core::rag::document::Document,
    ) -> Result<NodeId, OxidbError>;

    /// Add a relationship between nodes
    async fn add_relationship(
        &mut self,
        source: NodeId,
        target: NodeId,
        relationship_type: &str,
        weight: f64,
    ) -> Result<(), OxidbError>;

    /// Update node embeddings
    async fn update_embeddings(&mut self, node_id: NodeId) -> Result<(), OxidbError>;

    /// Get reasoning paths between nodes
    async fn get_reasoning_paths(
        &self,
        start: NodeId,
        end: NodeId,
        max_depth: usize,
    ) -> Result<Vec<ReasoningPath>, OxidbError>;

    /// Clear the knowledge graph
    async fn clear(&mut self) -> Result<(), OxidbError>;
}

/// Implementation of the GraphRAG engine
pub struct GraphRAGEngineImpl {
    graph_store: Arc<Mutex<dyn GraphStore>>,
    embedder: Arc<dyn crate::core::rag::embedder::EmbeddingModel + Send + Sync>,
    #[allow(dead_code)]
    config: GraphRAGConfig,
    entities: HashMap<NodeId, KnowledgeNode>,
    relationships: HashMap<(NodeId, NodeId), KnowledgeEdge>,
    /// Atomic counter for generating unique node IDs
    next_node_id: Arc<AtomicU64>,
}

impl GraphRAGEngineImpl {
    /// Create a new GraphRAG engine
    pub fn new(
        graph_store: Arc<Mutex<dyn GraphStore>>,
        embedder: Arc<dyn crate::core::rag::embedder::EmbeddingModel + Send + Sync>,
        config: GraphRAGConfig,
    ) -> Self {
        Self {
            graph_store,
            embedder,
            config,
            entities: HashMap::new(),
            relationships: HashMap::new(),
            next_node_id: Arc::new(AtomicU64::new(1)), // Start from 1 to avoid 0 as a special value
        }
    }
    
    /// Generate a unique node ID using atomic counter
    fn generate_node_id(&self) -> NodeId {
        self.next_node_id.fetch_add(1, Ordering::SeqCst)
    }
}

#[async_trait]
impl GraphRAGEngine for GraphRAGEngineImpl {
    async fn query(&self, context: &GraphRAGContext) -> Result<GraphRAGResult, OxidbError> {
        // Retrieve nodes based on query embedding similarity
        let query_embedding = self.embedder.embed(&context.query).await?;
        
        // Collect all matching documents with their scores using zero-cost iterator chains
        let mut matching_docs: Vec<(KnowledgeNode, f64)> = self.entities
            .values()
            .filter_map(|node| {
                node.embedding.as_ref().and_then(|embedding| {
                    match crate::core::vector::similarity::cosine_similarity(&query_embedding.vector, &embedding.vector) {
                        Ok(similarity) => {
                            let similarity_f64 = f64::from(similarity);
                            if similarity_f64 >= context.similarity_threshold {
                                Some((node.clone(), similarity_f64))
                            } else {
                                None
                            }
                        }
                        Err(_) => None,
                    }
                })
            })
            .collect();
        
        // Sort by score descending
        matching_docs.sort_unstable_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        
        // Take only the top max_results
        matching_docs.truncate(context.max_results);
        
        // Separate documents and scores
        let (documents, scores): (Vec<KnowledgeNode>, Vec<f64>) = matching_docs
            .into_iter()
            .unzip();
        
        Ok(GraphRAGResult {
            documents,
            reasoning_paths: Vec::new(),
            scores,
            metadata: HashMap::new(),
        })
    }

    async fn add_document(
        &mut self,
        document: &crate::core::rag::document::Document,
    ) -> Result<NodeId, OxidbError> {
        // Generate embedding for the document
        let embedding = self.embedder.embed(&document.content).await?;
        
        // Generate a unique node ID using atomic counter
        let node_id = self.generate_node_id();
        
        // Create knowledge node from document
        let knowledge_node = KnowledgeNode {
            id: node_id,
            node_type: "document".to_string(),
            content: document.content.clone(),
            embedding: Some(embedding),
            metadata: document.metadata.clone().unwrap_or_default(),
        };
        
        // Store in local cache
        self.entities.insert(node_id, knowledge_node);
        
        // Add to graph store
        let graph_data = crate::core::graph::GraphData::new("document".to_string())
            .with_properties(document.metadata.clone().unwrap_or_default());
        let mut graph_store = self.graph_store.lock().map_err(|_| OxidbError::Internal("Failed to acquire graph_store lock".to_string()))?;
        graph_store.add_node(graph_data)?;
        
        Ok(node_id)
    }

    async fn add_relationship(
        &mut self,
        source: NodeId,
        target: NodeId,
        relationship_type: &str,
        weight: f64,
    ) -> Result<(), OxidbError> {
        // Add the edge to the graph store
        let relationship = crate::core::graph::Relationship {
            name: relationship_type.to_string(),
            direction: crate::core::graph::RelationshipDirection::Outgoing,
        };
        
        let edge_data = Some(crate::core::graph::GraphData::new(relationship_type.to_string())
            .with_property("weight".to_string(), crate::core::common::types::Value::Float(weight)));
        
        let edge_id = self.graph_store.lock().unwrap().add_edge(source, target, relationship, edge_data)?;
        
        // Store knowledge edge
        let edge = KnowledgeEdge {
            id: edge_id,
            source,
            target,
            relationship_type: relationship_type.to_string(),
            weight,
            properties: HashMap::new(),
        };
        self.relationships.insert((source, target), edge);
        Ok(())
    }

    async fn update_embeddings(&mut self, node_id: NodeId) -> Result<(), OxidbError> {
        // Update the embedding for the node using the embedder
        if let Some(node) = self.entities.get_mut(&node_id) {
            let embedding = self.embedder.embed(&node.content).await?;
            node.embedding = Some(embedding);
            Ok(())
        } else {
            Err(OxidbError::NotFound(format!("Node not found: {}", node_id)))
        }
    }

    async fn get_reasoning_paths(
        &self,
        _start: NodeId,
        _end: NodeId,
        _max_depth: usize,
    ) -> Result<Vec<ReasoningPath>, OxidbError> {
        // TODO: Implement path finding algorithm
        // For now, return empty paths
        Ok(Vec::new())
    }

    async fn clear(&mut self) -> Result<(), OxidbError> {
        // Clear local state
        self.entities.clear();
        self.relationships.clear();
        // Graph store clearing would be done through specific methods
        Ok(())
    }
}