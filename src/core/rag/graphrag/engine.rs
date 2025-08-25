//! GraphRAG engine implementation
//!
//! Core engine for GraphRAG operations following SOLID principles.

use super::types::{
    GraphRAGConfig, GraphRAGContext, GraphRAGResult, KnowledgeEdge, KnowledgeNode, ReasoningPath,
};
use crate::core::common::OxidbError;
use crate::core::graph::{EdgeId, GraphStore, NodeId};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

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
    graph_store: Arc<Mutex<Box<dyn GraphStore>>>,
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
        graph_store: Arc<Mutex<Box<dyn GraphStore>>>,
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
        let mut matching_docs: Vec<(KnowledgeNode, f64)> = self
            .entities
            .values()
            .filter_map(|node| {
                node.embedding.as_ref().and_then(|embedding| {
                    match crate::core::vector::similarity::cosine_similarity(
                        &query_embedding.vector,
                        &embedding.vector,
                    ) {
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
        matching_docs.sort_unstable_by(|a, b| {
            match (b.1.is_nan(), a.1.is_nan()) {
                (true, true) => std::cmp::Ordering::Equal,
                (true, false) => std::cmp::Ordering::Greater, // NaN goes to the end
                (false, true) => std::cmp::Ordering::Less,
                (false, false) => b.1.partial_cmp(&a.1).unwrap(),
            }
        });

        // Take only the top max_results
        matching_docs.truncate(context.max_results);

        // Separate documents and scores
        let (documents, scores): (Vec<KnowledgeNode>, Vec<f64>) = matching_docs.into_iter().unzip();

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
        let mut graph_store = self
            .graph_store
            .lock()
            .map_err(|_| OxidbError::Internal("Failed to acquire graph_store lock".to_string()))?;
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

        let edge_data =
            Some(crate::core::graph::GraphData::new(relationship_type.to_string()).with_property(
                "weight".to_string(),
                crate::core::common::types::Value::Float(weight),
            ));

        let mut store = self.graph_store.lock().map_err(|_| {
            OxidbError::Internal(
                "Failed to acquire graph_store lock (possibly poisoned)".to_string(),
            )
        })?;
        let _edge_id = store.add_edge(source, target, relationship, edge_data)?;

        // Store knowledge edge
        let edge = KnowledgeEdge {
            id: 0,
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
            Err(OxidbError::NotFound(format!("Node not found: {node_id}")))
        }
    }

    async fn get_reasoning_paths(
        &self,
        start: NodeId,
        end: NodeId,
        max_depth: usize,
    ) -> Result<Vec<ReasoningPath>, OxidbError> {
        use std::collections::{HashMap, VecDeque};

        if start == end {
            // Return direct path for same node
            return Ok(vec![ReasoningPath {
                nodes: vec![start],
                edges: Vec::new(),
                score: 1.0,
                description: "Direct path to same node".to_string(),
            }]);
        }

        let mut queue = VecDeque::new();
        let mut visited = HashMap::new();
        let mut paths = Vec::new();

        // Initialize with starting node
        queue.push_back((start, Vec::new(), Vec::new(), 0));
        visited.insert(start, 0);

        while let Some((current_node, path_nodes, path_edges, depth)) = queue.pop_front() {
            if depth >= max_depth {
                continue;
            }

            // Check relationships from current node
            for ((source, target), relationship) in &self.relationships {
                let next_node = if *source == current_node {
                    *target
                } else if *target == current_node {
                    *source
                } else {
                    continue;
                };

                // Skip if we've already visited this node at a shorter or equal depth
                if let Some(&previous_depth) = visited.get(&next_node) {
                    if previous_depth <= depth + 1 {
                        continue;
                    }
                }

                visited.insert(next_node, depth + 1);

                let mut new_path_nodes = path_nodes.clone();
                let mut new_path_edges = path_edges.clone();

                if new_path_nodes.is_empty() {
                    new_path_nodes.push(current_node);
                }
                new_path_nodes.push(next_node);
                new_path_edges.push(relationship.id); // Store EdgeId instead of KnowledgeEdge

                // Check if we reached the target
                if next_node == end {
                    let score = self.calculate_path_score(&new_path_nodes, &new_path_edges);
                    let description =
                        self.generate_path_description(&new_path_nodes, &new_path_edges);

                    paths.push(ReasoningPath {
                        nodes: new_path_nodes,
                        edges: new_path_edges,
                        score,
                        description,
                    });
                } else {
                    // Continue searching from this node
                    queue.push_back((next_node, new_path_nodes, new_path_edges, depth + 1));
                }
            }
        }

        // Sort paths by score (highest first)
        paths.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));

        Ok(paths)
    }

    async fn clear(&mut self) -> Result<(), OxidbError> {
        // Clear local state
        self.entities.clear();
        self.relationships.clear();
        Ok(())
    }
}

impl GraphRAGEngineImpl {
    /// Calculate score for a reasoning path based on path length and edge weights
    fn calculate_path_score(&self, _nodes: &[NodeId], edges: &[EdgeId]) -> f64 {
        if edges.is_empty() {
            return 1.0;
        }

        // Calculate score based on path length (shorter paths get higher scores)
        // and average edge weights
        let mut total_weight = 0.0;
        let mut valid_edges = 0;

        for edge_id in edges {
            // Find the relationship with this edge ID
            if let Some((_key, relationship)) =
                self.relationships.iter().find(|(_k, r)| &r.id == edge_id)
            {
                total_weight += relationship.weight;
                valid_edges += 1;
            }
        }

        if valid_edges == 0 {
            return 0.1; // Low score for invalid paths
        }

        let avg_weight = total_weight / valid_edges as f64;
        let length_penalty = 1.0 / (edges.len() as f64).sqrt();

        avg_weight * length_penalty
    }

    /// Generate human-readable description for a reasoning path
    fn generate_path_description(&self, nodes: &[NodeId], edges: &[EdgeId]) -> String {
        if nodes.len() <= 1 {
            return "Direct reference".to_string();
        }

        let mut description = String::new();
        description.push_str("Reasoning path: ");

        for (i, &edge_id) in edges.iter().enumerate() {
            if i > 0 {
                description.push_str(" → ");
            }

            // Find the relationship for this edge ID
            if let Some((_key, relationship)) =
                self.relationships.iter().find(|(_k, r)| r.id == edge_id)
            {
                let source_name = self
                    .entities
                    .get(&relationship.source)
                    .map(|e| e.content.as_str())
                    .unwrap_or("Unknown");
                let target_name = self
                    .entities
                    .get(&relationship.target)
                    .map(|e| e.content.as_str())
                    .unwrap_or("Unknown");

                description.push_str(&format!(
                    "{} --[{}]--> {}",
                    source_name, relationship.relationship_type, target_name
                ));
            } else {
                description.push_str("Unknown connection");
            }
        }

        description
    }
}
