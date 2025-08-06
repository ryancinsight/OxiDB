//! Type definitions for GraphRAG
//!
//! This module contains all data structures used by the GraphRAG system,
//! following the Single Responsibility Principle by separating data
//! representation from business logic.

use crate::core::graph::{NodeId, EdgeId};
use crate::core::common::types::Value;
use crate::core::rag::document::Embedding;
use std::collections::HashMap;

/// Represents a knowledge node in the graph with semantic information
#[derive(Debug, Clone)]
pub struct KnowledgeNode {
    /// Unique identifier
    pub id: NodeId,
    /// Node type (e.g., "concept", "entity", "document")
    pub node_type: String,
    /// Node content/description
    pub content: String,
    /// Optional embedding vector for semantic similarity
    pub embedding: Option<Embedding>,
    /// Additional metadata
    pub metadata: HashMap<String, Value>,
}

/// Represents a knowledge edge/relationship in the graph
#[derive(Debug, Clone)]
pub struct KnowledgeEdge {
    /// Unique identifier
    pub id: EdgeId,
    /// Source node
    pub source: NodeId,
    /// Target node
    pub target: NodeId,
    /// Relationship type
    pub relationship_type: String,
    /// Edge weight/strength
    pub weight: f64,
    /// Additional properties
    pub properties: HashMap<String, Value>,
}

/// Context for GraphRAG operations
#[derive(Debug, Clone)]
pub struct GraphRAGContext {
    /// Query or prompt
    pub query: String,
    /// Maximum number of results
    pub max_results: usize,
    /// Minimum similarity threshold
    pub similarity_threshold: f64,
    /// Maximum traversal depth
    pub max_depth: usize,
    /// Additional context parameters
    pub parameters: HashMap<String, Value>,
}

/// Result from GraphRAG query
#[derive(Debug)]
pub struct GraphRAGResult {
    /// Retrieved documents/nodes
    pub documents: Vec<KnowledgeNode>,
    /// Reasoning paths taken
    pub reasoning_paths: Vec<ReasoningPath>,
    /// Relevance scores
    pub scores: Vec<f64>,
    /// Additional metadata
    pub metadata: HashMap<String, Value>,
}

/// Represents a reasoning path through the graph
#[derive(Debug, Clone)]
pub struct ReasoningPath {
    /// Sequence of nodes in the path
    pub nodes: Vec<NodeId>,
    /// Edges connecting the nodes
    pub edges: Vec<EdgeId>,
    /// Total path score
    pub score: f64,
    /// Path description
    pub description: String,
}

/// Configuration for GraphRAG engine
#[derive(Debug, Clone)]
pub struct GraphRAGConfig {
    /// Default similarity threshold
    pub default_similarity_threshold: f64,
    /// Maximum traversal depth
    pub max_traversal_depth: usize,
    /// Enable caching
    pub enable_caching: bool,
}

impl Default for GraphRAGConfig {
    fn default() -> Self {
        Self {
            default_similarity_threshold: 0.7,
            max_traversal_depth: 3,
            enable_caching: true,
        }
    }
}



/// Edge information for graph traversal
#[allow(dead_code)]
pub(super) struct EdgeInfo {
    pub source: NodeId,
    pub target: NodeId,
    pub edge_type: String,
    pub weight: f64,
}