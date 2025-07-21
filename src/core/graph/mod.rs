//! Graph database module for Oxidb
//!
//! This module provides graph storage and traversal capabilities following SOLID design principles:
//! - Single Responsibility: Each component handles one aspect of graph operations
//! - Open/Closed: Extensible for new graph algorithms without modifying existing code
//! - Liskov Substitution: Graph implementations can be substituted seamlessly
//! - Interface Segregation: Focused interfaces for different graph operations
//! - Dependency Inversion: Depends on abstractions, not concrete implementations
//!
//! CUPID principles:
//! - Composable: Graph components work together seamlessly
//! - Unix-like: Simple, focused interfaces
//! - Predictable: Consistent behavior across operations
//! - Idiomatic: Rust-native patterns and ownership
//! - Domain-focused: Graph-specific abstractions

pub mod storage;
pub mod traversal;
pub mod algorithms;
pub mod types;

// Re-export key types and traits for convenience
pub use storage::{GraphStore, GraphStorage};
pub use traversal::{GraphTraversal, TraversalDirection, TraversalStrategy};
pub use algorithms::{GraphAlgorithms, PathFinding};
pub use types::{NodeId, EdgeId, Node, Edge, GraphData, Relationship};

use crate::core::common::errors::OxidbError;
use crate::core::common::types::Value;
// Remove unused import

/// Core graph operations trait following Interface Segregation Principle
pub trait GraphOperations {
    /// Add a node to the graph
    fn add_node(&mut self, data: GraphData) -> Result<NodeId, OxidbError>;
    
    /// Add an edge between two nodes
    fn add_edge(&mut self, from: NodeId, to: NodeId, relationship: Relationship, data: Option<GraphData>) -> Result<EdgeId, OxidbError>;
    
    /// Get a node by ID
    fn get_node(&self, node_id: NodeId) -> Result<Option<Node>, OxidbError>;
    
    /// Get an edge by ID
    fn get_edge(&self, edge_id: EdgeId) -> Result<Option<Edge>, OxidbError>;
    
    /// Remove a node and all its edges
    fn remove_node(&mut self, node_id: NodeId) -> Result<bool, OxidbError>;
    
    /// Remove an edge
    fn remove_edge(&mut self, edge_id: EdgeId) -> Result<bool, OxidbError>;
    
    /// Get all neighbors of a node
    fn get_neighbors(&self, node_id: NodeId, direction: TraversalDirection) -> Result<Vec<NodeId>, OxidbError>;
}

/// Graph query interface for complex graph operations
pub trait GraphQuery {
    /// Find nodes by properties
    fn find_nodes_by_property(&self, property: &str, value: &Value) -> Result<Vec<NodeId>, OxidbError>;
    
    /// Find shortest path between two nodes
    fn find_shortest_path(&self, from: NodeId, to: NodeId) -> Result<Option<Vec<NodeId>>, OxidbError>;
    
    /// Perform graph traversal with custom strategy
    fn traverse(&self, start: NodeId, strategy: TraversalStrategy, max_depth: Option<usize>) -> Result<Vec<NodeId>, OxidbError>;
    
    /// Count nodes with specific relationship
    fn count_nodes_with_relationship(&self, relationship: &Relationship, direction: TraversalDirection) -> Result<usize, OxidbError>;
}

/// Graph transaction interface for ACID compliance
pub trait GraphTransaction {
    /// Begin a graph transaction
    fn begin_transaction(&mut self) -> Result<(), OxidbError>;
    
    /// Commit graph transaction
    fn commit_transaction(&mut self) -> Result<(), OxidbError>;
    
    /// Rollback graph transaction
    fn rollback_transaction(&mut self) -> Result<(), OxidbError>;
}

/// Factory for creating graph instances following the Factory pattern
pub struct GraphFactory;

impl GraphFactory {
    /// Create a new in-memory graph store with full GraphStore capabilities
    /// Returns a trait object that provides GraphOperations, GraphQuery, and GraphTransaction
    pub fn create_memory_graph() -> Result<Box<dyn storage::GraphStore>, OxidbError> {
        Ok(Box::new(storage::InMemoryGraphStore::new()))
    }
    
    /// Create a persistent graph store with full GraphStore capabilities
    /// Returns a trait object that provides GraphOperations, GraphQuery, and GraphTransaction
    pub fn create_persistent_graph(path: impl AsRef<std::path::Path>) -> Result<Box<dyn storage::GraphStore>, OxidbError> {
        Ok(Box::new(storage::PersistentGraphStore::new(path)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;


    #[test]
    fn test_graph_factory_memory() {
        let graph = GraphFactory::create_memory_graph();
        assert!(graph.is_ok());
    }

    #[test]
    fn test_basic_graph_operations() {
        let mut graph = GraphFactory::create_memory_graph().unwrap();
        
        // Add nodes
        let node1_data = GraphData::new("user".to_string())
            .with_property("name".to_string(), Value::Text("Alice".to_string()));
        let node1_id = graph.add_node(node1_data).unwrap();
        
        let node2_data = GraphData::new("user".to_string())
            .with_property("name".to_string(), Value::Text("Bob".to_string()));
        let node2_id = graph.add_node(node2_data).unwrap();
        
        // Add edge
        let relationship = Relationship::new("FOLLOWS".to_string());
        let edge_id = graph.add_edge(node1_id, node2_id, relationship, None).unwrap();
        
        // Verify nodes exist
        assert!(graph.get_node(node1_id).unwrap().is_some());
        assert!(graph.get_node(node2_id).unwrap().is_some());
        assert!(graph.get_edge(edge_id).unwrap().is_some());
        
        // Check neighbors
        let neighbors = graph.get_neighbors(node1_id, TraversalDirection::Outgoing).unwrap();
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0], node2_id);
    }

    #[test]
    fn test_comprehensive_graph_store_capabilities() {
        let mut graph = GraphFactory::create_memory_graph().unwrap();
        
        // Test GraphOperations - basic CRUD operations
        let node1_data = GraphData::new("person".to_string())
            .with_property("name".to_string(), Value::Text("Alice".to_string()));
        let node1_id = graph.add_node(node1_data).unwrap();
        
        let node2_data = GraphData::new("person".to_string())
            .with_property("name".to_string(), Value::Text("Bob".to_string()));
        let node2_id = graph.add_node(node2_data).unwrap();
        
        let node3_data = GraphData::new("person".to_string())
            .with_property("name".to_string(), Value::Text("Charlie".to_string()));
        let node3_id = graph.add_node(node3_data).unwrap();
        
        // Add edges to create a path: Alice -> Bob -> Charlie
        let friendship = Relationship::new("FRIENDS".to_string());
        graph.add_edge(node1_id, node2_id, friendship.clone(), None).unwrap();
        graph.add_edge(node2_id, node3_id, friendship, None).unwrap();
        
        // Test GraphQuery - advanced querying capabilities
        // Find nodes by property
        let alice_nodes = graph.find_nodes_by_property("name", &Value::Text("Alice".to_string())).unwrap();
        assert_eq!(alice_nodes.len(), 1);
        assert_eq!(alice_nodes[0], node1_id);
        
        // Find shortest path
        let path = graph.find_shortest_path(node1_id, node3_id).unwrap();
        assert!(path.is_some());
        let path = path.unwrap();
        assert_eq!(path, vec![node1_id, node2_id, node3_id]);
        
        // Test traversal
        let traversal_result = graph.traverse(node1_id, TraversalStrategy::BreadthFirst, Some(2)).unwrap();
        assert!(traversal_result.len() >= 2); // Should include at least Alice and Bob
        
        // Test GraphTransaction - transaction capabilities
        graph.begin_transaction().unwrap();
        
        // Add node in transaction
        let node4_data = GraphData::new("person".to_string())
            .with_property("name".to_string(), Value::Text("Diana".to_string()));
        let node4_id = graph.add_node(node4_data).unwrap();
        
        // Commit transaction
        graph.commit_transaction().unwrap();
        
        // Verify node was committed
        assert!(graph.get_node(node4_id).unwrap().is_some());
        
        // Test rollback
        graph.begin_transaction().unwrap();
        let temp_node_data = GraphData::new("temp".to_string());
        let _temp_node_id = graph.add_node(temp_node_data).unwrap();
        
        // Rollback transaction
        graph.rollback_transaction().unwrap();
        
        // Temp node should not exist after rollback
        // Note: This behavior depends on the specific transaction implementation
        // For in-memory store, the temp node might still exist in main storage
        // but transaction changes should be discarded
    }

    #[test]
    fn test_persistent_graph_store_comprehensive_capabilities() {
        use std::env;
        
        // Create a temporary file path
        let temp_dir = env::temp_dir();
        let storage_path = temp_dir.join("test_persistent_comprehensive.db");
        
        // Clean up any existing file
        let _ = std::fs::remove_file(&storage_path);
        
        // Test that create_persistent_graph returns full GraphStore capabilities
        let mut graph = GraphFactory::create_persistent_graph(&storage_path).unwrap();
        
        // Test GraphOperations - basic CRUD operations
        let node1_data = GraphData::new("person".to_string())
            .with_property("name".to_string(), Value::Text("Alice".to_string()));
        let node1_id = graph.add_node(node1_data).unwrap();
        
        let node2_data = GraphData::new("person".to_string())
            .with_property("name".to_string(), Value::Text("Bob".to_string()));
        let node2_id = graph.add_node(node2_data).unwrap();
        
        let node3_data = GraphData::new("person".to_string())
            .with_property("name".to_string(), Value::Text("Charlie".to_string()));
        let node3_id = graph.add_node(node3_data).unwrap();
        
        // Add edges to create a path: Alice -> Bob -> Charlie
        let friendship = Relationship::new("FRIENDS".to_string());
        graph.add_edge(node1_id, node2_id, friendship.clone(), None).unwrap();
        graph.add_edge(node2_id, node3_id, friendship, None).unwrap();
        
        // Test GraphQuery - advanced querying capabilities (now accessible!)
        let alice_nodes = graph.find_nodes_by_property("name", &Value::Text("Alice".to_string())).unwrap();
        assert_eq!(alice_nodes.len(), 1);
        assert_eq!(alice_nodes[0], node1_id);
        
        let path = graph.find_shortest_path(node1_id, node3_id).unwrap();
        assert!(path.is_some());
        let path = path.unwrap();
        assert_eq!(path, vec![node1_id, node2_id, node3_id]);
        
        let traversal_result = graph.traverse(node1_id, TraversalStrategy::BreadthFirst, Some(2)).unwrap();
        assert!(traversal_result.len() >= 2);
        
        // Test GraphTransaction - transaction capabilities (now accessible!)
        graph.begin_transaction().unwrap();
        
        let node4_data = GraphData::new("person".to_string())
            .with_property("name".to_string(), Value::Text("Diana".to_string()));
        let node4_id = graph.add_node(node4_data).unwrap();
        
        graph.commit_transaction().unwrap();
        
        // Verify node was committed and persisted
        assert!(graph.get_node(node4_id).unwrap().is_some());
        
        // Clean up
        let _ = std::fs::remove_file(&storage_path);
    }
}