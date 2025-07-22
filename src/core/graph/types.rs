//! Graph data types for Oxidb
//!
//! This module defines the core data structures used in graph operations.
//! Following SOLID principles with clear, single-purpose types.

use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use crate::core::common::types::Value;

/// Unique identifier for graph nodes
pub type NodeId = u64;

/// Unique identifier for graph edges
pub type EdgeId = u64;

/// Graph node containing data and metadata
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Node {
    pub id: NodeId,
    pub data: GraphData,
    pub created_at: u64, // Unix timestamp
    pub updated_at: u64, // Unix timestamp
}

/// Graph edge representing relationships between nodes
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Edge {
    pub id: EdgeId,
    pub from_node: NodeId,
    pub to_node: NodeId,
    pub relationship: Relationship,
    pub data: Option<GraphData>,
    pub created_at: u64, // Unix timestamp
    pub weight: Option<f64>, // Optional edge weight for algorithms
}

/// Relationship type between nodes
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Relationship {
    pub name: String,
    pub direction: RelationshipDirection,
}

/// Direction of relationships
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RelationshipDirection {
    Outgoing,
    Incoming,
    Bidirectional,
}

/// Graph data container with properties
/// Uses Value for actual property values, following SOLID's Single Responsibility Principle
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphData {
    pub label: String, // Node/Edge type label
    pub properties: HashMap<String, Value>, // Changed from DataType to Value
}

impl Node {
    /// Create a new node with the given data
    #[must_use] pub fn new(id: NodeId, data: GraphData) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
            
        Self {
            id,
            data,
            created_at: now,
            updated_at: now,
        }
    }

    /// Update node data
    pub fn update_data(&mut self, data: GraphData) {
        self.data = data;
        self.updated_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
    }

    /// Get a property value
    #[must_use] pub fn get_property(&self, key: &str) -> Option<&Value> {
        self.data.properties.get(key)
    }

    /// Set a property value
    pub fn set_property(&mut self, key: String, value: Value) {
        self.data.properties.insert(key, value);
        self.updated_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
    }
    
    /// Get property as a specific type with type safety
    #[must_use] pub fn get_property_as<T>(&self, key: &str) -> Option<T> 
    where 
        T: for<'a> TryFrom<&'a Value>
    {
        self.get_property(key)?.try_into().ok()
    }
}

impl Edge {
    /// Create a new edge
    #[must_use] pub fn new(
        id: EdgeId,
        from_node: NodeId,
        to_node: NodeId,
        relationship: Relationship,
        data: Option<GraphData>,
    ) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
            
        Self {
            id,
            from_node,
            to_node,
            relationship,
            data,
            created_at: now,
            weight: None,
        }
    }

    /// Create a weighted edge
    #[must_use] pub fn new_weighted(
        id: EdgeId,
        from_node: NodeId,
        to_node: NodeId,
        relationship: Relationship,
        weight: f64,
        data: Option<GraphData>,
    ) -> Self {
        let mut edge = Self::new(id, from_node, to_node, relationship, data);
        edge.weight = Some(weight);
        edge
    }

    /// Check if edge connects the given nodes
    #[must_use] pub fn connects(&self, node1: NodeId, node2: NodeId) -> bool {
        (self.from_node == node1 && self.to_node == node2) ||
        (self.from_node == node2 && self.to_node == node1 && 
         self.relationship.direction == RelationshipDirection::Bidirectional)
    }

    /// Get the other node in the relationship
    #[must_use] pub fn other_node(&self, node_id: NodeId) -> Option<NodeId> {
        if self.from_node == node_id {
            Some(self.to_node)
        } else if self.to_node == node_id && 
                  self.relationship.direction == RelationshipDirection::Bidirectional {
            Some(self.from_node)
        } else {
            None
        }
    }
}

impl Relationship {
    /// Create a new outgoing relationship
    #[must_use] pub const fn new(name: String) -> Self {
        Self {
            name,
            direction: RelationshipDirection::Outgoing,
        }
    }

    /// Create a bidirectional relationship
    #[must_use] pub const fn bidirectional(name: String) -> Self {
        Self {
            name,
            direction: RelationshipDirection::Bidirectional,
        }
    }

    /// Create an incoming relationship
    #[must_use] pub const fn incoming(name: String) -> Self {
        Self {
            name,
            direction: RelationshipDirection::Incoming,
        }
    }
}

impl GraphData {
    /// Create new graph data with a label
    #[must_use] pub fn new(label: String) -> Self {
        Self {
            label,
            properties: HashMap::new(),
        }
    }

    /// Add a property to the graph data (builder pattern)
    #[must_use] pub fn with_property(mut self, key: String, value: Value) -> Self {
        self.properties.insert(key, value);
        self
    }

    /// Add multiple properties
    #[must_use] pub fn with_properties(mut self, properties: HashMap<String, Value>) -> Self {
        self.properties.extend(properties);
        self
    }

    /// Get a property value
    #[must_use] pub fn get_property(&self, key: &str) -> Option<&Value> {
        self.properties.get(key)
    }

    /// Set a property value
    pub fn set_property(&mut self, key: String, value: Value) {
        self.properties.insert(key, value);
    }

    /// Check if has property
    #[must_use] pub fn has_property(&self, key: &str) -> bool {
        self.properties.contains_key(key)
    }

    /// Get all property keys
    #[must_use] pub fn property_keys(&self) -> Vec<&String> {
        self.properties.keys().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;


    #[test]
    fn test_node_creation() {
        let data = GraphData::new("user".to_string())
            .with_property("name".to_string(), Value::Text("Alice".to_string()))
            .with_property("age".to_string(), Value::Integer(30));
        
        let node = Node::new(1, data);
        assert_eq!(node.id, 1);
        assert_eq!(node.data.label, "user");
        assert_eq!(node.get_property("name"), Some(&Value::Text("Alice".to_string())));
        assert_eq!(node.get_property("age"), Some(&Value::Integer(30)));
    }

    #[test]
    fn test_edge_creation() {
        let relationship = Relationship::new("FOLLOWS".to_string());
        let edge = Edge::new(1, 1, 2, relationship, None);
        
        assert_eq!(edge.id, 1);
        assert_eq!(edge.from_node, 1);
        assert_eq!(edge.to_node, 2);
        assert_eq!(edge.relationship.name, "FOLLOWS");
        assert!(edge.connects(1, 2));
        assert!(!edge.connects(1, 3));
    }

    #[test]
    fn test_bidirectional_relationship() {
        let relationship = Relationship::bidirectional("FRIENDS".to_string());
        let edge = Edge::new(1, 1, 2, relationship, None);
        
        assert!(edge.connects(1, 2));
        assert!(edge.connects(2, 1));
        assert_eq!(edge.other_node(1), Some(2));
        assert_eq!(edge.other_node(2), Some(1));
    }

    #[test]
    fn test_graph_data_builder() {
        let data = GraphData::new("product".to_string())
            .with_property("name".to_string(), Value::Text("iPhone".to_string()))
            .with_property("price".to_string(), Value::Float(999.99))
            .with_property("in_stock".to_string(), Value::Boolean(true));
        
        assert_eq!(data.label, "product");
        assert_eq!(data.properties.len(), 3);
        assert!(data.has_property("name"));
        assert!(data.has_property("price"));
        assert!(data.has_property("in_stock"));
        assert!(!data.has_property("description"));
    }
}