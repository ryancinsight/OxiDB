use crate::core::query::commands::Key as PrimaryKey;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Node identifier type
pub type NodeId = usize;

/// Vector type for HNSW
pub type Vector = Vec<f32>;

/// HNSW node representing a vector with connections across multiple layers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HnswNode {
    /// Unique identifier for this node
    pub id: NodeId,

    /// The vector data stored in this node
    pub vector: Vector,

    /// Primary key for the original data
    pub primary_key: PrimaryKey,

    /// Connections for each layer (layer 0 is base layer)
    /// connections[i] contains neighbor node IDs for layer i
    pub connections: Vec<HashSet<NodeId>>,

    /// Maximum layer this node exists in
    pub layer: usize,
}

impl HnswNode {
    /// Create a new HNSW node
    pub fn new(id: NodeId, vector: Vector, primary_key: PrimaryKey, layer: usize) -> Self {
        let connections = (0..=layer).map(|_| HashSet::new()).collect();

        Self { id, vector, primary_key, connections, layer }
    }

    /// Get connections for a specific layer
    pub fn connections_at_layer(&self, layer: usize) -> Option<&HashSet<NodeId>> {
        self.connections.get(layer)
    }

    /// Get mutable connections for a specific layer
    pub fn connections_at_layer_mut(&mut self, layer: usize) -> Option<&mut HashSet<NodeId>> {
        self.connections.get_mut(layer)
    }

    /// Add a connection to a specific layer
    pub fn add_connection(&mut self, layer: usize, neighbor_id: NodeId) -> bool {
        if let Some(layer_connections) = self.connections.get_mut(layer) {
            layer_connections.insert(neighbor_id)
        } else {
            false
        }
    }

    /// Remove a connection from a specific layer
    pub fn remove_connection(&mut self, layer: usize, neighbor_id: NodeId) -> bool {
        if let Some(layer_connections) = self.connections.get_mut(layer) {
            layer_connections.remove(&neighbor_id)
        } else {
            false
        }
    }

    /// Get the number of connections at a specific layer
    pub fn connection_count_at_layer(&self, layer: usize) -> usize {
        self.connections_at_layer(layer).map(|conns| conns.len()).unwrap_or(0)
    }

    /// Check if this node has a connection to another node at a specific layer
    pub fn has_connection(&self, layer: usize, neighbor_id: NodeId) -> bool {
        self.connections_at_layer(layer).map(|conns| conns.contains(&neighbor_id)).unwrap_or(false)
    }

    /// Get the dimension of the vector
    pub fn dimension(&self) -> usize {
        self.vector.len()
    }

    /// Calculate Euclidean distance to another vector
    pub fn distance_to(&self, other_vector: &Vector) -> f32 {
        euclidean_distance(&self.vector, other_vector)
    }

    /// Calculate cosine similarity to another vector
    pub fn cosine_similarity_to(&self, other_vector: &Vector) -> f32 {
        cosine_similarity(&self.vector, other_vector)
    }
}

/// Calculate Euclidean distance between two vectors
pub fn euclidean_distance(a: &Vector, b: &Vector) -> f32 {
    if a.len() != b.len() {
        return f32::INFINITY;
    }

    a.iter().zip(b.iter()).map(|(x, y)| (x - y).powi(2)).sum::<f32>().sqrt()
}

/// Calculate cosine similarity between two vectors
pub fn cosine_similarity(a: &Vector, b: &Vector) -> f32 {
    if a.len() != b.len() {
        return 0.0;
    }

    let dot_product: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x.powi(2)).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x.powi(2)).sum::<f32>().sqrt();

    if norm_a == 0.0 || norm_b == 0.0 {
        0.0
    } else {
        dot_product / (norm_a * norm_b)
    }
}

/// Distance function type for HNSW
#[derive(Debug, Clone, Copy)]
pub enum DistanceFunction {
    Euclidean,
    Cosine,
}

impl DistanceFunction {
    /// Calculate distance between two vectors using this distance function
    pub fn calculate(&self, a: &Vector, b: &Vector) -> f32 {
        match self {
            DistanceFunction::Euclidean => euclidean_distance(a, b),
            DistanceFunction::Cosine => 1.0 - cosine_similarity(a, b), // Convert similarity to distance
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_creation() {
        let vector = vec![1.0, 2.0, 3.0];
        let pk = b"test_key".to_vec();
        let node = HnswNode::new(0, vector.clone(), pk.clone(), 2);

        assert_eq!(node.id, 0);
        assert_eq!(node.vector, vector);
        assert_eq!(node.primary_key, pk);
        assert_eq!(node.layer, 2);
        assert_eq!(node.connections.len(), 3); // layers 0, 1, 2
    }

    #[test]
    fn test_node_connections() {
        let vector = vec![1.0, 2.0, 3.0];
        let pk = b"test_key".to_vec();
        let mut node = HnswNode::new(0, vector, pk, 1);

        // Add connections
        assert!(node.add_connection(0, 1));
        assert!(node.add_connection(0, 2));
        assert!(node.add_connection(1, 3));

        // Check connections
        assert!(node.has_connection(0, 1));
        assert!(node.has_connection(0, 2));
        assert!(node.has_connection(1, 3));
        assert!(!node.has_connection(0, 3));

        // Check counts
        assert_eq!(node.connection_count_at_layer(0), 2);
        assert_eq!(node.connection_count_at_layer(1), 1);

        // Remove connection
        assert!(node.remove_connection(0, 1));
        assert!(!node.has_connection(0, 1));
        assert_eq!(node.connection_count_at_layer(0), 1);
    }

    #[test]
    fn test_distance_functions() {
        let vec1 = vec![1.0, 0.0, 0.0];
        let vec2 = vec![0.0, 1.0, 0.0];
        let vec3 = vec![1.0, 0.0, 0.0]; // Same as vec1

        // Euclidean distance
        assert!((euclidean_distance(&vec1, &vec2) - 1.414_214).abs() < 0.001);
        assert!(euclidean_distance(&vec1, &vec3) < 0.001);

        // Cosine similarity
        assert!(cosine_similarity(&vec1, &vec2).abs() < 0.001); // Orthogonal vectors
        assert!((cosine_similarity(&vec1, &vec3) - 1.0).abs() < 0.001); // Same vectors
    }

    #[test]
    fn test_node_distance_calculations() {
        let vector1 = vec![1.0, 0.0, 0.0];
        let vector2 = vec![0.0, 1.0, 0.0];
        let pk = b"test".to_vec();

        let node = HnswNode::new(0, vector1, pk, 0);

        assert!((node.distance_to(&vector2) - 1.414_214).abs() < 0.001);
        assert!(node.cosine_similarity_to(&vector2).abs() < 0.001);
    }

    #[test]
    fn test_distance_function_enum() {
        let vec1 = vec![1.0, 0.0];
        let vec2 = vec![0.0, 1.0];

        let euclidean = DistanceFunction::Euclidean;
        let cosine = DistanceFunction::Cosine;

        assert!((euclidean.calculate(&vec1, &vec2) - 1.414_214).abs() < 0.001);
        assert!((cosine.calculate(&vec1, &vec2) - 1.0).abs() < 0.001); // 1 - 0 (orthogonal)
    }
}
