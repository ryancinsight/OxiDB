use rand::Rng;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};

use super::error::{HnswError, HnswResult};
use super::node::{DistanceFunction, HnswNode, NodeId, Vector};

/// Priority queue item for distance-based searches
#[derive(Debug, Clone, PartialEq)]
pub struct SearchCandidate {
    pub node_id: NodeId,
    pub distance: f32,
}

impl Eq for SearchCandidate {}

impl Ord for SearchCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for min-heap behavior (smallest distance first)
        other.distance.partial_cmp(&self.distance).unwrap_or(Ordering::Equal)
    }
}

impl PartialOrd for SearchCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// HNSW Graph structure managing the hierarchical navigable small world
#[derive(Debug)]
pub struct HnswGraph {
    /// All nodes in the graph
    nodes: HashMap<NodeId, HnswNode>,

    /// Entry point node ID (highest layer node)
    entry_point: Option<NodeId>,

    /// Next node ID to assign
    next_node_id: NodeId,

    /// Vector dimension
    dimension: usize,

    /// Maximum connections per layer (M parameter)
    max_connections: usize,

    /// Maximum connections for layer 0 (M_L parameter, typically 2*M)
    max_connections_layer0: usize,

    /// Construction parameter (ef_construction)
    ef_construction: usize,

    /// Distance function to use
    distance_function: DistanceFunction,

    /// Maximum layer level
    max_layer: usize,

    /// Layer assignment probability multiplier (ml parameter)
    ml: f64,
}

impl HnswGraph {
    /// Create a new HNSW graph
    pub fn new(
        dimension: usize,
        max_connections: usize,
        ef_construction: usize,
        distance_function: DistanceFunction,
    ) -> Self {
        Self {
            nodes: HashMap::new(),
            entry_point: None,
            next_node_id: 0,
            dimension,
            max_connections,
            max_connections_layer0: max_connections * 2,
            ef_construction,
            distance_function,
            max_layer: 0,
            ml: 1.0 / (2.0_f64).ln(),
        }
    }

    /// Get the number of nodes in the graph
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Check if the graph is empty
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Get entry point node ID
    pub fn entry_point(&self) -> Option<NodeId> {
        self.entry_point
    }

    /// Get a node by ID
    pub fn get_node(&self, node_id: NodeId) -> Option<&HnswNode> {
        self.nodes.get(&node_id)
    }

    /// Get a mutable node by ID
    #[allow(dead_code)]
    pub fn get_node_mut(&mut self, node_id: NodeId) -> Option<&mut HnswNode> {
        self.nodes.get_mut(&node_id)
    }

    /// Generate random layer for a new node
    pub fn random_layer(&self) -> usize {
        let mut rng = rand::thread_rng();
        let level = (-rng.gen::<f64>().ln() * self.ml).floor() as usize;
        level.min(16) // Cap at reasonable maximum
    }

    /// Calculate distance between two vectors
    pub fn distance(&self, a: &Vector, b: &Vector) -> f32 {
        self.distance_function.calculate(a, b)
    }

    /// Insert a new vector into the graph
    pub fn insert_node(&mut self, vector: Vector, primary_key: Vec<u8>) -> HnswResult<NodeId> {
        if vector.len() != self.dimension {
            return Err(HnswError::DimensionMismatch {
                expected: self.dimension,
                actual: vector.len(),
            });
        }

        let node_id = self.next_node_id;
        self.next_node_id += 1;

        let layer = self.random_layer();
        let mut new_node = HnswNode::new(node_id, vector.clone(), primary_key, layer);

        // If this is the first node, make it the entry point
        if self.is_empty() {
            self.entry_point = Some(node_id);
            self.max_layer = layer;
            self.nodes.insert(node_id, new_node);
            return Ok(node_id);
        }

        // Search for closest nodes starting from top layer
        let mut current_closest = vec![self.entry_point.unwrap()];

        // Search from top layer down to node's layer + 1
        for lc in (layer + 1..=self.max_layer).rev() {
            current_closest = self.search_layer(&vector, &current_closest, 1, lc)?;
        }

        // Insert and connect from node's layer down to 0
        for lc in (0..=layer).rev() {
            let candidates =
                self.search_layer(&vector, &current_closest, self.ef_construction, lc)?;

            // Select neighbors
            let max_conn = if lc == 0 { self.max_connections_layer0 } else { self.max_connections };
            let neighbors = self.select_neighbors_simple(&vector, &candidates, max_conn)?;

            // Add connections
            for &neighbor_id in &neighbors {
                new_node.add_connection(lc, neighbor_id);

                // Add bidirectional connection
                if let Some(neighbor) = self.nodes.get_mut(&neighbor_id) {
                    neighbor.add_connection(lc, node_id);

                    // Prune connections if needed
                    self.prune_connections(neighbor_id, lc, max_conn)?;
                }
            }

            current_closest = neighbors;
        }

        // Update entry point if this node is on a higher layer
        if layer > self.max_layer {
            self.entry_point = Some(node_id);
            self.max_layer = layer;
        }

        self.nodes.insert(node_id, new_node);
        Ok(node_id)
    }

    /// Search for k nearest neighbors
    pub fn search(&self, query: &Vector, k: usize) -> HnswResult<Vec<NodeId>> {
        if query.len() != self.dimension {
            return Err(HnswError::DimensionMismatch {
                expected: self.dimension,
                actual: query.len(),
            });
        }

        if self.is_empty() {
            return Ok(Vec::new());
        }

        let entry_point = self.entry_point.ok_or(HnswError::EmptyGraph)?;
        let mut current_closest = vec![entry_point];

        // Search from top layer down to layer 1
        for lc in (1..=self.max_layer).rev() {
            current_closest = self.search_layer(query, &current_closest, 1, lc)?;
        }

        // Search layer 0 with ef = max(ef_construction, k)
        let ef = self.ef_construction.max(k);
        let candidates = self.search_layer(query, &current_closest, ef, 0)?;

        // Return top k candidates
        Ok(candidates.into_iter().take(k).collect())
    }

    /// Search within a specific layer
    fn search_layer(
        &self,
        query: &Vector,
        entry_points: &[NodeId],
        num_closest: usize,
        layer: usize,
    ) -> HnswResult<Vec<NodeId>> {
        let mut visited = std::collections::HashSet::new();
        let mut candidates = BinaryHeap::new();
        let mut w = BinaryHeap::new();

        // Initialize with entry points
        for &ep in entry_points {
            if let Some(node) = self.get_node(ep) {
                let distance = self.distance(query, &node.vector);
                candidates.push(SearchCandidate { node_id: ep, distance });
                w.push(SearchCandidate {
                    node_id: ep,
                    distance: -distance, // Negative for max-heap behavior
                });
                visited.insert(ep);
            }
        }

        while let Some(candidate) = candidates.pop() {
            // Get furthest point in w
            let furthest = w.peek().map(|c| -c.distance).unwrap_or(f32::INFINITY);

            if candidate.distance > furthest {
                break;
            }

            // Check neighbors
            if let Some(node) = self.get_node(candidate.node_id) {
                if let Some(connections) = node.connections_at_layer(layer) {
                    for &neighbor_id in connections {
                        if !visited.contains(&neighbor_id) {
                            visited.insert(neighbor_id);

                            if let Some(neighbor) = self.get_node(neighbor_id) {
                                let distance = self.distance(query, &neighbor.vector);

                                if w.len() < num_closest {
                                    candidates
                                        .push(SearchCandidate { node_id: neighbor_id, distance });
                                    w.push(SearchCandidate {
                                        node_id: neighbor_id,
                                        distance: -distance,
                                    });
                                } else if let Some(furthest) = w.peek() {
                                    if distance < -furthest.distance {
                                        candidates.push(SearchCandidate {
                                            node_id: neighbor_id,
                                            distance,
                                        });
                                        w.pop();
                                        w.push(SearchCandidate {
                                            node_id: neighbor_id,
                                            distance: -distance,
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Convert max-heap to sorted result (closest first)
        let mut result: Vec<_> = w.into_vec();
        result.sort_by(|a, b| (-a.distance).partial_cmp(&(-b.distance)).unwrap_or(Ordering::Equal));

        Ok(result.into_iter().map(|c| c.node_id).collect())
    }

    /// Simple neighbor selection (closest neighbors)
    fn select_neighbors_simple(
        &self,
        query: &Vector,
        candidates: &[NodeId],
        max_neighbors: usize,
    ) -> HnswResult<Vec<NodeId>> {
        let mut scored_candidates: Vec<_> = candidates
            .iter()
            .filter_map(|&node_id| {
                self.get_node(node_id).map(|node| SearchCandidate {
                    node_id,
                    distance: self.distance(query, &node.vector),
                })
            })
            .collect();

        scored_candidates
            .sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap_or(Ordering::Equal));

        Ok(scored_candidates.into_iter().take(max_neighbors).map(|c| c.node_id).collect())
    }

    /// Prune connections for a node if it exceeds maximum
    fn prune_connections(
        &mut self,
        node_id: NodeId,
        layer: usize,
        max_connections: usize,
    ) -> HnswResult<()> {
        if let Some(node) = self.nodes.get(&node_id) {
            if node.connection_count_at_layer(layer) <= max_connections {
                return Ok(());
            }

            // Get current connections and their distances
            let connections: Vec<NodeId> = node
                .connections_at_layer(layer)
                .unwrap_or(&std::collections::HashSet::new())
                .iter()
                .cloned()
                .collect();

            let node_vector = node.vector.clone();

            // Find closest connections to keep
            let to_keep =
                self.select_neighbors_simple(&node_vector, &connections, max_connections)?;

            // Remove connections not in the keep list
            let to_remove: Vec<NodeId> =
                connections.into_iter().filter(|id| !to_keep.contains(id)).collect();

            // Update connections
            if let Some(node) = self.nodes.get_mut(&node_id) {
                for &remove_id in &to_remove {
                    node.remove_connection(layer, remove_id);
                }
            }

            // Remove bidirectional connections
            for remove_id in to_remove {
                if let Some(neighbor) = self.nodes.get_mut(&remove_id) {
                    neighbor.remove_connection(layer, node_id);
                }
            }
        }

        Ok(())
    }

    /// Remove a node from the graph
    pub fn remove_node(&mut self, node_id: NodeId) -> HnswResult<bool> {
        if let Some(node) = self.nodes.remove(&node_id) {
            // Remove all connections to this node
            for layer in 0..=node.layer {
                if let Some(connections) = node.connections_at_layer(layer) {
                    for &neighbor_id in connections {
                        if let Some(neighbor) = self.nodes.get_mut(&neighbor_id) {
                            neighbor.remove_connection(layer, node_id);
                        }
                    }
                }
            }

            // Update entry point if necessary
            if self.entry_point == Some(node_id) {
                self.entry_point = self.find_new_entry_point();
            }

            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Find a new entry point after the current one is removed
    fn find_new_entry_point(&mut self) -> Option<NodeId> {
        let mut max_layer = 0;
        let mut entry_candidate = None;

        for (node_id, node) in &self.nodes {
            if node.layer >= max_layer {
                max_layer = node.layer;
                entry_candidate = Some(*node_id);
            }
        }

        self.max_layer = max_layer;
        entry_candidate
    }
}

#[cfg(test)]
mod tests {
    use super::super::node::DistanceFunction;
    use super::*;

    #[test]
    fn test_graph_creation() {
        let graph = HnswGraph::new(3, 16, 200, DistanceFunction::Euclidean);
        assert_eq!(graph.dimension, 3);
        assert_eq!(graph.max_connections, 16);
        assert_eq!(graph.ef_construction, 200);
        assert!(graph.is_empty());
        assert!(graph.entry_point().is_none());
    }

    #[test]
    fn test_single_node_insertion() {
        let mut graph = HnswGraph::new(3, 16, 200, DistanceFunction::Euclidean);
        let vector = vec![1.0, 2.0, 3.0];
        let pk = b"test".to_vec();

        let node_id = graph.insert_node(vector.clone(), pk).unwrap();
        assert_eq!(node_id, 0);
        assert_eq!(graph.len(), 1);
        assert_eq!(graph.entry_point(), Some(0));

        let node = graph.get_node(0).unwrap();
        assert_eq!(node.vector, vector);
        assert_eq!(node.id, 0);
    }

    #[test]
    fn test_multiple_node_insertion() {
        let mut graph = HnswGraph::new(2, 4, 10, DistanceFunction::Euclidean);

        let vectors = [vec![1.0, 0.0], vec![0.0, 1.0], vec![2.0, 0.0], vec![0.0, 2.0]];

        for (i, vector) in vectors.iter().enumerate() {
            let pk = format!("pk_{}", i).as_bytes().to_vec();
            let node_id = graph.insert_node(vector.clone(), pk).unwrap();
            assert_eq!(node_id, i);
        }

        assert_eq!(graph.len(), 4);
        assert!(graph.entry_point().is_some());
    }

    #[test]
    fn test_search() {
        let mut graph = HnswGraph::new(2, 4, 10, DistanceFunction::Euclidean);

        let vectors = [vec![1.0, 0.0], vec![0.0, 1.0], vec![2.0, 0.0], vec![0.0, 2.0]];

        for (i, vector) in vectors.iter().enumerate() {
            let pk = format!("pk_{}", i).as_bytes().to_vec();
            graph.insert_node(vector.clone(), pk).unwrap();
        }

        // Search for closest to [1.1, 0.0] (should be node 0: [1.0, 0.0])
        let query = vec![1.1, 0.0];
        let results = graph.search(&query, 2).unwrap();

        assert!(!results.is_empty());
        assert!(results.contains(&0)); // Should contain the closest vector
    }

    #[test]
    fn test_node_removal() {
        let mut graph = HnswGraph::new(2, 4, 10, DistanceFunction::Euclidean);

        let vector = vec![1.0, 0.0];
        let pk = b"test".to_vec();
        let node_id = graph.insert_node(vector, pk).unwrap();

        assert_eq!(graph.len(), 1);
        assert!(graph.remove_node(node_id).unwrap());
        assert_eq!(graph.len(), 0);
        assert!(graph.entry_point().is_none());
    }

    #[test]
    fn test_dimension_mismatch() {
        let mut graph = HnswGraph::new(3, 16, 200, DistanceFunction::Euclidean);
        let wrong_vector = vec![1.0, 2.0]; // 2D instead of 3D
        let pk = b"test".to_vec();

        let result = graph.insert_node(wrong_vector, pk);
        assert!(matches!(result, Err(HnswError::DimensionMismatch { .. })));
    }
}
