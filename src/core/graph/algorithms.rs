//! Graph algorithms for Oxidb
//!
//! This module provides various graph algorithms including pathfinding,
//! centrality measures, and community detection. Following SOLID principles
//! with extensible algorithm implementations.

use super::traversal::TraversalEngine;
use super::types::NodeId;
use crate::core::common::OxidbError;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};

/// Graph algorithms trait following Interface Segregation Principle
pub trait GraphAlgorithms {
    /// Calculate betweenness centrality for all nodes
    fn betweenness_centrality(&self) -> Result<HashMap<NodeId, f64>, OxidbError>;

    /// Calculate closeness centrality for all nodes
    fn closeness_centrality(&self) -> Result<HashMap<NodeId, f64>, OxidbError>;

    /// Calculate degree centrality for all nodes
    fn degree_centrality(&self) -> Result<HashMap<NodeId, f64>, OxidbError>;

    /// Detect communities using simple modularity-based approach
    fn detect_communities(&self) -> Result<Vec<Vec<NodeId>>, OxidbError>;

    /// Check if the graph is connected
    fn is_connected(&self) -> Result<bool, OxidbError>;

    /// Calculate graph diameter
    fn diameter(&self) -> Result<Option<usize>, OxidbError>;
}

/// Pathfinding algorithms trait
pub trait PathFinding {
    /// Find shortest path between two nodes (unweighted)
    fn shortest_path(&self, start: NodeId, end: NodeId) -> Result<Option<Vec<NodeId>>, OxidbError>;

    /// Find shortest path between two nodes (weighted using Dijkstra)
    fn dijkstra_shortest_path(
        &self,
        start: NodeId,
        end: NodeId,
    ) -> Result<Option<(Vec<NodeId>, f64)>, OxidbError>;

    /// Find all shortest paths from a source node
    fn all_shortest_paths(
        &self,
        source: NodeId,
    ) -> Result<HashMap<NodeId, Vec<NodeId>>, OxidbError>;

    /// Find k shortest paths between two nodes
    fn k_shortest_paths(
        &self,
        start: NodeId,
        end: NodeId,
        k: usize,
    ) -> Result<Vec<Vec<NodeId>>, OxidbError>;
}

/// Priority queue item for Dijkstra's algorithm
#[derive(Debug, Clone, PartialEq)]
struct DijkstraItem {
    node: NodeId,
    distance: f64,
    path: Vec<NodeId>,
}

impl Eq for DijkstraItem {}

impl Ord for DijkstraItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for min-heap behavior (smallest distance first)
        other.distance.partial_cmp(&self.distance).unwrap_or(Ordering::Equal)
    }
}

impl PartialOrd for DijkstraItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Centrality calculator implementing various centrality measures
pub struct CentralityCalculator;

impl CentralityCalculator {
    /// Calculate betweenness centrality using Brandes' algorithm
    pub fn betweenness_centrality<F>(
        all_nodes: &[NodeId],
        get_neighbors: F,
    ) -> Result<HashMap<NodeId, f64>, OxidbError>
    where
        F: Fn(NodeId) -> Result<Vec<NodeId>, OxidbError>,
    {
        let mut centrality = HashMap::new();

        // Initialize centrality scores
        for &node in all_nodes {
            centrality.insert(node, 0.0);
        }

        // For each node as source
        for &source in all_nodes {
            let mut stack = Vec::new();
            let mut predecessors: HashMap<NodeId, Vec<NodeId>> = HashMap::new();
            let mut distances: HashMap<NodeId, i32> = HashMap::new();
            let mut sigma: HashMap<NodeId, f64> = HashMap::new();
            let mut delta: HashMap<NodeId, f64> = HashMap::new();

            // Initialize
            for &node in all_nodes {
                predecessors.insert(node, Vec::new());
                distances.insert(node, -1);
                sigma.insert(node, 0.0);
                delta.insert(node, 0.0);
            }

            distances.insert(source, 0);
            sigma.insert(source, 1.0);

            let mut queue = VecDeque::new();
            queue.push_back(source);

            // BFS to find shortest paths
            while let Some(current) = queue.pop_front() {
                stack.push(current);
                let neighbors = get_neighbors(current)?;

                for neighbor in neighbors {
                    // First time we encounter this neighbor?
                    if distances[&neighbor] < 0 {
                        queue.push_back(neighbor);
                        distances.insert(neighbor, distances[&current] + 1);
                    }

                    // Is this a shortest path to neighbor?
                    if distances[&neighbor] == distances[&current] + 1 {
                        *sigma.get_mut(&neighbor).unwrap() += sigma[&current];
                        predecessors.get_mut(&neighbor).unwrap().push(current);
                    }
                }
            }

            // Accumulation phase
            while let Some(node) = stack.pop() {
                for &pred in &predecessors[&node] {
                    let contribution = (sigma[&pred] / sigma[&node]) * (1.0 + delta[&node]);
                    *delta.get_mut(&pred).unwrap() += contribution;
                }

                if node != source {
                    *centrality.get_mut(&node).unwrap() += delta[&node];
                }
            }
        }

        // Normalize for undirected graph
        let n = all_nodes.len() as f64;
        if n > 2.0 {
            let normalization = 2.0 / ((n - 1.0) * (n - 2.0));
            for value in centrality.values_mut() {
                *value *= normalization;
            }
        }

        Ok(centrality)
    }

    /// Calculate closeness centrality
    pub fn closeness_centrality<F>(
        all_nodes: &[NodeId],
        get_neighbors: F,
    ) -> Result<HashMap<NodeId, f64>, OxidbError>
    where
        F: Fn(NodeId) -> Result<Vec<NodeId>, OxidbError>,
    {
        let mut centrality = HashMap::new();

        for &source in all_nodes {
            let distances = Self::single_source_shortest_paths(source, all_nodes, &get_neighbors)?;

            let total_distance: f64 = distances.values().map(|&d| d as f64).sum();
            let reachable_nodes = distances.len() as f64;

            if total_distance > 0.0 && reachable_nodes > 1.0 {
                let closeness = (reachable_nodes - 1.0) / total_distance;
                centrality.insert(source, closeness);
            } else {
                centrality.insert(source, 0.0);
            }
        }

        Ok(centrality)
    }

    /// Calculate degree centrality
    pub fn degree_centrality<F>(
        all_nodes: &[NodeId],
        get_neighbors: F,
    ) -> Result<HashMap<NodeId, f64>, OxidbError>
    where
        F: Fn(NodeId) -> Result<Vec<NodeId>, OxidbError>,
    {
        let mut centrality = HashMap::new();
        let n = all_nodes.len() as f64;

        for &node in all_nodes {
            let neighbors = get_neighbors(node)?;
            let degree = neighbors.len() as f64;

            // Normalize by maximum possible degree (n-1)
            let normalized_degree = if n > 1.0 { degree / (n - 1.0) } else { 0.0 };
            centrality.insert(node, normalized_degree);
        }

        Ok(centrality)
    }

    /// Single source shortest paths using BFS
    fn single_source_shortest_paths<F>(
        source: NodeId,
        _all_nodes: &[NodeId],
        get_neighbors: F,
    ) -> Result<HashMap<NodeId, usize>, OxidbError>
    where
        F: Fn(NodeId) -> Result<Vec<NodeId>, OxidbError>,
    {
        let mut distances = HashMap::new();
        let mut queue = VecDeque::new();

        distances.insert(source, 0);
        queue.push_back(source);

        while let Some(current) = queue.pop_front() {
            let current_distance = distances[&current];
            let neighbors = get_neighbors(current)?;

            for neighbor in neighbors {
                if let std::collections::hash_map::Entry::Vacant(e) = distances.entry(neighbor) {
                    e.insert(current_distance + 1);
                    queue.push_back(neighbor);
                }
            }
        }

        Ok(distances)
    }
}

/// Pathfinding algorithms implementation
pub struct PathFinder;

impl PathFinder {
    /// Dijkstra's algorithm for weighted shortest paths
    pub fn dijkstra<F>(
        start: NodeId,
        end: Option<NodeId>,
        get_weighted_neighbors: F,
    ) -> Result<HashMap<NodeId, (f64, Vec<NodeId>)>, OxidbError>
    where
        F: Fn(NodeId) -> Result<Vec<(NodeId, f64)>, OxidbError>,
    {
        let mut distances: HashMap<NodeId, f64> = HashMap::new();
        let mut paths: HashMap<NodeId, Vec<NodeId>> = HashMap::new();
        let mut heap = BinaryHeap::new();
        let mut visited = HashSet::new();

        distances.insert(start, 0.0);
        paths.insert(start, vec![start]);
        heap.push(DijkstraItem { node: start, distance: 0.0, path: vec![start] });

        while let Some(DijkstraItem { node: current, distance: current_dist, path: current_path }) =
            heap.pop()
        {
            if visited.contains(&current) {
                continue;
            }

            visited.insert(current);

            // If we have a target and reached it, we can stop
            if let Some(target) = end {
                if current == target {
                    break;
                }
            }

            let neighbors = get_weighted_neighbors(current)?;

            for (neighbor, weight) in neighbors {
                if visited.contains(&neighbor) {
                    continue;
                }

                let new_distance = current_dist + weight;

                if !distances.contains_key(&neighbor) || new_distance < distances[&neighbor] {
                    distances.insert(neighbor, new_distance);

                    let mut new_path = current_path.clone();
                    new_path.push(neighbor);
                    paths.insert(neighbor, new_path.clone());

                    heap.push(DijkstraItem {
                        node: neighbor,
                        distance: new_distance,
                        path: new_path,
                    });
                }
            }
        }

        // Combine distances and paths
        let mut result = HashMap::new();
        for (node, distance) in distances {
            if let Some(path) = paths.get(&node) {
                result.insert(node, (distance, path.clone()));
            }
        }

        Ok(result)
    }

    /// A* algorithm for heuristic-based pathfinding
    pub fn a_star<F, H>(
        start: NodeId,
        goal: NodeId,
        get_weighted_neighbors: F,
        heuristic: H,
    ) -> Result<Option<(Vec<NodeId>, f64)>, OxidbError>
    where
        F: Fn(NodeId) -> Result<Vec<(NodeId, f64)>, OxidbError>,
        H: Fn(NodeId, NodeId) -> f64,
    {
        let mut open_set = BinaryHeap::new();
        let mut came_from: HashMap<NodeId, NodeId> = HashMap::new();
        let mut g_score: HashMap<NodeId, f64> = HashMap::new();
        let mut f_score: HashMap<NodeId, f64> = HashMap::new();

        g_score.insert(start, 0.0);
        f_score.insert(start, heuristic(start, goal));

        open_set.push(DijkstraItem {
            node: start,
            distance: -f_score[&start], // Negative for min-heap behavior
            path: vec![start],
        });

        while let Some(DijkstraItem { node: current, .. }) = open_set.pop() {
            if current == goal {
                // Reconstruct path
                let mut path = vec![current];
                let mut current_node = current;

                while let Some(&parent) = came_from.get(&current_node) {
                    path.push(parent);
                    current_node = parent;
                }

                path.reverse();
                return Ok(Some((path, g_score[&goal])));
            }

            let neighbors = get_weighted_neighbors(current)?;

            for (neighbor, weight) in neighbors {
                let tentative_g_score = g_score[&current] + weight;

                if !g_score.contains_key(&neighbor) || tentative_g_score < g_score[&neighbor] {
                    came_from.insert(neighbor, current);
                    g_score.insert(neighbor, tentative_g_score);

                    let f_score_neighbor = tentative_g_score + heuristic(neighbor, goal);
                    f_score.insert(neighbor, f_score_neighbor);

                    open_set.push(DijkstraItem {
                        node: neighbor,
                        distance: -f_score_neighbor, // Negative for min-heap behavior
                        path: vec![],                // Not used in A*
                    });
                }
            }
        }

        Ok(None) // No path found
    }
}

/// Community detection algorithms
pub struct CommunityDetector;

impl CommunityDetector {
    /// Simple community detection using connected components
    pub fn connected_components<F>(
        all_nodes: Vec<NodeId>,
        get_neighbors: F,
    ) -> Result<Vec<Vec<NodeId>>, OxidbError>
    where
        F: Fn(NodeId) -> Result<Vec<NodeId>, OxidbError>,
    {
        TraversalEngine::connected_components(all_nodes, get_neighbors)
    }

    /// Label propagation algorithm for community detection
    pub fn label_propagation<F>(
        all_nodes: &[NodeId],
        get_neighbors: F,
        max_iterations: usize,
    ) -> Result<HashMap<NodeId, usize>, OxidbError>
    where
        F: Fn(NodeId) -> Result<Vec<NodeId>, OxidbError>,
    {
        let mut labels: HashMap<NodeId, usize> = HashMap::new();

        // Initialize each node with its own label
        for (i, &node) in all_nodes.iter().enumerate() {
            labels.insert(node, i);
        }

        for _iteration in 0..max_iterations {
            let mut changed = false;
            let mut new_labels = labels.clone();

            for &node in all_nodes {
                let neighbors = get_neighbors(node)?;

                if neighbors.is_empty() {
                    continue;
                }

                // Count neighbor labels
                let mut label_counts: HashMap<usize, usize> = HashMap::new();
                for neighbor in neighbors {
                    if let Some(&label) = labels.get(&neighbor) {
                        *label_counts.entry(label).or_insert(0) += 1;
                    }
                }

                // Find most frequent label
                if let Some((&most_frequent_label, _)) =
                    label_counts.iter().max_by_key(|(_, &count)| count)
                {
                    if labels[&node] != most_frequent_label {
                        new_labels.insert(node, most_frequent_label);
                        changed = true;
                    }
                }
            }

            labels = new_labels;

            if !changed {
                break; // Converged
            }
        }

        Ok(labels)
    }
}

/// Graph metrics calculator
pub struct GraphMetrics;

impl GraphMetrics {
    /// Calculate clustering coefficient for a node
    /// Optimized version with O(k * `k_avg`) complexity using `HashSet` lookups
    pub fn clustering_coefficient<F>(node: NodeId, get_neighbors: F) -> Result<f64, OxidbError>
    where
        F: Fn(NodeId) -> Result<Vec<NodeId>, OxidbError>,
    {
        let neighbors = get_neighbors(node)?;
        let degree = neighbors.len();

        if degree < 2 {
            return Ok(0.0);
        }

        let mut edges_between_neighbors = 0;

        // Optimize by checking each neighbor's connections only once
        // and using HashSet for O(1) lookups
        for i in 0..neighbors.len() {
            let neighbor_i = neighbors[i];

            // Get neighbors of neighbor_i and convert to HashSet for O(1) lookups
            let neighbors_of_i = get_neighbors(neighbor_i)?;
            let neighbors_set: std::collections::HashSet<NodeId> =
                neighbors_of_i.into_iter().collect();

            // Check connections to remaining neighbors (j > i to avoid double counting)
            for j in (i + 1)..neighbors.len() {
                let neighbor_j = neighbors[j];

                // O(1) lookup instead of O(k) contains() on Vec
                if neighbors_set.contains(&neighbor_j) {
                    edges_between_neighbors += 1;
                }
            }
        }

        let possible_edges = degree * (degree - 1) / 2;
        Ok(f64::from(edges_between_neighbors) / possible_edges as f64)
    }

    /// Calculate average clustering coefficient for the graph
    pub fn average_clustering_coefficient<F>(
        all_nodes: &[NodeId],
        get_neighbors: F,
    ) -> Result<f64, OxidbError>
    where
        F: Fn(NodeId) -> Result<Vec<NodeId>, OxidbError> + Copy,
    {
        let mut total_clustering = 0.0;
        let mut valid_nodes = 0;

        for &node in all_nodes {
            let clustering = Self::clustering_coefficient(node, get_neighbors)?;
            if clustering.is_finite() {
                total_clustering += clustering;
                valid_nodes += 1;
            }
        }

        if valid_nodes > 0 {
            Ok(total_clustering / f64::from(valid_nodes))
        } else {
            Ok(0.0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_degree_centrality() {
        // Mock graph: 1 connected to 2,3; 2 connected to 1,3; 3 connected to 1,2
        let get_neighbors = |node: NodeId| -> Result<Vec<NodeId>, OxidbError> {
            match node {
                1 => Ok(vec![2, 3]),
                2 => Ok(vec![1, 3]),
                3 => Ok(vec![1, 2]),
                _ => Ok(vec![]),
            }
        };

        let all_nodes = vec![1, 2, 3];
        let centrality =
            CentralityCalculator::degree_centrality(&all_nodes, get_neighbors).unwrap();

        // Each node has degree 2, normalized by (n-1) = 2, so centrality = 1.0
        assert_eq!(centrality[&1], 1.0);
        assert_eq!(centrality[&2], 1.0);
        assert_eq!(centrality[&3], 1.0);
    }

    #[test]
    fn test_dijkstra() {
        // Mock weighted graph: 1 --(1)--> 2 --(2)--> 3
        let get_weighted_neighbors = |node: NodeId| -> Result<Vec<(NodeId, f64)>, OxidbError> {
            match node {
                1 => Ok(vec![(2, 1.0)]),
                2 => Ok(vec![(3, 2.0)]),
                3 => Ok(vec![]),
                _ => Ok(vec![]),
            }
        };

        let result = PathFinder::dijkstra(1, Some(3), get_weighted_neighbors).unwrap();

        assert!(result.contains_key(&3));
        let (distance, path) = &result[&3];
        assert_eq!(*distance, 3.0);
        assert_eq!(*path, vec![1, 2, 3]);
    }

    #[test]
    fn test_clustering_coefficient() {
        // Triangle graph: 1-2, 2-3, 3-1
        let get_neighbors = |node: NodeId| -> Result<Vec<NodeId>, OxidbError> {
            match node {
                1 => Ok(vec![2, 3]),
                2 => Ok(vec![1, 3]),
                3 => Ok(vec![1, 2]),
                _ => Ok(vec![]),
            }
        };

        let clustering = GraphMetrics::clustering_coefficient(1, get_neighbors).unwrap();
        assert_eq!(clustering, 1.0); // Perfect triangle
    }

    #[test]
    fn test_clustering_coefficient_optimization() {
        // Test with a larger graph to verify the optimization works correctly
        // Star graph with center node connected to many nodes, but no edges between outer nodes
        let get_neighbors = |node: NodeId| -> Result<Vec<NodeId>, OxidbError> {
            match node {
                1 => Ok((2..=10).collect()), // Center node connected to nodes 2-10
                n if n >= 2 && n <= 10 => Ok(vec![1]), // Outer nodes only connected to center
                _ => Ok(vec![]),
            }
        };

        // Center node should have clustering coefficient of 0 (no edges between neighbors)
        let clustering = GraphMetrics::clustering_coefficient(1, get_neighbors).unwrap();
        assert_eq!(clustering, 0.0);
    }

    #[test]
    fn test_clustering_coefficient_partial_connections() {
        // Test case with some but not all possible edges between neighbors
        let get_neighbors = |node: NodeId| -> Result<Vec<NodeId>, OxidbError> {
            match node {
                1 => Ok(vec![2, 3, 4, 5]), // Node 1 connected to 2, 3, 4, 5
                2 => Ok(vec![1, 3]),       // 2 connected to 1, 3
                3 => Ok(vec![1, 2, 4]),    // 3 connected to 1, 2, 4
                4 => Ok(vec![1, 3]),       // 4 connected to 1, 3
                5 => Ok(vec![1]),          // 5 connected only to 1
                _ => Ok(vec![]),
            }
        };

        let clustering = GraphMetrics::clustering_coefficient(1, get_neighbors).unwrap();

        // Node 1 has 4 neighbors: 2, 3, 4, 5
        // Possible edges between neighbors: 4*3/2 = 6
        // Actual edges: (2,3), (3,4) = 2 edges
        // Clustering coefficient: 2/6 = 1/3 ≈ 0.333...
        assert!((clustering - (1.0 / 3.0)).abs() < 1e-10);
    }

    #[test]
    fn test_connected_components() {
        // Two separate components: {1,2} and {3,4}
        let get_neighbors = |node: NodeId| -> Result<Vec<NodeId>, OxidbError> {
            match node {
                1 => Ok(vec![2]),
                2 => Ok(vec![1]),
                3 => Ok(vec![4]),
                4 => Ok(vec![3]),
                _ => Ok(vec![]),
            }
        };

        let all_nodes = vec![1, 2, 3, 4];
        let components = CommunityDetector::connected_components(all_nodes, get_neighbors).unwrap();

        assert_eq!(components.len(), 2);
        assert!(components.contains(&vec![1, 2]));
        assert!(components.contains(&vec![3, 4]));
    }
}
