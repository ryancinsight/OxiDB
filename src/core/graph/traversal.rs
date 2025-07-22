//! Graph traversal algorithms for Oxidb
//!
//! This module provides various graph traversal strategies and algorithms.
//! Following SOLID principles with extensible traversal strategies.

use super::types::{EdgeId, NodeId};
use crate::core::common::OxidbError;
use std::collections::{HashMap, HashSet, VecDeque};

/// Traversal direction for graph operations
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TraversalDirection {
    Outgoing,
    Incoming,
    Both,
}

/// Graph traversal strategy
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TraversalStrategy {
    BreadthFirst,
    DepthFirst,
}

/// Graph traversal trait following Interface Segregation Principle
pub trait GraphTraversal {
    /// Perform breadth-first traversal
    fn breadth_first_search(
        &self,
        start: NodeId,
        max_depth: Option<usize>,
    ) -> Result<Vec<NodeId>, OxidbError>;

    /// Perform depth-first traversal
    fn depth_first_search(
        &self,
        start: NodeId,
        max_depth: Option<usize>,
    ) -> Result<Vec<NodeId>, OxidbError>;

    /// Find all nodes within a certain distance
    fn find_nodes_within_distance(
        &self,
        start: NodeId,
        distance: usize,
    ) -> Result<Vec<NodeId>, OxidbError>;

    /// Get all connected components
    fn connected_components(&self) -> Result<Vec<Vec<NodeId>>, OxidbError>;
}

/// Traversal visitor pattern for custom operations during traversal
pub trait TraversalVisitor {
    /// Called when visiting a node
    fn visit_node(&mut self, node_id: NodeId, depth: usize) -> Result<bool, OxidbError>; // Return false to stop traversal

    /// Called when visiting an edge
    fn visit_edge(
        &mut self,
        edge_id: EdgeId,
        from: NodeId,
        to: NodeId,
        depth: usize,
    ) -> Result<bool, OxidbError>;

    /// Called when backtracking (for DFS)
    fn backtrack(&mut self, node_id: NodeId, depth: usize) -> Result<(), OxidbError>;
}

/// Traversal result containing the path and metadata
#[derive(Debug, Clone)]
pub struct TraversalResult {
    pub path: Vec<NodeId>,
    pub depths: HashMap<NodeId, usize>,
    pub parent_map: HashMap<NodeId, NodeId>,
    pub total_nodes_visited: usize,
}

impl TraversalResult {
    /// Create a new traversal result
    #[must_use]
    pub fn new() -> Self {
        Self {
            path: Vec::new(),
            depths: HashMap::new(),
            parent_map: HashMap::new(),
            total_nodes_visited: 0,
        }
    }

    /// Add a node to the result
    pub fn add_node(&mut self, node_id: NodeId, depth: usize, parent: Option<NodeId>) {
        self.path.push(node_id);
        self.depths.insert(node_id, depth);
        if let Some(parent_id) = parent {
            self.parent_map.insert(node_id, parent_id);
        }
        self.total_nodes_visited += 1;
    }

    /// Get the depth of a node
    #[must_use]
    pub fn get_depth(&self, node_id: NodeId) -> Option<usize> {
        self.depths.get(&node_id).copied()
    }

    /// Get the parent of a node
    #[must_use]
    pub fn get_parent(&self, node_id: NodeId) -> Option<NodeId> {
        self.parent_map.get(&node_id).copied()
    }

    /// Reconstruct path from start to a specific node
    #[must_use]
    pub fn path_to_node(&self, target: NodeId) -> Option<Vec<NodeId>> {
        let mut path = Vec::new();
        let mut current = target;

        path.push(current);

        while let Some(parent) = self.get_parent(current) {
            path.push(parent);
            current = parent;
        }

        path.reverse();
        Some(path)
    }
}

impl Default for TraversalResult {
    fn default() -> Self {
        Self::new()
    }
}

/// Generic traversal engine implementing various algorithms
pub struct TraversalEngine;

impl TraversalEngine {
    /// Perform breadth-first search with visitor pattern
    pub fn bfs_with_visitor<V, F>(
        start: NodeId,
        max_depth: Option<usize>,
        get_neighbors: F,
        mut visitor: V,
    ) -> Result<TraversalResult, OxidbError>
    where
        V: TraversalVisitor,
        F: Fn(NodeId) -> Result<Vec<NodeId>, OxidbError>,
    {
        let mut result = TraversalResult::new();
        let mut queue = VecDeque::new();
        let mut visited = HashSet::new();

        queue.push_back((start, 0, None));

        while let Some((current, depth, parent)) = queue.pop_front() {
            if let Some(max_d) = max_depth {
                if depth > max_d {
                    continue;
                }
            }

            if visited.contains(&current) {
                continue;
            }

            visited.insert(current);
            result.add_node(current, depth, parent);

            // Visit the node
            if !visitor.visit_node(current, depth)? {
                break; // Visitor requested to stop
            }

            // Get neighbors and add to queue
            let neighbors = get_neighbors(current)?;
            for neighbor in neighbors {
                if !visited.contains(&neighbor) {
                    queue.push_back((neighbor, depth + 1, Some(current)));
                }
            }
        }

        Ok(result)
    }

    /// Perform depth-first search with visitor pattern
    pub fn dfs_with_visitor<V, F>(
        start: NodeId,
        max_depth: Option<usize>,
        get_neighbors: F,
        mut visitor: V,
    ) -> Result<TraversalResult, OxidbError>
    where
        V: TraversalVisitor,
        F: Fn(NodeId) -> Result<Vec<NodeId>, OxidbError>,
    {
        let mut result = TraversalResult::new();
        let mut stack = Vec::new();
        let mut visited = HashSet::new();

        stack.push((start, 0, None, false)); // (node, depth, parent, backtracking)

        while let Some((current, depth, parent, is_backtrack)) = stack.pop() {
            if is_backtrack {
                visitor.backtrack(current, depth)?;
                continue;
            }

            if let Some(max_d) = max_depth {
                if depth > max_d {
                    continue;
                }
            }

            if visited.contains(&current) {
                continue;
            }

            visited.insert(current);
            result.add_node(current, depth, parent);

            // Visit the node
            if !visitor.visit_node(current, depth)? {
                break; // Visitor requested to stop
            }

            // Add backtrack marker
            stack.push((current, depth, parent, true));

            // Get neighbors and add to stack (in reverse order for consistent ordering)
            let neighbors = get_neighbors(current)?;
            for neighbor in neighbors.into_iter().rev() {
                if !visited.contains(&neighbor) {
                    stack.push((neighbor, depth + 1, Some(current), false));
                }
            }
        }

        Ok(result)
    }

    /// Find shortest path between two nodes using BFS
    pub fn shortest_path<F>(
        start: NodeId,
        target: NodeId,
        get_neighbors: F,
    ) -> Result<Option<Vec<NodeId>>, OxidbError>
    where
        F: Fn(NodeId) -> Result<Vec<NodeId>, OxidbError>,
    {
        if start == target {
            return Ok(Some(vec![start]));
        }

        let mut queue = VecDeque::new();
        let mut visited = HashSet::new();
        let mut parent = HashMap::new();

        queue.push_back(start);
        visited.insert(start);

        while let Some(current) = queue.pop_front() {
            let neighbors = get_neighbors(current)?;

            for neighbor in neighbors {
                if !visited.contains(&neighbor) {
                    visited.insert(neighbor);
                    parent.insert(neighbor, current);
                    queue.push_back(neighbor);

                    if neighbor == target {
                        // Reconstruct path
                        let mut path = Vec::new();
                        let mut current_node = target;

                        while let Some(&prev) = parent.get(&current_node) {
                            path.push(current_node);
                            current_node = prev;
                        }
                        path.push(start);
                        path.reverse();

                        return Ok(Some(path));
                    }
                }
            }
        }

        Ok(None) // No path found
    }

    /// Find all connected components in the graph
    pub fn connected_components<F>(
        all_nodes: Vec<NodeId>,
        get_neighbors: F,
    ) -> Result<Vec<Vec<NodeId>>, OxidbError>
    where
        F: Fn(NodeId) -> Result<Vec<NodeId>, OxidbError>,
    {
        let mut components = Vec::new();
        let mut global_visited = HashSet::new();

        for &node in &all_nodes {
            if !global_visited.contains(&node) {
                let mut component = Vec::new();
                let mut stack = vec![node];
                let mut local_visited = HashSet::new();

                while let Some(current) = stack.pop() {
                    if local_visited.contains(&current) {
                        continue;
                    }

                    local_visited.insert(current);
                    global_visited.insert(current);
                    component.push(current);

                    let neighbors = get_neighbors(current)?;
                    for neighbor in neighbors {
                        if !local_visited.contains(&neighbor) {
                            stack.push(neighbor);
                        }
                    }
                }

                if !component.is_empty() {
                    component.sort_unstable();
                    components.push(component);
                }
            }
        }

        Ok(components)
    }
}

/// Simple visitor that collects all visited nodes
pub struct CollectingVisitor {
    pub visited_nodes: Vec<NodeId>,
    pub visited_edges: Vec<EdgeId>,
}

impl CollectingVisitor {
    #[must_use]
    pub const fn new() -> Self {
        Self { visited_nodes: Vec::new(), visited_edges: Vec::new() }
    }
}

impl Default for CollectingVisitor {
    fn default() -> Self {
        Self::new()
    }
}

impl TraversalVisitor for CollectingVisitor {
    fn visit_node(&mut self, node_id: NodeId, _depth: usize) -> Result<bool, OxidbError> {
        self.visited_nodes.push(node_id);
        Ok(true) // Continue traversal
    }

    fn visit_edge(
        &mut self,
        edge_id: EdgeId,
        _from: NodeId,
        _to: NodeId,
        _depth: usize,
    ) -> Result<bool, OxidbError> {
        self.visited_edges.push(edge_id);
        Ok(true) // Continue traversal
    }

    fn backtrack(&mut self, _node_id: NodeId, _depth: usize) -> Result<(), OxidbError> {
        // Nothing to do for collecting visitor
        Ok(())
    }
}

/// Visitor that stops at a specific node
pub struct TargetVisitor {
    pub target: NodeId,
    pub found: bool,
}

impl TargetVisitor {
    #[must_use]
    pub const fn new(target: NodeId) -> Self {
        Self { target, found: false }
    }
}

impl TraversalVisitor for TargetVisitor {
    fn visit_node(&mut self, node_id: NodeId, _depth: usize) -> Result<bool, OxidbError> {
        if node_id == self.target {
            self.found = true;
            Ok(false) // Stop traversal
        } else {
            Ok(true) // Continue traversal
        }
    }

    fn visit_edge(
        &mut self,
        _edge_id: EdgeId,
        _from: NodeId,
        _to: NodeId,
        _depth: usize,
    ) -> Result<bool, OxidbError> {
        Ok(true) // Continue traversal
    }

    fn backtrack(&mut self, _node_id: NodeId, _depth: usize) -> Result<(), OxidbError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_traversal_result() {
        let mut result = TraversalResult::new();

        result.add_node(1, 0, None);
        result.add_node(2, 1, Some(1));
        result.add_node(3, 2, Some(2));

        assert_eq!(result.get_depth(1), Some(0));
        assert_eq!(result.get_depth(2), Some(1));
        assert_eq!(result.get_depth(3), Some(2));

        assert_eq!(result.get_parent(2), Some(1));
        assert_eq!(result.get_parent(3), Some(2));
        assert_eq!(result.get_parent(1), None);

        let path = result.path_to_node(3).unwrap();
        assert_eq!(path, vec![1, 2, 3]);
    }

    #[test]
    fn test_collecting_visitor() {
        let mut visitor = CollectingVisitor::new();

        visitor.visit_node(1, 0).unwrap();
        visitor.visit_node(2, 1).unwrap();
        visitor.visit_edge(1, 1, 2, 1).unwrap();

        assert_eq!(visitor.visited_nodes, vec![1, 2]);
        assert_eq!(visitor.visited_edges, vec![1]);
    }

    #[test]
    fn test_target_visitor() {
        let mut visitor = TargetVisitor::new(5);

        assert!(visitor.visit_node(1, 0).unwrap()); // Continue
        assert!(visitor.visit_node(3, 1).unwrap()); // Continue
        assert!(!visitor.visit_node(5, 2).unwrap()); // Stop - found target
        assert!(visitor.found);
    }

    #[test]
    fn test_shortest_path_simple() {
        // Mock graph: 1 -> 2 -> 3
        let get_neighbors = |node: NodeId| -> Result<Vec<NodeId>, OxidbError> {
            match node {
                1 => Ok(vec![2]),
                2 => Ok(vec![1, 3]),
                3 => Ok(vec![2]),
                _ => Ok(vec![]),
            }
        };

        let path = TraversalEngine::shortest_path(1, 3, get_neighbors).unwrap().unwrap();
        assert_eq!(path, vec![1, 2, 3]);
    }

    #[test]
    fn test_connected_components() {
        // Mock graph with two components: {1, 2} and {3, 4}
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
        let components = TraversalEngine::connected_components(all_nodes, get_neighbors).unwrap();

        assert_eq!(components.len(), 2);
        assert!(components.contains(&vec![1, 2]));
        assert!(components.contains(&vec![3, 4]));
    }
}
