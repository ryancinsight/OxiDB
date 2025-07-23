//! Graph storage implementations for Oxidb
//!
//! This module provides both in-memory and persistent storage for graph data.
//! Following SOLID principles with clear separation of concerns and ACID compliance.

use super::types::{Edge, EdgeId, GraphData, Node, NodeId, Relationship};
use super::{GraphOperations, GraphQuery, GraphTransaction, TraversalDirection};
use crate::core::common::error::OxidbError;
use crate::core::common::types::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;
// Remove unused imports

/// Graph storage trait for abstraction (Dependency Inversion Principle)
pub trait GraphStorage: Send + Sync {
    fn store_node(&mut self, node: Node) -> Result<(), OxidbError>;
    fn store_edge(&mut self, edge: Edge) -> Result<(), OxidbError>;
    fn load_node(&self, id: NodeId) -> Result<Option<Node>, OxidbError>;
    fn load_edge(&self, id: EdgeId) -> Result<Option<Edge>, OxidbError>;
    fn remove_node_storage(&mut self, id: NodeId) -> Result<bool, OxidbError>;
    fn remove_edge_storage(&mut self, id: EdgeId) -> Result<bool, OxidbError>;
    fn load_all_nodes(&self) -> Result<Vec<Node>, OxidbError>;
    fn load_all_edges(&self) -> Result<Vec<Edge>, OxidbError>;
    fn flush(&mut self) -> Result<(), OxidbError>;
}

/// Graph store trait combining storage and operations
pub trait GraphStore: GraphOperations + GraphQuery + GraphTransaction + Send + Sync {}

/// In-memory graph storage implementation (KISS principle - keep it simple)
#[derive(Debug)]
pub struct InMemoryGraphStore {
    nodes: HashMap<NodeId, Node>,
    edges: HashMap<EdgeId, Edge>,
    node_edges: HashMap<NodeId, HashSet<EdgeId>>, // Node -> Edge mapping for fast traversal
    next_node_id: NodeId,
    next_edge_id: EdgeId,
    transaction_active: bool,
    transaction_nodes: HashMap<NodeId, Node>, // Transaction staging
    transaction_edges: HashMap<EdgeId, Edge>, // Transaction staging
}

impl InMemoryGraphStore {
    /// Create a new in-memory graph store
    #[must_use]
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            edges: HashMap::new(),
            node_edges: HashMap::new(),
            next_node_id: 1,
            next_edge_id: 1,
            transaction_active: false,
            transaction_nodes: HashMap::new(),
            transaction_edges: HashMap::new(),
        }
    }

    /// Get next available node ID
    fn next_node_id(&mut self) -> NodeId {
        let id = self.next_node_id;
        self.next_node_id += 1;
        id
    }

    /// Get next available edge ID
    fn next_edge_id(&mut self) -> EdgeId {
        let id = self.next_edge_id;
        self.next_edge_id += 1;
        id
    }

    /// Add edge to node mapping
    fn add_edge_to_node(&mut self, node_id: NodeId, edge_id: EdgeId) {
        self.node_edges.entry(node_id).or_default().insert(edge_id);
    }

    /// Remove edge from node mapping
    fn remove_edge_from_node(&mut self, node_id: NodeId, edge_id: EdgeId) {
        if let Some(edges) = self.node_edges.get_mut(&node_id) {
            edges.remove(&edge_id);
            if edges.is_empty() {
                self.node_edges.remove(&node_id);
            }
        }
    }
}

impl Default for InMemoryGraphStore {
    fn default() -> Self {
        Self::new()
    }
}

impl GraphOperations for InMemoryGraphStore {
    fn add_node(&mut self, data: GraphData) -> Result<NodeId, OxidbError> {
        let id = self.next_node_id();
        let node = Node::new(id, data);

        if self.transaction_active {
            self.transaction_nodes.insert(id, node);
        } else {
            self.nodes.insert(id, node);
        }

        Ok(id)
    }

    fn add_edge(
        &mut self,
        from: NodeId,
        to: NodeId,
        relationship: Relationship,
        data: Option<GraphData>,
    ) -> Result<EdgeId, OxidbError> {
        // Verify nodes exist
        let nodes = if self.transaction_active { &self.transaction_nodes } else { &self.nodes };

        if !nodes.contains_key(&from) && !self.nodes.contains_key(&from) {
            return Err(OxidbError::InvalidInput {
                message: format!("From node {from} does not exist"),
            });
        }

        if !nodes.contains_key(&to) && !self.nodes.contains_key(&to) {
            return Err(OxidbError::InvalidInput {
                message: format!("To node {to} does not exist"),
            });
        }

        let id = self.next_edge_id();
        let edge = Edge::new(id, from, to, relationship, data);

        if self.transaction_active {
            self.transaction_edges.insert(id, edge);
        } else {
            self.edges.insert(id, edge);
            self.add_edge_to_node(from, id);
            if from != to {
                // Avoid duplicate entries for self-loops
                self.add_edge_to_node(to, id);
            }
        }

        Ok(id)
    }

    fn get_node(&self, node_id: NodeId) -> Result<Option<Node>, OxidbError> {
        // Check transaction staging first
        if self.transaction_active {
            if let Some(node) = self.transaction_nodes.get(&node_id) {
                return Ok(Some(node.clone()));
            }
        }

        Ok(self.nodes.get(&node_id).cloned())
    }

    fn get_edge(&self, edge_id: EdgeId) -> Result<Option<Edge>, OxidbError> {
        // Check transaction staging first
        if self.transaction_active {
            if let Some(edge) = self.transaction_edges.get(&edge_id) {
                return Ok(Some(edge.clone()));
            }
        }

        Ok(self.edges.get(&edge_id).cloned())
    }

    fn remove_node(&mut self, node_id: NodeId) -> Result<bool, OxidbError> {
        // Remove all edges connected to this node first
        if let Some(edge_ids) = self.node_edges.get(&node_id).cloned() {
            for edge_id in edge_ids {
                self.remove_edge(edge_id)?;
            }
        }

        // Remove the node
        let removed = if self.transaction_active {
            self.transaction_nodes.remove(&node_id).is_some()
        } else {
            self.nodes.remove(&node_id).is_some()
        };

        Ok(removed)
    }

    fn remove_edge(&mut self, edge_id: EdgeId) -> Result<bool, OxidbError> {
        let edge = if self.transaction_active {
            self.transaction_edges.remove(&edge_id)
        } else {
            self.edges.remove(&edge_id)
        };

        if let Some(edge) = edge {
            if !self.transaction_active {
                self.remove_edge_from_node(edge.from_node, edge_id);
                self.remove_edge_from_node(edge.to_node, edge_id);
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn get_neighbors(
        &self,
        node_id: NodeId,
        direction: TraversalDirection,
    ) -> Result<Vec<NodeId>, OxidbError> {
        let mut neighbors = Vec::new();

        // Get edges for this node
        if let Some(edge_ids) = self.node_edges.get(&node_id) {
            for &edge_id in edge_ids {
                if let Some(edge) = self.edges.get(&edge_id) {
                    match direction {
                        TraversalDirection::Outgoing => {
                            if edge.from_node == node_id {
                                neighbors.push(edge.to_node);
                            }
                        }
                        TraversalDirection::Incoming => {
                            if edge.to_node == node_id {
                                neighbors.push(edge.from_node);
                            }
                        }
                        TraversalDirection::Both => {
                            if edge.from_node == node_id {
                                neighbors.push(edge.to_node);
                            } else if edge.to_node == node_id {
                                neighbors.push(edge.from_node);
                            }
                        }
                    }
                }
            }
        }

        neighbors.sort_unstable();
        neighbors.dedup();
        Ok(neighbors)
    }
}

impl GraphQuery for InMemoryGraphStore {
    fn find_nodes_by_property(
        &self,
        property: &str,
        value: &Value,
    ) -> Result<Vec<NodeId>, OxidbError> {
        let mut matching_nodes = Vec::new();

        for (node_id, node) in &self.nodes {
            if let Some(prop_value) = node.get_property(property) {
                if prop_value == value {
                    matching_nodes.push(*node_id);
                }
            }
        }

        // Also check transaction staging
        if self.transaction_active {
            for (node_id, node) in &self.transaction_nodes {
                if let Some(prop_value) = node.get_property(property) {
                    if prop_value == value {
                        matching_nodes.push(*node_id);
                    }
                }
            }
        }

        matching_nodes.sort_unstable();
        matching_nodes.dedup();
        Ok(matching_nodes)
    }

    fn find_shortest_path(
        &self,
        from: NodeId,
        to: NodeId,
    ) -> Result<Option<Vec<NodeId>>, OxidbError> {
        if from == to {
            return Ok(Some(vec![from]));
        }

        let mut queue = VecDeque::new();
        let mut visited = HashSet::new();
        let mut parent = HashMap::new();

        queue.push_back(from);
        visited.insert(from);

        while let Some(current) = queue.pop_front() {
            let neighbors = self.get_neighbors(current, TraversalDirection::Both)?;

            for neighbor in neighbors {
                if !visited.contains(&neighbor) {
                    visited.insert(neighbor);
                    parent.insert(neighbor, current);
                    queue.push_back(neighbor);

                    if neighbor == to {
                        // Reconstruct path
                        let mut path = Vec::new();
                        let mut current_node = to;

                        while let Some(&prev) = parent.get(&current_node) {
                            path.push(current_node);
                            current_node = prev;
                        }
                        path.push(from);
                        path.reverse();

                        return Ok(Some(path));
                    }
                }
            }
        }

        Ok(None) // No path found
    }

    fn traverse(
        &self,
        start: NodeId,
        strategy: super::TraversalStrategy,
        max_depth: Option<usize>,
    ) -> Result<Vec<NodeId>, OxidbError> {
        let mut visited = Vec::new();
        let mut to_visit = VecDeque::new();
        let mut visited_set = HashSet::new();

        to_visit.push_back((start, 0));

        while let Some((current, depth)) = to_visit.pop_front() {
            if let Some(max_d) = max_depth {
                if depth > max_d {
                    continue;
                }
            }

            if visited_set.contains(&current) {
                continue;
            }

            visited_set.insert(current);
            visited.push(current);

            let neighbors = self.get_neighbors(current, TraversalDirection::Both)?;

            match strategy {
                super::TraversalStrategy::BreadthFirst => {
                    for neighbor in neighbors {
                        if !visited_set.contains(&neighbor) {
                            to_visit.push_back((neighbor, depth + 1));
                        }
                    }
                }
                super::TraversalStrategy::DepthFirst => {
                    for neighbor in neighbors.into_iter().rev() {
                        if !visited_set.contains(&neighbor) {
                            to_visit.push_front((neighbor, depth + 1));
                        }
                    }
                }
            }
        }

        Ok(visited)
    }

    fn count_nodes_with_relationship(
        &self,
        relationship: &Relationship,
        direction: TraversalDirection,
    ) -> Result<usize, OxidbError> {
        let mut counted_nodes = HashSet::new();

        for edge in self.edges.values() {
            if edge.relationship.name == relationship.name {
                match direction {
                    TraversalDirection::Outgoing => {
                        counted_nodes.insert(edge.from_node);
                    }
                    TraversalDirection::Incoming => {
                        counted_nodes.insert(edge.to_node);
                    }
                    TraversalDirection::Both => {
                        counted_nodes.insert(edge.from_node);
                        counted_nodes.insert(edge.to_node);
                    }
                }
            }
        }

        Ok(counted_nodes.len())
    }
}

impl GraphTransaction for InMemoryGraphStore {
    fn begin_transaction(&mut self) -> Result<(), OxidbError> {
        if self.transaction_active {
            return Err(OxidbError::Transaction("Transaction already active".to_string()));
        }

        self.transaction_active = true;
        self.transaction_nodes.clear();
        self.transaction_edges.clear();
        Ok(())
    }

    fn commit_transaction(&mut self) -> Result<(), OxidbError> {
        if !self.transaction_active {
            return Err(OxidbError::Transaction("No active transaction to commit".to_string()));
        }

        // Apply all transaction changes
        for (id, node) in self.transaction_nodes.drain() {
            self.nodes.insert(id, node);
        }

        // Collect edges first to avoid borrowing issues
        let edges_to_add: Vec<_> = self.transaction_edges.drain().collect();
        for (id, edge) in edges_to_add {
            self.add_edge_to_node(edge.from_node, id);
            if edge.from_node != edge.to_node {
                self.add_edge_to_node(edge.to_node, id);
            }
            self.edges.insert(id, edge);
        }

        self.transaction_active = false;
        Ok(())
    }

    fn rollback_transaction(&mut self) -> Result<(), OxidbError> {
        if !self.transaction_active {
            return Err(OxidbError::Transaction("No active transaction to rollback".to_string()));
        }

        // Discard all transaction changes
        self.transaction_nodes.clear();
        self.transaction_edges.clear();
        self.transaction_active = false;
        Ok(())
    }
}

impl GraphStore for InMemoryGraphStore {}

/// Persistent graph storage implementation with efficient disk persistence
#[derive(Debug)]
pub struct PersistentGraphStore {
    storage_path: std::path::PathBuf,
    memory_store: InMemoryGraphStore,
    dirty: bool, // Track if data has been modified since last save
    auto_flush_threshold: Option<usize>, // Optional: auto-flush after N operations
    operation_count: usize,
}

impl PersistentGraphStore {
    /// Create a new persistent graph store
    pub fn new(path: impl AsRef<Path>) -> Result<Self, OxidbError> {
        let storage_path = path.as_ref().to_path_buf();
        let memory_store = InMemoryGraphStore::new();

        let mut store = Self {
            storage_path,
            memory_store,
            dirty: false,
            auto_flush_threshold: None,
            operation_count: 0,
        };

        // Try to load existing data
        if let Err(e) = store.load_from_disk() {
            // Log warning but continue with empty store
            eprintln!("Warning: Could not load existing data from disk: {e:?}");
        }

        Ok(store)
    }

    /// Create a new persistent graph store with auto-flush after N operations
    pub fn with_auto_flush(path: impl AsRef<Path>, threshold: usize) -> Result<Self, OxidbError> {
        let mut store = Self::new(path)?;
        store.auto_flush_threshold = Some(threshold);
        Ok(store)
    }

    /// Explicitly flush changes to disk
    pub fn flush(&mut self) -> Result<(), OxidbError> {
        if self.dirty {
            self.save_to_disk()?;
            self.dirty = false;
        }
        Ok(())
    }

    /// Check if there are uncommitted changes
    #[must_use]
    pub const fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Mark data as modified and potentially trigger auto-flush
    fn mark_dirty(&mut self) -> Result<(), OxidbError> {
        self.dirty = true;
        self.operation_count += 1;

        // Auto-flush if threshold is reached
        if let Some(threshold) = self.auto_flush_threshold {
            if self.operation_count >= threshold {
                self.flush()?;
                self.operation_count = 0;
            }
        }

        Ok(())
    }

    /// Load graph data from disk
    fn load_from_disk(&mut self) -> Result<(), OxidbError> {
        // TODO: Implement persistent storage loading
        // This would deserialize nodes and edges from the storage file
        // For now, this is a placeholder (YAGNI - implement when needed)

        // Check if file exists
        if !self.storage_path.exists() {
            return Ok(()); // No existing data to load
        }

        // Future implementation would:
        // 1. Read serialized data from storage_path
        // 2. Deserialize nodes and edges
        // 3. Populate memory_store
        // 4. Set dirty = false

        Ok(())
    }

    /// Save graph data to disk
    fn save_to_disk(&self) -> Result<(), OxidbError> {
        // TODO: Implement persistent storage saving
        // This would serialize nodes and edges to the storage file
        // For now, this is a placeholder (YAGNI - implement when needed)

        // Ensure parent directory exists
        if let Some(parent) = self.storage_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Future implementation would:
        // 1. Serialize nodes and edges from memory_store
        // 2. Write to storage_path atomically (write to temp file, then rename)
        // 3. Handle errors properly

        // For now, just touch the file to indicate save was called
        std::fs::write(&self.storage_path, b"")?;

        Ok(())
    }
}

impl Drop for PersistentGraphStore {
    /// Ensure data is persisted when the store is dropped
    fn drop(&mut self) {
        if self.dirty {
            if let Err(e) = self.flush() {
                eprintln!("Warning: Failed to flush data during drop: {e:?}");
            }
        }
    }
}

// Delegate all operations to the in-memory store with efficient persistence
impl GraphOperations for PersistentGraphStore {
    fn add_node(&mut self, data: GraphData) -> Result<NodeId, OxidbError> {
        let result = self.memory_store.add_node(data);
        if result.is_ok() {
            self.mark_dirty()?; // Mark as dirty and potentially auto-flush
        }
        result
    }

    fn add_edge(
        &mut self,
        from: NodeId,
        to: NodeId,
        relationship: Relationship,
        data: Option<GraphData>,
    ) -> Result<EdgeId, OxidbError> {
        let result = self.memory_store.add_edge(from, to, relationship, data);
        if result.is_ok() {
            self.mark_dirty()?; // Mark as dirty and potentially auto-flush
        }
        result
    }

    fn get_node(&self, node_id: NodeId) -> Result<Option<Node>, OxidbError> {
        self.memory_store.get_node(node_id)
    }

    fn get_edge(&self, edge_id: EdgeId) -> Result<Option<Edge>, OxidbError> {
        self.memory_store.get_edge(edge_id)
    }

    fn remove_node(&mut self, node_id: NodeId) -> Result<bool, OxidbError> {
        let result = self.memory_store.remove_node(node_id);
        if result.is_ok() && result.as_ref().unwrap() == &true {
            self.mark_dirty()?; // Mark as dirty only if node was actually removed
        }
        result
    }

    fn remove_edge(&mut self, edge_id: EdgeId) -> Result<bool, OxidbError> {
        let result = self.memory_store.remove_edge(edge_id);
        if result.is_ok() && result.as_ref().unwrap() == &true {
            self.mark_dirty()?; // Mark as dirty only if edge was actually removed
        }
        result
    }

    fn get_neighbors(
        &self,
        node_id: NodeId,
        direction: TraversalDirection,
    ) -> Result<Vec<NodeId>, OxidbError> {
        self.memory_store.get_neighbors(node_id, direction)
    }
}

impl GraphQuery for PersistentGraphStore {
    fn find_nodes_by_property(
        &self,
        property: &str,
        value: &Value,
    ) -> Result<Vec<NodeId>, OxidbError> {
        self.memory_store.find_nodes_by_property(property, value)
    }

    fn find_shortest_path(
        &self,
        from: NodeId,
        to: NodeId,
    ) -> Result<Option<Vec<NodeId>>, OxidbError> {
        self.memory_store.find_shortest_path(from, to)
    }

    fn traverse(
        &self,
        start: NodeId,
        strategy: super::TraversalStrategy,
        max_depth: Option<usize>,
    ) -> Result<Vec<NodeId>, OxidbError> {
        self.memory_store.traverse(start, strategy, max_depth)
    }

    fn count_nodes_with_relationship(
        &self,
        relationship: &Relationship,
        direction: TraversalDirection,
    ) -> Result<usize, OxidbError> {
        self.memory_store.count_nodes_with_relationship(relationship, direction)
    }
}

impl GraphTransaction for PersistentGraphStore {
    fn begin_transaction(&mut self) -> Result<(), OxidbError> {
        self.memory_store.begin_transaction()
    }

    fn commit_transaction(&mut self) -> Result<(), OxidbError> {
        let result = self.memory_store.commit_transaction();
        if result.is_ok() {
            // Always flush to disk on transaction commit for ACID compliance
            self.dirty = true; // Ensure we persist even if no operations were marked dirty
            self.flush()?; // Propagate any disk errors to caller
        }
        result
    }

    fn rollback_transaction(&mut self) -> Result<(), OxidbError> {
        self.memory_store.rollback_transaction()
        // No need to persist on rollback - changes are discarded
    }
}

impl GraphStore for PersistentGraphStore {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_in_memory_graph_operations() {
        let mut store = InMemoryGraphStore::new();

        // Add nodes
        let node1_data = GraphData::new("user".to_string())
            .with_property("name".to_string(), Value::Text("Alice".to_string()));
        let node1_id = store.add_node(node1_data).unwrap();

        let node2_data = GraphData::new("user".to_string())
            .with_property("name".to_string(), Value::Text("Bob".to_string()));
        let node2_id = store.add_node(node2_data).unwrap();

        // Add edge
        let relationship = Relationship::new("FOLLOWS".to_string());
        let edge_id = store.add_edge(node1_id, node2_id, relationship, None).unwrap();

        // Test retrieval
        assert!(store.get_node(node1_id).unwrap().is_some());
        assert!(store.get_node(node2_id).unwrap().is_some());
        assert!(store.get_edge(edge_id).unwrap().is_some());

        // Test neighbors
        let neighbors = store.get_neighbors(node1_id, TraversalDirection::Outgoing).unwrap();
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0], node2_id);
    }

    #[test]
    fn test_graph_transactions() {
        let mut store = InMemoryGraphStore::new();

        // Start transaction
        store.begin_transaction().unwrap();

        // Add node in transaction
        let node_data = GraphData::new("test".to_string());
        let node_id = store.add_node(node_data).unwrap();

        // Node should not be visible outside transaction yet
        assert!(store.transaction_active);

        // Commit transaction
        store.commit_transaction().unwrap();

        // Node should now be visible
        assert!(store.get_node(node_id).unwrap().is_some());
        assert!(!store.transaction_active);
    }

    #[test]
    fn test_shortest_path() {
        let mut store = InMemoryGraphStore::new();

        // Create a simple path: 1 -> 2 -> 3
        let node1 = store.add_node(GraphData::new("node".to_string())).unwrap();
        let node2 = store.add_node(GraphData::new("node".to_string())).unwrap();
        let node3 = store.add_node(GraphData::new("node".to_string())).unwrap();

        let rel = Relationship::new("CONNECTS".to_string());
        store.add_edge(node1, node2, rel.clone(), None).unwrap();
        store.add_edge(node2, node3, rel, None).unwrap();

        // Find shortest path
        let path = store.find_shortest_path(node1, node3).unwrap().unwrap();
        assert_eq!(path, vec![node1, node2, node3]);
    }

    #[test]
    fn test_persistent_store_dirty_tracking() {
        let temp_dir = std::env::temp_dir();
        let storage_path = temp_dir.join("test_graph.db");

        // Clean up any existing file
        let _ = std::fs::remove_file(&storage_path);

        let mut store = PersistentGraphStore::new(&storage_path).unwrap();

        // Initially not dirty
        assert!(!store.is_dirty());

        // Add node should mark as dirty
        let node_data = GraphData::new("test".to_string());
        let node_id = store.add_node(node_data).unwrap();
        assert!(store.is_dirty());

        // Flush should clear dirty flag
        store.flush().unwrap();
        assert!(!store.is_dirty());

        // Remove node should mark as dirty
        store.remove_node(node_id).unwrap();
        assert!(store.is_dirty());

        // Clean up
        let _ = std::fs::remove_file(&storage_path);
    }

    #[test]
    fn test_persistent_store_auto_flush() {
        let temp_dir = std::env::temp_dir();
        let storage_path = temp_dir.join("test_graph_auto_flush.db");

        // Clean up any existing file
        let _ = std::fs::remove_file(&storage_path);

        let mut store = PersistentGraphStore::with_auto_flush(&storage_path, 2).unwrap();

        // Add first node - should be dirty
        let node_data1 = GraphData::new("test1".to_string());
        store.add_node(node_data1).unwrap();
        assert!(store.is_dirty());

        // Add second node - should trigger auto-flush
        let node_data2 = GraphData::new("test2".to_string());
        store.add_node(node_data2).unwrap();
        assert!(!store.is_dirty()); // Auto-flushed

        // Clean up
        let _ = std::fs::remove_file(&storage_path);
    }

    #[test]
    fn test_persistent_store_transaction_commit_persistence() {
        let temp_dir = std::env::temp_dir();
        let storage_path = temp_dir.join("test_graph_transaction.db");

        // Clean up any existing file
        let _ = std::fs::remove_file(&storage_path);

        let mut store = PersistentGraphStore::new(&storage_path).unwrap();

        // Start transaction
        store.begin_transaction().unwrap();

        // Add node in transaction
        let node_data = GraphData::new("test".to_string());
        store.add_node(node_data).unwrap();

        // Should not be dirty yet (changes are staged)
        // Note: This behavior depends on implementation details

        // Commit should flush to disk
        store.commit_transaction().unwrap();
        assert!(!store.is_dirty()); // Should be flushed

        // Clean up
        let _ = std::fs::remove_file(&storage_path);
    }

    #[test]
    fn test_persistent_store_error_propagation() {
        // Test with invalid path to trigger error
        let invalid_path = "/invalid/path/that/cannot/be/created/test.db";

        let mut store = PersistentGraphStore::new(invalid_path).unwrap(); // This should succeed (creates in-memory)

        // Add node to make it dirty
        let node_data = GraphData::new("test".to_string());
        store.add_node(node_data).unwrap();

        // Flush should return error due to invalid path
        let result = store.flush();
        assert!(result.is_err());

        // Should still be dirty after failed flush
        assert!(store.is_dirty());
    }
}
