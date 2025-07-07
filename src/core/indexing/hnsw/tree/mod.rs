use std::collections::HashMap;

use crate::core::query::commands::{Key as PrimaryKey, Value as TraitValue};
use super::error::{HnswError, HnswResult};
use super::graph::HnswGraph;
use super::node::{NodeId, Vector, DistanceFunction};

/// Main HNSW Index implementation for vector similarity search
#[derive(Debug)]
pub struct HnswIndex {
    /// Name of this index
    pub name: String,
    
    /// The underlying HNSW graph
    graph: HnswGraph,
    
    /// Map from primary keys to node IDs for efficient lookups
    pk_to_node: HashMap<PrimaryKey, NodeId>,
    
    /// Vector dimension
    dimension: usize,
}

impl HnswIndex {
    /// Create a new HNSW index
    pub fn new(
        name: String,
        dimension: usize,
        max_connections: usize,
        ef_construction: usize,
        distance_function: DistanceFunction,
    ) -> HnswResult<Self> {
        let graph = HnswGraph::new(dimension, max_connections, ef_construction, distance_function);
        
        Ok(Self {
            name,
            graph,
            pk_to_node: HashMap::new(),
            dimension,
        })
    }

    /// Parse a vector value from bytes
    pub fn parse_vector_value(&self, value: &TraitValue) -> Result<Vector, crate::core::common::OxidbError> {
        // Expected format: dimension (4 bytes) + f32 values
        if value.len() < 4 {
            return Err(crate::core::common::OxidbError::Index(
                "Vector value too short".to_string()
            ));
        }

        let dimension_bytes = &value[0..4];
        let dimension = u32::from_le_bytes([
            dimension_bytes[0],
            dimension_bytes[1], 
            dimension_bytes[2],
            dimension_bytes[3]
        ]) as usize;

        if dimension != self.dimension {
            return Err(crate::core::common::OxidbError::VectorDimensionMismatch {
                dim1: self.dimension,
                dim2: dimension,
            });
        }

        let expected_len = 4 + (dimension * 4); // 4 bytes per f32
        if value.len() != expected_len {
            return Err(crate::core::common::OxidbError::Index(
                format!("Invalid vector length: expected {}, got {}", expected_len, value.len())
            ));
        }

        let mut vector = Vec::with_capacity(dimension);
        for i in 0..dimension {
            let start_idx = 4 + (i * 4);
            let end_idx = start_idx + 4;
            let float_bytes = &value[start_idx..end_idx];
            let float_value = f32::from_le_bytes([
                float_bytes[0],
                float_bytes[1],
                float_bytes[2],
                float_bytes[3],
            ]);
            vector.push(float_value);
        }

        Ok(vector)
    }

    /// Insert a vector with its primary key
    pub fn insert_vector(&mut self, vector: Vector, primary_key: PrimaryKey) -> HnswResult<()> {
        // Check if primary key already exists
        if self.pk_to_node.contains_key(&primary_key) {
            return Err(HnswError::Generic(
                "Primary key already exists in index".to_string()
            ));
        }

        // Insert into graph
        let node_id = self.graph.insert_node(vector, primary_key.clone())?;
        
        // Update primary key mapping
        self.pk_to_node.insert(primary_key, node_id);
        
        Ok(())
    }

    /// Search for similar vectors
    pub fn search_vector(&self, query: &Vector, k: usize) -> HnswResult<Vec<(f32, PrimaryKey)>> {
        let node_ids = self.graph.search(query, k)?;
        
        let mut results = Vec::new();
        for node_id in node_ids {
            if let Some(node) = self.graph.get_node(node_id) {
                let distance = self.graph.distance(query, &node.vector);
                results.push((distance, node.primary_key.clone()));
            }
        }
        
        // Sort by distance (closest first)
        results.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        
        Ok(results)
    }

    /// Delete a vector by its vector value and optionally primary key
    pub fn delete_vector(&mut self, vector: &Vector, primary_key: Option<&PrimaryKey>) -> HnswResult<bool> {
        match primary_key {
            Some(pk) => {
                // Delete by primary key
                if let Some(&node_id) = self.pk_to_node.get(pk) {
                    // Verify vector matches
                    if let Some(node) = self.graph.get_node(node_id) {
                        let distance = self.graph.distance(vector, &node.vector);
                        if distance < 1e-6 { // Very small epsilon for float comparison
                            self.graph.remove_node(node_id)?;
                            self.pk_to_node.remove(pk);
                            Ok(true)
                        } else {
                            Err(HnswError::Generic(
                                "Vector does not match primary key".to_string()
                            ))
                        }
                    } else {
                        Err(HnswError::NodeNotFound(node_id))
                    }
                } else {
                    Ok(false) // Primary key not found
                }
            }
            None => {
                // Delete by finding the closest vector
                let search_results = self.graph.search(vector, 1)?;
                if let Some(&node_id) = search_results.first() {
                    if let Some(node) = self.graph.get_node(node_id) {
                        let distance = self.graph.distance(vector, &node.vector);
                        if distance < 1e-6 { // Very small epsilon for float comparison
                            let pk = node.primary_key.clone();
                            self.graph.remove_node(node_id)?;
                            self.pk_to_node.remove(&pk);
                            Ok(true)
                        } else {
                            Ok(false) // No exact match found
                        }
                    } else {
                        Err(HnswError::NodeNotFound(node_id))
                    }
                } else {
                    Ok(false) // No nodes in graph
                }
            }
        }
    }

    /// Get statistics about the index
    pub fn stats(&self) -> IndexStats {
        IndexStats {
            node_count: self.graph.len(),
            dimension: self.dimension,
            entry_point: self.graph.entry_point(),
        }
    }

    /// Get the number of vectors in the index
    pub fn len(&self) -> usize {
        self.graph.len()
    }

    /// Check if the index is empty
    pub fn is_empty(&self) -> bool {
        self.graph.is_empty()
    }

    /// Clear all vectors from the index
    pub fn clear(&mut self) {
        self.graph = HnswGraph::new(
            self.dimension,
            16, // Default max_connections
            200, // Default ef_construction
            DistanceFunction::Euclidean, // Default distance function
        );
        self.pk_to_node.clear();
    }

    /// Search with a custom ef parameter for fine-tuning search quality vs speed
    pub fn search_with_ef(&self, query: &Vector, k: usize, ef: usize) -> HnswResult<Vec<(f32, PrimaryKey)>> {
        // Create a temporary graph with different ef_construction for search
        // Note: In a real implementation, you'd want to store ef separately or modify the graph search
        let node_ids = self.graph.search(query, k.max(ef))?;
        
        let mut results = Vec::new();
        for node_id in node_ids.into_iter().take(k) {
            if let Some(node) = self.graph.get_node(node_id) {
                let distance = self.graph.distance(query, &node.vector);
                results.push((distance, node.primary_key.clone()));
            }
        }
        
        // Sort by distance (closest first)
        results.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        
        Ok(results)
    }

    /// Get a vector by its primary key
    pub fn get_vector(&self, primary_key: &PrimaryKey) -> Option<&Vector> {
        if let Some(&node_id) = self.pk_to_node.get(primary_key) {
            self.graph.get_node(node_id).map(|node| &node.vector)
        } else {
            None
        }
    }

    /// Check if a primary key exists in the index
    pub fn contains_key(&self, primary_key: &PrimaryKey) -> bool {
        self.pk_to_node.contains_key(primary_key)
    }
}

/// Statistics about the HNSW index
#[derive(Debug, Clone)]
pub struct IndexStats {
    pub node_count: usize,
    pub dimension: usize,
    pub entry_point: Option<NodeId>,
}

#[cfg(test)]
mod tests {
    use super::*;
    
    fn create_test_index() -> HnswIndex {
        HnswIndex::new(
            "test_index".to_string(),
            3, // dimension
            16, // max_connections
            200, // ef_construction
            DistanceFunction::Euclidean,
        ).unwrap()
    }
    
    fn vector_bytes(data: Vec<f32>) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(data.len() as u32).to_le_bytes());
        for value in data {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        bytes
    }

    #[test]
    fn test_index_creation() {
        let index = create_test_index();
        assert_eq!(index.name, "test_index");
        assert_eq!(index.dimension, 3);
        assert!(index.is_empty());
        assert_eq!(index.len(), 0);
    }

    #[test]
    fn test_vector_parsing() {
        let index = create_test_index();
        let vector_data = vec![1.0, 2.0, 3.0];
        let bytes = vector_bytes(vector_data.clone());
        
        let parsed = index.parse_vector_value(&bytes).unwrap();
        assert_eq!(parsed, vector_data);
    }

    #[test]
    fn test_vector_insertion() {
        let mut index = create_test_index();
        let vector = vec![1.0, 2.0, 3.0];
        let pk = b"test_key".to_vec();
        
        assert!(index.insert_vector(vector.clone(), pk.clone()).is_ok());
        assert_eq!(index.len(), 1);
        assert!(!index.is_empty());
        assert!(index.contains_key(&pk));
        assert_eq!(index.get_vector(&pk), Some(&vector));
    }

    #[test]
    fn test_vector_search() {
        let mut index = create_test_index();
        
        // Insert test vectors
        let vectors = vec![
            (vec![1.0, 0.0, 0.0], b"vec1".to_vec()),
            (vec![0.0, 1.0, 0.0], b"vec2".to_vec()),
            (vec![0.0, 0.0, 1.0], b"vec3".to_vec()),
            (vec![1.0, 1.0, 0.0], b"vec4".to_vec()),
        ];

        for (vector, pk) in vectors {
            index.insert_vector(vector, pk).unwrap();
        }

        // Search for closest to [0.9, 0.1, 0.0] (should be vec1)
        let query = vec![0.9, 0.1, 0.0];
        let results = index.search_vector(&query, 2).unwrap();
        
        assert!(!results.is_empty());
        assert_eq!(results[0].1, b"vec1".to_vec()); // Closest should be vec1
        assert!(results[0].0 < results[1].0); // First result should be closer
    }

    #[test]
    fn test_vector_deletion() {
        let mut index = create_test_index();
        let vector = vec![1.0, 2.0, 3.0];
        let pk = b"test_key".to_vec();
        
        // Insert and then delete
        index.insert_vector(vector.clone(), pk.clone()).unwrap();
        assert_eq!(index.len(), 1);
        
        assert!(index.delete_vector(&vector, Some(&pk)).unwrap());
        assert_eq!(index.len(), 0);
        assert!(!index.contains_key(&pk));
    }

    #[test]
    fn test_duplicate_primary_key() {
        let mut index = create_test_index();
        let vector1 = vec![1.0, 2.0, 3.0];
        let vector2 = vec![4.0, 5.0, 6.0];
        let pk = b"same_key".to_vec();
        
        // First insertion should succeed
        assert!(index.insert_vector(vector1, pk.clone()).is_ok());
        
        // Second insertion with same primary key should fail
        assert!(index.insert_vector(vector2, pk).is_err());
    }

    #[test]
    fn test_index_stats() {
        let mut index = create_test_index();
        let vector = vec![1.0, 2.0, 3.0];
        let pk = b"test_key".to_vec();
        
        // Empty index stats
        let stats = index.stats();
        assert_eq!(stats.node_count, 0);
        assert_eq!(stats.dimension, 3);
        assert!(stats.entry_point.is_none());
        
        // After insertion
        index.insert_vector(vector, pk).unwrap();
        let stats = index.stats();
        assert_eq!(stats.node_count, 1);
        assert!(stats.entry_point.is_some());
    }

    #[test]
    fn test_index_clear() {
        let mut index = create_test_index();
        let vector = vec![1.0, 2.0, 3.0];
        let pk = b"test_key".to_vec();
        
        index.insert_vector(vector, pk).unwrap();
        assert!(!index.is_empty());
        
        index.clear();
        assert!(index.is_empty());
        assert_eq!(index.len(), 0);
    }

    #[test]
    fn test_dimension_mismatch() {
        let index = create_test_index(); // 3D index
        let wrong_vector_bytes = vector_bytes(vec![1.0, 2.0]); // 2D vector
        
        let result = index.parse_vector_value(&wrong_vector_bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_search_with_ef() {
        let mut index = create_test_index();
        
        // Insert multiple vectors
        for i in 0..10 {
            let vector = vec![i as f32, 0.0, 0.0];
            let pk = format!("vec_{}", i).as_bytes().to_vec();
            index.insert_vector(vector, pk).unwrap();
        }

        let query = vec![5.0, 0.0, 0.0];
        let results_low_ef = index.search_with_ef(&query, 3, 3).unwrap();
        let results_high_ef = index.search_with_ef(&query, 3, 10).unwrap();
        
        // Both should return results, high ef might be more accurate
        assert!(!results_low_ef.is_empty());
        assert!(!results_high_ef.is_empty());
        assert_eq!(results_low_ef.len(), 3);
        assert_eq!(results_high_ef.len(), 3);
    }
} 