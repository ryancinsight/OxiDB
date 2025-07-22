mod error;
mod graph;
mod node;
pub mod tree;

pub use error::HnswError;
pub use node::{HnswNode, NodeId, Vector};
pub use tree::HnswIndex;

use crate::core::common::OxidbError as CommonError;
use crate::core::indexing::traits::Index;
use crate::core::query::commands::{Key as TraitPrimaryKey, Value as TraitValue};

// Convert HnswError to common error type
fn map_hnsw_error_to_common(hnsw_error: HnswError) -> CommonError {
    match hnsw_error {
        HnswError::Io(io_err) => CommonError::Io(io_err),
        HnswError::Serialization(ser_err) => CommonError::Serialization(ser_err),
        HnswError::NodeNotFound(node_id) => {
            CommonError::Index(format!("HNSW node not found: {node_id}"))
        }
        HnswError::InvalidVector(msg) => CommonError::Index(format!("Invalid vector: {msg}")),
        HnswError::DimensionMismatch { expected, actual } => {
            CommonError::VectorDimensionMismatch { dim1: expected, dim2: actual }
        }
        HnswError::GraphError(msg) => CommonError::Index(format!("HNSW graph error: {msg}")),
        HnswError::Generic(msg) => CommonError::Index(format!("HNSW error: {msg}")),
        HnswError::LayerIndexOutOfBounds { index } => {
            CommonError::Index(format!("HNSW layer index out of bounds: {index}"))
        }
        HnswError::MaxConnectionsExceeded { current, max } => {
            CommonError::Index(format!("HNSW max connections exceeded: {current}/{max}"))
        }
        HnswError::EmptyGraph => CommonError::Index("HNSW graph is empty".to_string()),
        HnswError::InvalidEntryPoint { node_id } => {
            CommonError::Index(format!("HNSW invalid entry point: {node_id}"))
        }
    }
}

impl Index for HnswIndex {
    fn name(&self) -> &str {
        &self.name
    }

    fn insert(
        &mut self,
        value: &TraitValue,
        primary_key: &TraitPrimaryKey,
    ) -> Result<(), CommonError> {
        // For HNSW, the value should be a vector (serialized f32 array)
        let vector = self.parse_vector_value(value)?;
        self.insert_vector(vector, primary_key.clone()).map_err(map_hnsw_error_to_common)
    }

    fn find(&self, value: &TraitValue) -> Result<Option<Vec<TraitPrimaryKey>>, CommonError> {
        let vector = self.parse_vector_value(value)?;
        self.search_vector(&vector, 1).map_err(map_hnsw_error_to_common).map(|results| {
            if results.is_empty() {
                None
            } else {
                Some(results.into_iter().map(|(_, pk)| pk).collect())
            }
        })
    }

    fn save(&self) -> Result<(), CommonError> {
        // HNSW uses in-memory structure for now, could be extended for persistence
        Ok(())
    }

    fn load(&mut self) -> Result<(), CommonError> {
        // HNSW loads automatically if needed
        Ok(())
    }

    fn delete(
        &mut self,
        value: &TraitValue,
        primary_key_to_remove: Option<&TraitPrimaryKey>,
    ) -> Result<(), CommonError> {
        let vector = self.parse_vector_value(value)?;
        match primary_key_to_remove {
            Some(pk) => self.delete_vector(&vector, Some(pk)),
            None => self.delete_vector(&vector, None),
        }
        .map(|_| ()) // Convert bool result to ()
        .map_err(map_hnsw_error_to_common)
    }

    fn update(
        &mut self,
        old_value: &TraitValue,
        new_value: &TraitValue,
        primary_key: &TraitPrimaryKey,
    ) -> Result<(), CommonError> {
        // For HNSW, update is delete old + insert new
        let old_vector = self.parse_vector_value(old_value)?;
        let new_vector = self.parse_vector_value(new_value)?;

        self.delete_vector(&old_vector, Some(primary_key))
            .and_then(|_| self.insert_vector(new_vector, primary_key.clone()))
            .map_err(map_hnsw_error_to_common)
    }
}

#[cfg(test)]
mod tests {
    use super::node::DistanceFunction;
    use super::*;
    use crate::core::indexing::traits::Index;

    fn vector_val(data: Vec<f32>) -> Vec<u8> {
        // Simple encoding: dimension (4 bytes) + f32 values
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(data.len() as u32).to_le_bytes());
        for value in data {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        bytes
    }

    fn vector_pk(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    #[test]
    fn test_hnsw_trait_implementation() {
        let mut hnsw = HnswIndex::new(
            "test_hnsw".to_string(),
            3,                           // dimension
            16,                          // max connections
            200,                         // ef_construction
            DistanceFunction::Euclidean, // distance function
        )
        .unwrap();

        let vec1 = vector_val(vec![1.0, 0.0, 0.0]);
        let pk1 = vector_pk("pk1");

        // Test trait methods
        assert_eq!(Index::name(&hnsw), "test_hnsw");
        assert!(Index::insert(&mut hnsw, &vec1, &pk1).is_ok());
        assert!(Index::find(&hnsw, &vec1).unwrap().is_some());
        assert!(Index::delete(&mut hnsw, &vec1, Some(&pk1)).is_ok());
        assert!(Index::find(&hnsw, &vec1).unwrap().is_none());
    }
}
