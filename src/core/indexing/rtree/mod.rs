mod error;
mod geometry;
mod node;
mod page_io;
pub mod tree;

pub use tree::RTreeIndex;
pub use node::{RTreeNode, MBR};
pub use geometry::{Point, Rectangle, BoundingBox};
pub use error::RTreeError;

use crate::core::indexing::traits::Index;
use crate::core::common::OxidbError as CommonError;
use crate::core::query::commands::{Key as TraitPrimaryKey, Value as TraitValue};

// Convert RTreeError to common error type
fn map_rtree_error_to_common(rtree_error: RTreeError) -> CommonError {
    match rtree_error {
        RTreeError::Io(io_err) => CommonError::Io(io_err),
        RTreeError::Serialization(ser_err) => CommonError::Serialization(format!("{:?}", ser_err)),
        RTreeError::NodeNotFound(page_id) => CommonError::Index(format!("R-tree node not found: {}", page_id)),
        RTreeError::InvalidGeometry(msg) => CommonError::Index(format!("Invalid geometry: {}", msg)),
        RTreeError::UnexpectedNodeType => CommonError::Index("Unexpected R-tree node type".into()),
        RTreeError::TreeLogicError(msg) => CommonError::Index(format!("R-tree logic error: {}", msg)),
        RTreeError::Generic(msg) => CommonError::Index(format!("R-tree error: {}", msg)),
    }
}

impl Index for RTreeIndex {
    fn name(&self) -> &str {
        &self.name
    }

    fn insert(
        &mut self,
        value: &TraitValue,
        primary_key: &TraitPrimaryKey,
    ) -> Result<(), CommonError> {
        // For R-tree, the value should be a spatial object (e.g., serialized rectangle)
        // We'll parse it as a rectangle for now
        let geometry = self.parse_spatial_value(value)?;
        self.insert_spatial(geometry, primary_key.clone())
            .map_err(map_rtree_error_to_common)
    }

    fn find(&self, value: &TraitValue) -> Result<Option<Vec<TraitPrimaryKey>>, CommonError> {
        let geometry = self.parse_spatial_value(value)?;
        self.find_spatial(&geometry).map_err(map_rtree_error_to_common)
    }

    fn save(&self) -> Result<(), CommonError> {
        // R-tree uses page manager for persistence, so this is a no-op
        Ok(())
    }

    fn load(&mut self) -> Result<(), CommonError> {
        // R-tree loads automatically from page manager
        Ok(())
    }

    fn delete(
        &mut self,
        value: &TraitValue,
        primary_key_to_remove: Option<&TraitPrimaryKey>,
    ) -> Result<(), CommonError> {
        let geometry = self.parse_spatial_value(value)?;
        match primary_key_to_remove {
            Some(pk) => self.delete_spatial(&geometry, Some(pk)),
            None => self.delete_spatial(&geometry, None),
        }
        .map(|_| ()) // Convert bool result to ()
        .map_err(map_rtree_error_to_common)
    }

    fn update(
        &mut self,
        old_value: &TraitValue,
        new_value: &TraitValue,
        primary_key: &TraitPrimaryKey,
    ) -> Result<(), CommonError> {
        // For R-tree, update is delete old + insert new
        let old_geometry = self.parse_spatial_value(old_value)?;
        let new_geometry = self.parse_spatial_value(new_value)?;
        
        self.delete_spatial(&old_geometry, Some(primary_key))
            .and_then(|_| self.insert_spatial(new_geometry, primary_key.clone()))
            .map_err(map_rtree_error_to_common)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::indexing::traits::Index;
    use tempfile::TempDir;
    
    fn spatial_val(min_x: f64, min_y: f64, max_x: f64, max_y: f64) -> Vec<u8> {
        // Simple encoding: 4 f64 values as bytes
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&min_x.to_le_bytes());
        bytes.extend_from_slice(&min_y.to_le_bytes());
        bytes.extend_from_slice(&max_x.to_le_bytes());
        bytes.extend_from_slice(&max_y.to_le_bytes());
        bytes
    }
    
    fn spatial_pk(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    #[test]
    fn test_rtree_trait_implementation() {
        let temp_dir = TempDir::new().unwrap();
        let mut rtree = RTreeIndex::new(
            "test_rtree".to_string(),
            temp_dir.path().join("test_rtree.rtree"),
            10,
        ).unwrap();

        let rect1 = spatial_val(0.0, 0.0, 10.0, 10.0);
        let pk1 = spatial_pk("pk1");

        // Test trait methods
        assert_eq!(Index::name(&rtree), "test_rtree");
        assert!(Index::insert(&mut rtree, &rect1, &pk1).is_ok());
        assert!(Index::find(&rtree, &rect1).unwrap().is_some());
        assert!(Index::delete(&mut rtree, &rect1, Some(&pk1)).is_ok());
        assert!(Index::find(&rtree, &rect1).unwrap().is_none());
    }
} 