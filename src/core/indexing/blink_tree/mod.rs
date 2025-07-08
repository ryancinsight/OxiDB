mod error;
mod node;
mod page_io;
pub mod tree;

pub use error::BlinkTreeError;
pub use node::{BlinkTreeNode, KeyType, PageId, PrimaryKey};
pub use tree::BlinkTreeIndex;

use crate::core::common::OxidbError as CommonError;
use crate::core::indexing::traits::Index;
use crate::core::query::commands::{Key as TraitPrimaryKey, Value as TraitValue};

// Convert BlinkTreeError to common error type
fn map_blink_error_to_common(blink_error: BlinkTreeError) -> CommonError {
    match blink_error {
        BlinkTreeError::Io(io_err) => CommonError::Io(io_err),
        BlinkTreeError::Serialization(ser_err) => {
            CommonError::Serialization(format!("{:?}", ser_err))
        }
        BlinkTreeError::NodeNotFound(page_id) => {
            CommonError::Index(format!("Blink tree node not found: {}", page_id))
        }
        BlinkTreeError::PageFull(msg) => {
            CommonError::Index(format!("Blink tree page full: {}", msg))
        }
        BlinkTreeError::UnexpectedNodeType => {
            CommonError::Index("Unexpected Blink tree node type".into())
        }
        BlinkTreeError::TreeLogicError(msg) => {
            CommonError::Index(format!("Blink tree logic error: {}", msg))
        }
        BlinkTreeError::ConcurrencyError(msg) => {
            CommonError::Index(format!("Blink tree concurrency error: {}", msg))
        }
        BlinkTreeError::BorrowError(msg) => {
            CommonError::Index(format!("Blink tree borrow error: {}", msg))
        }
        BlinkTreeError::Generic(msg) => CommonError::Index(format!("Blink tree error: {}", msg)),
    }
}

impl Index for BlinkTreeIndex {
    fn name(&self) -> &str {
        &self.name
    }

    fn insert(
        &mut self,
        value: &TraitValue,
        primary_key: &TraitPrimaryKey,
    ) -> Result<(), CommonError> {
        self.insert(value.clone(), primary_key.clone()).map_err(map_blink_error_to_common)
    }

    fn find(&self, value: &TraitValue) -> Result<Option<Vec<TraitPrimaryKey>>, CommonError> {
        self.find_primary_keys(value).map_err(map_blink_error_to_common)
    }

    fn save(&self) -> Result<(), CommonError> {
        // Blink tree uses page manager for persistence, so this is a no-op
        Ok(())
    }

    fn load(&mut self) -> Result<(), CommonError> {
        // Blink tree loads automatically from page manager
        Ok(())
    }

    fn delete(
        &mut self,
        value: &TraitValue,
        primary_key_to_remove: Option<&TraitPrimaryKey>,
    ) -> Result<(), CommonError> {
        match primary_key_to_remove {
            Some(pk) => BlinkTreeIndex::delete(self, value, Some(pk)),
            None => BlinkTreeIndex::delete(self, value, None),
        }
        .map(|_| ()) // Convert bool result to ()
        .map_err(map_blink_error_to_common)
    }

    fn update(
        &mut self,
        old_value: &TraitValue,
        new_value: &TraitValue,
        primary_key: &TraitPrimaryKey,
    ) -> Result<(), CommonError> {
        // For Blink tree, update is delete old + insert new
        BlinkTreeIndex::delete(self, old_value, Some(primary_key))
            .and_then(|_| BlinkTreeIndex::insert(self, new_value.clone(), primary_key.clone()))
            .map_err(map_blink_error_to_common)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::indexing::traits::Index; // Import the trait explicitly
    use tempfile::TempDir;

    type TestValue = Vec<u8>;
    type TestKey = Vec<u8>;

    fn trait_val(s: &str) -> TestValue {
        s.as_bytes().to_vec()
    }

    fn trait_pk(s: &str) -> TestKey {
        s.as_bytes().to_vec()
    }

    // Basic integration test to ensure trait implementation works
    #[test]
    fn test_blink_tree_trait_implementation() {
        let temp_dir = TempDir::new().unwrap();
        let mut blink_tree = BlinkTreeIndex::new(
            "test_blink".to_string(),
            temp_dir.path().join("test_blink.blink"),
            5,
        )
        .unwrap();

        let val1 = trait_val("apple");
        let pk1 = trait_pk("pk1");

        // Test trait methods
        assert_eq!(Index::name(&blink_tree), "test_blink");
        assert!(Index::insert(&mut blink_tree, &val1, &pk1).is_ok());
        assert!(Index::find(&blink_tree, &val1).unwrap().is_some());
        assert!(Index::delete(&mut blink_tree, &val1, Some(&pk1)).is_ok());
        assert!(Index::find(&blink_tree, &val1).unwrap().is_none());
    }
}
