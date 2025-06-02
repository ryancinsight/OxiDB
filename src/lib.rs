// src/lib.rs
pub mod core;
pub mod api;

// Optional: Re-export key types/traits for easier use by library consumers later
pub use api::Oxidb;
pub use crate::core::common::error::DbError; // Ensure crate:: prefix if DbError is not already in api's scope

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }

    #[test]
    fn basic_oxidb_operations() {
        use crate::Oxidb; // Correct import for items re-exported in lib.rs
        use tempfile::NamedTempFile;

        let temp_file = NamedTempFile::new().expect("Failed to create temp file for DB");
        let mut db = Oxidb::new(temp_file.path()).expect("Failed to create Oxidb instance");

        let key1 = b"int_key1".to_vec();
        let value1 = b"int_value1".to_vec();

        // Insert
        assert!(db.insert(key1.clone(), value1.clone()).is_ok());

        // Get
        match db.get(key1.clone()) {
            Ok(Some(v)) => assert_eq!(v, value1),
            Ok(None) => panic!("Key not found after insert"),
            Err(e) => panic!("Error during get: {:?}", e),
        }

        // Delete
        match db.delete(key1.clone()) {
            Ok(true) => (), // Successfully deleted
            Ok(false) => panic!("Key not found for deletion"),
            Err(e) => panic!("Error during delete: {:?}", e),
        }

        // Get after delete
        match db.get(key1.clone()) {
            Ok(None) => (), // Correctly not found
            Ok(Some(_)) => panic!("Key found after delete"),
            Err(e) => panic!("Error during get after delete: {:?}", e),
        }

        // Test inserting another key to make sure the DB is still usable
        let key2 = b"int_key2".to_vec();
        let value2 = b"int_value2".to_vec();
        assert!(db.insert(key2.clone(), value2.clone()).is_ok());
        match db.get(key2.clone()) {
            Ok(Some(v)) => assert_eq!(v, value2),
            _ => panic!("Second key not processed correctly"),
        }
    }
}
