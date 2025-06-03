//! # Oxidb: A Simple Key-Value Store
//!
//! `oxidb` is a learning project implementing a basic file-based key-value store
//! in Rust. It features:
//! - In-memory caching for quick access.
//! - A Write-Ahead Log (WAL) for durability and crash recovery.
//! - Explicit persistence to a main data file.
//! - Automatic data saving when the database instance is dropped.
//!
//! This crate exposes the main `Oxidb` struct for database interaction and `DbError`
//! for error handling.

pub mod core;
pub mod api;

// Re-export key types for easier use by library consumers.
// Oxidb is the main entry point for database operations.
pub use api::Oxidb;
// DbError is the primary error type used throughout the crate.
pub use crate::core::common::error::DbError;

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
        let value1_str = "int_value1".to_string();

        // Insert
        assert!(db.insert(key1.clone(), value1_str.clone()).is_ok());

        // Get
        match db.get(key1.clone()) {
            Ok(Some(v_str)) => assert_eq!(v_str, value1_str),
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
        let value2_str = "int_value2".to_string();
        assert!(db.insert(key2.clone(), value2_str.clone()).is_ok());
        match db.get(key2.clone()) {
            Ok(Some(v_str)) => assert_eq!(v_str, value2_str),
            _ => panic!("Second key not processed correctly"),
        }
    }

    // Helper function to derive WAL path from DB path for testing
    // This needs to align with how SimpleFileKvStore generates its WAL path.
    // Assuming '.db' extension for main file, so WAL is '.db.wal'
    // If no extension, WAL is '.wal'
    // If other extension (e.g. '.oxdb'), WAL is '.oxdb.wal'
    fn derive_wal_path_for_lib_test(db_path: &std::path::Path) -> std::path::PathBuf {
        let mut wal_path = db_path.to_path_buf();
        let original_extension = wal_path.extension().and_then(std::ffi::OsStr::to_str);

        if let Some(ext_str) = original_extension {
            wal_path.set_extension(format!("{}.wal", ext_str));
        } else {
            wal_path.set_extension("wal");
        }
        wal_path
    }


    #[test]
    fn oxidb_instance_restart_with_pending_wal_operations() {
        use crate::Oxidb;
        use tempfile::NamedTempFile;
        // use std::path::Path; // Not strictly required if using .path() from NamedTempFile and PathBuf methods

        let temp_db_file = NamedTempFile::new().expect("Failed to create temp file");
        let db_path = temp_db_file.path().to_path_buf(); // Keep PathBuf for derive_wal_path
        let wal_path = derive_wal_path_for_lib_test(&db_path);

        let key_a = b"key_a_wal_restart".to_vec();
        let val_a_initial_str = "val_a_initial".to_string();
        let val_a_updated_str = "val_a_updated".to_string();
        let key_b = b"key_b_wal_restart".to_vec();
        let val_b_str = "val_b".to_string();

        {
            let mut db = Oxidb::new(&db_path).expect("Failed to create Oxidb (instance 1)");
            db.insert(key_a.clone(), val_a_initial_str.clone()).unwrap();
            db.insert(key_b.clone(), val_b_str.clone()).unwrap();
            db.insert(key_a.clone(), val_a_updated_str.clone()).unwrap(); // Update key A
            db.delete(key_b.clone()).unwrap(); // Delete key B
            
            // At this point, after several operations, the WAL file should exist.
            assert!(wal_path.exists(), "WAL file should exist before forgetting the DB instance.");

            std::mem::forget(db); // Simulate crash, Drop is not called
        }
        
        // WAL should still exist after "crash" (std::mem::forget)
        assert!(wal_path.exists(), "WAL file should persist after simulated crash (forget).");

        {
            let mut db_restarted = Oxidb::new(&db_path).expect("Failed to create Oxidb (instance 2)");
            
            // Verify operations
            assert_eq!(db_restarted.get(key_a.clone()).unwrap(), Some(val_a_updated_str.clone()), "Key A should have updated value");
            assert_eq!(db_restarted.get(key_b.clone()).unwrap(), None, "Key B should be deleted");

            // Persist the replayed data
            db_restarted.persist().unwrap();
            assert!(!wal_path.exists(), "WAL file should be cleared after persist on restarted DB.");
        }
    }

    #[test]
    fn oxidb_persistence_across_instances_with_explicit_persist() {
        use crate::Oxidb;
        use tempfile::NamedTempFile;

        let temp_db_file = NamedTempFile::new().expect("Failed to create temp file");
        let db_path = temp_db_file.path();

        let key_c = b"key_c_persist".to_vec();
        let val_c_str = "val_c".to_string();
        let key_d = b"key_d_persist".to_vec();
        let val_d_str = "val_d".to_string();

        // Instance 1
        {
            let mut db1 = Oxidb::new(db_path).expect("Failed to create Oxidb (instance 1)");
            db1.insert(key_c.clone(), val_c_str.clone()).unwrap();
            db1.persist().unwrap();
        }

        // Instance 2
        {
            let mut db2 = Oxidb::new(db_path).expect("Failed to create Oxidb (instance 2)");
            assert_eq!(db2.get(key_c.clone()).unwrap(), Some(val_c_str.clone()), "Key C should be present in instance 2");
            
            db2.insert(key_d.clone(), val_d_str.clone()).unwrap();
            db2.persist().unwrap();
        }

        // Instance 3
        {
            let mut db3 = Oxidb::new(db_path).expect("Failed to create Oxidb (instance 3)");
            assert_eq!(db3.get(key_c.clone()).unwrap(), Some(val_c_str.clone()), "Key C should be present in instance 3");
            assert_eq!(db3.get(key_d.clone()).unwrap(), Some(val_d_str.clone()), "Key D should be present in instance 3");
        }
    }
}
