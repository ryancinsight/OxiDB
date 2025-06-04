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

   #[test]
   fn test_oxidb_new_from_config_file_custom_paths() {
       use crate::Oxidb;
       // use crate::core::config::Config; // Not needed for this test's direct logic
       use std::fs::{self, File};
       use std::io::Write;
       use tempfile::tempdir;

       let dir = tempdir().unwrap();
       let custom_db_path = dir.path().join("custom.db");
       let custom_indexes_path = dir.path().join("custom_idx/");
       let config_file_path = dir.path().join("Oxidb.toml");

       // It's crucial to use raw string literals or escape backslashes for Windows paths in TOML
       let config_content = format!(
           r#"
           database_file_path = "{}"
           index_base_path = "{}"
           "#,
           custom_db_path.to_str().unwrap().replace("\\", "/"),
           custom_indexes_path.to_str().unwrap().replace("\\", "/")
       );

       let mut file = File::create(&config_file_path).unwrap();
       writeln!(file, "{}", config_content).unwrap();

       let mut db = Oxidb::new_from_config_file(&config_file_path).expect("Failed to create Oxidb from config file");

       let key = b"test_key".to_vec();
       let value = "test_value".to_string();
       db.insert(key.clone(), value.clone()).unwrap();
       db.persist().unwrap();
       drop(db);

       assert!(custom_db_path.exists(), "Custom database file '{}' should exist", custom_db_path.display());
       assert!(custom_indexes_path.exists(), "Custom index base path directory '{}' should exist", custom_indexes_path.display());

       let default_index_file = custom_indexes_path.join("default_value_index.idx");
       assert!(default_index_file.exists(), "Default index file '{}' should exist in custom index path", default_index_file.display());
   }

   #[test]
   fn test_oxidb_new_from_missing_config_file_uses_defaults() {
       use crate::Oxidb;
       use tempfile::tempdir;
       use std::path::Path;
       use std::fs; // Added for cleanup

       let dir = tempdir().unwrap(); // Provides a clean directory for the test
       let current_test_dir = dir.path();

       let non_existent_config_path = current_test_dir.join("non_existent.toml");

       // Define default paths relative to the temporary test directory
       let default_db_path = current_test_dir.join("oxidb.db");
       let default_indexes_path = current_test_dir.join("oxidb_indexes/");

        // Change CWD for this test to ensure default paths are created inside tempdir
        let original_cwd = std::env::current_dir().unwrap();
        std::env::set_current_dir(current_test_dir).unwrap();

       let mut db = Oxidb::new_from_config_file(&non_existent_config_path).expect("Failed to create Oxidb with non-existent config");

       let key = b"test_key_defaults".to_vec();
       let value = "test_value_defaults".to_string();
       db.insert(key.clone(), value.clone()).unwrap();
       db.persist().unwrap();
       drop(db);

       assert!(default_db_path.exists(), "Default database file '{}' should exist", default_db_path.display());
       assert!(default_indexes_path.exists(), "Default index base path directory '{}' should exist", default_indexes_path.display());

       let default_index_file = default_indexes_path.join("default_value_index.idx");
       assert!(default_index_file.exists(), "Default index file '{}' should exist in default index path", default_index_file.display());

       // Restore CWD
       std::env::set_current_dir(original_cwd).unwrap();

       // Cleanup is handled by tempdir drop. Explicit removal inside tempdir is not strictly necessary
       // but shown here if not using tempdir for default path creation.
       // if default_db_path.exists() { fs::remove_file(&default_db_path).unwrap(); }
       // if default_indexes_path.exists() { fs::remove_dir_all(&default_indexes_path).unwrap(); }
   }

   #[test]
   fn test_oxidb_new_from_malformed_config_file_returns_error() {
       use crate::Oxidb;
       use std::fs::File;
       use std::io::Write;
       use tempfile::NamedTempFile;

       let mut temp_config_file = NamedTempFile::new().unwrap();
       writeln!(temp_config_file, "this is not valid toml").unwrap();

       let result = Oxidb::new_from_config_file(temp_config_file.path());
       assert!(result.is_err());
       match result.unwrap_err() {
           crate::DbError::ConfigError(msg) => {
               assert!(msg.contains("Failed to parse config file"));
           }
           e => panic!("Expected ConfigError, got {:?}", e),
       }
   }
}
