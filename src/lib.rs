#![warn(clippy::unwrap_used)]
#![warn(clippy::expect_used)]
#![warn(clippy::missing_docs_in_private_items)]
#![warn(clippy::panic)]
#![warn(clippy::arithmetic_side_effects)]
#![warn(clippy::cast_possible_truncation)]
#![warn(clippy::cast_possible_wrap)]
#![warn(clippy::cast_precision_loss)]
#![warn(clippy::cast_sign_loss)]
#![forbid(unsafe_code)]
#![deny(warnings, deprecated, unused, clippy::todo,clippy::module_inception, clippy::wildcard_imports, clippy::correctness, clippy::perf, clippy::style, clippy::complexity, clippy::nursery, clippy::pedantic)]
#![warn(clippy::missing_const_for_fn, clippy::approx_constant, clippy::all)]

//! # Oxidb: A Minimal Pure Rust LibSQL Alternative
//!
//! `oxidb` is a minimal, dependency-free SQL database implementation in pure Rust.
//! It features:
//! - Zero-cost abstractions with iterator combinators
//! - Pure stdlib implementation (no external dependencies)
//! - ACID compliance with proper transaction management
//! - B+ tree indexing with proper deletion handling
//! - Write-Ahead Logging for durability
//! - SQL-like query interface
//!
//! This crate follows SOLID, DRY, KISS, YAGNI principles and leverages
//! Rust's zero-cost abstractions for optimal performance.

pub mod api;
pub mod core;
pub mod event_engine;

// Re-export key types for easier use by library consumers
pub use api::Oxidb;
pub use crate::core::common::OxidbError;

/// Core result type for the library
pub type Result<T> = std::result::Result<T, OxidbError>;

#[cfg(test)]
mod tests {
    use crate::Oxidb;
    use std::fs;
    use std::io::Write;
    use std::path::Path;
    use tempfile::NamedTempFile;

    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }

    #[test]
    fn basic_oxidb_operations() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file for DB");
        let mut db = Oxidb::new(temp_file.path()).expect("Failed to create Oxidb instance");

        let key1 = b"int_key1".to_vec();
        let value1_str = "int_value1".to_string();

        assert!(db.insert(key1.clone(), value1_str.clone()).is_ok());

        match db.get(key1.clone()) {
            Ok(Some(v_str)) => assert_eq!(v_str, value1_str),
            Ok(None) => panic!("Key not found after insert"),
            Err(e) => panic!("Error during get: {:?}", e),
        }

        match db.delete(key1.clone()) {
            Ok(true) => (),
            Ok(false) => panic!("Key not found for deletion"),
            Err(e) => panic!("Error during delete: {:?}", e),
        }

        match db.get(key1.clone()) {
            Ok(None) => (),
            Ok(Some(_)) => panic!("Key found after delete"),
            Err(e) => panic!("Error during get after delete: {:?}", e),
        }

        let key2 = b"int_key2".to_vec();
        let value2_str = "int_value2".to_string();
        assert!(db.insert(key2.clone(), value2_str.clone()).is_ok());
        match db.get(key2.clone()) {
            Ok(Some(v_str)) => assert_eq!(v_str, value2_str),
            _ => panic!("Second key not processed correctly"),
        }
    }

    /// Derive WAL path for testing - zero-cost abstraction
    fn derive_wal_path_for_lib_test(db_path: &Path) -> std::path::PathBuf {
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
        let temp_db_file = NamedTempFile::new().expect("Failed to create temp file");
        let db_path = temp_db_file.path().to_path_buf();
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
            db.insert(key_a.clone(), val_a_updated_str.clone()).unwrap();
            db.delete(key_b.clone()).unwrap();

            assert!(wal_path.exists(), "WAL file should exist before forgetting the DB instance.");
            std::mem::forget(db);
        }

        assert!(wal_path.exists(), "WAL file should persist after simulated crash (forget).");

        {
            let mut db_restarted =
                Oxidb::new(&db_path).expect("Failed to create Oxidb (instance 2)");

            assert_eq!(
                db_restarted.get(key_a.clone()).unwrap(),
                Some(val_a_updated_str.clone()),
                "Key A should have updated value"
            );
            assert_eq!(db_restarted.get(key_b.clone()).unwrap(), None, "Key B should be deleted");

            db_restarted.persist().unwrap();
            assert!(
                !wal_path.exists(),
                "WAL file should be cleared after persist on restarted DB."
            );
        }
    }

    #[test]
    fn oxidb_persistence_across_instances_with_explicit_persist() {
        let temp_db_file = NamedTempFile::new().expect("Failed to create temp file");
        let db_path = temp_db_file.path();

        let key_c = b"key_c_persist".to_vec();
        let val_c_str = "val_c".to_string();
        let key_d = b"key_d_persist".to_vec();
        let val_d_str = "val_d".to_string();

        {
            let mut db1 = Oxidb::new(db_path).expect("Failed to create Oxidb (instance 1)");
            db1.insert(key_c.clone(), val_c_str.clone()).unwrap();
            db1.persist().unwrap();
        }

        {
            let mut db2 = Oxidb::new(db_path).expect("Failed to create Oxidb (instance 2)");
            assert_eq!(
                db2.get(key_c.clone()).unwrap(),
                Some(val_c_str.clone()),
                "Key C should be present in instance 2"
            );

            db2.insert(key_d.clone(), val_d_str.clone()).unwrap();
            db2.persist().unwrap();
        }

        {
            let mut db3 = Oxidb::new(db_path).expect("Failed to create Oxidb (instance 3)");
            assert_eq!(
                db3.get(key_c.clone()).unwrap(),
                Some(val_c_str.clone()),
                "Key C should be present in instance 3"
            );
            assert_eq!(
                db3.get(key_d.clone()).unwrap(),
                Some(val_d_str.clone()),
                "Key D should be present in instance 3"
            );
        }
    }

    #[test]
    fn test_minimal_config_functionality() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let custom_data_dir = dir.path().join("custom_data");
        fs::create_dir_all(&custom_data_dir).unwrap();
        let custom_db_filename = "my_db.oxidb";

        // Test with minimal config - no external TOML dependency
        let db_path = custom_data_dir.join(custom_db_filename);
        let mut db = Oxidb::new(&db_path).unwrap();

        // Test basic operations
        let test_key = b"test_key".to_vec();
        let test_value = "test_value".to_string();
        
        db.insert(test_key.clone(), test_value.clone()).unwrap();
        let retrieved = db.get(test_key.clone()).unwrap();
        assert_eq!(retrieved, Some(test_value));

        db.persist().unwrap();
        assert!(db_path.exists(), "Database file should exist after persist");
    }

    #[test] 
    fn test_zero_cost_abstractions() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let mut db = Oxidb::new(temp_file.path()).expect("Failed to create Oxidb instance");

        // Test iterator-based operations
        let keys: Vec<Vec<u8>> = (0..10)
            .map(|i| format!("key_{}", i).into_bytes())
            .collect();
        
        let values: Vec<String> = (0..10)
            .map(|i| format!("value_{}", i))
            .collect();

        // Use iterator combinators for batch operations
        keys.iter()
            .zip(values.iter())
            .try_for_each(|(key, value)| {
                db.insert(key.clone(), value.clone())
            })
            .expect("Batch insert should succeed");

        // Verify all keys exist using iterator combinators
        let all_exist = keys.iter()
            .zip(values.iter())
            .all(|(key, expected_value)| {
                db.get(key.clone())
                    .map(|opt| opt.as_ref() == Some(expected_value))
                    .unwrap_or(false)
            });

        assert!(all_exist, "All inserted key-value pairs should be retrievable");
    }
}
