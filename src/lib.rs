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

pub mod api;
pub mod core;

// Re-export key types for easier use by library consumers.
// Oxidb is the main entry point for database operations.
pub use api::Oxidb;
// OxidbError is the primary error type used throughout the crate.
pub use crate::core::common::OxidbError; // Changed

#[cfg(test)]
mod tests {
    // Imports used by tests in this module
    use crate::Oxidb;
    use std::fs::{self, File}; // fs and File are used
    use std::io::Write;
    use std::path::{Path, PathBuf}; // Path is used
    use tempfile::NamedTempFile; // Write is used by file.write_all
                                 // Read is not directly used in these tests it seems.

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

    fn derive_wal_path_for_lib_test(db_path: &Path) -> PathBuf {
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
    fn test_oxidb_new_from_config_file_custom_paths() {
        // Config is used here
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let custom_data_dir = dir.path().join("custom_data");
        fs::create_dir_all(&custom_data_dir).unwrap(); // fs is used here
        let custom_db_filename = "my_db.oxidb";
        let custom_index_dir = "my_indices";

        let config_content = format!(
            r#"
           database_file_path = "{}/{}"
           wal_enabled = true
           index_base_path = "{}/{}"
           "#,
            custom_data_dir.to_str().unwrap().replace("\\", "/"),
            custom_db_filename,
            custom_data_dir.to_str().unwrap().replace("\\", "/"), // Assuming index dir is also under custom_data_dir for this test
            custom_index_dir
        );

        let config_file_path = dir.path().join("custom_config.toml");
        let mut file = File::create(&config_file_path).unwrap(); // File is used here
        file.write_all(config_content.as_bytes()).unwrap(); // Write is used here

        let mut db = Oxidb::new_from_config_file(config_file_path.clone()).unwrap();

        let expected_db_path = custom_data_dir.join(custom_db_filename);
        assert_eq!(db.database_path(), expected_db_path);
        // For index_path, the config now specifies a full path for index_base_path
        let expected_index_path = custom_data_dir.join(custom_index_dir);
        assert_eq!(db.index_path(), expected_index_path);

        db.execute_query_str("INSERT test 1").unwrap();
        let val = db.execute_query_str("GET test").unwrap();
        // This test was missing DataType import if not for the global one.
        assert_eq!(
            val,
            crate::core::query::executor::ExecutionResult::Value(Some(
                crate::core::types::DataType::Integer(1)
            ))
        );

        fs::remove_file(&config_file_path).ok();
        fs::remove_dir_all(&custom_data_dir).ok();
    }

    #[test]
    fn test_oxidb_new_from_missing_config_file_uses_defaults() {
        // Config is used here
        use tempfile::tempdir;
        // Path is used by PathBuf::from if not already imported
        // fs is used by fs::remove_file etc. if not already imported

        let dir = tempdir().unwrap();
        let current_test_dir = dir.path();

        let non_existent_config_path = current_test_dir.join("non_existent.toml");

        let default_db_path = current_test_dir.join("oxidb.db");
        let default_indexes_path = current_test_dir.join("oxidb_indexes/");

        let original_cwd = std::env::current_dir().unwrap();
        std::env::set_current_dir(current_test_dir).unwrap();

        let mut db = Oxidb::new_from_config_file(non_existent_config_path)
            .expect("Failed to create Oxidb with non-existent config");

        assert_eq!(
            current_test_dir.join(db.database_path()),
            default_db_path,
            "Database path should match default absolute path"
        );
        assert_eq!(
            current_test_dir.join(db.index_path()),
            default_indexes_path,
            "Index path should match default absolute path"
        );

        let key = b"test_key_defaults".to_vec();
        let value = "test_value_defaults".to_string();
        db.insert(key.clone(), value.clone()).unwrap();
        db.persist().unwrap();
        drop(db);

        assert!(
            default_db_path.exists(),
            "Default database file '{}' should exist",
            default_db_path.display()
        );
        assert!(
            default_indexes_path.exists(),
            "Default index base path directory '{}' should exist",
            default_indexes_path.display()
        );

        let default_index_file = default_indexes_path.join("default_value_index.idx");
        assert!(
            default_index_file.exists(),
            "Default index file '{}' should exist in default index path",
            default_index_file.display()
        );

        std::env::set_current_dir(original_cwd).unwrap();
    }

    #[test]
    fn test_oxidb_new_from_malformed_config_file_returns_error() {
        // File is used here
        // Write is used here
        use tempfile::NamedTempFile;

        let mut temp_config_file = NamedTempFile::new().unwrap();
        writeln!(temp_config_file, "this is not valid toml").unwrap();

        let result = Oxidb::new_from_config_file(temp_config_file.path());
        assert!(result.is_err());
        match result.unwrap_err() {
            crate::OxidbError::Configuration(msg) => { // Changed DbError::ConfigError to OxidbError::Configuration
                assert!(msg.contains("Failed to parse config file"));
            }
            e => panic!("Expected OxidbError::Configuration, got {:?}", e), // Changed
        }
    }
}
