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
#![deny(
    deprecated,
    unused,
    clippy::todo,
    clippy::module_inception,
    clippy::wildcard_imports,
    clippy::correctness
)]
#![warn(
    warnings,
    clippy::perf,
    clippy::style,
    clippy::complexity,
    clippy::nursery,
    clippy::pedantic
)]
#![warn(clippy::missing_const_for_fn, clippy::approx_constant, clippy::all)]

//! # Oxidb: A High-Performance Rust Database
//!
//! `oxidb` is a sophisticated, pure-Rust database system designed for production use.
//! It provides ACID-compliant transactions, advanced indexing strategies, and vector
//! operations for RAG (Retrieval-Augmented Generation) applications.
//!
//! ## Key Features
//!
//! - **ACID Compliance**: Full transaction support with durability guarantees
//! - **Advanced Indexing**: B+ Tree, Blink Tree (concurrent), Hash Index, and HNSW vector similarity
//! - **SQL Support**: Comprehensive SQL parser with DDL and DML operations
//! - **Vector Operations**: Native support for vector embeddings and similarity search
//! - **Memory Safety**: Pure Rust implementation with zero unsafe code
//! - **High Performance**: Optimized storage engine with Write-Ahead Logging
//! - **Dual APIs**: Modern ergonomic `Connection` API and legacy `Oxidb` API
//!
//! ## Quick Start
//!
//! ### Basic Usage with Connection API (Recommended)
//!
//! ```rust
//! use oxidb::{Connection, QueryResult};
//!
//! # fn main() -> Result<(), oxidb::OxidbError> {
//! // Create an in-memory database
//! let mut conn = Connection::open_in_memory()?;
//!
//! // Create a table with unique name
//! let table_name = format!("users_{}", std::process::id());
//! conn.execute(&format!("CREATE TABLE {} (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)", table_name))?;
//!
//! // Insert data with transactions
//! conn.begin_transaction()?;
//! conn.execute(&format!("INSERT INTO {} (id, name, age) VALUES (1, 'Alice', 30)", table_name))?;
//! conn.execute(&format!("INSERT INTO {} (id, name, age) VALUES (2, 'Bob', 25)", table_name))?;
//! conn.commit()?;
//!
//! // Query data
//! let result = conn.execute(&format!("SELECT * FROM {} WHERE age > 25", table_name))?;
//! match result {
//!     QueryResult::Data(data) => {
//!         println!("Found {} users", data.row_count());
//!         for row in data.rows() {
//!             println!("User: {:?}", row);
//!         }
//!     }
//!     _ => println!("No data returned"),
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ### File-based Database
//!
//! ```rust,no_run
//! use oxidb::Connection;
//!
//! # fn main() -> Result<(), oxidb::OxidbError> {
//! // Create or open a file-based database
//! let mut conn = Connection::open("my_database.db")?;
//!
//! // Use the database normally
//! conn.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price FLOAT)")?;
//! conn.execute("INSERT INTO products (name, price) VALUES ('Laptop', 999.99)")?;
//!
//! // Data is automatically persisted
//! conn.persist()?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Vector Operations for RAG
//!
//! ```rust
//! use oxidb::Connection;
//!
//! # fn main() -> Result<(), oxidb::OxidbError> {
//! let mut conn = Connection::open_in_memory()?;
//!
//! // Create a table with native vector embeddings and unique name
//! let table_name = format!("documents_{}", std::process::id());
//! conn.execute(&format!("CREATE TABLE {} (id INTEGER PRIMARY KEY, content TEXT, embedding VECTOR[3])", table_name))?;
//!
//! // Insert document with vector embedding (no quotes around vector literal)
//! conn.execute(&format!("INSERT INTO {} (id, content, embedding) VALUES (1, 'Sample document', [0.1, 0.2, 0.3])", table_name))?;
//!
//! // Query documents (similarity search would be implemented via custom functions)
//! let result = conn.execute(&format!("SELECT * FROM {}", table_name))?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Architecture
//!
//! Oxidb follows a layered architecture:
//!
//! - **API Layer**: User-facing interfaces (`Connection`, `Oxidb`)
//! - **Query Processing**: SQL parsing, binding, optimization, and execution
//! - **Transaction Management**: ACID compliance with 2PL and deadlock detection
//! - **Storage Engine**: Page-based storage with WAL and crash recovery
//! - **Indexing**: Multiple index types for different use cases
//! - **Vector Operations**: Similarity search and RAG framework integration
//!
//! ## Performance
//!
//! Oxidb is designed for high performance with:
//! - Zero-copy operations where possible
//! - Efficient indexing strategies
//! - Concurrent access support (Blink Tree)
//! - Optimized storage layouts
//! - Comprehensive benchmarking suite
//!
//! Run benchmarks with: `cargo bench`
//!
//! ## Safety and Reliability
//!
//! - **Memory Safety**: 100% safe Rust code (no `unsafe` blocks)
//! - **ACID Guarantees**: Full transaction support with durability
//! - **Crash Recovery**: WAL-based recovery ensures data consistency
//! - **Comprehensive Testing**: 683+ unit tests covering all major functionality
//! - **Error Handling**: Comprehensive error types with context
//!
//! ## Examples
//!
//! See the `examples/` directory for comprehensive usage examples:
//! - `connection_api_demo.rs`: Basic API usage
//! - `todo_app/`: Complete application example
//! - `graphrag_demo/`: Vector operations and RAG integration

pub mod api;
pub mod core;
pub mod event_engine;

#[cfg(target_arch = "wasm32")]
pub mod wasm;

// Public API exports
// Note: Oxidb struct is deprecated and removed. Use Connection API instead.
pub use api::{Connection, QueryResult, QueryResultData, Row};
pub use crate::core::types::Value;

// Core module exports for advanced users
pub use crate::core::common::OxidbError;

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    // Imports used by tests in this module
    use crate::api::types::Oxidb;
    use crate::core::common::OxidbError;
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
            Ok(None) => unreachable!("Key not found after insert"),
            Err(e) => unreachable!("Error during get: {e:?}"),
        }

        match db.delete(key1.clone()) {
            Ok(true) => (),
            Ok(false) => unreachable!("Key not found for deletion"),
            Err(e) => unreachable!("Error during delete: {e:?}"),
        }

        match db.get(key1) {
            Ok(None) => (),
            Ok(Some(_)) => unreachable!("Key found after delete"),
            Err(e) => unreachable!("Error during get after delete: {e:?}"),
        }

        let key2 = b"int_key2".to_vec();
        let value2_str = "int_value2".to_string();
        assert!(db.insert(key2.clone(), value2_str.clone()).is_ok());
        match db.get(key2) {
            Ok(Some(v_str)) => assert_eq!(v_str, value2_str),
            _ => unreachable!("Second key not processed correctly"),
        }
    }

    fn derive_wal_path_for_lib_test(db_path: &Path) -> PathBuf {
        let mut wal_path = db_path.to_path_buf();
        let original_extension = wal_path.extension().and_then(std::ffi::OsStr::to_str);

        if let Some(ext_str) = original_extension {
            wal_path.set_extension(format!("{ext_str}.wal"));
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
                db_restarted.get(key_a).unwrap(),
                Some(val_a_updated_str.clone()),
                "Key A should have updated value"
            );
            assert_eq!(db_restarted.get(key_b).unwrap(), None, "Key B should be deleted");

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
        let value_c_str = "val_c".to_string();
        let key_d = b"key_d_persist".to_vec();
        let value_d_str = "val_d".to_string();

        {
            let mut db1 = Oxidb::new(db_path).expect("Failed to create Oxidb (instance 1)");
            db1.insert(key_c.clone(), value_c_str.clone()).unwrap();
            db1.persist().unwrap();
        }

        {
            let mut db2 = Oxidb::new(db_path).expect("Failed to create Oxidb (instance 2)");
            assert_eq!(
                db2.get(key_c.clone()).unwrap(),
                Some(value_c_str.clone()),
                "Key C should be present in instance 2"
            );

            db2.insert(key_d.clone(), value_d_str.clone()).unwrap();
            db2.persist().unwrap();
        }

        {
            let mut db3 = Oxidb::new(db_path).expect("Failed to create Oxidb (instance 3)");
            assert_eq!(
                db3.get(key_c).unwrap(),
                Some(value_c_str.clone()),
                "Key C should be present in instance 3"
            );
            assert_eq!(
                db3.get(key_d).unwrap(),
                Some(value_d_str.clone()),
                "Key D should be present in instance 3"
            );
        }
    }

    #[test]
    fn test_oxidb_new_from_config_file_custom_paths() {
        // Config is used here
        use crate::core::types::DataType;
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
            custom_data_dir.to_str().unwrap().replace('\\', "/"),
            custom_db_filename,
            custom_data_dir.to_str().unwrap().replace('\\', "/"), // Assuming index dir is also under custom_data_dir for this test
            custom_index_dir
        );

        let config_file_path = dir.path().join("custom_config.toml");
        let mut file = File::create(&config_file_path).unwrap(); // File is used here
        file.write_all(config_content.as_bytes()).unwrap(); // Write is used here

        let mut db = Oxidb::new_from_config_file(config_file_path.clone()).unwrap();

        let expected_db_path = custom_data_dir.join(custom_db_filename);
        assert_eq!(db.database_path(), expected_db_path.to_str().unwrap());
        // For index_path, the config now specifies a full path for index_base_path
        let expected_index_path = custom_data_dir.join(custom_index_dir);
        assert_eq!(db.index_path(), expected_index_path.to_str().unwrap());

        db.execute_query_str("INSERT test 1").unwrap();
        let val = db.execute_query_str("GET test").unwrap();
        assert_eq!(
            val,
            crate::core::query::executor::ExecutionResult::Value(Some(DataType::Integer(1)))
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
        db.insert(key, value).unwrap();
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

        // Write the malformed config to temp file
        temp_config_file.write_all(b"invalid_toml_content = [").unwrap();

        let result = Oxidb::new_from_config_file(temp_config_file.path());
        match result {
            Err(OxidbError::Configuration(msg)) => {
                assert!(msg.contains("Failed to parse config file"));
            }
            Err(other) => panic!("Expected Configuration error, got: {:?}", other),
            Ok(_) => panic!("Expected error but got success"),
        }
    }
}
