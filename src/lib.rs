#![forbid(unsafe_code)]
#![deny(
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    clippy::cargo,
    clippy::complexity,
    clippy::correctness,
    clippy::perf,
    clippy::style,
    clippy::suspicious,
    deprecated,
    unused,
    clippy::todo,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unimplemented,
    clippy::unreachable,
    clippy::missing_safety_doc,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::missing_docs
)]
#![allow(
    clippy::module_name_repetitions,
    clippy::must_use_candidate,
    clippy::missing_const_for_fn,
    clippy::option_if_let_else,
    clippy::cognitive_complexity,
    clippy::too_many_lines,
    clippy::similar_names,
    clippy::struct_excessive_bools,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::float_cmp,
    clippy::doc_markdown,
    clippy::wildcard_imports,
    clippy::struct_field_names,
    clippy::module_inception,
    clippy::missing_fields_in_debug,
    clippy::use_self,
    clippy::return_self_not_must_use,
    clippy::bool_to_int_with_if,
    clippy::partial_pub_fields,
    clippy::multiple_crate_versions,
    clippy::single_match_else,
    clippy::implicit_hasher,
    clippy::linkedlist,
    clippy::default_trait_access,
    clippy::missing_transmute_annotations,
    clippy::multiple_inherent_impl,
    clippy::get_unwrap,
    clippy::impl_trait_in_params,
    clippy::future_not_send,
    clippy::type_complexity,
    clippy::result_large_err,
    clippy::large_stack_frames,
    clippy::significant_drop_in_scrutinee,
    clippy::significant_drop_tightening,
    clippy::items_after_statements,
    clippy::match_wildcard_for_single_variants,
    clippy::needless_pass_by_value,
    clippy::redundant_closure_for_method_calls,
    clippy::unused_async,
    clippy::unnecessary_wraps,
    clippy::trivially_copy_pass_by_ref,
    clippy::match_same_arms,
    clippy::explicit_deref_methods,
    clippy::explicit_iter_loop,
    clippy::explicit_into_iter_loop,
    clippy::from_iter_instead_of_collect,
    clippy::if_not_else,
    clippy::equatable_if_let,
    clippy::or_fun_call,
    clippy::iter_without_into_iter,
    clippy::infinite_loop,
    clippy::ref_as_ptr,
    clippy::ref_option_ref,
    clippy::option_option,
    clippy::match_bool,
    clippy::let_underscore_untyped,
    clippy::empty_enum_variants_with_brackets,
    clippy::pattern_type_mismatch,
    clippy::ignored_unit_patterns,
    clippy::redundant_pub_crate,
    clippy::allow_attributes,
    clippy::no_effect_underscore_binding,
    clippy::used_underscore_binding,
    clippy::tests_outside_test_module
)]

//! # Oxidb - A Rust-based Relational Database
//!
//! Oxidb is a lightweight, embedded relational database written in Rust. It provides
//! ACID compliance, SQL support, and various indexing strategies for efficient data retrieval.
//!
//! ## Features
//!
//! - **ACID Compliance**: Full transaction support with atomicity, consistency, isolation, and durability
//! - **SQL Support**: Comprehensive SQL query language support
//! - **Multiple Index Types**: B-Tree, Hash, HNSW for vector similarity search
//! - **MVCC**: Multi-Version Concurrency Control for better concurrent access
//! - **WAL**: Write-Ahead Logging for crash recovery
//! - **Query Optimization**: Cost-based query optimizer
//! - **Vector Operations**: Built-in support for vector similarity search
//! - **Graph Operations**: Native graph database capabilities
//! - **RAG Support**: Retrieval-Augmented Generation with hybrid search
//!
//! ## Quick Start
//!
//! ```no_run
//! use oxidb::api::Connection;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Open a database connection
//! let conn = Connection::open("my_database.db")?;
//!
//! // Create a table
//! conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")?;
//!
//! // Insert data
//! conn.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")?;
//!
//! // Query data
//! let result = conn.query("SELECT * FROM users WHERE age > 25")?;
//! 
//! # Ok(())
//! # }
//! ```
//!
//! ## Architecture
//!
//! Oxidb follows a modular architecture with clear separation of concerns:
//!
//! - **API Layer**: Public interface for database operations
//! - **Query Layer**: SQL parsing, planning, and execution
//! - **Storage Layer**: Persistent storage with buffer pool management
//! - **Transaction Layer**: ACID transaction management with MVCC
//! - **Index Layer**: Various indexing strategies for efficient data access
//! - **Recovery Layer**: WAL-based crash recovery
//!
//! ## Examples
//!
//! The `examples/` directory contains various examples demonstrating different features:
//!
//! - `basic_usage.rs`: Simple CRUD operations
//! - `transactions.rs`: Transaction handling
//! - `vector_search.rs`: Vector similarity search
//! - `graph_operations.rs`: Graph database features
//! - `todo_app/`: Complete application example
//!
//! ## Performance
//!
//! Oxidb is designed with performance in mind:
//!
//! - Zero-copy operations where possible
//! - Efficient memory management with custom allocators
//! - Lock-free data structures for concurrent access
//! - Optimized query execution with vectorized operations

pub mod api;
pub mod core;
pub mod wasm;

// Public API exports
pub use api::{Connection, QueryResult, Row};
pub use crate::core::common::types::Value;

// Core module exports for advanced users
pub use crate::core::common::OxidbError;

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_connection_basic_operations() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        
        // Test connection creation
        let mut conn = Connection::open(&db_path).expect("Failed to create connection");
        
        // Test table creation
        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
            .expect("Failed to create table");
        
        // Test insertion
        conn.execute("INSERT INTO test (id, value) VALUES (1, 'hello')")
            .expect("Failed to insert data");
        
        // Test query
        let result = conn.query("SELECT * FROM test WHERE id = 1")
            .expect("Failed to query data");
        
        assert!(!result.is_empty());
        assert_eq!(result.row_count(), 1);
    }

    #[test]
    fn test_transaction_rollback() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_tx.db");
        
        let mut conn = Connection::open(&db_path).expect("Failed to create connection");
        
        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
            .expect("Failed to create table");
        
        // Start transaction
        conn.execute("BEGIN").expect("Failed to begin transaction");
        
        // Insert data
        conn.execute("INSERT INTO test (id, value) VALUES (1, 'test')")
            .expect("Failed to insert data");
        
        // Rollback
        conn.execute("ROLLBACK").expect("Failed to rollback");
        
        // Verify data was not persisted
        let result = conn.query("SELECT * FROM test")
            .expect("Failed to query data");
        
        assert!(result.is_empty());
        assert_eq!(result.row_count(), 0);
    }
}
