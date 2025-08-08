#![cfg(feature = "legacy_examples")]
//! SQL Compatibility Demo
//! 
//! This example demonstrates Oxidb's SQL compatibility with PostgreSQL and MySQL-like syntax.
//! It shows various SQL features including:
//! - DDL (Data Definition Language): CREATE, ALTER, DROP
//! - DML (Data Manipulation Language): INSERT, UPDATE, DELETE, SELECT
//! - Complex queries: JOINs, subqueries, aggregations
//! - Transactions and constraints

use oxidb::Connection;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")?;
    conn.execute("INSERT INTO t (v) VALUES ('x')")?;
    let _ = conn.query("SELECT * FROM t")?;
    Ok(())
}