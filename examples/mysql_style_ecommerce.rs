#![cfg(feature = "legacy_examples")]
//! MySQL-Style E-commerce Database Example
//! 
//! This example demonstrates usage patterns similar to MySQL.

use oxidb::{Connection, QueryResult};
use oxidb::core::common::OxidbError;

fn main() -> Result<(), Box<dyn std::error::Error>> {
	println!("🛒 MySQL-Style E-commerce Database Demo");
	println!("{}", "=".repeat(60));

	let mut conn = Connection::open_in_memory()?;
	cleanup_tables(&mut conn)?;
	create_schema(&mut conn)?;
	seed_data(&mut conn)?;
	if let QueryResult::Data(ds) = conn.query("SELECT COUNT(*) FROM users")? {
		if let Some(row) = ds.rows.get(0) {
			match row.get(0) {
				Some(oxidb::Value::Integer(n)) => println!("Users inserted: {}", n),
				Some(oxidb::Value::Float(f)) => println!("Users inserted: {}", f),
				other => println!("Users inserted (unparsed): {:?}", other),
			}
		}
	}
	println!("\n✅ MySQL-style e-commerce demo completed successfully!");
	Ok(())
}

fn cleanup_tables(conn: &mut Connection) -> Result<(), OxidbError> {
	let tables = [
		"order_items",
		"orders",
		"product_reviews",
		"products",
		"categories",
		"customers",
		"users",
	];
	for table in tables {
		let _ = conn.execute(&format!("DROP TABLE IF EXISTS {}", table));
	}
	Ok(())
}

fn create_schema(conn: &mut Connection) -> Result<(), OxidbError> {
	let create_users = r#"
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			username TEXT,
			email TEXT
		)
	"#;
	conn.execute(create_users)?;

	let create_customers = r#"
		CREATE TABLE customers (
			id INTEGER PRIMARY KEY,
			user_id INTEGER,
			first_name TEXT,
			last_name TEXT
		)
	"#;
	conn.execute(create_customers)?;

	Ok(())
}

fn seed_data(conn: &mut Connection) -> Result<(), OxidbError> {
	let users = [
		("admin", "admin@example.com"),
		("john_doe", "john@example.com"),
		("jane_smith", "jane@example.com"),
	];
	for (i, (u, e)) in users.iter().enumerate() {
		conn.execute(&format!(
			"INSERT INTO users (id, username, email) VALUES ({}, '{}', '{}')",
			i + 1,
			u,
			e
		))?;
	}

	let customers = [
		(1, "John", "Doe"),
		(2, "Jane", "Smith"),
	];
	for (i, f, l) in customers {
		conn.execute(&format!(
			"INSERT INTO customers (id, user_id, first_name, last_name) VALUES ({}, {}, '{}', '{}')",
			i,
			i,
			f,
			l
		))?;
	}
	Ok(())
}