#![cfg(feature = "legacy_examples")]
//! PostgreSQL-Style Analytics & Data Warehousing Demo
//! 
//! This example demonstrates database usage patterns akin to PostgreSQL analytics.

use oxidb::{Connection, QueryResult};
use oxidb::core::common::OxidbError;
use chrono::{Datelike};

fn main() -> Result<(), Box<dyn std::error::Error>> {
	println!("🐘 PostgreSQL-Style Analytics & Data Warehousing Demo");
	println!("{}", "=".repeat(65));

	let mut conn = Connection::open_in_memory()?;
	setup_analytics_schema(&mut conn)?;
	generate_sample_data(&mut conn)?;
	// Run a small validation query so the example compiles and runs quickly
	if let QueryResult::Data(ds) = conn.query("SELECT * FROM time_dimension LIMIT 3")? {
		println!("Rows in time_dimension (sample): {}", ds.rows.len());
	}
	println!("\n✅ PostgreSQL-style analytics demo completed successfully!");
	Ok(())
}

fn setup_analytics_schema(conn: &mut Connection) -> Result<(), OxidbError> {
	let _ = conn.execute("DROP TABLE IF EXISTS time_dimension");
	let _ = conn.execute("DROP TABLE IF EXISTS sales_events");

	let create_time_dim = r#"
		CREATE TABLE time_dimension (
			date_key INTEGER PRIMARY KEY,
			full_date TEXT NOT NULL,
			year INTEGER NOT NULL,
			quarter INTEGER NOT NULL,
			month INTEGER NOT NULL,
			month_name TEXT NOT NULL,
			day_of_month INTEGER NOT NULL,
			day_of_week INTEGER NOT NULL,
			day_name TEXT NOT NULL,
			week_of_year INTEGER NOT NULL,
			is_weekend BOOLEAN NOT NULL
		)
	"#;
	conn.execute(create_time_dim)?;

	let create_sales = r#"
		CREATE TABLE sales_events (
			id INTEGER PRIMARY KEY,
			transaction_id TEXT NOT NULL,
			customer_id INTEGER NOT NULL,
			product_id INTEGER NOT NULL,
			date_key INTEGER NOT NULL,
			quantity INTEGER NOT NULL,
			total_amount REAL NOT NULL
		)
	"#;
	conn.execute(create_sales)?;
	Ok(())
}

fn generate_sample_data(conn: &mut Connection) -> Result<(), OxidbError> {
	let base_date = chrono::NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
	for i in 0..30 {
		let current_date = base_date + chrono::Duration::days(i);
		let date_key = current_date.format("%Y%m%d").to_string().parse::<i32>().unwrap();
		let sql = format!(
			"INSERT INTO time_dimension (date_key, full_date, year, quarter, month, month_name, day_of_month, day_of_week, day_name, week_of_year, is_weekend) VALUES ({}, '{}', {}, {}, {}, '{}', {}, {}, '{}', {}, {})",
			date_key,
			current_date,
			current_date.year(),
			(current_date.month() - 1) / 3 + 1,
			current_date.month(),
			current_date.format("%B"),
			current_date.day(),
			current_date.weekday().number_from_monday(),
			current_date.format("%A"),
			current_date.iso_week().week(),
			(current_date.weekday().number_from_monday() >= 6)
		);
		conn.execute(&sql)?;
	}
	Ok(())
}