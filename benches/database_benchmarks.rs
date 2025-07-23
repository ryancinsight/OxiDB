//! Comprehensive benchmarking suite for OxiDB
//!
//! This benchmark suite measures raw database performance using parameterized queries
//! to avoid string allocation overhead that would skew results.

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use oxidb::{Connection, Value};
use std::time::Duration;

/// Benchmark INSERT operations using parameterized queries
/// This avoids string allocation overhead and measures pure database performance
fn bench_insert_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_operations");
    group.measurement_time(Duration::from_secs(5)); // Reduced for faster iteration

    let size = 100;
    group.throughput(Throughput::Elements(size as u64));

    group.bench_function("parameterized_batch_inserts", |b| {
        b.iter_batched(
            || {
                // Setup: Create connection and table (not measured)
                let mut conn = Connection::open_in_memory().unwrap();
                let create_sql =
                    "CREATE TABLE bench_insert (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)";
                conn.execute(create_sql).unwrap();
                conn.begin_transaction().unwrap();
                conn
            },
            |mut conn| {
                // Measured operation: INSERT using parameterized queries
                let insert_sql = "INSERT INTO bench_insert (id, name, value) VALUES (?, ?, ?)";
                for i in 1..=size {
                    let params = [
                        Value::Integer(i),
                        Value::Text(format!("Item{}", i)), // Only used in measured section, kept minimal
                        Value::Integer(i * 10),
                    ];
                    black_box(conn.execute_with_params(insert_sql, &params).unwrap());
                }
                conn.commit().unwrap();
            },
            criterion::BatchSize::SmallInput,
        );
    });

    // Additional benchmark: measure pure INSERT performance without transaction overhead
    group.bench_function("single_parameterized_inserts", |b| {
        b.iter_batched(
            || {
                let mut conn = Connection::open_in_memory().unwrap();
                let create_sql =
                    "CREATE TABLE bench_single (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)";
                conn.execute(create_sql).unwrap();
                conn
            },
            |mut conn| {
                let insert_sql = "INSERT INTO bench_single (id, name, value) VALUES (?, ?, ?)";
                let params =
                    [Value::Integer(1), Value::Text("TestItem".to_string()), Value::Integer(100)];
                black_box(conn.execute_with_params(insert_sql, &params).unwrap());
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

/// Benchmark SELECT operations using parameterized queries where applicable
fn bench_select_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("select_operations");
    group.measurement_time(Duration::from_secs(5));

    let size = 100;
    group.throughput(Throughput::Elements(size as u64));

    group.bench_function("full_table_scan", |b| {
        b.iter_batched(
            || {
                // Setup: Create and populate table using parameterized queries
                let mut conn = Connection::open_in_memory().unwrap();
                let create_sql =
                    "CREATE TABLE bench_select (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)";
                conn.execute(create_sql).unwrap();

                conn.begin_transaction().unwrap();
                let insert_sql = "INSERT INTO bench_select (id, name, age) VALUES (?, ?, ?)";
                for i in 1..=size {
                    let params = [
                        Value::Integer(i),
                        Value::Text(format!("User{}", i)),
                        Value::Integer((i % 100) + 18),
                    ];
                    conn.execute_with_params(insert_sql, &params).unwrap();
                }
                conn.commit().unwrap();
                conn
            },
            |mut conn| {
                // Measured operation: Full table scan
                let select_sql = "SELECT * FROM bench_select";
                black_box(conn.execute(select_sql).unwrap());
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("parameterized_point_select", |b| {
        b.iter_batched(
            || {
                // Setup: Create and populate table
                let mut conn = Connection::open_in_memory().unwrap();
                let create_sql = "CREATE TABLE bench_point_select (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)";
                conn.execute(create_sql).unwrap();

                conn.begin_transaction().unwrap();
                let insert_sql = "INSERT INTO bench_point_select (id, name, age) VALUES (?, ?, ?)";
                for i in 1..=size {
                    let params = [
                        Value::Integer(i),
                        Value::Text(format!("User{}", i)),
                        Value::Integer((i % 100) + 18),
                    ];
                    conn.execute_with_params(insert_sql, &params).unwrap();
                }
                conn.commit().unwrap();
                conn
            },
            |mut conn| {
                // Measured operation: Point select using parameterized query
                let select_sql = "SELECT * FROM bench_point_select WHERE id = ?";
                let params = [Value::Integer(50)]; // Select middle record
                black_box(conn.execute_with_params(select_sql, &params).unwrap());
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("parameterized_filtered_select", |b| {
        b.iter_batched(
            || {
                // Setup: Create and populate table
                let mut conn = Connection::open_in_memory().unwrap();
                let create_sql = "CREATE TABLE bench_filtered_select (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)";
                conn.execute(create_sql).unwrap();

                conn.begin_transaction().unwrap();
                let insert_sql = "INSERT INTO bench_filtered_select (id, name, age) VALUES (?, ?, ?)";
                for i in 1..=size {
                    let params = [
                        Value::Integer(i),
                        Value::Text(format!("User{}", i)),
                        Value::Integer((i % 100) + 18),
                    ];
                    conn.execute_with_params(insert_sql, &params).unwrap();
                }
                conn.commit().unwrap();
                conn
            },
            |mut conn| {
                // Measured operation: Filtered select using parameterized query (simple comparison)
                let select_sql = "SELECT * FROM bench_filtered_select WHERE age > ?";
                let params = [Value::Integer(25)];
                black_box(conn.execute_with_params(select_sql, &params).unwrap());
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, bench_insert_operations, bench_select_operations);
criterion_main!(benches);
