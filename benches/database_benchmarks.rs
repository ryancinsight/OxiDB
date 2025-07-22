//! Comprehensive benchmarking suite for OxiDB

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use oxidb::Connection;
use std::time::Duration;

/// Benchmark INSERT operations
fn bench_insert_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_operations");
    group.measurement_time(Duration::from_secs(10));
    
    let size = 100;
    group.throughput(Throughput::Elements(size as u64));
    group.bench_function("batch_inserts", |b| {
        b.iter(|| {
            let mut conn = Connection::open_in_memory().unwrap();
            let create_sql = "CREATE TABLE bench_insert (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)";
            conn.execute(create_sql).unwrap();
            
            conn.begin_transaction().unwrap();
            for i in 1..=size {
                let insert_sql = format!("INSERT INTO bench_insert (id, name, value) VALUES ({}, 'Item{}', {})", i, i, i * 10);
                black_box(conn.execute(&insert_sql).unwrap());
            }
            conn.commit().unwrap();
        });
    });
    
    group.finish();
}

/// Benchmark SELECT operations
fn bench_select_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("select_operations");
    group.measurement_time(Duration::from_secs(10));
    
    let size = 100;
    group.throughput(Throughput::Elements(size as u64));
    
    group.bench_function("full_table_scan", |b| {
        b.iter_batched(
            || {
                let mut conn = Connection::open_in_memory().unwrap();
                let create_sql = "CREATE TABLE bench_select (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)";
                conn.execute(create_sql).unwrap();
                
                conn.begin_transaction().unwrap();
                for i in 1..=size {
                    let insert_sql = format!("INSERT INTO bench_select (id, name, age) VALUES ({}, 'User{}', {})", i, i, (i % 100) + 18);
                    conn.execute(&insert_sql).unwrap();
                }
                conn.commit().unwrap();
                conn
            },
            |mut conn| {
                let select_sql = "SELECT * FROM bench_select";
                black_box(conn.execute(select_sql).unwrap());
            },
            criterion::BatchSize::SmallInput,
        );
    });
    
    group.finish();
}

criterion_group!(benches, bench_insert_operations, bench_select_operations);
criterion_main!(benches);
