//! Zero-Cost SQL Demo
//!
//! This example demonstrates OxidDB's zero-cost abstractions including:
//! - Zero-copy data views
//! - Efficient iterator combinators
//! - Borrowed data structures
//! - Window functions

use oxidb::core::zero_cost::{
    BorrowedValue, BorrowedRow, TableView, StringView
};
use oxidb::core::zero_cost::borrowed::BorrowedPredicate;
use oxidb::core::zero_cost::iterators::RowRefIterator;
use oxidb::core::query::executor::zero_cost::{QueryResult, Row, QueryMetadata, WindowIterator, WindowRefIterator};
use oxidb::api::types::Row as ApiRow;
use oxidb::core::common::types::Value;
use oxidb::core::types::DataType;
use std::borrow::Cow;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 OxidDB Zero-Cost SQL Demo");
    println!("============================\n");
    
    // Demonstrate zero-cost abstractions
    demonstrate_zero_copy_views()?;
    demonstrate_efficient_iterators();
    demonstrate_borrowed_structures()?;
    demonstrate_window_functions()?;
    demonstrate_query_result_handling()?;
    
    println!("\n✅ All demonstrations completed successfully!");
    Ok(())
}

/// Demonstrate zero-copy data views
fn demonstrate_zero_copy_views() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Zero-Copy Data Views ===");
    
    // Sample employee data
    let sample_data = vec![
        vec![
            DataType::Integer(1),
            DataType::String("Alice".to_string()),
            DataType::String("Engineering".to_string()),
            DataType::Integer(100000),
        ],
        vec![
            DataType::Integer(2),
            DataType::String("Bob".to_string()),
            DataType::String("Sales".to_string()),
            DataType::Integer(90000),
        ],
        vec![
            DataType::Integer(3),
            DataType::String("Charlie".to_string()),
            DataType::String("Engineering".to_string()),
            DataType::Integer(110000),
        ],
    ];
    
    // Convert to Row format for TableView
    let rows: Vec<ApiRow> = sample_data.iter()
        .map(|data| ApiRow::from_slice(
            &data.iter().map(|dt| match dt {
                DataType::Integer(i) => Value::Integer(*i),
                DataType::String(s) => Value::Text(s.clone()),
                _ => Value::Null,
            }).collect::<Vec<_>>()
        ))
        .collect();
    
    let column_names = vec!["id".to_string(), "name".to_string(), "department".to_string(), "salary".to_string()];
    
    // Create a zero-copy table view
    let table_view = TableView::new(&rows, Cow::Borrowed(&column_names));
    
    println!("📊 Table view created with {} rows and {} columns", 
             table_view.row_count(), table_view.column_count());
    
    // Manual column analysis (since ColumnView API is different)
    println!("\n💰 Salary column analysis (zero-copy):");
    
    let mut total_salary = 0i64;
    let mut count = 0;
    
    for row in &rows {
        if let Some(Value::Integer(salary)) = row.get(3) {
            total_salary += salary;
            count += 1;
        }
    }
    
    if count > 0 {
        println!("  Average salary: ${}", total_salary / count);
        println!("  Total payroll: ${}", total_salary);
    }
    
    // String views for zero-copy string operations
    println!("\n📝 String views (zero allocation):");
    let test_string = "Hello, OxidDB!";
    let string_view = StringView::Borrowed(test_string);
    
    println!("  Original: {}", string_view);
    println!("  Length: {}", string_view.len());
    println!("  Is borrowed: {}", matches!(string_view, StringView::Borrowed(_)));
    
    Ok(())
}

/// Demonstrate efficient iterator usage
fn demonstrate_efficient_iterators() {
    println!("\n=== Efficient Iterators ===");
    
    // Sample data
    let employees = vec![
        ApiRow::from_slice(&[
            Value::Text("Alice".to_string()),
            Value::Integer(100000),
            Value::Text("Engineering".to_string()),
        ]),
        ApiRow::from_slice(&[
            Value::Text("Bob".to_string()),
            Value::Integer(90000),
            Value::Text("Engineering".to_string()),
        ]),
        ApiRow::from_slice(&[
            Value::Text("Charlie".to_string()),
            Value::Integer(120000),
            Value::Text("Sales".to_string()),
        ]),
        ApiRow::from_slice(&[
            Value::Text("David".to_string()),
            Value::Integer(95000),
            Value::Text("Engineering".to_string()),
        ]),
        ApiRow::from_slice(&[
            Value::Text("Eve".to_string()),
            Value::Integer(110000),
            Value::Text("Sales".to_string()),
        ]),
    ];
    
    // Zero-copy iteration
    println!("\n1. High earners (salary > 100k) - zero allocation:");
    let row_iter = RowRefIterator::new(&employees);
    
    for row in row_iter {
        if let Some(Value::Integer(salary)) = row.get(1) {
            if *salary > 100000 {
                if let Some(Value::Text(name)) = row.get(0) {
                    println!("  - {} earns ${}", name, salary);
                }
            }
        }
    }
    
    // Manual projection (since ColumnProjection API is different)
    println!("\n2. Department listing (projected view):");
    for (i, row) in employees.iter().enumerate() {
        if let (Some(Value::Text(name)), Some(Value::Text(dept))) = 
            (row.get(0), row.get(2)) {
            println!("  {}. {} - {}", i + 1, name, dept);
        }
    }
    
    // Efficient aggregation
    println!("\n3. Department statistics:");
    use std::collections::HashMap;
    let mut dept_stats: HashMap<String, (i64, i32)> = HashMap::new();
    
    for row in &employees {
        if let (Some(Value::Text(dept)), Some(Value::Integer(salary))) = 
            (row.get(2), row.get(1)) {
            let entry = dept_stats.entry(dept.clone()).or_insert((0, 0));
            entry.0 += salary;
            entry.1 += 1;
        }
    }
    
    for (dept, (total, count)) in &dept_stats {
        println!("  {} - {} employees, average salary: ${}", 
                dept, count, total / *count as i64);
    }
}

/// Demonstrate borrowed data structures
fn demonstrate_borrowed_structures() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Borrowed Data Structures ===");
    
    // Create borrowed values
    let int_value = BorrowedValue::Integer(42);
    let bool_value = BorrowedValue::Boolean(true);
    let float_value = BorrowedValue::Float(3.14);
    
    println!("\n1. Borrowed values:");
    println!("  Integer: {:?} (size: {} bytes)", 
            int_value, std::mem::size_of_val(&int_value));
    println!("  Boolean: {:?} (size: {} bytes)", 
            bool_value, std::mem::size_of_val(&bool_value));
    println!("  Float: {:?} (size: {} bytes)", 
            float_value, std::mem::size_of_val(&float_value));
    
    // Borrowed row using Value types
    let row_values = vec![
        Value::Integer(100),
        Value::Text("Test".to_string()),
        Value::Boolean(true),
    ];
    
    let borrowed_row = BorrowedRow::new(&row_values);
    
    println!("\n2. Borrowed row with {} columns:", borrowed_row.len());
    for (i, value) in borrowed_row.iter().enumerate() {
        println!("  Column {}: {:?}", i, value);
    }
    
    // Create an ApiRow for predicate evaluation
    let test_row = ApiRow::from_slice(&[
        Value::Integer(100),
        Value::Text("Test".to_string()),
        Value::Boolean(true),
    ]);
    
    // Borrowed predicates for efficient filtering
    println!("\n3. Borrowed predicates:");
    let predicate = BorrowedPredicate::new(
        0, // column index
        oxidb::core::zero_cost::borrowed::ComparisonOp::GreaterThan,
        BorrowedValue::Integer(50),
    );
    
    println!("  Predicate: column[0] > 50");
    if predicate.evaluate(&test_row) {
        println!("  ✓ Predicate matches!");
    } else {
        println!("  ✗ Predicate does not match");
    }
    
    Ok(())
}

/// Demonstrate window functions
fn demonstrate_window_functions() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Window Functions ===");
    
    // Time series data
    let time_series: Vec<Vec<DataType>> = vec![
        vec![DataType::Integer(1), DataType::Float(oxidb::core::types::OrderedFloat(100.0))],
        vec![DataType::Integer(2), DataType::Float(oxidb::core::types::OrderedFloat(105.0))],
        vec![DataType::Integer(3), DataType::Float(oxidb::core::types::OrderedFloat(103.0))],
        vec![DataType::Integer(4), DataType::Float(oxidb::core::types::OrderedFloat(108.0))],
        vec![DataType::Integer(5), DataType::Float(oxidb::core::types::OrderedFloat(112.0))],
        vec![DataType::Integer(6), DataType::Float(oxidb::core::types::OrderedFloat(110.0))],
    ];
    
    println!("📊 Time series with {} data points", time_series.len());
    
    // 1. Zero-copy window iteration (when data is pre-loaded)
    println!("\n1. Moving Average (3-period) - Zero Copy:");
    let window_iter = WindowRefIterator::new(&time_series, 3);
    
    for (i, window) in window_iter.enumerate() {
        let sum: f64 = window.iter()
            .filter_map(|row| match row.get(1) {
                Some(DataType::Float(f)) => Some(f.0),
                _ => None,
            })
            .sum();
        let avg = sum / window.len() as f64;
        println!("  Period {}: {:.2}", i + 3, avg);
    }
    
    // 2. Streaming window (for data from iterators)
    println!("\n2. Streaming Window Analysis:");
    let streaming_data = time_series.clone().into_iter();
    let mut stream_window = WindowIterator::new(streaming_data, 3);
    
    let mut period = 1;
    while let Some(window) = stream_window.next() {
        let values: Vec<f64> = window.iter()
            .filter_map(|row| match row.get(1) {
                Some(DataType::Float(f)) => Some(f.0),
                _ => None,
            })
            .collect();
        
        if !values.is_empty() {
            let min = values.iter().min_by(|a, b| a.partial_cmp(b).unwrap()).unwrap();
            let max = values.iter().max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap();
            println!("  Window {}: min={:.1}, max={:.1}, range={:.1}", 
                    period, min, max, max - min);
            period += 1;
        }
    }
    
    Ok(())
}

/// Demonstrate zero-cost query result handling
fn demonstrate_query_result_handling() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Zero-Cost Query Results ===");
    
    // Simulate query results
    let column_names = vec!["product".to_string(), "sales".to_string(), "region".to_string()];
    let result_data = vec![
        vec![DataType::String("Widget".to_string()), DataType::Integer(1000), DataType::String("North".to_string())],
        vec![DataType::String("Gadget".to_string()), DataType::Integer(1500), DataType::String("South".to_string())],
        vec![DataType::String("Doohickey".to_string()), DataType::Integer(800), DataType::String("East".to_string())],
    ];
    
    // Create zero-cost query result
    let query_result = QueryResult {
        columns: Cow::Owned(column_names.clone()),
        rows: Box::new(result_data.iter().map(|row| Row::from_borrowed(row.as_slice()))),
        metadata: QueryMetadata {
            rows_affected: 0,
            execution_time_us: 150,
            used_index: false,
            index_name: None,
        },
    };
    
    println!("📋 Query executed in {} μs", query_result.metadata.execution_time_us);
    println!("Columns: {:?}", query_result.columns);
    
    println!("\nResults (zero-copy iteration):");
    for (i, row) in query_result.rows.enumerate() {
        print!("  Row {}: ", i + 1);
        for j in 0..row.len() {
            if let Some(value) = row.get(j) {
                match value {
                    DataType::String(s) => print!("{}", s),
                    DataType::Integer(n) => print!("{}", n),
                    _ => print!("?"),
                }
                if j < row.len() - 1 {
                    print!(", ");
                }
            }
        }
        println!();
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_zero_copy_views() {
        assert!(demonstrate_zero_copy_views().is_ok());
    }
    
    #[test]
    fn test_efficient_iterators() {
        demonstrate_efficient_iterators();
    }
    
    #[test]
    fn test_borrowed_structures() {
        assert!(demonstrate_borrowed_structures().is_ok());
    }
    
    #[test]
    fn test_window_functions() {
        assert!(demonstrate_window_functions().is_ok());
    }
}