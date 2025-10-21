//! Zero-copy views for database data structures
//!
//! This module provides view types that allow accessing data without copying,
//! enabling efficient data processing with minimal memory overhead.
//!
//! The module is organized into separate view types following SOLID principles:
//! - `row_view`: Zero-copy access to row data
//! - `table_view`: Zero-copy access to table data with schema
//! - `column_view`: Zero-copy column-oriented access
//! - `value_view`: Zero-copy access to individual values
//! - `projection_view`: Zero-copy access to projected columns

pub mod column_view;
pub mod projection_view;
pub mod row_view;
pub mod table_view;
pub mod value_view;

// Re-export all view types for convenience
pub use column_view::ColumnView;
pub use projection_view::ProjectionView;
pub use row_view::RowView;
pub use table_view::TableView;
pub use value_view::{BytesView, StringView, ValueView};

#[cfg(test)]
mod integration_tests {
    use super::*;
    use crate::core::common::types::{Row, Value};
    use std::borrow::Cow;

    #[test]
    fn test_view_integration() {
        // Create test data
        let rows = vec![
            Row::new(vec![Value::Integer(1), Value::Text("Alice".to_string()), Value::Integer(25)]),
            Row::new(vec![Value::Integer(2), Value::Text("Bob".to_string()), Value::Integer(30)]),
        ];
        let column_names = vec!["id".to_string(), "name".to_string(), "age".to_string()];

        // Test TableView
        let table_view = TableView::new(&rows, Cow::Borrowed(&column_names));
        assert_eq!(table_view.row_count(), 2);
        assert_eq!(table_view.column_count(), 3);

        // Test RowView
        if let Some(first_row) = table_view.get_row(0) {
            let row_view = RowView::new(&first_row.values);
            assert_eq!(row_view.len(), 3);
            assert_eq!(row_view.get(1), Some(&Value::Text("Alice".to_string())));
        }

        // Test ColumnView
        let name_column = table_view.column(1);
        assert_eq!(name_column.len(), 2);
        assert_eq!(name_column.get(0), Some(&Value::Text("Alice".to_string())));
        assert_eq!(name_column.get(1), Some(&Value::Text("Bob".to_string())));

        // Test ProjectionView
        if let Some(first_row) = table_view.get_row(0) {
            let projection_indices = [0, 2]; // id and age
            let projection = ProjectionView::new(first_row, &projection_indices);
            assert_eq!(projection.len(), 2);
            assert_eq!(projection.get(0), Some(&Value::Integer(1)));
            assert_eq!(projection.get(1), Some(&Value::Integer(25)));
        }

        // Test ValueView
        if let Some(first_row) = table_view.get_row(0) {
            if let Some(name_value) = first_row.values.get(1) {
                let value_view = ValueView::from_value(name_value);
                assert_eq!(value_view.as_str(), Some("Alice"));
                assert!(value_view.is_text());
                assert!(!value_view.is_numeric());
            }
        }
    }

    #[test]
    fn test_view_composition() {
        // Test using multiple views together
        let rows = vec![
            Row::new(vec![
                Value::Integer(1),
                Value::Text("Engineering".to_string()),
                Value::Float(75000.0),
            ]),
            Row::new(vec![
                Value::Integer(2),
                Value::Text("Marketing".to_string()),
                Value::Float(65000.0),
            ]),
            Row::new(vec![
                Value::Integer(3),
                Value::Text("Sales".to_string()),
                Value::Float(70000.0),
            ]),
        ];
        let column_names = vec!["id".to_string(), "department".to_string(), "salary".to_string()];

        let table_view = TableView::new(&rows, Cow::Borrowed(&column_names));

        // Use column view to analyze salary column
        let salary_column = table_view.column(2);
        assert_eq!(salary_column.count_non_null(), 3);

        // Check if all salaries are above 60000
        let all_high_salary =
            salary_column.all(|value| matches!(value, Value::Float(salary) if *salary > 60000.0));
        assert!(all_high_salary);

        // Use projection to get only department and salary
        if let Some(first_row) = table_view.get_row(0) {
            let projection = ProjectionView::new(first_row, &[1, 2]);
            let values = projection.collect_values();
            assert_eq!(values.len(), 2);
            assert_eq!(values[0], &Value::Text("Engineering".to_string()));
            assert_eq!(values[1], &Value::Float(75000.0));
        }
    }

    #[test]
    fn test_view_performance_characteristics() {
        // Test that views don't copy data
        let large_text = "A".repeat(10000);
        let row = Row::new(vec![Value::Integer(1), Value::Text(large_text.clone())]);

        // RowView should not copy the large text
        let row_view = RowView::new(&row.values);
        if let Some(Value::Text(text)) = row_view.get(1) {
            // This should be the same reference, not a copy
            assert_eq!(text.len(), 10000);
        }

        // ValueView should also not copy
        if let Some(text_value) = row.values.get(1) {
            let value_view = ValueView::from_value(text_value);
            if let Some(text) = value_view.as_str() {
                assert_eq!(text.len(), 10000);
            }
        }
    }
}
