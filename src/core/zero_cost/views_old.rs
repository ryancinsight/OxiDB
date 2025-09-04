//! Zero-copy views for database data structures
//!
//! This module has been refactored to follow SOLID principles with each
//! view type in its own module. All functionality is re-exported for
//! backward compatibility.

pub mod views;

// Re-export all view types for backward compatibility
pub use views::*;
    use super::*;

    #[test]
    fn test_row_view() {
        let values =
            vec![Value::Integer(42), Value::Text("hello".to_string()), Value::Boolean(true)];

        let view = RowView::new(&values);
        assert_eq!(view.len(), 3);
        assert_eq!(view.get(0), Some(&Value::Integer(42)));
        assert_eq!(view.get(1), Some(&Value::Text("hello".to_string())));
        assert_eq!(view.get(2), Some(&Value::Boolean(true)));
        assert_eq!(view.get(3), None);
    }

    #[test]
    fn test_value_view() {
        let text_value = Value::Text("test".to_string());
        let view = ValueView::from_value(&text_value);

        assert_eq!(view.as_str(), Some("test"));
        assert_eq!(view.as_integer(), None);
        assert!(!view.is_null());
    }

    #[test]
    fn test_column_view() {
        let rows = vec![
            Row::from_slice(&[Value::Integer(1), Value::Text("a".to_string())]),
            Row::from_slice(&[Value::Integer(2), Value::Text("b".to_string())]),
            Row::from_slice(&[Value::Integer(3), Value::Text("c".to_string())]),
        ];

        let col_view = ColumnView::new(&rows, 0);
        assert_eq!(col_view.get(0), Some(&Value::Integer(1)));
        assert_eq!(col_view.get(1), Some(&Value::Integer(2)));
        assert_eq!(col_view.count_non_null(), 3);

        assert!(col_view.all(|v| matches!(v, Value::Integer(_))));
        assert!(col_view.any(|v| matches!(v, Value::Integer(2))));
    }
}
