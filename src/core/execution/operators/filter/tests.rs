// src/core/execution/operators/filter/tests.rs
use crate::core::common::OxidbError;
use crate::core::execution::operators::filter::FilterOperator; // Import the FilterOperator
use crate::core::execution::{ExecutionOperator, Tuple};
use crate::core::optimizer::Expression;
use crate::core::types::DataType;
use std::sync::{Arc, Mutex};

// Mock ExecutionOperator for testing purposes
struct MockInputOperator {
    tuples: Arc<Mutex<Vec<Tuple>>>,
    // iter_idx: usize, // Unused
}

impl MockInputOperator {
    fn new(data: Vec<Tuple>) -> Self {
        MockInputOperator {
            tuples: Arc::new(Mutex::new(data)),
            // iter_idx: 0, // Unused
        }
    }
}

impl ExecutionOperator for MockInputOperator {
    fn execute(
        &mut self,
    ) -> Result<Box<dyn Iterator<Item = Result<Tuple, OxidbError>> + Send + Sync>, OxidbError> {
        // Simple iterator that clones data on each call to execute
        // This is okay for tests but not for a real operator.
        let data_clone = self.tuples.lock().unwrap().clone();
        Ok(Box::new(data_clone.into_iter().map(Ok)))
    }
}

// Helper to create a simple literal expression
fn literal(value: DataType) -> Box<Expression> {
    Box::new(Expression::Literal(value))
}

// Helper to create a column expression (using index as string for now, as per FilterOperator logic)
fn column(index: usize) -> Box<Expression> {
    Box::new(Expression::Column(index.to_string()))
}

// Helper to create a comparison operation
fn compare_op(left: Box<Expression>, op: &str, right: Box<Expression>) -> Expression {
    Expression::CompareOp { left, op: op.to_string(), right }
}

// Helper to create a binary logical operation
fn binary_op(left: Box<Expression>, op: &str, right: Box<Expression>) -> Expression {
    Expression::BinaryOp { left, op: op.to_string(), right }
}

#[test]
fn test_filter_operator_simple_equals() -> Result<(), OxidbError> {
    let input_tuples = vec![
        vec![DataType::Integer(1), DataType::String("apple".to_string())],
        vec![DataType::Integer(2), DataType::String("banana".to_string())],
        vec![DataType::Integer(3), DataType::String("apple".to_string())],
    ];
    let mock_input = MockInputOperator::new(input_tuples);

    // Predicate: tuple[1] == "apple"
    let predicate = compare_op(column(1), "=", literal(DataType::String("apple".to_string())));

    let mut filter_op = FilterOperator::new(Box::new(mock_input), predicate);
    let mut result_iter = filter_op.execute()?;

    let first_filtered = result_iter.next().unwrap()?;
    assert_eq!(first_filtered, vec![DataType::Integer(1), DataType::String("apple".to_string())]);

    let second_filtered = result_iter.next().unwrap()?;
    assert_eq!(second_filtered, vec![DataType::Integer(3), DataType::String("apple".to_string())]);

    assert!(result_iter.next().is_none());
    Ok(())
}

#[test]
fn test_filter_operator_greater_than_or_equal() -> Result<(), OxidbError> {
    let input_tuples = vec![
        vec![DataType::Integer(10)],
        vec![DataType::Integer(20)],
        vec![DataType::Integer(15)],
        vec![DataType::Integer(25)],
    ];
    let mock_input = MockInputOperator::new(input_tuples);
    let predicate = compare_op(column(0), ">=", literal(DataType::Integer(20)));
    let mut filter_op = FilterOperator::new(Box::new(mock_input), predicate);
    let results: Vec<Tuple> = filter_op.execute()?.collect::<Result<_, _>>()?;

    assert_eq!(results.len(), 2);
    assert!(results.contains(&vec![DataType::Integer(20)]));
    assert!(results.contains(&vec![DataType::Integer(25)]));
    Ok(())
}

#[test]
fn test_filter_operator_less_than_or_equal() -> Result<(), OxidbError> {
    let input_tuples = vec![
        vec![DataType::Float(10.5)],
        vec![DataType::Float(20.0)],
        vec![DataType::Float(15.5)],
        vec![DataType::Float(5.5)],
    ];
    let mock_input = MockInputOperator::new(input_tuples);
    let predicate = compare_op(column(0), "<=", literal(DataType::Float(15.5)));
    let mut filter_op = FilterOperator::new(Box::new(mock_input), predicate);
    let results: Vec<Tuple> = filter_op.execute()?.collect::<Result<_, _>>()?;

    assert_eq!(results.len(), 3);
    assert!(results.contains(&vec![DataType::Float(10.5)]));
    assert!(results.contains(&vec![DataType::Float(15.5)]));
    assert!(results.contains(&vec![DataType::Float(5.5)]));
    Ok(())
}

#[test]
fn test_filter_operator_and() -> Result<(), OxidbError> {
    let input_tuples = vec![
        vec![DataType::Integer(1), DataType::String("apple".to_string())], // Pass
        vec![DataType::Integer(2), DataType::String("banana".to_string())], // Fail (col1)
        vec![DataType::Integer(3), DataType::String("apple".to_string())], // Pass
        vec![DataType::Integer(4), DataType::String("orange".to_string())], // Fail (col0)
    ];
    let mock_input = MockInputOperator::new(input_tuples);

    // Predicate: tuple[0] < 3 AND tuple[1] == "apple"
    let cond1 = compare_op(column(0), "<", literal(DataType::Integer(3)));
    let cond2 = compare_op(column(1), "=", literal(DataType::String("apple".to_string())));
    let predicate = binary_op(Box::new(cond1), "AND", Box::new(cond2));

    let mut filter_op = FilterOperator::new(Box::new(mock_input), predicate);
    let results: Vec<Tuple> = filter_op.execute()?.collect::<Result<_, _>>()?;

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], vec![DataType::Integer(1), DataType::String("apple".to_string())]);
    Ok(())
}

#[test]
fn test_filter_operator_or() -> Result<(), OxidbError> {
    let input_tuples = vec![
        vec![DataType::Integer(50), DataType::Float(5.0)], // Pass (col0)
        vec![DataType::Integer(10), DataType::Float(15.0)], // Pass (col1)
        vec![DataType::Integer(5), DataType::Float(2.0)],  // Fail
        vec![DataType::Integer(60), DataType::Float(12.0)], // Pass (both)
    ];
    let mock_input = MockInputOperator::new(input_tuples);

    // Predicate: tuple[0] > 40 OR tuple[1] > 10.0
    let cond1 = compare_op(column(0), ">", literal(DataType::Integer(40)));
    let cond2 = compare_op(column(1), ">", literal(DataType::Float(10.0)));
    let predicate = binary_op(Box::new(cond1), "OR", Box::new(cond2));

    let mut filter_op = FilterOperator::new(Box::new(mock_input), predicate);
    let results: Vec<Tuple> = filter_op.execute()?.collect::<Result<_, _>>()?;

    assert_eq!(results.len(), 3);
    assert!(results.contains(&vec![DataType::Integer(50), DataType::Float(5.0)]));
    assert!(results.contains(&vec![DataType::Integer(10), DataType::Float(15.0)]));
    assert!(results.contains(&vec![DataType::Integer(60), DataType::Float(12.0)]));
    Ok(())
}

#[test]
fn test_filter_operator_and_short_circuit() -> Result<(), OxidbError> {
    // Test that the right side of AND is not evaluated if the left side is false.
    // We "prove" this by making the right side an expression that would error if evaluated.
    let input_tuples = vec![
        vec![DataType::Integer(100)], // This tuple will make left side of AND false
    ];
    let mock_input = MockInputOperator::new(input_tuples);

    // Predicate: tuple[0] < 50 AND (tuple[0] / "string_error") -> this would normally error
    let cond_left_false = compare_op(column(0), "<", literal(DataType::Integer(50))); // 100 < 50 is false
    let cond_right_error =
        compare_op(column(0), "=", literal(DataType::String("error".to_string()))); // Type mismatch error if evaluated against Integer(100)

    let predicate = binary_op(Box::new(cond_left_false), "AND", Box::new(cond_right_error));

    let mut filter_op = FilterOperator::new(Box::new(mock_input), predicate);
    let results: Vec<Tuple> = filter_op.execute()?.collect::<Result<_, _>>()?;

    assert!(results.is_empty(), "Expected no results due to short-circuit AND"); // If it didn't short-circuit, an error would occur
    Ok(())
}

#[test]
fn test_filter_operator_or_short_circuit() -> Result<(), OxidbError> {
    // Test that the right side of OR is not evaluated if the left side is true.
    let input_tuples = vec![
        vec![DataType::Integer(10)], // This tuple will make left side of OR true
    ];
    let mock_input = MockInputOperator::new(input_tuples);

    // Predicate: tuple[0] < 50 OR (tuple[0] / "string_error")
    let cond_left_true = compare_op(column(0), "<", literal(DataType::Integer(50))); // 10 < 50 is true
    let cond_right_error =
        compare_op(column(0), "=", literal(DataType::String("error".to_string())));

    let predicate = binary_op(Box::new(cond_left_true), "OR", Box::new(cond_right_error));

    let mut filter_op = FilterOperator::new(Box::new(mock_input), predicate);
    let results: Vec<Tuple> = filter_op.execute()?.collect::<Result<_, _>>()?;

    assert_eq!(results.len(), 1, "Expected one result due to short-circuit OR");
    assert_eq!(results[0], vec![DataType::Integer(10)]);
    Ok(())
}

#[test]
fn test_filter_operator_nested_logical_ops() -> Result<(), OxidbError> {
    let input_tuples = vec![
        // (val1 > 10 AND val2 == "A") OR val3 == true
        // Tuple: Integer, String, Boolean
        vec![DataType::Integer(5), DataType::String("A".to_string()), DataType::Boolean(true)], // (F && T) || T -> T
        vec![DataType::Integer(15), DataType::String("B".to_string()), DataType::Boolean(false)], // (T && F) || F -> F
        vec![DataType::Integer(20), DataType::String("A".to_string()), DataType::Boolean(false)], // (T && T) || F -> T
        vec![DataType::Integer(5), DataType::String("B".to_string()), DataType::Boolean(false)], // (F && F) || F -> F
    ];
    let mock_input = MockInputOperator::new(input_tuples);

    let cond1_left = compare_op(column(0), ">", literal(DataType::Integer(10)));
    let cond1_right = compare_op(column(1), "=", literal(DataType::String("A".to_string())));
    let and_expr = binary_op(Box::new(cond1_left), "AND", Box::new(cond1_right));

    let cond2 = compare_op(column(2), "=", literal(DataType::Boolean(true)));
    let predicate = binary_op(Box::new(and_expr), "OR", Box::new(cond2));

    let mut filter_op = FilterOperator::new(Box::new(mock_input), predicate);
    let results: Vec<Tuple> = filter_op.execute()?.collect::<Result<_, _>>()?;

    assert_eq!(results.len(), 2);
    assert!(results.contains(&vec![
        DataType::Integer(5),
        DataType::String("A".to_string()),
        DataType::Boolean(true)
    ]));
    assert!(results.contains(&vec![
        DataType::Integer(20),
        DataType::String("A".to_string()),
        DataType::Boolean(false)
    ]));
    Ok(())
}

#[test]
fn test_filter_unsupported_operator_in_compare() -> Result<(), OxidbError> {
    let mock_input = MockInputOperator::new(vec![vec![DataType::Integer(1)]]);
    let predicate = compare_op(column(0), "IS NULL", literal(DataType::Null)); // IS NULL not directly supported by this op structure
    let mut filter_op = FilterOperator::new(Box::new(mock_input), predicate);
    let result: Result<Vec<Tuple>, OxidbError> = filter_op.execute()?.collect();
    assert!(result.is_err());
    match result.err().unwrap() {
        OxidbError::NotImplemented { feature } => {
            assert!(feature.contains("Operator 'IS NULL' not implemented in CompareOp."))
        }
        e => panic!("Expected NotImplemented error, got {:?}", e),
    }
    Ok(())
}

#[test]
fn test_filter_unsupported_operator_in_binary() -> Result<(), OxidbError> {
    let mock_input = MockInputOperator::new(vec![vec![DataType::Boolean(true)]]);
    let predicate =
        binary_op(literal(DataType::Boolean(true)), "XOR", literal(DataType::Boolean(false)));
    let mut filter_op = FilterOperator::new(Box::new(mock_input), predicate);
    let result: Result<Vec<Tuple>, OxidbError> = filter_op.execute()?.collect();
    assert!(result.is_err());
    match result.err().unwrap() {
        OxidbError::NotImplemented { feature } => {
            assert!(feature.contains("Logical operator 'XOR' not implemented in BinaryOp."))
        }
        e => panic!("Expected NotImplemented error, got {:?}", e),
    }
    Ok(())
}

#[test]
fn test_filter_unsupported_expression_type() -> Result<(), OxidbError> {
    let mock_input = MockInputOperator::new(vec![vec![DataType::Integer(1)]]);
    // Using a Column as the top-level predicate is not directly evaluatable to bool without context
    let predicate = Expression::Column("0".to_string());
    let mut filter_op = FilterOperator::new(Box::new(mock_input), predicate);
    let result: Result<Vec<Tuple>, OxidbError> = filter_op.execute()?.collect();
    assert!(result.is_err());
    match result.err().unwrap() {
        OxidbError::NotImplemented { feature } => {
            assert!(feature.contains("not supported as a predicate yet"))
        }
        e => panic!("Expected NotImplemented error, got {:?}", e),
    }
    Ok(())
}
