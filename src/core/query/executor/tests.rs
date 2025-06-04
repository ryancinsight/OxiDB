use crate::core::query::executor::{ExecutionResult, QueryExecutor}; // Adjusted path
use crate::core::query::commands::{Command, SqlCondition, SelectColumnSpec, Key, SqlAssignment}; // Adjusted path
use crate::core::types::{DataType, SimpleMap};
use crate::core::storage::engine::InMemoryKvStore;
use std::path::PathBuf;
// use crate::core::common::serialization::serialize_data_type; // Removed
// use crate::core::transaction::transaction::UndoOperation; // Removed


// Helper to create a default QueryExecutor for tests
fn create_test_executor() -> QueryExecutor<InMemoryKvStore> {
    let store = InMemoryKvStore::new();
    let index_path = PathBuf::from("test_indexes_executor_select"); // Unique path for tests
    // Clean up any old index files before test, if necessary (not strictly needed for InMemory)
    // std::fs::remove_dir_all(&index_path).ok();
    QueryExecutor::new(store, index_path).unwrap()
}

// Helper to create sample DataType::Map for testing SELECT
fn create_sample_map_data(name: &str, age: i64, city: &str, active: bool) -> (Key, DataType) {
    let mut map = SimpleMap::new();
    map.insert("name".as_bytes().to_vec(), DataType::String(name.to_string()));
    map.insert("age".as_bytes().to_vec(), DataType::Integer(age));
    map.insert("city".as_bytes().to_vec(), DataType::String(city.to_string()));
    map.insert("is_active".as_bytes().to_vec(), DataType::Boolean(active));
    // Use name as key for simplicity in these tests
    (name.as_bytes().to_vec(), DataType::Map(map))
}

// Mocked version of execute_select for testing the logic without store iteration
// This now needs to refer to the compare_data_types from utils
use crate::core::query::executor::utils::compare_data_types; // Adjusted path

fn execute_select_logic(
    columns: SelectColumnSpec,
    condition: Option<SqlCondition>,
    all_data: Vec<(Key, DataType)> // Provide data directly
) -> Result<ExecutionResult, crate::core::common::error::DbError> { // Added DbError namespace
    let mut results: Vec<DataType> = Vec::new();

    for (_key, data_value) in all_data {
        let mut matches_condition = true;
        if let Some(ref cond) = condition {
            matches_condition = false;
            if let DataType::Map(ref map_value) = data_value {
                if let Some(field_value_from_map) = map_value.get(&cond.column.as_bytes().to_vec()) {
                    match compare_data_types(field_value_from_map, &cond.value, &cond.operator) {
                        Ok(cmp_result) => matches_condition = cmp_result,
                        Err(e) => { eprintln!("Condition comparison error: {}", e); }
                    }
                }
            }
        }

        if matches_condition {
            match columns {
                SelectColumnSpec::All => {
                    results.push(data_value.clone());
                }
                SelectColumnSpec::Specific(ref selected_columns) => {
                    if let DataType::Map(ref map_value) = data_value {
                        let mut selected_map = SimpleMap::new();
                        for col_name in selected_columns {
                            if let Some(val) = map_value.get(&col_name.as_bytes().to_vec()) {
                                selected_map.insert(col_name.as_bytes().to_vec(), val.clone());
                            }
                        }
                        if !selected_map.is_empty() || selected_columns.is_empty() {
                            results.push(DataType::Map(selected_map));
                        } else if selected_columns.iter().any(|c| map_value.contains_key(&c.as_bytes().to_vec())) {
                            results.push(DataType::Map(selected_map));
                        }
                    }
                }
            }
        }
    }
    Ok(ExecutionResult::Values(results))
}


#[test]
fn test_select_all_no_condition() {
    let data = vec![
        create_sample_map_data("Alice", 30, "New York", true),
        create_sample_map_data("Bob", 24, "London", false),
    ];
    let expected_results = data.iter().map(|(_, val)| val.clone()).collect::<Vec<_>>();

    let result = execute_select_logic(SelectColumnSpec::All, None, data).unwrap();
    match result {
        ExecutionResult::Values(res_data) => assert_eq!(res_data, expected_results),
        _ => panic!("Expected Values result"),
    }
}

#[test]
fn test_select_all_with_matching_condition() {
    let data = vec![
        create_sample_map_data("Alice", 30, "New York", true),
        create_sample_map_data("Bob", 24, "London", false),
        create_sample_map_data("Carol", 30, "Paris", true),
    ];
    let condition = Some(SqlCondition {
        column: "age".to_string(),
        operator: "=".to_string(),
        value: DataType::Integer(30),
    });
    let expected_data = vec![data[0].1.clone(), data[2].1.clone()];

    let result = execute_select_logic(SelectColumnSpec::All, condition, data).unwrap();
    match result {
        ExecutionResult::Values(res_data) => assert_eq!(res_data, expected_data),
        _ => panic!("Expected Values result"),
    }
}

#[test]
fn test_select_all_with_string_condition() {
    let data = vec![
        create_sample_map_data("Alice", 30, "New York", true),
        create_sample_map_data("Bob", 24, "London", false),
    ];
    let condition = Some(SqlCondition {
        column: "city".to_string(),
        operator: "=".to_string(),
        value: DataType::String("London".to_string()),
    });
    let expected_data = vec![data[1].1.clone()];

    let result = execute_select_logic(SelectColumnSpec::All, condition, data).unwrap();
     match result {
        ExecutionResult::Values(res_data) => assert_eq!(res_data, expected_data),
        _ => panic!("Expected Values result"),
    }
}


#[test]
fn test_select_all_with_non_matching_condition() {
    let data = vec![
        create_sample_map_data("Alice", 30, "New York", true),
        create_sample_map_data("Bob", 24, "London", false),
    ];
    let condition = Some(SqlCondition {
        column: "age".to_string(),
        operator: "=".to_string(),
        value: DataType::Integer(100),
    });

    let result = execute_select_logic(SelectColumnSpec::All, condition, data).unwrap();
    match result {
        ExecutionResult::Values(res_data) => assert!(res_data.is_empty()),
        _ => panic!("Expected empty Values result"),
    }
}

#[test]
fn test_select_specific_cols_no_condition() {
    let data = vec![
        create_sample_map_data("Alice", 30, "New York", true),
        create_sample_map_data("Bob", 24, "London", false),
    ];
    let columns = SelectColumnSpec::Specific(vec!["name".to_string(), "city".to_string()]);

    let mut expected_map1 = SimpleMap::new();
    expected_map1.insert("name".as_bytes().to_vec(), DataType::String("Alice".to_string()));
    expected_map1.insert("city".as_bytes().to_vec(), DataType::String("New York".to_string()));

    let mut expected_map2 = SimpleMap::new();
    expected_map2.insert("name".as_bytes().to_vec(), DataType::String("Bob".to_string()));
    expected_map2.insert("city".as_bytes().to_vec(), DataType::String("London".to_string()));

    let expected_data = vec![DataType::Map(expected_map1), DataType::Map(expected_map2)];

    let result = execute_select_logic(columns, None, data).unwrap();
    match result {
        ExecutionResult::Values(res_data) => assert_eq!(res_data, expected_data),
        _ => panic!("Expected Values result"),
    }
}

#[test]
fn test_select_specific_cols_with_matching_condition() {
    let data = vec![
        create_sample_map_data("Alice", 30, "New York", true),
        create_sample_map_data("Bob", 24, "London", false),
        create_sample_map_data("Carol", 30, "Paris", true),
    ];
    let columns = SelectColumnSpec::Specific(vec!["name".to_string(), "is_active".to_string()]);
    let condition = Some(SqlCondition {
        column: "city".to_string(),
        operator: "=".to_string(),
        value: DataType::String("Paris".to_string()),
    });

    let mut expected_map_carol = SimpleMap::new();
    expected_map_carol.insert("name".as_bytes().to_vec(), DataType::String("Carol".to_string()));
    expected_map_carol.insert("is_active".as_bytes().to_vec(), DataType::Boolean(true));
    let expected_data = vec![DataType::Map(expected_map_carol)];

    let result = execute_select_logic(columns, condition, data).unwrap();
    match result {
        ExecutionResult::Values(res_data) => assert_eq!(res_data, expected_data),
        _ => panic!("Expected Values result"),
    }
}

#[test]
fn test_select_specific_col_missing_in_some_rows() {
        let (key_alice, data_alice) = create_sample_map_data("Alice", 30, "New York", true);

    let mut map_bob_incomplete = SimpleMap::new();
    map_bob_incomplete.insert("name".as_bytes().to_vec(), DataType::String("Bob".to_string()));
    map_bob_incomplete.insert("age".as_bytes().to_vec(), DataType::Integer(24));
    map_bob_incomplete.insert("is_active".as_bytes().to_vec(), DataType::Boolean(false));
        let data_bob = DataType::Map(map_bob_incomplete);
        let key_bob = b"bob_key".to_vec(); // Dummy key for Bob

        let data = vec![(key_alice, data_alice), (key_bob, data_bob)]; // Changed to Vec<(Key, DataType)>
    let columns = SelectColumnSpec::Specific(vec!["name".to_string(), "city".to_string()]);

    let mut expected_map_alice = SimpleMap::new();
    expected_map_alice.insert("name".as_bytes().to_vec(), DataType::String("Alice".to_string()));
    expected_map_alice.insert("city".as_bytes().to_vec(), DataType::String("New York".to_string()));

    let mut expected_map_bob = SimpleMap::new();
    expected_map_bob.insert("name".as_bytes().to_vec(), DataType::String("Bob".to_string()));

    let expected_data = vec![DataType::Map(expected_map_alice), DataType::Map(expected_map_bob)];

    let result = execute_select_logic(columns, None, data).unwrap();
    match result {
        ExecutionResult::Values(res_data) => assert_eq!(res_data, expected_data),
        _ => panic!("Expected Values result"),
    }
}

#[test]
fn test_select_with_greater_than_condition() {
    let data = vec![
        create_sample_map_data("Alice", 30, "New York", true),
        create_sample_map_data("Bob", 24, "London", false),
        create_sample_map_data("Carol", 35, "Paris", true),
    ];
    let condition = Some(SqlCondition {
        column: "age".to_string(),
        operator: ">".to_string(),
        value: DataType::Integer(25),
    });
    let expected_data = vec![data[0].1.clone(), data[2].1.clone()];

    let result = execute_select_logic(SelectColumnSpec::All, condition, data).unwrap();
    match result {
        ExecutionResult::Values(res_data) => assert_eq!(res_data, expected_data),
        _ => panic!("Expected Values result"),
    }
}

#[test]
fn test_select_condition_on_boolean() {
    let data = vec![
        create_sample_map_data("Alice", 30, "New York", true),
        create_sample_map_data("Bob", 24, "London", false),
        create_sample_map_data("Carol", 35, "Paris", true),
    ];
    let condition = Some(SqlCondition {
        column: "is_active".to_string(),
        operator: "=".to_string(),
        value: DataType::Boolean(true),
    });
    let expected_data = vec![data[0].1.clone(), data[2].1.clone()];

    let result = execute_select_logic(SelectColumnSpec::All, condition, data).unwrap();
    match result {
        ExecutionResult::Values(res_data) => assert_eq!(res_data, expected_data),
        _ => panic!("Expected Values result"),
    }
}

#[test]
fn test_executor_select_all_no_condition_empty_store() {
    let mut executor = create_test_executor();
    let command = Command::Select {
        columns: SelectColumnSpec::All,
        source: "any_table".to_string(),
        condition: None,
    };
    let result = executor.execute_command(command).unwrap();
    match result {
        ExecutionResult::Values(data) => assert!(data.is_empty()),
        _ => panic!("Expected empty Values result from actual executor due to no store iteration"),
    }
}

// --- Tests for UPDATE command ---
use crate::core::common::error::DbError; // Ensure DbError is in scope for apply_update_logic_to_item

fn apply_update_logic_to_item(
    initial_data: &DataType,
    assignments: &[SqlAssignment],
    condition: Option<&SqlCondition>,
) -> Result<Option<DataType>, DbError> {
    let mut current_data = initial_data.clone();

    let mut matches_where = true;
    if let Some(ref cond) = condition {
        matches_where = false;
        if let DataType::Map(ref map_value) = current_data {
            if let Some(field_value_from_map) = map_value.get(&cond.column.as_bytes().to_vec()) {
                match compare_data_types(field_value_from_map, &cond.value, &cond.operator) {
                    Ok(cmp_result) => matches_where = cmp_result,
                    Err(e) => { eprintln!("Update test: Condition comparison error: {}", e); return Err(e); }
                }
            }
        }
    }

    if !matches_where {
        return Ok(None);
    }

    if let DataType::Map(ref mut map_data) = current_data {
        for assignment in assignments {
            map_data.insert(assignment.column.as_bytes().to_vec(), assignment.value.clone());
        }
    } else {
        if !assignments.is_empty() {
            return Err(DbError::UnsupportedOperation(
                "Cannot apply field assignments to non-Map DataType".to_string(),
            ));
        }
    }
    Ok(Some(current_data))
}

#[test]
fn test_update_apply_assignments_no_condition() {
    let (_key, initial_data) = create_sample_map_data("Alice", 30, "New York", true);
    let assignments = vec![
        SqlAssignment { column: "age".to_string(), value: DataType::Integer(31) },
        SqlAssignment { column: "city".to_string(), value: DataType::String("Boston".to_string()) },
    ];

    let updated_data_opt = apply_update_logic_to_item(&initial_data, &assignments, None).unwrap();
    assert!(updated_data_opt.is_some());
    let updated_data = updated_data_opt.unwrap();

    if let DataType::Map(map) = updated_data {
        assert_eq!(map.get("name".as_bytes()), Some(&DataType::String("Alice".to_string())));
        assert_eq!(map.get("age".as_bytes()), Some(&DataType::Integer(31)));
        assert_eq!(map.get("city".as_bytes()), Some(&DataType::String("Boston".to_string())));
        assert_eq!(map.get("is_active".as_bytes()), Some(&DataType::Boolean(true)));
    } else {
        panic!("Expected updated data to be a Map");
    }
}

#[test]
fn test_update_condition_met_applies_assignments() {
    let (_key, initial_data) = create_sample_map_data("Bob", 24, "London", false);
    let assignments = vec![
        SqlAssignment { column: "is_active".to_string(), value: DataType::Boolean(true) },
    ];
        let condition_opt = Some(SqlCondition { // Renamed for clarity
        column: "name".to_string(),
        operator: "=".to_string(),
        value: DataType::String("Bob".to_string()),
    });

        let updated_data_opt = apply_update_logic_to_item(&initial_data, &assignments, condition_opt.as_ref()).unwrap();
    assert!(updated_data_opt.is_some());
    if let DataType::Map(map) = updated_data_opt.unwrap() {
        assert_eq!(map.get("is_active".as_bytes()), Some(&DataType::Boolean(true)));
        assert_eq!(map.get("age".as_bytes()), Some(&DataType::Integer(24)));
    } else {
        panic!("Expected updated data to be a Map");
    }
}

#[test]
fn test_update_condition_not_met_no_change() {
    let (_key, initial_data) = create_sample_map_data("Carol", 35, "Paris", true);
    let assignments = vec![
        SqlAssignment { column: "age".to_string(), value: DataType::Integer(36) },
    ];
        let condition_opt = Some(SqlCondition { // Renamed for clarity
        column: "city".to_string(),
        operator: "=".to_string(),
        value: DataType::String("London".to_string()),
    });

        let updated_data_opt = apply_update_logic_to_item(&initial_data, &assignments, condition_opt.as_ref()).unwrap();
    assert!(updated_data_opt.is_none());
}

#[test]
fn test_update_on_non_map_type_fails_with_assignments() {
    let initial_data = DataType::String("just a string".to_string());
    let assignments = vec![
        SqlAssignment { column: "any".to_string(), value: DataType::Integer(1) },
    ];

    let result = apply_update_logic_to_item(&initial_data, &assignments, None);
    assert!(matches!(result, Err(DbError::UnsupportedOperation(_))));
}

#[test]
fn test_update_on_non_map_type_no_assignments_no_condition() {
    let initial_data = DataType::String("just a string".to_string());
    let assignments = vec![];

    let updated_data_opt = apply_update_logic_to_item(&initial_data, &assignments, None).unwrap();
    assert!(updated_data_opt.is_some());
    assert_eq!(updated_data_opt.unwrap(), initial_data);
}

#[test]
fn test_executor_update_empty_keys_to_update() {
    let mut executor = create_test_executor();
    let command = Command::Update {
        source: "any_table".to_string(),
        assignments: vec![SqlAssignment { column: "foo".to_string(), value: DataType::String("bar".to_string())}],
        condition: None,
    };
    let result = executor.execute_command(command).unwrap();
    assert_eq!(result, ExecutionResult::Success);
}

// Example sketch for inactive test:
// #[test]
// fn test_update_within_transaction_adds_to_undo_log() {
//     let mut executor = create_test_executor();
//     executor.execute_command(Command::BeginTransaction).unwrap();
//
//     let test_key = "test_update_key".as_bytes().to_vec();
//     let initial_map_data = create_sample_map_data("Test", 40, "TestCity", true).1;
//     let serialized_initial = serialize_data_type(&initial_map_data).unwrap();
//
//     let assignments = vec![SqlAssignment { column: "age".to_string(), value: DataType::Integer(41) }];
//     let update_command = Command::Update {
//         source: "test_table".to_string(),
//         assignments,
//         condition: None,
//     };
//     // executor.execute_command(update_command).unwrap();
//
//     // let active_tx = executor.transaction_manager.get_active_transaction().unwrap();
//     // assert!(!active_tx.undo_log.is_empty());
//     // if let Some(UndoOperation::RevertUpdate { key, old_value }) = active_tx.undo_log.last() {
//     //     assert_eq!(key, &test_key);
//     //     assert_eq!(*old_value, serialized_initial);
//     // } else {
//     //     panic!("Expected RevertUpdate in undo log");
//     // }
// }
