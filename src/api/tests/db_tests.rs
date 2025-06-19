use crate::api::Oxidb;
use crate::core::common::OxidbError;
use crate::core::query::commands::Command; // Added import for Command
use crate::core::query::executor::ExecutionResult;
use crate::core::types::{DataType, JsonSafeMap}; // Import JsonSafeMap
use std::path::{Path, PathBuf};
use tempfile::NamedTempFile;

// Helper function to create a NamedTempFile and return its path for tests
// This avoids repeating NamedTempFile::new().expect("...").path()
fn get_temp_db_path() -> PathBuf {
    NamedTempFile::new().expect("Failed to create temp file for db path").path().to_path_buf()
}

#[test]
fn test_oxidb_insert_and_get() {
    let db_path = get_temp_db_path();
    let mut db = Oxidb::new(&db_path).expect("Failed to create Oxidb instance for insert_and_get test");

    let key = b"api_key_1".to_vec();
    let value_str = "api_value_1".to_string();

    // Test insert (now takes String)
    let insert_result = db.insert(key.clone(), value_str.clone());
    assert!(insert_result.is_ok());

    // Test get (now returns Option<String>)
    let get_result = db.get(key.clone());
    assert!(get_result.is_ok(), "get operation failed");
    assert_eq!(get_result.expect("get_result was Err"), Some(value_str));
}

#[test]
fn test_oxidb_get_non_existent() {
    let db_path = get_temp_db_path();
    let mut db = Oxidb::new(&db_path).expect("Failed to create Oxidb instance for get_non_existent test");
    let key = b"api_non_existent".to_vec();
    let get_result = db.get(key);
    assert!(get_result.is_ok(), "get_non_existent operation failed");
    assert_eq!(get_result.expect("get_result for non_existent was Err"), None); // Stays None
}

#[test]
fn test_oxidb_delete() {
    let db_path = get_temp_db_path();
    let mut db = Oxidb::new(&db_path).expect("Failed to create Oxidb instance for delete test");
    let key = b"api_delete_key".to_vec();
    let value_str = "api_delete_value".to_string();

    db.insert(key.clone(), value_str.clone()).expect("Insert failed before delete test");
    let get_inserted_result = db.get(key.clone());
    assert!(get_inserted_result.is_ok(), "Get after insert failed in delete test");
    assert_eq!(get_inserted_result.expect("get_inserted_result was Err"), Some(value_str)); // Verify insert

    let delete_result = db.delete(key.clone());
    assert!(delete_result.is_ok(), "Delete operation failed");
    assert!(delete_result.expect("delete_result was Err")); // Key existed and was deleted

    let get_deleted_result = db.get(key.clone());
    assert!(get_deleted_result.is_ok(), "Get after delete failed");
    assert_eq!(get_deleted_result.expect("get_deleted_result was Err"), None); // Verify deleted
}

#[test]
fn test_oxidb_delete_non_existent() {
    let db_path = get_temp_db_path();
    let mut db = Oxidb::new(&db_path).expect("Failed to create Oxidb instance for delete_non_existent test");
    let key = b"api_delete_non_existent".to_vec();

    let delete_result = db.delete(key.clone());
    assert!(delete_result.is_ok(), "Delete non_existent operation failed");
    assert!(!delete_result.expect("delete_result for non_existent was Err")); // Key did not exist
}

#[test]
fn test_oxidb_update() {
    let db_path = get_temp_db_path();
    let mut db = Oxidb::new(&db_path).expect("Failed to create Oxidb instance for update test");
    let key = b"api_update_key".to_vec();
    let value1_str = "value1".to_string();
    let value2_str = "value2".to_string();

    db.insert(key.clone(), value1_str.clone()).expect("First insert failed in update test");
    let get_v1_result = db.get(key.clone());
    assert!(get_v1_result.is_ok(), "Get v1 failed in update test");
    assert_eq!(get_v1_result.expect("get_v1_result was Err"), Some(value1_str));

    db.insert(key.clone(), value2_str.clone()).expect("Second insert (update) failed in update test"); // This is an update
    let get_v2_result = db.get(key.clone());
    assert!(get_v2_result.is_ok(), "Get v2 failed in update test");
    assert_eq!(get_v2_result.expect("get_v2_result was Err"), Some(value2_str));
}

// Helper function to derive WAL path from DB path for testing
fn derive_wal_path_for_test(db_path: &Path) -> PathBuf {
    let mut wal_path = db_path.to_path_buf();
    let original_extension = wal_path.extension().map(|s| s.to_os_string());
    if let Some(ext) = original_extension {
        let mut new_ext = ext;
        new_ext.push(".wal");
        wal_path.set_extension(new_ext);
    } else {
        wal_path.set_extension("wal");
    }
    wal_path
}

#[test]
fn test_oxidb_persist_method() {
    let db_path = get_temp_db_path();
    let wal_path = derive_wal_path_for_test(&db_path);

    let key = b"persist_key".to_vec();
    let value_str = "persist_value".to_string(); // Changed to String

    {
        let mut db = Oxidb::new(&db_path).expect("Failed to create Oxidb instance for persist test (first instance)");
        db.insert(key.clone(), value_str.clone()).expect("Insert failed in persist test"); // Use String value
                                                            // Data is in WAL and cache. Main file might be empty or have old data.
                                                            // WAL file should exist if inserts happened.
                                                            // (This check depends on SimpleFileKvStore's WAL behavior after insert)
                                                            // For this test, we assume WAL is written to on put.
                                                            // Replace direct store access with API usage:
        if db.get(key.clone()).expect("Get failed during WAL check in persist test").is_some() {
            assert!(wal_path.exists(), "WAL file should exist after insert before persist.");
        }

        let persist_result = db.persist();
        assert!(persist_result.is_ok(), "Persist operation failed");

        // After persist, WAL should be cleared, and data should be in the main file.
        assert!(!wal_path.exists(), "WAL file should not exist after persist.");
    }

    // Re-load the database
    let mut reloaded_db = Oxidb::new(&db_path).expect("Failed to create Oxidb instance for persist test (reloaded)");
    let get_result = reloaded_db.get(key.clone());
    assert!(get_result.is_ok(), "Get after reload failed in persist test");
    assert_eq!(get_result.expect("get_result after reload was Err"), Some(value_str)); // Assert against String value
}

// Tests for execute_query_str
#[test]
fn test_execute_query_str_get_ok() {
    let db_path = get_temp_db_path();
    let mut db = Oxidb::new(&db_path).expect("Failed to create Oxidb for get_ok test");
    // Insert using the API's insert which now takes String and stores as DataType::String
    db.insert(b"mykey".to_vec(), "myvalue".to_string()).expect("Insert failed in get_ok test");

    let result = db.execute_query_str("GET mykey");
    match result {
        // Expecting DataType::String from the executor
        Ok(ExecutionResult::Value(Some(DataType::String(val_str)))) => {
            assert_eq!(val_str, "myvalue")
        }
        _ => panic!("Expected Value(Some(DataType::String(...))), got {:?}", result),
    }
}

#[test]
fn test_oxidb_find_by_index() {
    let db_path = get_temp_db_path();
    let mut db = Oxidb::new(&db_path).expect("Failed to create Oxidb instance for find_by_index test");

    let key1 = b"idx_key1".to_vec();
    let val1 = "indexed_value_common".to_string();
    let key2 = b"idx_key2".to_vec();
    let val2 = "indexed_value_common".to_string(); // Same value as key1
    let key3 = b"idx_key3".to_vec();
    let val3 = "another_value".to_string();

    db.insert(key1.clone(), val1.clone()).expect("Insert 1 failed");
    db.insert(key2.clone(), val2.clone()).expect("Insert 2 failed");
    db.insert(key3.clone(), val3.clone()).expect("Insert 3 failed");

    // Find by the common value
    let find_result = db.find_by_index("default_value_index".to_string(), DataType::String(val1.clone()));
    assert!(find_result.is_ok(), "find_by_index failed: {:?}", find_result.err());

    match find_result.unwrap() {
        Some(mut values_vec) => {
            // The values returned are the full DataType::String values of the records found
            assert_eq!(values_vec.len(), 2, "Expected two records for the common indexed value");
            // Sort for consistent comparison as order is not guaranteed
            values_vec.sort_by(|a, b| format!("{:?}", a).cmp(&format!("{:?}", b)));
            assert_eq!(values_vec[0], DataType::String(val1.clone()));
            assert_eq!(values_vec[1], DataType::String(val2.clone())); // val1 and val2 are identical strings
        }
        None => panic!("Expected Some(Vec<DataType>), got None for common value"),
    }

    // Find by a unique value
    let find_result_unique = db.find_by_index("default_value_index".to_string(), DataType::String(val3.clone()));
    assert!(find_result_unique.is_ok(), "find_by_index for unique value failed: {:?}", find_result_unique.err());
    match find_result_unique.unwrap() {
        Some(values_vec) => {
            assert_eq!(values_vec.len(), 1, "Expected one record for the unique indexed value");
            assert_eq!(values_vec[0], DataType::String(val3.clone()));
        }
        None => panic!("Expected Some(Vec<DataType>), got None for unique value"),
    }

    // Find by a non-existent value
    let find_result_none = db.find_by_index("default_value_index".to_string(), DataType::String("non_existent_value".to_string()));
    assert!(find_result_none.is_ok(), "find_by_index for non-existent value failed: {:?}", find_result_none.err());
    assert!(find_result_none.unwrap().is_none(), "Expected None for non-existent value");

    // Find on non-existent index
    let find_result_no_index = db.find_by_index("wrong_index_name".to_string(), DataType::String(val1.clone()));
    assert!(find_result_no_index.is_err(), "Expected error for non-existent index");
    match find_result_no_index.err().unwrap() {
        OxidbError::Index(msg) => assert!(msg.contains("not found for find operation")),
        other_err => panic!("Expected OxidbError::Index, got {:?}", other_err),
    }
}

#[test]
fn test_execute_query_str_get_not_found() {
    let db_path = get_temp_db_path();
    let mut db = Oxidb::new(&db_path).expect("Failed to create Oxidb for get_not_found test");
    let result = db.execute_query_str("GET nonkey");
    match result {
        Ok(ExecutionResult::Value(None)) => {} // Expected
        _ => panic!("Expected Value(None), got {:?}", result),
    }
}

#[test]
fn test_execute_query_str_insert_ok() {
    let db_path = get_temp_db_path();
    let mut db = Oxidb::new(&db_path).expect("Failed to create Oxidb for insert_ok test");
    // The parser will turn "newvalue" into DataType::String("newvalue")
    let result = db.execute_query_str("INSERT newkey newvalue");
    match result {
        Ok(ExecutionResult::Success) => {} // Expected
        _ => panic!("Expected Success, got {:?}", result),
    }
    // db.get now returns Option<String>
    assert_eq!(db.get(b"newkey".to_vec()).expect("Get failed after insert_ok"), Some("newvalue".to_string()));
}

#[test]
fn test_execute_query_str_insert_with_quotes_ok() {
    let db_path = get_temp_db_path();
    let mut db = Oxidb::new(&db_path).expect("Failed to create Oxidb for insert_with_quotes test");
    // Parser turns "\"quoted value\"" into DataType::String("quoted value")
    let result = db.execute_query_str("INSERT qkey \"quoted value\"");
    match result {
        Ok(ExecutionResult::Success) => {} // Expected
        _ => panic!("Expected Success, got {:?}", result),
    }
    assert_eq!(db.get(b"qkey".to_vec()).expect("Get failed after insert_with_quotes"), Some("quoted value".to_string()));
}

#[test]
fn test_execute_query_str_insert_integer_via_parser() {
    let db_path = get_temp_db_path();
    let mut db = Oxidb::new(&db_path).expect("Failed to create Oxidb for insert_integer test");
    // Parser turns "123" into DataType::Integer(123)
    let result = db.execute_query_str("INSERT intkey 123");
    match result {
        Ok(ExecutionResult::Success) => {} // Expected
        _ => panic!("Expected Success, got {:?}", result),
    }
    // db.get now returns Option<String>
    assert_eq!(db.get(b"intkey".to_vec()).expect("Get failed after insert_integer"), Some("123".to_string()));
}

#[test]
fn test_execute_query_str_insert_boolean_via_parser() {
    let db_path = get_temp_db_path();
    let mut db = Oxidb::new(&db_path).expect("Failed to create Oxidb for insert_boolean test");
    // Parser turns "true" into DataType::Boolean(true)
    let result = db.execute_query_str("INSERT boolkey true");
    match result {
        Ok(ExecutionResult::Success) => {} // Expected
        _ => panic!("Expected Success, got {:?}", result),
    }
    assert_eq!(db.get(b"boolkey".to_vec()).expect("Get failed after insert_boolean"), Some("true".to_string()));
}

#[test]
fn test_execute_query_str_delete_ok() {
    let db_path = get_temp_db_path();
    let mut db = Oxidb::new(&db_path).expect("Failed to create Oxidb for delete_ok test");
    db.insert(b"delkey".to_vec(), "delvalue".to_string()).expect("Insert failed in delete_ok test"); // Use String for insert
    let result = db.execute_query_str("DELETE delkey");
    match result {
        Ok(ExecutionResult::Deleted(true)) => {} // Expected
        _ => panic!("Expected Deleted(true), got {:?}", result),
    }
    assert_eq!(db.get(b"delkey".to_vec()).expect("Get after delete_ok failed"), None);
}

#[test]
fn test_execute_query_str_delete_not_found() {
    let db_path = get_temp_db_path();
    let mut db = Oxidb::new(&db_path).expect("Failed to create Oxidb for delete_not_found test");
    let result = db.execute_query_str("DELETE nonkey");
    match result {
        Ok(ExecutionResult::Deleted(false)) => {} // Expected
        _ => panic!("Expected Deleted(false), got {:?}", result),
    }
}

#[test]
fn test_execute_query_str_parse_error() {
    let db_path = get_temp_db_path();
    let mut db = Oxidb::new(&db_path).expect("Failed to create Oxidb for parse_error test");
    let result = db.execute_query_str("GARBAGE COMMAND");
    match result {
        Err(OxidbError::SqlParsing(_)) => {} // Expected, changed from InvalidQuery to SqlParsing
        _ => panic!("Expected OxidbError::SqlParsing, got {:?}", result),
    }
}

#[test]
fn test_execute_query_str_empty_query() {
    let db_path = get_temp_db_path();
    let mut db = Oxidb::new(&db_path).expect("Failed to create Oxidb for empty_query test");
    let result = db.execute_query_str("");
    match result {
        Err(OxidbError::SqlParsing(msg)) => assert_eq!(msg, "Input query string cannot be empty."), // Changed
        _ => panic!("Expected OxidbError::SqlParsing for empty string, got {:?}", result),
    }
}

#[test]
fn test_execute_query_str_update_ok() {
    let db_path = get_temp_db_path();
    let mut db = Oxidb::new(&db_path).expect("Failed to create Oxidb for update_ok test");

    let key_alice = b"users_alice".to_vec();
    let val_alice_map_initial = DataType::Map(JsonSafeMap(vec![ // Use JsonSafeMap
        (b"name".to_vec(), DataType::String("Alice".to_string())),
        (b"city".to_vec(), DataType::String("London".to_string())),
        (b"country".to_vec(), DataType::String("UK".to_string())),
    ].into_iter().collect()));
    // Insert initial data directly using executor to ensure it's a map
    db.executor.execute_command(Command::Insert { key: key_alice.clone(), value: val_alice_map_initial.clone() }).unwrap();

    // Perform the update
    let update_query = "UPDATE users SET city = 'New York' WHERE name = 'Alice'";
    let update_result = db.execute_query_str(update_query);
    match update_result {
        Ok(ExecutionResult::Updated { count }) => assert_eq!(count, 1, "UPDATE: Expected 1 row updated"),
        _ => panic!("UPDATE: Expected Updated {{ count: 1 }}, got {:?}", update_result),
    }

    // Get the updated data using execute_query_str to see the raw ExecutionResult
    let get_alice_result_from_executor = db.execute_query_str("GET users_alice");

    match get_alice_result_from_executor {
        Ok(ExecutionResult::Value(Some(ref data_type_val))) => {
            let mut file_debug_content = format!("Retrieved DataType: {:?}\n", data_type_val);

            // Simulate the string conversion that Oxidb::get() would do on data_type_val
            let simulated_api_string_result = match data_type_val {
                DataType::Map(wrapped_map) => serde_json::to_string(wrapped_map), // wrapped_map is JsonSafeMap
                DataType::String(s) => Ok(s.clone()),
                DataType::Integer(i) => Ok(i.to_string()),
                DataType::Boolean(b) => Ok(b.to_string()),
                DataType::Float(f) => Ok(f.to_string()),
                DataType::Null => Ok("NULL".to_string()),
                // The following Boolean, Float, and Null are unreachable and have been removed.
                DataType::JsonBlob(json_val) => serde_json::to_string(json_val),
            };

            match simulated_api_string_result {
                Ok(s) => {
                    file_debug_content.push_str(&format!("Simulated API string: {}\n", s));
                    eprintln!("RAW DATA STRING (SIMULATED FROM Oxidb::get): {}", s);
                    // The test will panic here if s is not the expected JSON for a map,
                    // when it tries to deserialize it into serde_json::Value or HashMap.
                    // This happens if the `expect` below is uncommented and used.
                    // For now, we primarily want to see the string s.
                }
                Err(e) => {
                    file_debug_content.push_str(&format!("Error simulating API string: {}\n", e));
                    eprintln!("ERROR DURING SIMULATED STRING CONVERSION: {:?}", e);
                }
            }

            use std::io::Write;
            let path = "/tmp/debug_output.txt";
            match std::fs::File::create(path) {
                Ok(mut file) => {
                    if let Err(e) = file.write_all(file_debug_content.as_bytes()) {
                        eprintln!("Failed to write debug data to file: {}", e);
                    } else {
                        eprintln!("Problematic data written to: {}", path);
                    }
                },
                Err(e) => {
                    eprintln!("Failed to create debug file: {}", e);
                }
            }

            // Now, proceed with assertions based on the known data_type_val
            if let DataType::Map(JsonSafeMap(map_val)) = data_type_val {
                 println!("[Test] Retrieved DataType::Map for assertions: {:?}", map_val);
                 assert_eq!(map_val.get(b"city".as_ref()), Some(&DataType::String("New York".to_string())));
                 assert_eq!(map_val.get(b"country".as_ref()), Some(&DataType::String("UK".to_string())));
            } else {
                // This panic will now include the actual data_type_val if it's not a Map
                panic!("[Test] Expected DataType::Map for key_alice, but got: {:?}", data_type_val);
            }
        }
        Ok(ExecutionResult::Value(None)) => {
             eprintln!("Problematic data written to: /tmp/debug_output.txt (GET for key_alice Value was None)");
             std::fs::write("/tmp/debug_output.txt", "ExecutionResult::Value(None) for key_alice").expect("Failed to write None case");
             panic!("[Test] Expected Value(Some(_)) for key_alice, got Value(None)");
        }
        Err(e) => { // Err from db.execute_query_str("GET users_alice")
            eprintln!("Problematic data written to: /tmp/debug_output.txt (GET for key_alice failed)");
            std::fs::write("/tmp/debug_output.txt", format!("GET for key_alice failed: {:?}", e)).expect("Failed to write error case");
            panic!("[Test] GET query for key_alice failed: {:?}", e); // This is where the original panic happens
        }
        other_exec_res => { // Other Ok(ExecutionResult::Variant)
            eprintln!("Problematic data written to: /tmp/debug_output.txt (Unexpected ExecutionResult for key_alice)");
            std::fs::write("/tmp/debug_output.txt", format!("Unexpected ExecutionResult for key_alice: {:?}", other_exec_res)).expect("Failed to write other case");
            panic!("[Test] Expected ExecutionResult::Value(Some(DataType::Map(...))) for key_alice, got: {:?}", other_exec_res);
        }
    }

    // Test update that affects 0 rows
    let update_query_no_match = "UPDATE users SET city = 'Berlin' WHERE name = 'Unknown'";
    let result_no_match = db.execute_query_str(update_query_no_match);
    match result_no_match {
        Ok(ExecutionResult::Updated { count }) => assert_eq!(count, 0, "UPDATE no match: Expected 0 rows updated"),
        _ => panic!("UPDATE no match: Expected Updated {{ count: 0 }}, got {:?}", result_no_match),
    }
}
