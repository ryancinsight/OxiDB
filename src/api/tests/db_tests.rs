use crate::api::Oxidb;
use crate::core::common::OxidbError;
use crate::core::query::executor::ExecutionResult;
use crate::core::types::DataType;
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
