use crate::api::db::Oxidb;
use crate::core::common::error::DbError;
use crate::core::query::executor::ExecutionResult;
use crate::core::types::DataType;
use tempfile::NamedTempFile;
use std::path::{Path, PathBuf};

// Helper function to create a NamedTempFile and return its path for tests
// This avoids repeating NamedTempFile::new().unwrap().path()
fn get_temp_db_path() -> PathBuf {
    NamedTempFile::new().unwrap().path().to_path_buf()
}

#[test]
fn test_oxidb_insert_and_get() {
    let db_path = get_temp_db_path();
    let mut db = Oxidb::new(&db_path).unwrap();

    let key = b"api_key_1".to_vec();
    let value_str = "api_value_1".to_string();

    // Test insert (now takes String)
    let insert_result = db.insert(key.clone(), value_str.clone());
    assert!(insert_result.is_ok());

    // Test get (now returns Option<String>)
    let get_result = db.get(key.clone());
    assert!(get_result.is_ok());
    assert_eq!(get_result.unwrap(), Some(value_str));
}

#[test]
fn test_oxidb_get_non_existent() {
    let db_path = get_temp_db_path();
    let mut db = Oxidb::new(&db_path).unwrap();
    let key = b"api_non_existent".to_vec();
    let get_result = db.get(key);
    assert!(get_result.is_ok());
    assert_eq!(get_result.unwrap(), None); // Stays None
}

#[test]
fn test_oxidb_delete() {
    let db_path = get_temp_db_path();
    let mut db = Oxidb::new(&db_path).unwrap();
    let key = b"api_delete_key".to_vec();
    let value_str = "api_delete_value".to_string();

    db.insert(key.clone(), value_str.clone()).unwrap();
    let get_inserted_result = db.get(key.clone());
    assert!(get_inserted_result.is_ok());
    assert_eq!(get_inserted_result.unwrap(), Some(value_str)); // Verify insert

    let delete_result = db.delete(key.clone());
    assert!(delete_result.is_ok());
    assert_eq!(delete_result.unwrap(), true); // Key existed and was deleted

    let get_deleted_result = db.get(key.clone());
    assert!(get_deleted_result.is_ok());
    assert_eq!(get_deleted_result.unwrap(), None); // Verify deleted
}

#[test]
fn test_oxidb_delete_non_existent() {
    let db_path = get_temp_db_path();
    let mut db = Oxidb::new(&db_path).unwrap();
    let key = b"api_delete_non_existent".to_vec();

    let delete_result = db.delete(key.clone());
    assert!(delete_result.is_ok());
    assert_eq!(delete_result.unwrap(), false); // Key did not exist
}

#[test]
fn test_oxidb_update() {
    let db_path = get_temp_db_path();
    let mut db = Oxidb::new(&db_path).unwrap();
    let key = b"api_update_key".to_vec();
    let value1_str = "value1".to_string();
    let value2_str = "value2".to_string();

    db.insert(key.clone(), value1_str.clone()).unwrap();
    let get_v1_result = db.get(key.clone());
    assert!(get_v1_result.is_ok());
    assert_eq!(get_v1_result.unwrap(), Some(value1_str));

    db.insert(key.clone(), value2_str.clone()).unwrap(); // This is an update
    let get_v2_result = db.get(key.clone());
    assert!(get_v2_result.is_ok());
    assert_eq!(get_v2_result.unwrap(), Some(value2_str));
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
        let mut db = Oxidb::new(&db_path).unwrap();
        db.insert(key.clone(), value_str.clone()).unwrap(); // Use String value
        // Data is in WAL and cache. Main file might be empty or have old data.
        // WAL file should exist if inserts happened.
        // (This check depends on SimpleFileKvStore's WAL behavior after insert)
        // For this test, we assume WAL is written to on put.
        // Replace direct store access with API usage:
        if db.get(key.clone()).unwrap().is_some() {
             assert!(wal_path.exists(), "WAL file should exist after insert before persist.");
        }


        let persist_result = db.persist();
        assert!(persist_result.is_ok());

        // After persist, WAL should be cleared, and data should be in the main file.
        assert!(!wal_path.exists(), "WAL file should not exist after persist.");
    }

    // Re-load the database
    let mut reloaded_db = Oxidb::new(&db_path).unwrap();
    let get_result = reloaded_db.get(key.clone());
    assert!(get_result.is_ok());
    assert_eq!(get_result.unwrap(), Some(value_str)); // Assert against String value
}

// Tests for execute_query_str
#[test]
fn test_execute_query_str_get_ok() {
    let db_path = get_temp_db_path();
    let mut db = Oxidb::new(&db_path).unwrap();
    // Insert using the API's insert which now takes String and stores as DataType::String
    db.insert(b"mykey".to_vec(), "myvalue".to_string()).unwrap();

    let result = db.execute_query_str("GET mykey");
    match result {
        // Expecting DataType::String from the executor
        Ok(ExecutionResult::Value(Some(DataType::String(val_str)))) => assert_eq!(val_str, "myvalue"),
        _ => panic!("Expected Value(Some(DataType::String(...))), got {:?}", result),
    }
}

#[test]
fn test_execute_query_str_get_not_found() {
    let db_path = get_temp_db_path();
    let mut db = Oxidb::new(&db_path).unwrap();
    let result = db.execute_query_str("GET nonkey");
    match result {
        Ok(ExecutionResult::Value(None)) => {} // Expected
        _ => panic!("Expected Value(None), got {:?}", result),
    }
}

#[test]
fn test_execute_query_str_insert_ok() {
    let db_path = get_temp_db_path();
    let mut db = Oxidb::new(&db_path).unwrap();
    // The parser will turn "newvalue" into DataType::String("newvalue")
    let result = db.execute_query_str("INSERT newkey newvalue");
    match result {
        Ok(ExecutionResult::Success) => {} // Expected
        _ => panic!("Expected Success, got {:?}", result),
    }
    // db.get now returns Option<String>
    assert_eq!(db.get(b"newkey".to_vec()).unwrap(), Some("newvalue".to_string()));
}

#[test]
fn test_execute_query_str_insert_with_quotes_ok() {
    let db_path = get_temp_db_path();
    let mut db = Oxidb::new(&db_path).unwrap();
    // Parser turns "\"quoted value\"" into DataType::String("quoted value")
    let result = db.execute_query_str("INSERT qkey \"quoted value\"");
    match result {
        Ok(ExecutionResult::Success) => {} // Expected
        _ => panic!("Expected Success, got {:?}", result),
    }
    assert_eq!(db.get(b"qkey".to_vec()).unwrap(), Some("quoted value".to_string()));
}

#[test]
fn test_execute_query_str_insert_integer_via_parser() {
    let db_path = get_temp_db_path();
    let mut db = Oxidb::new(&db_path).unwrap();
    // Parser turns "123" into DataType::Integer(123)
    let result = db.execute_query_str("INSERT intkey 123");
    match result {
        Ok(ExecutionResult::Success) => {} // Expected
        _ => panic!("Expected Success, got {:?}", result),
    }
    // db.get now returns Option<String>
    assert_eq!(db.get(b"intkey".to_vec()).unwrap(), Some("123".to_string()));
}

#[test]
fn test_execute_query_str_insert_boolean_via_parser() {
    let db_path = get_temp_db_path();
    let mut db = Oxidb::new(&db_path).unwrap();
    // Parser turns "true" into DataType::Boolean(true)
    let result = db.execute_query_str("INSERT boolkey true");
    match result {
        Ok(ExecutionResult::Success) => {} // Expected
        _ => panic!("Expected Success, got {:?}", result),
    }
    assert_eq!(db.get(b"boolkey".to_vec()).unwrap(), Some("true".to_string()));
}


#[test]
fn test_execute_query_str_delete_ok() {
    let db_path = get_temp_db_path();
    let mut db = Oxidb::new(&db_path).unwrap();
    db.insert(b"delkey".to_vec(), "delvalue".to_string()).unwrap(); // Use String for insert
    let result = db.execute_query_str("DELETE delkey");
    match result {
        Ok(ExecutionResult::Deleted(true)) => {} // Expected
        _ => panic!("Expected Deleted(true), got {:?}", result),
    }
    assert_eq!(db.get(b"delkey".to_vec()).unwrap(), None);
}

#[test]
fn test_execute_query_str_delete_not_found() {
    let db_path = get_temp_db_path();
    let mut db = Oxidb::new(&db_path).unwrap();
    let result = db.execute_query_str("DELETE nonkey");
    match result {
        Ok(ExecutionResult::Deleted(false)) => {} // Expected
        _ => panic!("Expected Deleted(false), got {:?}", result),
    }
}

#[test]
fn test_execute_query_str_parse_error() {
    let db_path = get_temp_db_path();
    let mut db = Oxidb::new(&db_path).unwrap();
    let result = db.execute_query_str("GARBAGE COMMAND");
    match result {
        Err(DbError::InvalidQuery(_)) => {} // Expected
        _ => panic!("Expected InvalidQuery, got {:?}", result),
    }
}

#[test]
fn test_execute_query_str_empty_query() {
    let db_path = get_temp_db_path();
    let mut db = Oxidb::new(&db_path).unwrap();
    let result = db.execute_query_str("");
    match result {
        Err(DbError::InvalidQuery(msg)) => assert_eq!(msg, "Input query string cannot be empty."),
        _ => panic!("Expected InvalidQuery for empty string, got {:?}", result),
    }
}
