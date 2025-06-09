use crate::core::common::error::OxidbError;
use std::io;
use std::error::Error; // Import the Error trait

#[test]
fn test_error_display_and_source() {
    // Test Io variant
    let io_err = OxidbError::Io(io::Error::new(io::ErrorKind::NotFound, "file not found"));
    assert_eq!(format!("{}", io_err), "IO Error: file not found");
    assert!(io_err.source().is_some());

    // Test Json variant
    let json_err_str = "{\"a\":"; // Invalid JSON
    let serde_err = serde_json::from_str::<serde_json::Value>(json_err_str).unwrap_err();
    let json_err = OxidbError::Json(serde_err);
    assert!(format!("{}", json_err).contains("JSON Serialization/Deserialization Error"));
    assert!(json_err.source().is_some());

    // Test other variants (source might be None for these)
    let parsing_err = OxidbError::Parsing("syntax error".to_string());
    assert_eq!(format!("{}", parsing_err), "Parsing Error: syntax error");
    assert!(parsing_err.source().is_none());

    let internal_err = OxidbError::Internal("something went wrong".to_string());
    assert_eq!(format!("{}", internal_err), "Internal Error: something went wrong");
    assert!(internal_err.source().is_none());

    let not_found_err = OxidbError::NotFound { key: "my_key".to_string() };
    assert_eq!(format!("{}", not_found_err), "Key not found: my_key");
    assert!(not_found_err.source().is_none());
}

#[test]
fn test_from_std_io_error() {
    let std_io_err = io::Error::new(io::ErrorKind::PermissionDenied, "access denied");
    let oxidb_err: OxidbError = std_io_err.into();
    match oxidb_err {
        OxidbError::Io(e) => assert_eq!(e.kind(), io::ErrorKind::PermissionDenied),
        _ => panic!("Expected OxidbError::Io variant"),
    }
}

#[test]
fn test_from_serde_json_error() {
    let json_err_str = "[1, 2"; // Invalid JSON
    let serde_err = serde_json::from_str::<serde_json::Value>(json_err_str).unwrap_err();
    let original_serde_kind = serde_err.classify(); // Store kind before moving

    let oxidb_err: OxidbError = serde_err.into();
    match oxidb_err {
        OxidbError::Json(e) => {
            assert_eq!(e.classify(), original_serde_kind);
            // Optionally, check the string representation if it's stable enough
            // assert!(e.to_string().contains("expected `]` at line 1 column 5"));
        }
        _ => panic!("Expected OxidbError::Json variant"),
    }
}

// Example of creating other variants
#[test]
fn test_other_error_variants() {
    let _ = OxidbError::Serialization("could not serialize".to_string());
    let _ = OxidbError::Deserialization("could not deserialize".to_string());
    let _ = OxidbError::SqlParsing("invalid SELECT".to_string());
    let _ = OxidbError::Execution("runtime error".to_string());
    let _ = OxidbError::Storage("disk full".to_string());
    let _ = OxidbError::Transaction("abort".to_string());
    let _ = OxidbError::AlreadyExists { name: "table1".to_string() };
    let _ = OxidbError::NotImplemented { feature: "window functions".to_string() };
    let _ = OxidbError::InvalidInput { message: "negative count".to_string() };
    let _ = OxidbError::Index("corrupted index".to_string());
    let _ = OxidbError::Lock("deadlock detected".to_string());
    let _ = OxidbError::NoActiveTransaction;
    let _ = OxidbError::LockConflict { key: vec![1], current_tx: 1, locked_by_tx: Some(2) };
    let _ = OxidbError::LockAcquisitionTimeout { key: vec![2], current_tx: 3 };
    let _ = OxidbError::Configuration("bad timeout value".to_string());
    let _ = OxidbError::Type("type mismatch".to_string());
}
