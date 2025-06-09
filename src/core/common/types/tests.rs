use super::*;
use serde_json;

#[test]
fn test_data_type_serialization() {
    let dt = DataType::Integer;
    let serialized = serde_json::to_string(&dt).unwrap();
    assert_eq!(serialized, "\"Integer\"");
    let deserialized: DataType = serde_json::from_str(&serialized).unwrap();
    assert_eq!(deserialized, dt);

    let dt = DataType::Text;
    let serialized = serde_json::to_string(&dt).unwrap();
    assert_eq!(serialized, "\"Text\"");
    let deserialized: DataType = serde_json::from_str(&serialized).unwrap();
    assert_eq!(deserialized, dt);

    let dt = DataType::Boolean;
    let serialized = serde_json::to_string(&dt).unwrap();
    assert_eq!(serialized, "\"Boolean\"");
    let deserialized: DataType = serde_json::from_str(&serialized).unwrap();
    assert_eq!(deserialized, dt);

    let dt = DataType::Blob;
    let serialized = serde_json::to_string(&dt).unwrap();
    assert_eq!(serialized, "\"Blob\"");
    let deserialized: DataType = serde_json::from_str(&serialized).unwrap();
    assert_eq!(deserialized, dt);

    let dt = DataType::Null;
    let serialized = serde_json::to_string(&dt).unwrap();
    assert_eq!(serialized, "\"Null\"");
    let deserialized: DataType = serde_json::from_str(&serialized).unwrap();
    assert_eq!(deserialized, dt);
}

#[test]
fn test_value_serialization_and_get_type() {
    let v = Value::Integer(100);
    assert_eq!(v.get_type(), DataType::Integer);
    let serialized = serde_json::to_string(&v).unwrap();
    assert_eq!(serialized, "{\"Integer\":100}");
    let deserialized: Value = serde_json::from_str(&serialized).unwrap();
    assert_eq!(deserialized, v);

    let v = Value::Text("hello".to_string());
    assert_eq!(v.get_type(), DataType::Text);
    let serialized = serde_json::to_string(&v).unwrap();
    assert_eq!(serialized, "{\"Text\":\"hello\"}");
    let deserialized: Value = serde_json::from_str(&serialized).unwrap();
    assert_eq!(deserialized, v);

    let v = Value::Boolean(true);
    assert_eq!(v.get_type(), DataType::Boolean);
    let serialized = serde_json::to_string(&v).unwrap();
    assert_eq!(serialized, "{\"Boolean\":true}");
    let deserialized: Value = serde_json::from_str(&serialized).unwrap();
    assert_eq!(deserialized, v);

    let v = Value::Blob(vec![0, 1, 2]);
    assert_eq!(v.get_type(), DataType::Blob);
    let serialized = serde_json::to_string(&v).unwrap();
    assert_eq!(serialized, "{\"Blob\":[0,1,2]}");
    let deserialized: Value = serde_json::from_str(&serialized).unwrap();
    assert_eq!(deserialized, v);

    let v = Value::Null;
    assert_eq!(v.get_type(), DataType::Null);
    let serialized = serde_json::to_string(&v).unwrap();
    assert_eq!(serialized, "\"Null\"");
    let deserialized: Value = serde_json::from_str(&serialized).unwrap();
    assert_eq!(deserialized, v);
}

#[test]
fn test_row_serialization() {
    let row = Row {
        values: vec![Value::Integer(1), Value::Text("test".to_string())],
    };
    let serialized = serde_json::to_string(&row).unwrap();
    assert_eq!(
        serialized,
        "{\"values\":[{\"Integer\":1},{\"Text\":\"test\"}]}"
    );
    let deserialized: Row = serde_json::from_str(&serialized).unwrap();
    assert_eq!(deserialized, row);
}

#[test]
fn test_schema_serialization_and_get_column_index() {
    let schema = Schema {
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Text,
            },
        ],
    };

    assert_eq!(schema.get_column_index("id"), Some(0));
    assert_eq!(schema.get_column_index("name"), Some(1));
    assert_eq!(schema.get_column_index("age"), None);

    let serialized = serde_json::to_string(&schema).unwrap();
    assert_eq!(
        serialized,
        "{\"columns\":[{\"name\":\"id\",\"data_type\":\"Integer\"},{\"name\":\"name\",\"data_type\":\"Text\"}]}"
    );
    let deserialized: Schema = serde_json::from_str(&serialized).unwrap();
    assert_eq!(deserialized, schema);
}

#[test]
fn test_value_partial_ord() {
    assert!(Value::Integer(10) > Value::Integer(5));
    assert!(Value::Text("xyz".to_string()) > Value::Text("abc".to_string()));
    assert!(Value::Boolean(true) > Value::Boolean(false));
    // Note: Comparing different Value variants with PartialOrd might yield None
    // For example, Value::Integer(10) > Value::Text("abc".to_string()) would be false,
    // and Value::Integer(10).partial_cmp(&Value::Text("...")) would be None.
    // This is expected for PartialOrd.
}
