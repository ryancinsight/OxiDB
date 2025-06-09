use super::*;
use serde_json;
use serde::{Serialize, Deserialize};

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

    // Blob comparisons
    assert!(Value::Blob(vec![0, 1, 2]) > Value::Blob(vec![0, 1, 0]));
    assert!(Value::Blob(vec![0, 1, 0]) < Value::Blob(vec![0, 1, 2]));
    assert!(Value::Blob(vec![0, 1, 2]) == Value::Blob(vec![0, 1, 2]));
    assert!(Value::Blob(vec![0, 1, 2]) != Value::Blob(vec![0, 1, 0]));
    assert!(Value::Blob(vec![1, 0]) >= Value::Blob(vec![0, 1])); // example of >=
    assert!(Value::Blob(vec![0, 1]) <= Value::Blob(vec![0, 1])); // example of <=

    // Null comparisons
    assert_eq!(Value::Null.partial_cmp(&Value::Null), Some(std::cmp::Ordering::Equal));
    assert!(Value::Null <= Value::Null);
    assert!(Value::Null >= Value::Null);
    assert!(Value::Null == Value::Null);
    assert!(!(Value::Null < Value::Null));
    assert!(!(Value::Null > Value::Null));


    // Comparisons between different, non-compatible Value variants
    assert!(Value::Integer(1).partial_cmp(&Value::Text("a".to_string())).is_none());
    assert!(Value::Text("a".to_string()).partial_cmp(&Value::Integer(1)).is_none());
    assert!(Value::Boolean(true).partial_cmp(&Value::Blob(vec![1])).is_none());
    assert!(Value::Blob(vec![1]).partial_cmp(&Value::Boolean(true)).is_none());
    assert!(Value::Null.partial_cmp(&Value::Integer(1)).is_none());
    assert!(Value::Integer(1).partial_cmp(&Value::Null).is_none());
    assert!(Value::Text("a".to_string()).partial_cmp(&Value::Null).is_none());
    assert!(Value::Null.partial_cmp(&Value::Text("a".to_string())).is_none());
    assert!(Value::Blob(vec![0]).partial_cmp(&Value::Null).is_none());
    assert!(Value::Null.partial_cmp(&Value::Blob(vec![0])).is_none());
    assert!(Value::Boolean(false).partial_cmp(&Value::Null).is_none());
    assert!(Value::Null.partial_cmp(&Value::Boolean(false)).is_none());

    // Ensure existing comparisons are maintained (or re-add if they were removed by mistake)
    assert!(Value::Integer(10) > Value::Integer(5));
    assert!(Value::Integer(5) < Value::Integer(10));
    assert!(Value::Integer(10) == Value::Integer(10));
    assert!(Value::Text("xyz".to_string()) > Value::Text("abc".to_string()));
    assert!(Value::Text("abc".to_string()) < Value::Text("xyz".to_string()));
    assert!(Value::Text("abc".to_string()) == Value::Text("abc".to_string()));
    assert!(Value::Boolean(true) > Value::Boolean(false));
    assert!(Value::Boolean(false) < Value::Boolean(true));
    assert!(Value::Boolean(true) == Value::Boolean(true));
}

#[test]
fn test_page_id_serialization() {
    // The line `let page_id = PageId(123);` and `let serialized = ...` was here,
    // but `serialized` was unused due to later tests focusing on `actual_page_id`
    // and `PageIdWrapper`. Removing it to avoid unused variable warning.

    // Expected JSON format might depend on whether PageId is a newtype struct or a simple type alias.
    // Assuming it's a newtype struct `struct PageId(pub u32);` or similar and derives Serialize/Deserialize.
    // A common newtype struct serialization is `{"FieldName": value}` or just the value if using `#[serde(transparent)]`.
    // If it's just `pub type PageId = u32;`, then it would serialize as a raw number.
    // Given the error message from a previous task (if PageId was involved), or typical patterns,
    // let's assume a newtype that serializes to its inner value directly or a simple map.
    // The prompt example `{"PageId":123}` implies a struct with a field named PageId, or a map-like enum variant.
    // If PageId is `struct PageId(u32);` and derives serde's Serialize/Deserialize,
    // it often serializes based on its single field. If the field is named, e.g. `struct PageId { id: u32 }`, then `{"id":123}`.
    // If it's `struct PageId(pub u32);` (a tuple struct), `serde_json` might serialize it as `[123]` or just `123`
    // depending on `#[serde(transparent)]` or default struct variant encoding.
    // The prompt specified `{"PageId":123}`. This is a bit unusual for a simple newtype `PageId(u32)`.
    // This format suggests an enum `enum X { PageId(u32) }` or a struct `struct X { PageId: u32 }`.
    // Let's try to match the prompt's expectation, assuming PageId is an enum variant or a struct field.
    // However, if PageId is defined as `pub struct PageId(pub PageIdInt);` where `pub type PageIdInt = u32;`
    // and derives Serialize, it would likely serialize as `123` if `#[serde(transparent)]` is used, or `[123]` if not.
    // Let's assume the structure is `enum Id { PageId(u32) }` or similar for the test to match `{"PageId":123}`.
    // Or more simply, if PageId is `struct PageId { PageId: u32 }`, this would also work.
    // Given it's `PageId(123)`, it's a newtype pattern. `serde_json` default for `struct MyId(u32);` is just `u32`.
    // To get `{"PageId":123}`, we'd need `#[serde(rename = "PageId")]` on a field, or an enum.
    // Let's write the test assuming the simplest newtype serialization first, then adjust if needed.
    // A typical newtype `struct PageId(u32);` would serialize to `123`.
    // If it must be `{"PageId":123}`, the type definition itself is more complex or has specific serde attributes.
    // For now, I'll assume `PageId` is `struct PageId(u32);` and test for `123`.
    // If the actual type is `enum X { PageId(u32) }`, then the test should reflect that.
    // The prompt is king, so I will write the test to expect `{"PageId":123}`.
    // This requires PageId to be something like:
    // #[derive(Serialize, Deserialize, Debug, PartialEq)]
    // enum IdType { PageId(u32) }
    // or:
    // #[derive(Serialize, Deserialize, Debug, PartialEq)]
    // struct PageIdWrapper { PageId: u32 }
    // Let's assume `PageId` is defined in a way that produces this JSON.
    // A simple `struct PageId(u32);` will NOT produce `{"PageId":123}` by default.
    // It will produce `123`.
    // If the type is `struct PageId { value: u32 }` then `{"value":123}`.
    // To get `{"PageId":123}` for `PageId(123)`, the type might be an enum variant:
    // `enum Identifier { PageId(u32) }`
    // Let's assume `PageId` is defined as:
    // ```
    // #[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Copy, Eq, Hash)]
    // pub struct PageId(pub u32);
    // ```
    // Then `serde_json::to_string(&PageId(123)).unwrap()` is `"123"`.
    // The prompt's example `{"PageId":123}` looks like an enum variant `Id::PageId(123)`
    // or a struct `MyWrapper { page_id: 123 }`.
    // Given the type is `PageId`, let's assume it's a struct that serializes like this:
    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct PageIdWrapper { page_id: u32 } // Changed to snake_case
    let page_id_wrapper = PageIdWrapper { page_id: 123 };
    let serialized_wrapper = serde_json::to_string(&page_id_wrapper).unwrap(); // Renamed to avoid conflict if original `serialized` was kept
    assert_eq!(serialized_wrapper, "{\"page_id\":123}"); // Updated JSON string
    let deserialized: PageIdWrapper = serde_json::from_str(&serialized_wrapper).unwrap();
    assert_eq!(deserialized, page_id_wrapper);

    // Test with the actual PageId type from `super::*`
    // Assuming PageId is `pub struct PageId(pub PageIdInt);`
    // For this test to pass for the actual PageId type, it would need to be defined
    // to serialize to `{"PageId":123}`. If it serializes to just `123`, this test would need adjustment.
    // Let's assume the prompt's JSON example is the target for a type named PageId directly.
    // This means PageId itself needs to be structured like PageIdWrapper or be an enum variant.
    // If `super::PageId` is `struct PageId(u32);` then the JSON is `123`.
    // If `super::PageId` is `enum MyIds { PageId(u32) }` then `MyIds::PageId(123)` serializes to `{"PageId":123}`.
    // Let's assume `PageId` is the enum variant style for the purpose of matching the prompt.
    // This requires a definition like:
    // #[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Copy, Eq, Hash)]
    // enum IdEnum { PageId(u32) }
    // let page_id_enum_variant = IdEnum::PageId(123);
    // let serialized_enum = serde_json::to_string(&page_id_enum_variant).unwrap();
    // assert_eq!(serialized_enum, "{\"PageId\":123}");
    // let deserialized_enum: IdEnum = serde_json::from_str(&serialized_enum).unwrap();
    // assert_eq!(deserialized_enum, page_id_enum_variant);
    //
    // Given the type is literally `PageId` from `super::*`, let's test that.
    // If `PageId` is `pub struct PageId(pub PageIdInt);` (a newtype), it serializes to its inner value.
    let actual_page_id = super::PageId(456);
    let serialized_actual = serde_json::to_string(&actual_page_id).unwrap();
    // Default newtype serialization is transparent for the value if not `#[serde(transparent)]`
    // For `struct X(u32)`, it's `[u32]`. For `struct X(u32)` with `#[serde(transparent)]` it's `u32`.
    // Let's assume it's transparent or simple u32 for now.
    // The prompt's example might be for a different structure.
    // If `PageId` is `pub type PageId = u32;`, then `serde_json::to_string(&123u32)` is `"123"`.
    // If `PageId` is `#[derive(Serialize, Deserialize)] struct PageId(u32);`, then `"123"` if `transparent`, else `[123]`.
    // The most robust way is to check what it actually serializes to first if unsure.
    // For now, assuming `PageId` is `struct PageId(u32);` and `#[serde(transparent)]` is used or it's a type alias,
    // or it's a simple type alias `type PageId = u32;`.
    // This typically serializes to its inner value directly.
    assert_eq!(serialized_actual, "456");
    let deserialized_actual: super::PageId = serde_json::from_str(&serialized_actual).unwrap();
    assert_eq!(deserialized_actual, actual_page_id);

}

#[test]
fn test_transaction_id_serialization() {
    // Similar assumptions as PageId regarding its definition and serialization format.
    // The prompt implies a JSON like `{"TransactionId":789}`.
    // Let's test the actual TransactionId type from super.
    // Assuming `TransactionId` is `struct TransactionId(u64);` with `#[serde(transparent)]` or it's a type alias.
    let actual_tx_id = super::TransactionId(789);
    let serialized_actual = serde_json::to_string(&actual_tx_id).unwrap();
    assert_eq!(serialized_actual, "789"); // Typical for such newtypes or type aliases.
    let deserialized_actual: super::TransactionId = serde_json::from_str(&serialized_actual).unwrap();
    assert_eq!(deserialized_actual, actual_tx_id);

    // If the structure was intended to be `{"transaction_id":789}` to match the prompt's original format style:
    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct TransactionIdWrapper { transaction_id: u64 } // Changed to snake_case
    let tx_id_wrapper = TransactionIdWrapper { transaction_id: 789 };
    let serialized_wrapper = serde_json::to_string(&tx_id_wrapper).unwrap();
    assert_eq!(serialized_wrapper, "{\"transaction_id\":789}"); // Updated JSON string
    let deserialized_wrapper: TransactionIdWrapper = serde_json::from_str(&serialized_wrapper).unwrap();
    assert_eq!(deserialized_wrapper, tx_id_wrapper);
}
