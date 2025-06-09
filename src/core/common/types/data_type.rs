#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DataType {
    Integer,
    Text,
    Boolean,
    Blob,
    Null,
}
