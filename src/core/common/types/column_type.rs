#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ColumnType {
    Integer,
    Float,
    Text,
    Boolean,
    Blob,
    Vector(Option<usize>), // Represents a vector of floats, optional dimension
    Null,
}
