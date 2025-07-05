#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DataType {
    Integer,
    Text,
    Boolean,
    Blob,
    Float64, // Added for floating point numbers
    Vector(Option<usize>), // Represents a vector of floats, optional dimension
    Null,
    Unsupported, // For Value types that don't map directly to a storable DataType
}
