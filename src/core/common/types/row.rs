use super::value::Value;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Row {
    pub values: Vec<Value>,
}

impl Row {
    /// Create a new Row with the given values
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }
    
    /// Create a new Row from a slice of values
    pub fn from_slice(values: &[Value]) -> Self {
        Self { values: values.to_vec() }
    }
}
