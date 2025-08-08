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

    /// Get a value by column index
    #[inline]
    pub fn get(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }

    /// Iterate over values in the row
    #[inline]
    pub fn iter(&self) -> std::slice::Iter<'_, Value> {
        self.values.iter()
    }

    /// Number of values in the row
    #[inline]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Whether the row has no values
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}
