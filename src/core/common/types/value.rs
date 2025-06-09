use super::data_type::DataType;

#[derive(Debug, Clone, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize)]
pub enum Value {
    Integer(i64),
    Text(String),
    Boolean(bool),
    Blob(Vec<u8>),
    Null,
}

impl Value {
    pub fn get_type(&self) -> DataType {
        match self {
            Value::Integer(_) => DataType::Integer,
            Value::Text(_) => DataType::Text,
            Value::Boolean(_) => DataType::Boolean,
            Value::Blob(_) => DataType::Blob,
            Value::Null => DataType::Null,
        }
    }
}
