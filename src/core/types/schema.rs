// src/core/types/schema.rs
use crate::core::types::DataType;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    // Add other constraints like nullable, primary_key, unique, default_value later
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Schema {
    pub columns: Vec<ColumnDef>,
    // Potentially include table name or other metadata
}

impl Schema {
    pub fn new(columns: Vec<ColumnDef>) -> Self {
        Schema { columns }
    }

    pub fn get_column_index(&self, column_name: &str) -> Option<usize> {
        self.columns.iter().position(|col| col.name == column_name)
    }
}
