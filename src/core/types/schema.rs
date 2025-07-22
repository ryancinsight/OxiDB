// src/core/types/schema.rs
use crate::core::types::DataType;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub is_primary_key: bool,
    pub is_unique: bool,
    pub is_nullable: bool,
    pub is_auto_increment: bool,
    // Add other constraints like default_value later
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Schema {
    pub columns: Vec<ColumnDef>,
    // Potentially include table name or other metadata
}

impl Schema {
    #[must_use] pub const fn new(columns: Vec<ColumnDef>) -> Self {
        Self { columns }
    }

    // Helper constructor for ColumnDef, assuming default constraints initially
    // This might be useful if creating ColumnDefs programmatically outside of parsing.
    // For parsing, these will be set explicitly.
    #[must_use] pub const fn new_column_def(name: String, data_type: DataType) -> ColumnDef {
        ColumnDef {
            name,
            data_type,
            is_primary_key: false,
            is_unique: false,
            is_nullable: true,        // Default to nullable
            is_auto_increment: false, // Default to no auto-increment
        }
    }

    #[must_use] pub fn get_column_index(&self, column_name: &str) -> Option<usize> {
        self.columns.iter().position(|col| col.name == column_name)
    }
}
