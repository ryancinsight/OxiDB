// src/core/types/schema.rs
use crate::core::common::types::ColumnType;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: ColumnType,
    pub is_primary_key: bool,
    pub is_unique: bool,
    pub is_nullable: bool,
    pub is_auto_increment: bool,
    // Add other constraints like default_value later
}

impl ColumnDef {
    /// Create a new column definition with basic properties
    #[must_use]
    pub const fn new(name: String, data_type: ColumnType, is_nullable: bool) -> Self {
        Self { 
            name, 
            data_type, 
            is_nullable,
            is_primary_key: false,
            is_unique: false,
            is_auto_increment: false,
        }
    }

    /// Create a new column definition with all constraint options
    #[must_use]
    pub const fn with_constraints(
        name: String, 
        data_type: ColumnType, 
        is_nullable: bool,
        is_primary_key: bool,
        is_unique: bool,
        is_auto_increment: bool,
    ) -> Self {
        Self { 
            name, 
            data_type, 
            is_nullable,
            is_primary_key,
            is_unique,
            is_auto_increment,
        }
    }

    /// Create a primary key column (non-nullable, unique)
    #[must_use]
    pub const fn primary_key(name: String, data_type: ColumnType) -> Self {
        Self {
            name,
            data_type,
            is_nullable: false,
            is_primary_key: true,
            is_unique: true,
            is_auto_increment: false,
        }
    }

    /// Create an auto-increment primary key column
    #[must_use]
    pub const fn auto_increment_primary_key(name: String, data_type: ColumnType) -> Self {
        Self {
            name,
            data_type,
            is_nullable: false,
            is_primary_key: true,
            is_unique: true,
            is_auto_increment: true,
        }
    }

    /// Create a unique column
    #[must_use]
    pub const fn unique(name: String, data_type: ColumnType, is_nullable: bool) -> Self {
        Self {
            name,
            data_type,
            is_nullable,
            is_primary_key: false,
            is_unique: true,
            is_auto_increment: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Schema {
    pub columns: Vec<ColumnDef>,
    // Potentially include table name or other metadata
}

impl Schema {
    #[must_use]
    pub const fn new(columns: Vec<ColumnDef>) -> Self {
        Self { columns }
    }

    // Helper constructor for ColumnDef, assuming default constraints initially
    // This might be useful if creating ColumnDefs programmatically outside of parsing.
    // For parsing, these will be set explicitly.
    #[must_use]
    pub const fn new_column_def(name: String, data_type: ColumnType) -> ColumnDef {
        ColumnDef {
            name,
            data_type,
            is_primary_key: false,
            is_unique: false,
            is_nullable: true,        // Default to nullable
            is_auto_increment: false, // Default to no auto-increment
        }
    }

    #[must_use]
    pub fn get_column_index(&self, column_name: &str) -> Option<usize> {
        self.columns.iter().position(|col| col.name == column_name)
    }
}
