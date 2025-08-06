// src/core/query/commands.rs

use crate::core::common::types::Value as ParamValue;
use crate::core::query::sql::ast::Statement;
use crate::core::types::{DataType, VectorData}; // Added VectorData

/// Represents a key for operations.
pub type Key = Vec<u8>;
/// Represents a value for operations.
pub type Value = Vec<u8>;

// Renamed from SqlCondition to be part of the new SqlConditionTree enum
#[derive(Debug, PartialEq, Clone)]
pub struct SqlSimpleCondition {
    pub column: String,
    pub operator: String, // e.g., "=", "!=", "<", ">", "<=", ">="
    pub value: DataType,  // Use DataType here
}

#[derive(Debug, PartialEq, Clone)]
pub enum SqlConditionTree {
    Comparison(SqlSimpleCondition),
    And(Box<SqlConditionTree>, Box<SqlConditionTree>),
    Or(Box<SqlConditionTree>, Box<SqlConditionTree>),
    Not(Box<SqlConditionTree>),
}

#[derive(Debug, PartialEq, Clone)]
pub struct SqlAssignment {
    pub column: String,
    pub value: DataType, // Use DataType here
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum SelectColumnSpec {
    Specific(Vec<String>), // List of column names
    All,                   // Represents SELECT *
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SqlOrderByExpr {
    pub expression: String, // Column name
    pub direction: Option<SqlOrderDirection>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum SqlOrderDirection {
    Asc,
    Desc,
}

/// A parameterized SQL statement with separate parameter values
/// This provides secure parameterized query execution
#[derive(Debug, Clone)]
pub struct ParameterizedCommand {
    pub statement: Statement,
    pub parameters: Vec<ParamValue>,
}

/// Enum defining the different types of commands the database can execute.
/// These are internal representations, not directly parsed from strings yet.
#[derive(Debug, PartialEq, Clone)] // For testing and inspection
pub enum Command {
    // Transaction control commands
    BeginTransaction,
    CommitTransaction,
    RollbackTransaction,
    Vacuum, // Database maintenance command
    // SQL-like commands
    Select {
        columns: SelectColumnSpec,
        source: String,                      // Table/source name
        condition: Option<SqlConditionTree>, // Changed
        order_by: Option<Vec<SqlOrderByExpr>>,
        limit: Option<u64>,
    },
    Update {
        source: String, // Table/source name
        assignments: Vec<SqlAssignment>,
        condition: Option<SqlConditionTree>, // Changed
    },
    CreateTable {
        table_name: String,
        columns: Vec<crate::core::types::schema::ColumnDef>, // Ensuring correct path
    },
    SqlInsert {
        // For SQL INSERT INTO table (cols) VALUES (vals)
        table_name: String,
        columns: Option<Vec<String>>, // None if columns are not specified
        values: Vec<Vec<DataType>>,   // Outer Vec for rows, inner Vec for values in a row
    },
    SqlDelete {
        table_name: String,
        condition: Option<SqlConditionTree>, // Changed
    },
    SimilaritySearch {
        table_name: String,
        vector_column_name: String,
        query_vector: VectorData,
        top_k: usize,
    },
    DropTable {
        table_name: String,
        if_exists: bool,
    },
    // Parameterized SQL statement execution
    ParameterizedSql {
        statement: Statement,
        parameters: Vec<ParamValue>,
    },
}

// Example of how these might be constructed (not strictly part of this file,
// but for conceptual clarity - API layer will do this)
// pub fn create_insert_command(key: Key, value: Value) -> Command {
//     Command::Insert { key, value }
// }
//
// pub fn create_get_command(key: Key) -> Command {
//     Command::Get { key }
// }
