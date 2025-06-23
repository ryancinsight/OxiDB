#[derive(Debug, PartialEq, Clone)]
pub enum AstLiteralValue {
    String(String),
    Number(String), // Keep as string to preserve exact representation initially
    Boolean(bool),
    Null, // Added Null for completeness
    Vector(Vec<AstLiteralValue>), // Represents a list of literals, e.g., [1.0, 2.0, 3.0]
}

#[derive(Debug, PartialEq, Clone)]
pub struct Condition {
    pub column: String,
    pub operator: String, // e.g., "=", "!=", "<", ">", "IS NULL", "IS NOT NULL"
    pub value: AstLiteralValue,
}

/// Represents a tree of conditions for WHERE clauses.
#[derive(Debug, PartialEq, Clone)]
pub enum ConditionTree {
    Comparison(Condition), // A simple comparison like column = value
    And(Box<ConditionTree>, Box<ConditionTree>),
    Or(Box<ConditionTree>, Box<ConditionTree>),
    Not(Box<ConditionTree>),
    // Parenthesized(Box<ConditionTree>) // Can be implicitly handled by recursion and precedence
}

#[derive(Debug, PartialEq, Clone)]
pub struct Assignment {
    pub column: String,
    pub value: AstLiteralValue,
}

#[derive(Debug, PartialEq, Clone)]
pub enum SelectColumn {
    ColumnName(String),
    Asterisk, // For SELECT *
}

#[derive(Debug, PartialEq, Clone)]
pub struct SelectStatement {
    pub columns: Vec<SelectColumn>, // Changed to Vec<SelectColumn> to support specific columns or *
    pub source: String,             // Table name
    pub condition: Option<ConditionTree>,
    pub order_by: Option<Vec<OrderByExpr>>,
    pub limit: Option<AstLiteralValue>, // Using AstLiteralValue for now, translator will ensure it's a number
}

#[derive(Debug, PartialEq, Clone)]
pub struct OrderByExpr {
    pub expression: String, // For now, simple column name. Could be more complex Ast::Expression later.
    pub direction: Option<OrderDirection>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Debug, PartialEq, Clone)]
pub struct UpdateStatement {
    pub source: String, // Table name
    pub assignments: Vec<Assignment>,
    pub condition: Option<ConditionTree>,
}

// Future statements: InsertStatement, DeleteStatement, CreateTableStatement etc.

#[derive(Debug, PartialEq, Clone)]
pub enum AstColumnConstraint {
    NotNull,
    Unique,
    PrimaryKey,
    // Potentially others like Check(String), Default(AstLiteralValue) in the future
}

#[derive(Debug, PartialEq, Clone)]
pub enum AstDataType {
    Integer,
    Text,
    Boolean,
    Float,
    // SQL standard types that map to engine types
    // VARCHAR, CHAR, DECIMAL, etc. can be added here.
    // For now, keeping it simple.
    Vector { dimension: u32 }, // Represents VECTOR[dimension]
    // Adding other known types from the engine for completeness if they can be declared in SQL
    Blob, // If blobs can be declared directly in SQL like CREATE TABLE t (b BLOB)
    // NullType is usually implicit, not a declared column type.
}

#[derive(Debug, PartialEq, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: AstDataType, // Changed from String
    pub constraints: Vec<AstColumnConstraint>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct CreateTableStatement {
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct InsertStatement {
    pub table_name: String,
    pub columns: Option<Vec<String>>, // Optional: e.g., INSERT INTO foo VALUES (...) vs INSERT INTO foo (col1, col2) VALUES (...)
    pub values: Vec<Vec<AstLiteralValue>>, // Support for multi-value inserts: VALUES (...), (...)
}

#[derive(Debug, PartialEq, Clone)]
pub struct DeleteStatement {
    pub table_name: String,
    pub condition: Option<ConditionTree>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Statement {
    Select(SelectStatement),
    Update(UpdateStatement),
    CreateTable(CreateTableStatement),
    Insert(InsertStatement),
    Delete(DeleteStatement),
    DropTable(DropTableStatement),
}

#[derive(Debug, PartialEq, Clone)]
pub struct DropTableStatement {
    pub table_name: String,
    pub if_exists: bool, // Support IF EXISTS
}
