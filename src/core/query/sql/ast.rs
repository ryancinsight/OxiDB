#[derive(Debug, PartialEq, Clone)]
pub enum AstLiteralValue {
    String(String),
    Number(String), // Keep as string to preserve exact representation initially
    Boolean(bool),
    Null,                         // Added Null for completeness
    Vector(Vec<AstLiteralValue>), // Represents a list of literals, e.g., [1.0, 2.0, 3.0]
}

#[derive(Debug, PartialEq, Clone)]
pub enum AstExpressionValue {
    // ADDED
    Literal(AstLiteralValue),
    ColumnIdentifier(String),
    Parameter(u32), // Parameter placeholder with index (0-based)
}

#[derive(Debug, PartialEq, Clone)]
pub struct Condition {
    pub column: String,            // Left-hand side, always a column for now
    pub operator: String,          // e.g., "=", "!=", "<", ">", "IS NULL", "IS NOT NULL"
    pub value: AstExpressionValue, // Right-hand side - CHANGED
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
    pub value: AstExpressionValue,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum SelectColumn {
    ColumnName(String),
    Asterisk, // For SELECT *
}

#[derive(Debug, PartialEq, Clone)]
pub struct SelectStatement {
    pub columns: Vec<SelectColumn>,
    pub from_clause: TableReference,
    pub joins: Vec<JoinClause>,
    pub condition: Option<ConditionTree>,
    pub order_by: Option<Vec<OrderByExpr>>,
    pub limit: Option<AstLiteralValue>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    Cross,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct TableReference {
    pub name: String,
    pub alias: Option<String>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub right_source: TableReference,
    pub on_condition: Option<ConditionTree>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct OrderByExpr {
    pub expression: String,
    pub direction: Option<OrderDirection>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Debug, PartialEq, Clone)]
pub struct UpdateStatement {
    pub source: String,
    pub assignments: Vec<Assignment>,
    pub condition: Option<ConditionTree>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum AstColumnConstraint {
    NotNull,
    Unique,
    PrimaryKey,
    AutoIncrement,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum AstDataType {
    Integer,
    Text,
    Boolean,
    Float,
    Vector { dimension: u32 },
    Blob,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: AstDataType,
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
    pub columns: Option<Vec<String>>,
    pub values: Vec<Vec<AstExpressionValue>>,
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

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct DropTableStatement {
    pub table_name: String,
    pub if_exists: bool,
}
