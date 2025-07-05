#[derive(Debug, PartialEq, Clone)]
pub enum AstLiteralValue {
    String(String),
    Number(String), // Keep as string to preserve exact representation initially
    Boolean(bool),
    Null,                         // Added Null for completeness
    Vector(Vec<AstLiteralValue>), // Represents a list of literals, e.g., [1.0, 2.0, 3.0]
}

// Forward declaration for AstExpression if needed, or ensure order allows its use.

#[derive(Debug, PartialEq, Clone)]
pub enum AstArithmeticOperator {
    Plus,
    Minus,
    Multiply,
    Divide,
}

#[derive(Debug, PartialEq, Clone)]
pub enum AstLogicalOperator { // For AND, OR in condition trees if we make them explicit ops
    And,
    Or,
}

#[derive(Debug, PartialEq, Clone)]
pub enum AstComparisonOperator { // For =, !=, <, >, etc.
    Equals,
    NotEquals,
    LessThan,
    LessThanOrEquals,
    GreaterThan,
    GreaterThanOrEquals,
    IsNull,
    IsNotNull,
    // Add others like LIKE, BETWEEN, IN if needed by this enum
}


#[derive(Debug, PartialEq, Clone)]
pub enum AstExpression {
    Literal(AstLiteralValue),
    ColumnIdentifier(String),
    // UnaryOp { // Example for unary minus, etc.
    //     op: AstUnaryOperator, // e.g. Minus, Not
    //     expr: Box<AstExpression>,
    // },
    BinaryOp {
        left: Box<AstExpression>,
        op: AstArithmeticOperator, // Using specific enum for arithmetic
        right: Box<AstExpression>,
    },
    FunctionCall { // Moved from SelectColumn for general expression usage
        name: String,
        args: Vec<AstFunctionArg>, // AstFunctionArg might need to take AstExpression
        // over_clause: Option<AstOverClause>,
    },
    UnaryOp {
        op: AstUnaryOperator,
        expr: Box<AstExpression>,
    },
    // Case { .. }
    // Subquery(Box<SelectStatement>) // For IN (SELECT ...), etc.
    // Exists(Box<SelectStatement>)
}

#[derive(Debug, PartialEq, Clone)]
pub enum AstUnaryOperator {
    Plus,
    Minus,
    // Not, // Logical NOT - Handled by ConditionTree::Not for boolean logic for now
}


#[derive(Debug, PartialEq, Clone)]
pub struct Condition { // This might simplify or change if ConditionTree uses AstExpression directly
    // pub column: String, // LHS could become an AstExpression
    // pub operator: String, // Could become AstComparisonOperator
    // pub value: AstExpressionValue, // RHS could become an AstExpression
    // For now, keeping it simple for initial refactor, will adjust if ConditionTree changes significantly
    pub left: AstExpression, // Generalizing LHS
    pub operator: AstComparisonOperator, // Using enum
    pub right: AstExpression, // Generalizing RHS
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
    pub value: AstExpression, // Changed to AstExpression
}

#[derive(Debug, PartialEq, Clone)]
pub enum AstFunctionArg {
    Asterisk, // For COUNT(*)
    Expression(AstExpression), // Changed to AstExpression
    Distinct(Box<AstExpression>), // Changed to AstExpression
}

#[derive(Debug, PartialEq, Clone)]
pub enum SelectColumn {
    // ColumnName(String), // Replaced by Expression(AstExpression::ColumnIdentifier(...))
    // Asterisk, // Remains, or could be a special AstExpression variant if desired
    // FunctionCall { ... }, // Replaced by Expression(AstExpression::FunctionCall{...})
    Expression(AstExpression), // Represents a single expression in the select list
    Asterisk, // For SELECT * specifically, as it's not really an expression in the same way
}


// Placeholder for OVER clause if window functions are implemented later
// #[derive(Debug, PartialEq, Clone)]
// pub struct AstOverClause {
//     pub partition_by: Option<Vec<AstExpressionValue>>,
//     pub order_by: Option<Vec<OrderByExpr>>,
//     // Add frame specification (ROWS/RANGE BETWEEN ...) if needed
// }

#[derive(Debug, PartialEq, Clone)]
pub struct SelectStatement {
    pub distinct: bool, // Added for SELECT DISTINCT
    pub columns: Vec<SelectColumn>,
    pub from_clause: TableReference,
    pub joins: Vec<JoinClause>,
    pub condition: Option<ConditionTree>,
    pub order_by: Option<Vec<OrderByExpr>>,
    pub limit: Option<AstLiteralValue>,
    pub group_by: Option<Vec<AstExpression>>, // Changed to AstExpression
    pub having: Option<ConditionTree>,            // Added for HAVING
}

#[derive(Debug, PartialEq, Clone)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    Cross,
}

#[derive(Debug, PartialEq, Clone)]
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

#[derive(Debug, PartialEq, Clone)]
pub struct OrderByExpr {
    pub expression: AstExpression, // Changed to AstExpression
    pub direction: Option<OrderDirection>,
}

#[derive(Debug, PartialEq, Clone)]
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

#[derive(Debug, PartialEq, Clone)]
pub enum AstColumnConstraint {
    NotNull,
    Unique,
    PrimaryKey,
}

#[derive(Debug, PartialEq, Clone)]
pub enum AstDataType {
    Integer,
    Text,
    Boolean,
    Float,
    Vector { dimension: u32 },
    Blob,
}

#[derive(Debug, PartialEq, Clone)]
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
    pub values: Vec<Vec<AstLiteralValue>>,
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
    pub if_exists: bool,
}
