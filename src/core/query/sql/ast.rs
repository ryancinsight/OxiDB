#[derive(Debug, PartialEq, Clone)]
pub enum AstLiteralValue {
    String(String),
    Number(String), // Keep as string to preserve exact representation initially
    Boolean(bool),
    Null, // Added Null for completeness
}

#[derive(Debug, PartialEq, Clone)]
pub struct Condition {
    pub column: String,
    pub operator: String, // e.g., "=", "!=", "<", ">", "IS NULL", "IS NOT NULL"
    pub value: AstLiteralValue,
}

// More complex conditions (AND, OR, NOT) can be added later
// pub enum ConditionTree {
//     Single(Condition),
//     And(Box<ConditionTree>, Box<ConditionTree>),
//     Or(Box<ConditionTree>, Box<ConditionTree>),
//     Not(Box<ConditionTree>),
// }

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
    pub condition: Option<Condition>, // Simplified: Option<ConditionTree> for complex conditions
}

#[derive(Debug, PartialEq, Clone)]
pub struct UpdateStatement {
    pub source: String, // Table name
    pub assignments: Vec<Assignment>,
    pub condition: Option<Condition>, // Simplified: Option<ConditionTree> for complex conditions
}

// Future statements: InsertStatement, DeleteStatement, CreateTableStatement etc.

#[derive(Debug, PartialEq, Clone)]
pub enum Statement {
    Select(SelectStatement),
    Update(UpdateStatement),
    // Insert(InsertStatement),
    // Delete(DeleteStatement),
}
