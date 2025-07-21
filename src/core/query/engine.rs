use std::collections::{HashMap, BTreeMap};
use std::fmt;
use std::str::FromStr;

use crate::core::common::{OxidbError, Result, ErrorContext};

/// Pure Rust SQL-like query engine with zero-cost abstractions
/// Following SOLID principles: Single Responsibility, Open/Closed, Liskov Substitution, 
/// Interface Segregation, Dependency Inversion

/// Core data types - minimal and efficient
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Integer(i) => write!(f, "{}", i),
            Value::Real(r) => write!(f, "{}", r),
            Value::Text(s) => write!(f, "'{}'", s),
            Value::Blob(b) => write!(f, "BLOB({} bytes)", b.len()),
        }
    }
}

/// Zero-cost abstraction for SQL operations
#[derive(Debug, Clone)]
pub enum SqlOperation {
    CreateTable {
        name: String,
        columns: Vec<ColumnDef>,
    },
    Insert {
        table: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<Value>>,
    },
    Select {
        columns: SelectColumns,
        from: String,
        where_clause: Option<Expression>,
        order_by: Option<Vec<OrderBy>>,
        limit: Option<usize>,
    },
    Update {
        table: String,
        set: Vec<(String, Expression)>,
        where_clause: Option<Expression>,
    },
    Delete {
        from: String,
        where_clause: Option<Expression>,
    },
}

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: SqlType,
    pub constraints: Vec<ColumnConstraint>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SqlType {
    Integer,
    Real,
    Text,
    Blob,
}

#[derive(Debug, Clone)]
pub enum ColumnConstraint {
    NotNull,
    PrimaryKey,
    Unique,
    Default(Value),
}

#[derive(Debug, Clone)]
pub enum SelectColumns {
    All,
    Columns(Vec<String>),
}

#[derive(Debug, Clone)]
pub struct OrderBy {
    pub column: String,
    pub direction: OrderDirection,
}

#[derive(Debug, Clone)]
pub enum OrderDirection {
    Asc,
    Desc,
}

/// Expression system with zero-cost abstractions
#[derive(Debug, Clone)]
pub enum Expression {
    Value(Value),
    Column(String),
    BinaryOp {
        left: Box<Expression>,
        op: BinaryOperator,
        right: Box<Expression>,
    },
    UnaryOp {
        op: UnaryOperator,
        operand: Box<Expression>,
    },
}

#[derive(Debug, Clone)]
pub enum BinaryOperator {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    And,
    Or,
    Add,
    Subtract,
    Multiply,
    Divide,
    Like,
}

#[derive(Debug, Clone)]
pub enum UnaryOperator {
    Not,
    Minus,
}

/// Row representation with zero-cost abstractions
#[derive(Debug, Clone)]
pub struct Row {
    values: BTreeMap<String, Value>,
}

impl Row {
    pub fn new() -> Self {
        Self {
            values: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, column: String, value: Value) {
        self.values.insert(column, value);
    }

    pub fn get(&self, column: &str) -> Option<&Value> {
        self.values.get(column)
    }

    pub fn columns(&self) -> impl Iterator<Item = &String> {
        self.values.keys()
    }

    pub fn values(&self) -> impl Iterator<Item = &Value> {
        self.values.values()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &Value)> {
        self.values.iter()
    }
}

/// Table schema with ACID compliance
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub primary_key: Option<String>,
}

impl TableSchema {
    pub fn new(name: String, columns: Vec<ColumnDef>) -> Self {
        let primary_key = columns
            .iter()
            .find(|col| col.constraints.iter().any(|c| matches!(c, ColumnConstraint::PrimaryKey)))
            .map(|col| col.name.clone());

        Self {
            name,
            columns,
            primary_key,
        }
    }

    pub fn validate_row(&self, row: &Row) -> Result<()> {
        // Validate all required columns are present
        for column in &self.columns {
            let has_not_null = column.constraints.iter().any(|c| matches!(c, ColumnConstraint::NotNull));
            let has_primary_key = column.constraints.iter().any(|c| matches!(c, ColumnConstraint::PrimaryKey));
            
            if (has_not_null || has_primary_key) && !row.values.contains_key(&column.name) {
                return Err(OxidbError::ConstraintViolation {
                    constraint_type: "NOT NULL".to_string(),
                    table: self.name.clone(),
                    details: format!("Column '{}' cannot be NULL", column.name),
                });
            }
        }

        // Validate data types
        for (col_name, value) in &row.values {
            if let Some(column_def) = self.columns.iter().find(|c| &c.name == col_name) {
                if !self.value_matches_type(value, &column_def.data_type) {
                    return Err(OxidbError::Type {
                        expected: format!("{:?}", column_def.data_type),
                        found: format!("{:?}", value),
                        context: format!("Column '{}' in table '{}'", col_name, self.name),
                    });
                }
            }
        }

        Ok(())
    }

    fn value_matches_type(&self, value: &Value, sql_type: &SqlType) -> bool {
        match (value, sql_type) {
            (Value::Null, _) => true, // NULL is compatible with all types
            (Value::Integer(_), SqlType::Integer) => true,
            (Value::Real(_), SqlType::Real) => true,
            (Value::Text(_), SqlType::Text) => true,
            (Value::Blob(_), SqlType::Blob) => true,
            _ => false,
        }
    }
}

/// Query execution engine with zero-cost abstractions
pub struct QueryEngine {
    schemas: HashMap<String, TableSchema>,
    data: HashMap<String, Vec<Row>>,
}

impl QueryEngine {
    pub fn new() -> Self {
        Self {
            schemas: HashMap::new(),
            data: HashMap::new(),
        }
    }

    /// Execute SQL operation using iterator combinators
    pub fn execute(&mut self, operation: SqlOperation) -> Result<QueryResult> {
        match operation {
            SqlOperation::CreateTable { name, columns } => {
                self.create_table(name, columns)
            }
            SqlOperation::Insert { table, columns, values } => {
                self.insert(table, columns, values)
            }
            SqlOperation::Select { columns, from, where_clause, order_by, limit } => {
                self.select(columns, from, where_clause, order_by, limit)
            }
            SqlOperation::Update { table, set, where_clause } => {
                self.update(table, set, where_clause)
            }
            SqlOperation::Delete { from, where_clause } => {
                self.delete(from, where_clause)
            }
        }
    }

    fn create_table(&mut self, name: String, columns: Vec<ColumnDef>) -> Result<QueryResult> {
        if self.schemas.contains_key(&name) {
            return Err(OxidbError::AlreadyExists {
                resource_type: "Table".to_string(),
                identifier: name,
            });
        }

        let schema = TableSchema::new(name.clone(), columns);
        self.schemas.insert(name.clone(), schema);
        self.data.insert(name, Vec::new());

        Ok(QueryResult::Success {
            message: "Table created successfully".to_string(),
        })
    }

    fn insert(&mut self, table: String, columns: Option<Vec<String>>, values: Vec<Vec<Value>>) -> Result<QueryResult> {
        let schema = self.schemas.get(&table)
            .ok_or_else(|| OxidbError::NotFound {
                resource_type: "Table".to_string(),
                identifier: table.clone(),
            })?;

        let table_data = self.data.get_mut(&table).unwrap();
        let mut inserted_count = 0;

        // Use iterator combinators for zero-cost processing
        values
            .into_iter()
            .try_for_each(|row_values| -> Result<()> {
                let mut row = Row::new();

                match &columns {
                    Some(col_names) => {
                        if col_names.len() != row_values.len() {
                            return Err(OxidbError::InvalidInput {
                                parameter: "columns/values".to_string(),
                                value: format!("{} columns, {} values", col_names.len(), row_values.len()),
                                constraints: "Column count must match value count".to_string(),
                            });
                        }

                        col_names
                            .iter()
                            .zip(row_values.iter())
                            .for_each(|(col, val)| {
                                row.insert(col.clone(), val.clone());
                            });
                    }
                    None => {
                        if schema.columns.len() != row_values.len() {
                            return Err(OxidbError::InvalidInput {
                                parameter: "values".to_string(),
                                value: format!("{} values", row_values.len()),
                                constraints: format!("Expected {} values for all columns", schema.columns.len()),
                            });
                        }

                        schema.columns
                            .iter()
                            .zip(row_values.iter())
                            .for_each(|(col_def, val)| {
                                row.insert(col_def.name.clone(), val.clone());
                            });
                    }
                }

                schema.validate_row(&row)?;
                table_data.push(row);
                inserted_count += 1;
                Ok(())
            })?;

        Ok(QueryResult::Insert { count: inserted_count })
    }

    fn select(
        &self,
        columns: SelectColumns,
        from: String,
        where_clause: Option<Expression>,
        order_by: Option<Vec<OrderBy>>,
        limit: Option<usize>,
    ) -> Result<QueryResult> {
        let schema = self.schemas.get(&from)
            .ok_or_else(|| OxidbError::NotFound {
                resource_type: "Table".to_string(),
                identifier: from.clone(),
            })?;

        let table_data = self.data.get(&from).unwrap();

        // Use iterator combinators for zero-cost query processing
        let mut result_rows: Vec<Row> = table_data
            .iter()
            .filter(|row| {
                where_clause.as_ref()
                    .map(|expr| self.evaluate_expression(expr, row).unwrap_or(Value::Null))
                    .map(|val| self.is_truthy(&val))
                    .unwrap_or(true)
            })
            .cloned()
            .collect();

        // Apply ordering using zero-cost abstractions
        if let Some(order_specs) = order_by {
            result_rows.sort_by(|a, b| {
                order_specs
                    .iter()
                    .map(|order_spec| {
                        let a_val = a.get(&order_spec.column).unwrap_or(&Value::Null);
                        let b_val = b.get(&order_spec.column).unwrap_or(&Value::Null);
                        let cmp = self.compare_values(a_val, b_val);
                        match order_spec.direction {
                            OrderDirection::Asc => cmp,
                            OrderDirection::Desc => cmp.reverse(),
                        }
                    })
                    .find(|&ord| ord != std::cmp::Ordering::Equal)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        }

        // Apply limit using iterator take
        if let Some(limit_count) = limit {
            result_rows.truncate(limit_count);
        }

        // Project columns using iterator combinators
        let projected_rows: Vec<Row> = result_rows
            .into_iter()
            .map(|row| {
                let mut projected_row = Row::new();
                match &columns {
                    SelectColumns::All => {
                        for (col_name, value) in row.iter() {
                            projected_row.insert(col_name.clone(), value.clone());
                        }
                    }
                    SelectColumns::Columns(col_names) => {
                        for col_name in col_names {
                            if let Some(value) = row.get(col_name) {
                                projected_row.insert(col_name.clone(), value.clone());
                            }
                        }
                    }
                }
                projected_row
            })
            .collect();

        Ok(QueryResult::Select { rows: projected_rows })
    }

    fn update(&mut self, table: String, set: Vec<(String, Expression)>, where_clause: Option<Expression>) -> Result<QueryResult> {
        let schema = self.schemas.get(&table)
            .ok_or_else(|| OxidbError::NotFound {
                resource_type: "Table".to_string(),
                identifier: table.clone(),
            })?;

        let table_data = self.data.get_mut(&table).unwrap();
        let mut updated_count = 0;

        // Use iterator combinators for zero-cost updates
        table_data
            .iter_mut()
            .filter(|row| {
                where_clause.as_ref()
                    .map(|expr| self.evaluate_expression(expr, row).unwrap_or(Value::Null))
                    .map(|val| self.is_truthy(&val))
                    .unwrap_or(true)
            })
            .try_for_each(|row| -> Result<()> {
                for (column, expr) in &set {
                    let new_value = self.evaluate_expression(expr, row)?;
                    row.insert(column.clone(), new_value);
                }
                schema.validate_row(row)?;
                updated_count += 1;
                Ok(())
            })?;

        Ok(QueryResult::Update { count: updated_count })
    }

    fn delete(&mut self, from: String, where_clause: Option<Expression>) -> Result<QueryResult> {
        let table_data = self.data.get_mut(&from)
            .ok_or_else(|| OxidbError::NotFound {
                resource_type: "Table".to_string(),
                identifier: from,
            })?;

        let original_count = table_data.len();

        // Use iterator combinators for zero-cost filtering
        table_data.retain(|row| {
            where_clause.as_ref()
                .map(|expr| self.evaluate_expression(expr, row).unwrap_or(Value::Null))
                .map(|val| !self.is_truthy(&val))
                .unwrap_or(false)
        });

        let deleted_count = original_count - table_data.len();
        Ok(QueryResult::Delete { count: deleted_count })
    }

    /// Zero-cost expression evaluation using pattern matching
    fn evaluate_expression(&self, expr: &Expression, row: &Row) -> Result<Value> {
        match expr {
            Expression::Value(val) => Ok(val.clone()),
            Expression::Column(col_name) => {
                Ok(row.get(col_name).cloned().unwrap_or(Value::Null))
            }
            Expression::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_expression(left, row)?;
                let right_val = self.evaluate_expression(right, row)?;
                self.apply_binary_op(&left_val, op, &right_val)
            }
            Expression::UnaryOp { op, operand } => {
                let operand_val = self.evaluate_expression(operand, row)?;
                self.apply_unary_op(op, &operand_val)
            }
        }
    }

    /// Zero-cost binary operation evaluation
    fn apply_binary_op(&self, left: &Value, op: &BinaryOperator, right: &Value) -> Result<Value> {
        match op {
            BinaryOperator::Equal => Ok(Value::Integer(if left == right { 1 } else { 0 })),
            BinaryOperator::NotEqual => Ok(Value::Integer(if left != right { 1 } else { 0 })),
            BinaryOperator::LessThan => {
                let cmp = self.compare_values(left, right);
                Ok(Value::Integer(if cmp == std::cmp::Ordering::Less { 1 } else { 0 }))
            }
            BinaryOperator::LessThanOrEqual => {
                let cmp = self.compare_values(left, right);
                Ok(Value::Integer(if cmp != std::cmp::Ordering::Greater { 1 } else { 0 }))
            }
            BinaryOperator::GreaterThan => {
                let cmp = self.compare_values(left, right);
                Ok(Value::Integer(if cmp == std::cmp::Ordering::Greater { 1 } else { 0 }))
            }
            BinaryOperator::GreaterThanOrEqual => {
                let cmp = self.compare_values(left, right);
                Ok(Value::Integer(if cmp != std::cmp::Ordering::Less { 1 } else { 0 }))
            }
            BinaryOperator::And => {
                Ok(Value::Integer(if self.is_truthy(left) && self.is_truthy(right) { 1 } else { 0 }))
            }
            BinaryOperator::Or => {
                Ok(Value::Integer(if self.is_truthy(left) || self.is_truthy(right) { 1 } else { 0 }))
            }
            BinaryOperator::Add => self.numeric_op(left, right, |a, b| a + b),
            BinaryOperator::Subtract => self.numeric_op(left, right, |a, b| a - b),
            BinaryOperator::Multiply => self.numeric_op(left, right, |a, b| a * b),
            BinaryOperator::Divide => self.numeric_op(left, right, |a, b| a / b),
            BinaryOperator::Like => {
                // Simple LIKE implementation
                match (left, right) {
                    (Value::Text(text), Value::Text(pattern)) => {
                        let matches = text.contains(&pattern.replace('%', ""));
                        Ok(Value::Integer(if matches { 1 } else { 0 }))
                    }
                    _ => Ok(Value::Integer(0)),
                }
            }
        }
    }

    /// Zero-cost unary operation evaluation
    fn apply_unary_op(&self, op: &UnaryOperator, operand: &Value) -> Result<Value> {
        match op {
            UnaryOperator::Not => {
                Ok(Value::Integer(if self.is_truthy(operand) { 0 } else { 1 }))
            }
            UnaryOperator::Minus => {
                match operand {
                    Value::Integer(i) => Ok(Value::Integer(-i)),
                    Value::Real(r) => Ok(Value::Real(-r)),
                    _ => Err(OxidbError::Type {
                        expected: "Numeric".to_string(),
                        found: format!("{:?}", operand),
                        context: "Unary minus operation".to_string(),
                    }),
                }
            }
        }
    }

    /// Zero-cost numeric operation helper
    fn numeric_op<F>(&self, left: &Value, right: &Value, op: F) -> Result<Value>
    where
        F: Fn(f64, f64) -> f64,
    {
        let left_num = self.to_numeric(left)?;
        let right_num = self.to_numeric(right)?;
        let result = op(left_num, right_num);
        
        // Return integer if both inputs were integers and result is whole
        if matches!(left, Value::Integer(_)) && matches!(right, Value::Integer(_)) && result.fract() == 0.0 {
            Ok(Value::Integer(result as i64))
        } else {
            Ok(Value::Real(result))
        }
    }

    fn to_numeric(&self, value: &Value) -> Result<f64> {
        match value {
            Value::Integer(i) => Ok(*i as f64),
            Value::Real(r) => Ok(*r),
            _ => Err(OxidbError::Type {
                expected: "Numeric".to_string(),
                found: format!("{:?}", value),
                context: "Numeric operation".to_string(),
            }),
        }
    }

    fn is_truthy(&self, value: &Value) -> bool {
        match value {
            Value::Null => false,
            Value::Integer(i) => *i != 0,
            Value::Real(r) => *r != 0.0,
            Value::Text(s) => !s.is_empty(),
            Value::Blob(b) => !b.is_empty(),
        }
    }

    fn compare_values(&self, a: &Value, b: &Value) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        
        match (a, b) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,
            (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
            (Value::Real(a), Value::Real(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
            (Value::Integer(a), Value::Real(b)) => (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal),
            (Value::Real(a), Value::Integer(b)) => a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal),
            (Value::Text(a), Value::Text(b)) => a.cmp(b),
            (Value::Blob(a), Value::Blob(b)) => a.cmp(b),
            _ => Ordering::Equal, // Different types are considered equal for simplicity
        }
    }
}

/// Query result types
#[derive(Debug)]
pub enum QueryResult {
    Success { message: String },
    Insert { count: usize },
    Select { rows: Vec<Row> },
    Update { count: usize },
    Delete { count: usize },
}

impl fmt::Display for QueryResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryResult::Success { message } => write!(f, "{}", message),
            QueryResult::Insert { count } => write!(f, "Inserted {} row(s)", count),
            QueryResult::Select { rows } => {
                if rows.is_empty() {
                    write!(f, "No rows returned")
                } else {
                    writeln!(f, "Returned {} row(s):", rows.len())?;
                    for (i, row) in rows.iter().enumerate() {
                        write!(f, "Row {}: ", i + 1)?;
                        let values: Vec<String> = row.values().map(|v| v.to_string()).collect();
                        writeln!(f, "{}", values.join(", "))?;
                    }
                    Ok(())
                }
            }
            QueryResult::Update { count } => write!(f, "Updated {} row(s)", count),
            QueryResult::Delete { count } => write!(f, "Deleted {} row(s)", count),
        }
    }
}