// src/core/query/binder/binder.rs
use crate::core::query::sql::ast::Statement as AstStatement;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum BindError {
    #[error("Binding not yet implemented for statement: {statement_type}")]
    NotImplemented { statement_type: String },
}

#[derive(Debug)]
pub struct BoundStatement {
    pub message: String,
}

#[derive(Debug)]
pub struct Binder {}

impl Binder {
    pub fn new() -> Self {
        Binder {}
    }

    pub fn bind_statement(&self, statement: &AstStatement) -> Result<BoundStatement, BindError> {
        let stmt_type = match statement {
            AstStatement::Select(_) => "Select",
            AstStatement::Update(_) => "Update",
            AstStatement::CreateTable(_) => "CreateTable",
            AstStatement::Insert(_) => "Insert",
            _ => "Unknown",
        };
        eprintln!("[Binder] Attempting to bind statement: {:?}", stmt_type);
        Err(BindError::NotImplemented { statement_type: stmt_type.to_string() })
    }
}
