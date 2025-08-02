// src/core/query/binder/mod.rs

// Content from binder.rs
use crate::core::query::sql::ast::Statement as AstStatement;
use std::fmt;

#[derive(Debug)]
pub enum BindError {
    NotImplemented { statement_type: String },
}

impl fmt::Display for BindError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotImplemented { statement_type } => {
                write!(f, "Binding not yet implemented for statement: {}", statement_type)
            }
        }
    }
}

impl std::error::Error for BindError {}

#[derive(Debug)]
pub struct BoundStatement {
    pub message: String,
}

#[derive(Debug)]
pub struct Binder {}

impl Binder {
    #[must_use]
    pub const fn new() -> Self {
        Self {}
    }

    pub fn bind_statement(&self, statement: &AstStatement) -> Result<BoundStatement, BindError> {
        let stmt_type = match statement {
            AstStatement::Select(_) => "Select",
            AstStatement::Update(_) => "Update",
            AstStatement::CreateTable(_) => "CreateTable",
            AstStatement::Insert(_) => "Insert",
            AstStatement::Delete(_) => "Delete", // Added Delete arm
            AstStatement::DropTable(_) => "DropTable",
            // The _ arm is unreachable if all AstStatement variants are covered.
            // If AstStatement is non_exhaustive or has other variants, _ might be needed.
            // Assuming for now all variants are covered or it's okay for this to be exhaustive.
        };
        eprintln!("[Binder] Attempting to bind statement: {stmt_type:?}");
        Err(BindError::NotImplemented { statement_type: stmt_type.to_string() })
    }
}

impl Default for Binder {
    fn default() -> Self {
        Self::new()
    }
}
