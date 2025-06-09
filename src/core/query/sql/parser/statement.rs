use super::core::SqlParser;
use crate::core::query::sql::ast::{SelectStatement, Statement, UpdateStatement};
use crate::core::query::sql::errors::SqlParseError;
use crate::core::query::sql::tokenizer::Token; // For matching specific tokens like Token::Where

impl SqlParser {
    pub fn parse(&mut self) -> Result<Statement, SqlParseError> {
        if self.is_at_end() || (self.peek() == Some(&Token::EOF) && self.tokens.len() == 1) {
            return Err(SqlParseError::UnexpectedEOF);
        }

        let statement = match self.peek() {
            Some(Token::Select) => self.parse_select_statement(),
            Some(Token::Update) => self.parse_update_statement(),
            Some(_other_token) => {
                return Err(SqlParseError::UnknownStatementType(self.current_token_pos()))
            }
            None => return Err(SqlParseError::UnexpectedEOF), // Should be caught by is_at_end earlier
        }?;

        if !self.is_at_end() {
            return Err(SqlParseError::UnexpectedToken {
                expected: "end of statement".to_string(),
                found: format!("{:?}", self.peek().unwrap_or(&Token::EOF)),
                position: self.current_token_pos(),
            });
        }
        Ok(statement)
    }

    pub(super) fn parse_select_statement(&mut self) -> Result<Statement, SqlParseError> {
        self.consume(Token::Select)?;
        let columns = self.parse_select_column_list()?;
        self.consume(Token::From)?;
        let source = self.expect_identifier("Expected table name after FROM")?;
        let condition = if self.match_token(Token::Where) {
            self.consume(Token::Where)?;
            Some(self.parse_condition()?)
        } else {
            None
        };
        if self.match_token(Token::Semicolon) {
            self.consume(Token::Semicolon)?;
        }
        Ok(Statement::Select(SelectStatement { columns, source, condition }))
    }

    pub(super) fn parse_update_statement(&mut self) -> Result<Statement, SqlParseError> {
        self.consume(Token::Update)?;
        let source = self.expect_identifier("Expected table name after UPDATE")?;
        self.consume(Token::Set)?;
        let assignments = self.parse_assignment_list()?;
        let condition = if self.match_token(Token::Where) {
            self.consume(Token::Where)?;
            Some(self.parse_condition()?)
        } else {
            None
        };
        if self.match_token(Token::Semicolon) {
            self.consume(Token::Semicolon)?;
        }
        Ok(Statement::Update(UpdateStatement { source, assignments, condition }))
    }
}
