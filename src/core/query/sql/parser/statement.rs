use super::core::SqlParser;
use crate::core::query::sql::ast::{
    self, ColumnDef, CreateTableStatement, SelectStatement, Statement, UpdateStatement,
};
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
            Some(Token::Create) => self.parse_create_table_statement(),
            Some(Token::Insert) => self.parse_insert_statement(),
            Some(Token::Delete) => self.parse_delete_statement(), // Added
            Some(_other_token) => {
                return Err(SqlParseError::UnknownStatementType(self.current_token_pos()))
            }
            None => return Err(SqlParseError::UnexpectedEOF), // Should be caught by is_at_end earlier
        }?;

        // After specific statement node is parsed (e.g. Select, Update)
        // Check for optional semicolon
        if self.match_token(Token::Semicolon) {
            self.consume(Token::Semicolon)?;
        }

        // Now, after optional semicolon, the next token MUST be EOF.
        if self.peek() != Some(&Token::EOF) && !self.is_at_end() {
            return Err(SqlParseError::UnexpectedToken {
                expected: "end of statement or EOF".to_string(),
                found: format!("{:?}", self.peek().unwrap_or(&Token::EOF)),
                position: self.current_token_pos(),
            });
        }
        Ok(statement)
    }

    fn parse_create_table_statement(&mut self) -> Result<Statement, SqlParseError> {
        self.consume(Token::Create)?;
        self.consume(Token::Table)?;
        let table_name = self.expect_identifier("Expected table name after CREATE TABLE")?;

        self.consume(Token::LParen)?;

        let mut columns = Vec::new();
        if self.match_token(Token::RParen) {
            return Err(SqlParseError::UnexpectedToken {
                expected: "column definition".to_string(),
                found: ")".to_string(), // Found RParen instead of a column name
                position: self.current_token_pos(),
            });
        }

        loop {
            let column_name = self.expect_identifier("Expected column name in CREATE TABLE")?;
            let mut data_type_string =
                self.expect_identifier("Expected column data type in CREATE TABLE")?;

            // Check for and consume type parameters like VARCHAR(255)
            if self.match_token(Token::LParen) {
                self.consume(Token::LParen)?; // Consume '('
                data_type_string.push('(');

                let mut first_param_token = true;
                // Consume tokens within type parameters
                while !self.match_token(Token::RParen) && !self.is_at_end() {
                    if !first_param_token {
                        // If not the first token after '(', and we see a comma, consume it and add to string
                        if self.match_token(Token::Comma) {
                            self.consume_any(); // Consume comma
                            data_type_string.push_str(", ");
                        } else {
                            // If not a comma, assume it's part of a multi-token parameter or end of params
                            // This part might need more robust handling for complex type params
                        }
                    }
                    match self.peek().cloned() {
                        Some(Token::NumericLiteral(n)) => {
                            data_type_string.push_str(&n);
                            self.consume_any();
                        }
                        Some(Token::Identifier(s)) => {
                            data_type_string.push_str(&s);
                            self.consume_any();
                        }
                        // Potentially handle other literal types if type definitions can include them
                        _ => break, // Stop if unexpected token in type params
                    }
                    first_param_token = false;
                }
                if !self.match_token(Token::RParen) {
                    // Check if RParen is present after params
                    return Err(SqlParseError::UnexpectedToken {
                        expected: ")".to_string(),
                        found: format!("{:?}", self.peek().unwrap_or(&Token::EOF)),
                        position: self.current_token_pos(),
                    });
                }
                self.consume(Token::RParen)?; // Consume ')'
                data_type_string.push(')');
            }

            // Consume other constraints like PRIMARY KEY after type and its params
            while !self.match_token(Token::Comma)
                && !self.match_token(Token::RParen)
                && !self.is_at_end()
            {
                match self.peek() {
                    // Keywords like PRIMARY, KEY, NOT, NULL are often tokenized as Identifier
                    // if they are not specific keywords in the Token enum for this context.
                    Some(Token::Identifier(_)) => {
                        // For now, append to data_type_string to see what's captured.
                        // A proper system would parse these into AST constraint nodes.
                        if let Some(Token::Identifier(constraint_part)) = self.consume_any() {
                            data_type_string.push(' ');
                            data_type_string.push_str(&constraint_part);
                        }
                    }
                    // If specific keywords for constraints exist (e.g., Token::Primary, Token::Key), handle them here.
                    _ => break, // Stop if it's not an identifier-like constraint
                }
            }

            columns.push(ColumnDef { name: column_name, data_type: data_type_string });

            if self.match_token(Token::RParen) {
                // This RParen should be the one for the column list
                break;
            }

            let found_token_for_err = format!("{:?}", self.peek().unwrap_or(&Token::EOF));
            let current_pos_for_err = self.current_token_pos();
            self.consume(Token::Comma).map_err(|_| SqlParseError::UnexpectedToken {
                expected: "comma or )".to_string(),
                found: found_token_for_err,
                position: current_pos_for_err,
            })?;

            let trailing_comma_pos = self.current_token_pos();
            if self.match_token(Token::RParen) {
                return Err(SqlParseError::UnexpectedToken {
                    expected: "column definition".to_string(),
                    found: ")".to_string(),
                    position: trailing_comma_pos,
                });
            }
        }
        self.consume(Token::RParen)?; // Consume the final RParen of the column list

        // Semicolon handled by main parse()
        Ok(Statement::CreateTable(CreateTableStatement { table_name, columns }))
    }

    fn parse_insert_statement(&mut self) -> Result<Statement, SqlParseError> {
        self.consume(Token::Insert)?;
        self.consume(Token::Into)?;
        let table_name = self.expect_identifier("Expected table name after INSERT INTO")?;

        let mut columns: Option<Vec<String>> = None;
        if self.match_token(Token::LParen) {
            self.consume(Token::LParen)?;
            let mut cols = Vec::new();
            if !self.match_token(Token::RParen) {
                // Check if not empty list like "()"
                loop {
                    cols.push(self.expect_identifier("Expected column name in INSERT")?);
                    if self.match_token(Token::RParen) {
                        break;
                    }
                    self.consume(Token::Comma)?;
                }
            }
            self.consume(Token::RParen)?;
            columns = Some(cols);
        }

        self.consume(Token::Values)?;

        let mut values_list = Vec::new();
        loop { // Loop to parse multiple VALUES sets
            self.consume(Token::LParen)?;
            let mut current_values_set = Vec::new();
            if !self.match_token(Token::RParen) {
                // Check if not empty list like "()"
                loop {
                    current_values_set
                        .push(self.parse_literal_value("Expected value in VALUES clause")?);
                    if self.match_token(Token::RParen) {
                        break;
                    }
                    // Comma between values in a single set
                    self.consume(Token::Comma)?;
                }
            }
            self.consume(Token::RParen)?;
            values_list.push(current_values_set);

            // Check for comma between VALUES sets
            if self.match_token(Token::Comma) {
                self.consume(Token::Comma)?; // Consume comma and continue loop
            } else {
                break; // No more VALUES sets, or end of statement
            }
        }

        // Semicolon handled by main parse()

        Ok(Statement::Insert(ast::InsertStatement { table_name, columns, values: values_list }))
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
        // Semicolon handled by main parse()
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
        // Semicolon handled by main parse()
        Ok(Statement::Update(UpdateStatement { source, assignments, condition }))
    }

    // Placeholder for DELETE statement parsing
    fn parse_delete_statement(&mut self) -> Result<Statement, SqlParseError> {
        self.consume(Token::Delete)?;
        self.consume(Token::From)?;
        let table_name = self.expect_identifier("Expected table name after DELETE FROM")?;
        let condition = if self.match_token(Token::Where) {
            self.consume(Token::Where)?;
            Some(self.parse_condition()?)
        } else {
            None // Or error if WHERE clause is mandatory for DELETE
        };
        // Semicolon handled by main parse()

        // ast::DeleteStatement is now used.
        Ok(Statement::Delete(ast::DeleteStatement {
            table_name,
            condition,
        }))
    }
}
