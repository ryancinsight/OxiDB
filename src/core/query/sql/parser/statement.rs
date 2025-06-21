use super::core::SqlParser;
use crate::core::query::sql::ast::{
    self,
    CreateTableStatement,
    SelectStatement,
    Statement,
    UpdateStatement, // Removed ColumnDef
};
use crate::core::query::sql::ast; // Already imported but ensure it's available for AstDataType
use crate::core::query::sql::errors::SqlParseError;
use crate::core::query::sql::tokenizer::Token; // For matching specific tokens like Token::Where

impl SqlParser {
    // Adding the new method here
    fn parse_data_type_definition(&mut self) -> Result<ast::AstDataType, SqlParseError> {
        let type_ident_token_pos = self.current_token_pos();
        let type_name_ident = self.expect_identifier("Expected data type name (e.g., INTEGER, TEXT, VECTOR)")?;
        let type_name_upper = type_name_ident.to_uppercase();

        // Handle types with parameters like VARCHAR(255) or DECIMAL(10,2) if needed here
        // For now, we primarily focus on VECTOR[dim] and simple types.
        // The old logic for `data_type_string.push('('); ... data_type_string.push(')');`
        // would need to be integrated if we want to capture the full string for types
        // not explicitly handled by AstDataType variants with parameters.

        match type_name_upper.as_str() {
            "INTEGER" | "INT" => Ok(ast::AstDataType::Integer),
            "TEXT" | "STRING" => Ok(ast::AstDataType::Text), // VARCHAR would need param parsing
            "BOOLEAN" | "BOOL" => Ok(ast::AstDataType::Boolean),
            "FLOAT" | "REAL" | "DOUBLE" => Ok(ast::AstDataType::Float),
            "BLOB" => Ok(ast::AstDataType::Blob),
            "VECTOR" => {
                self.consume(Token::LBracket)?;
                let dim_token_pos = self.current_token_pos();
                let dim_str = match self.consume_any() {
                    Some(Token::NumericLiteral(s)) => s,
                    Some(other) => return Err(SqlParseError::UnexpectedToken {
                        expected: "numeric dimension for VECTOR type".to_string(),
                        found: format!("{:?}", other),
                        position: dim_token_pos,
                    }),
                    None => return Err(SqlParseError::UnexpectedEOF),
                };
                let dimension = dim_str.parse::<u32>().map_err(|_| SqlParseError::InvalidDataTypeParameter {
                    type_name: "VECTOR".to_string(),
                    parameter: dim_str.clone(), // Use clone if original dim_str is needed later
                    position: dim_token_pos,
                    reason: "Dimension must be a positive integer".to_string(),
                })?;
                if dimension == 0 {
                     return Err(SqlParseError::InvalidDataTypeParameter {
                        type_name: "VECTOR".to_string(),
                        parameter: dim_str,
                        position: dim_token_pos,
                        reason: "Dimension must be greater than 0".to_string(),
                    });
                }
                self.consume(Token::RBracket)?;
                Ok(ast::AstDataType::Vector { dimension })
            }
            // Example for a type with parameters (like VARCHAR)
            // "VARCHAR" => {
            //     if self.match_token(Token::LParen) {
            //         self.consume(Token::LParen)?;
            //         // Parse length, etc.
            //         // For now, AstDataType doesn't store these params for VARCHAR.
            //         // This would require AstDataType::Varchar { length: Option<u32> }
            //         // Skipping detailed parsing for now.
            //         while !self.match_token(Token::RParen) && !self.is_at_end() {
            //             self.consume_any(); // Just consume to get past params for now
            //         }
            //         self.consume(Token::RParen)?;
            //     }
            //     Ok(ast::AstDataType::Text) // Map to generic Text
            // }
            _ => Err(SqlParseError::UnknownDataType(type_name_ident, type_ident_token_pos)),
        }
    }

    // Helper to expect a specific identifier, case-insensitive
    fn expect_specific_identifier(
        &mut self,
        expected: &str,
        _error_msg_if_not_specific: &str,
    ) -> Result<String, SqlParseError> {
        let token_pos = self.current_token_pos();
        match self.consume_any() {
            Some(Token::Identifier(ident)) => {
                if ident.eq_ignore_ascii_case(expected) {
                    Ok(ident)
                } else {
                    Err(SqlParseError::UnexpectedToken {
                        expected: expected.to_string(),
                        found: format!("Identifier({})", ident),
                        position: token_pos,
                    })
                }
            }
            Some(other_token) => Err(SqlParseError::UnexpectedToken {
                expected: expected.to_string(),
                found: format!("{:?}", other_token),
                position: token_pos,
            }),
            None => Err(SqlParseError::UnexpectedToken {
                // Changed from CustomError
                expected: expected.to_string(),
                found: "EOF".to_string(),
                position: token_pos, // Position where the token was expected
            }),
        }
    }

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

            // Use the new method to parse data type definition
            let ast_data_type = self.parse_data_type_definition()?;

            let mut constraints = Vec::new();
            loop {
                // Peek and match for constraint keywords
                // Using eq_ignore_ascii_case for case-insensitivity of keywords
                if self.peek_is_identifier_str("NOT") {
                    self.consume_any(); // Consume NOT
                    self.expect_specific_identifier("NULL", "Expected NULL after NOT")?;
                    constraints.push(ast::AstColumnConstraint::NotNull);
                } else if self.peek_is_identifier_str("PRIMARY") {
                    self.consume_any(); // Consume PRIMARY
                    self.expect_specific_identifier("KEY", "Expected KEY after PRIMARY")?;
                    constraints.push(ast::AstColumnConstraint::PrimaryKey);
                } else if self.peek_is_identifier_str("UNIQUE") {
                    self.consume_any(); // Consume UNIQUE
                    constraints.push(ast::AstColumnConstraint::Unique);
                } else {
                    break; // No more constraint keywords for this column
                }
            }

            columns.push(ast::ColumnDef {
                name: column_name,
                data_type: ast_data_type, // Use the parsed AstDataType
                constraints,
            });

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
        loop {
            // Loop to parse multiple VALUES sets
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
        Ok(Statement::Delete(ast::DeleteStatement { table_name, condition }))
    }
}
