use super::core::SqlParser;
use crate::core::query::sql::ast::{Assignment, AstLiteralValue, Condition, SelectColumn};
use crate::core::query::sql::errors::SqlParseError;
use crate::core::query::sql::tokenizer::Token; // For matching specific tokens

impl SqlParser {
    pub(super) fn parse_select_column_list(&mut self) -> Result<Vec<SelectColumn>, SqlParseError> {
        let mut columns = Vec::new();
        if self.match_token(Token::Asterisk) {
            self.consume(Token::Asterisk)?;
            columns.push(SelectColumn::Asterisk);
            // After '*', we should not expect more columns in a simple list.
            // If a comma follows '*', it would be a syntax error caught by the next part of the statement parser.
            return Ok(columns);
        }
        loop {
            let col_name = self.expect_identifier("Expected column name or '*'")?;
            columns.push(SelectColumn::ColumnName(col_name));
            if !self.match_token(Token::Comma) {
                break;
            }
            self.consume(Token::Comma)?;
            // Handle trailing comma: if after a comma, we don't find another identifier, it's an error.
            if self.peek().is_none() || !matches!(self.peek(), Some(Token::Identifier(_))) {
                // If it's not an identifier, it's an error (e.g. "SELECT col1, FROM table")
                // The error will be more specifically "Expected Identifier" from next expect_identifier call if loop continued,
                // or caught by subsequent parsing rules. Let's make it explicit here for trailing comma.
                if self.peek() != Some(&Token::Identifier("".to_string()))
                    && self.peek().is_some()
                    && self.peek() != Some(&Token::From)
                {
                    // A bit of a hack to check type
                    return Err(SqlParseError::UnexpectedToken {
                        expected: "column name after comma".to_string(),
                        found: format!("{:?}", self.peek().unwrap_or(&Token::EOF)),
                        position: self.current_token_pos(),
                    });
                }
            }
        }
        if columns.is_empty() {
            return Err(SqlParseError::UnexpectedToken {
                expected: "column name or '*'".to_string(),
                found: format!("{:?}", self.peek().unwrap_or(&Token::EOF)), // EOF if nothing, or current token
                position: self.current_token_pos(),
            });
        }
        Ok(columns)
    }

    pub(super) fn parse_assignment_list(&mut self) -> Result<Vec<Assignment>, SqlParseError> {
        let mut assignments = Vec::new();
        loop {
            let column = self.expect_identifier("Expected column name for assignment")?;
            self.expect_operator("=", "Expected '=' after column name in SET clause")?;
            let value = self.parse_literal_value("Expected value for assignment")?;
            assignments.push(Assignment { column, value });
            if !self.match_token(Token::Comma) {
                break;
            }
            self.consume(Token::Comma)?;
            // Handle trailing comma for assignments
            if (self.peek().is_none() || !matches!(self.peek(), Some(Token::Identifier(_))))
                && self.peek() != Some(&Token::Identifier("".to_string()))
                && self.peek().is_some()
                && self.peek() != Some(&Token::Where)
                && self.peek() != Some(&Token::Semicolon)
                && self.peek() != Some(&Token::EOF)
            {
                return Err(SqlParseError::UnexpectedToken {
                    expected: "column name after comma in SET clause".to_string(),
                    found: format!("{:?}", self.peek().unwrap_or(&Token::EOF)),
                    position: self.current_token_pos(),
                });
            }
        }
        if assignments.is_empty() {
            return Err(SqlParseError::UnexpectedToken {
                expected: "assignment expression for SET clause".to_string(),
                found: format!("{:?}", self.peek().unwrap_or(&Token::EOF)),
                position: self.current_token_pos(),
            });
        }
        Ok(assignments)
    }

    pub(super) fn parse_condition(&mut self) -> Result<Condition, SqlParseError> {
        let column = self.expect_identifier("Expected column name for condition")?;
        let operator = self.expect_operator_any(
            &["=", "!=", "<", ">", "<=", ">="],
            "Expected operator in condition",
        )?;
        let value = self.parse_literal_value("Expected value for condition")?;
        Ok(Condition { column, operator, value })
    }

    pub(super) fn parse_literal_value(
        &mut self,
        error_msg_context: &str,
    ) -> Result<AstLiteralValue, SqlParseError> {
        // Store position before consuming, for more accurate error reporting
        let error_pos = self.current_token_pos();
        // Peek first to decide the parsing path for vectors
        if self.peek() == Some(&Token::LBracket) {
            self.consume(Token::LBracket)?; // Consume '['
            let mut elements = Vec::new();
            if self.peek() != Some(&Token::RBracket) { // Handle non-empty list
                loop {
                    elements.push(self.parse_literal_value("Expected value in vector literal")?);
                    if self.peek() == Some(&Token::RBracket) {
                        break;
                    }
                    self.consume(Token::Comma).map_err(|_e| SqlParseError::UnexpectedToken {
                        expected: "comma or ']' in vector literal".to_string(),
                        found: format!("{:?}", self.peek().unwrap_or(&Token::EOF)),
                        position: self.current_token_pos()
                    })?;
                    // Handle trailing comma before RBracket
                    if self.peek() == Some(&Token::RBracket) {
                         return Err(SqlParseError::UnexpectedToken {
                            expected: "value after comma in vector literal".to_string(),
                            found: "]".to_string(),
                            position: self.current_token_pos(),
                        });
                    }
                }
            }
            self.consume(Token::RBracket)?; // Consume ']'
            Ok(AstLiteralValue::Vector(elements))
        } else {
            // Existing literal parsing logic
            match self.consume_any() {
                Some(Token::StringLiteral(s)) => Ok(AstLiteralValue::String(s)),
                Some(Token::NumericLiteral(n)) => Ok(AstLiteralValue::Number(n)),
                Some(Token::BooleanLiteral(b)) => Ok(AstLiteralValue::Boolean(b)),
                // Handle NULL case-insensitively by checking the original identifier text
                Some(Token::Identifier(ident)) => {
                    if ident.to_uppercase() == "NULL" {
                        Ok(AstLiteralValue::Null)
                } else {
                    // If it's an identifier but not NULL, it's an unexpected token in a literal value context
                    Err(SqlParseError::UnexpectedToken {
                        expected: error_msg_context.to_string(), //"literal value (string, number, boolean, or NULL)".to_string(),
                        found: format!("Identifier({})", ident),
                        position: error_pos,
                    })
                }
            }
            Some(other) => Err(SqlParseError::UnexpectedToken {
                expected: error_msg_context.to_string(), // "literal value (string, number, boolean, or NULL)".to_string(),
                found: format!("{:?}", other),
                position: error_pos,
            }),
            None => Err(SqlParseError::UnexpectedEOF),
        }
    }
}
