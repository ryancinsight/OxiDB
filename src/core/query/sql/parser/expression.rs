use super::core::SqlParser;
use crate::core::query::sql::ast::{self, Assignment, AstLiteralValue, Condition, ConditionTree, SelectColumn};
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
            // After a comma, we must find another column name.
            // If we find FROM, or EOF, or anything not an identifier, it's an error (likely a trailing comma).
            match self.peek() {
                Some(Token::Identifier(_)) => { /* Good, loop will continue */ }
                Some(Token::Asterisk) => { /* Also valid after a comma, though unusual for hand-written SQL, e.g. SELECT col1, * */ }
                Some(next_token) => {
                    // If the next token is FROM, it's a clear trailing comma before FROM.
                    // Otherwise, it's some other unexpected token where an identifier was expected.
                    let found_str = format!("{:?}", next_token);
                    return Err(SqlParseError::UnexpectedToken {
                        expected: "column name or '*' after comma".to_string(),
                        found: found_str,
                        position: self.current_token_pos(),
                    });
                }
                None => { // EOF after comma
                    return Err(SqlParseError::UnexpectedToken {
                        expected: "column name or '*' after comma".to_string(),
                        found: "EOF".to_string(),
                        position: self.current_token_pos(),
                    });
                }
            }
        }
        if columns.is_empty() {
            // This case should ideally be caught by the first call to expect_identifier if nothing is matched.
            // However, keeping it as a safeguard.
            return Err(SqlParseError::UnexpectedToken {
                expected: "column name or '*' for select list".to_string(),
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
            // After a comma, we must find another assignment (starting with an identifier).
            match self.peek() {
                Some(Token::Identifier(_)) => { /* Good, loop will continue */ }
                Some(next_token) => {
                    // If it's not an identifier, it's an error (e.g., trailing comma before WHERE).
                    let found_str = format!("{:?}", next_token);
                    return Err(SqlParseError::UnexpectedToken {
                        expected: "column name for assignment after comma".to_string(),
                        found: found_str,
                        position: self.current_token_pos(),
                    });
                }
                None => { // EOF after comma
                    return Err(SqlParseError::UnexpectedToken {
                        expected: "column name for assignment after comma".to_string(),
                        found: "EOF".to_string(),
                        position: self.current_token_pos(),
                    });
                }
            }
        }
        if assignments.is_empty() {
            // This error should be triggered if the SET clause is present but no assignments are found.
            return Err(SqlParseError::UnexpectedToken {
                expected: "at least one assignment expression for SET clause".to_string(),
                found: format!("{:?}", self.peek().unwrap_or(&Token::EOF)),
                position: self.current_token_pos(),
            });
        }
        Ok(assignments)
    }

    // Parses a base condition, e.g., column = value or (condition_tree)
    fn parse_condition_factor(&mut self) -> Result<ast::ConditionTree, SqlParseError> {
        if self.peek_is_identifier_str("NOT") {
            self.consume_any().ok_or(SqlParseError::UnexpectedEOF)?; // Consume NOT
            let condition = self.parse_condition_factor()?; // Recurse for the condition to negate
            return Ok(ast::ConditionTree::Not(Box::new(condition)));
        }

        if self.match_token(Token::LParen) {
            self.consume(Token::LParen)?;
            let condition = self.parse_condition_expr()?; // Start parsing from the top precedence inside parentheses
            self.consume(Token::RParen)?;
            Ok(condition)
        } else {
            // Base case: a simple comparison or IS NULL / IS NOT NULL
            let column = self.expect_identifier("Expected column name, 'NOT', or '(' for condition")?;

            // Check for IS NULL / IS NOT NULL specifically
            if self.peek_is_identifier_str("IS") {
                self.consume_any().ok_or(SqlParseError::UnexpectedEOF)?; // Consume IS

                let mut is_not = false;
                if self.peek_is_identifier_str("NOT") {
                    self.consume_any().ok_or(SqlParseError::UnexpectedEOF)?; // Consume "NOT"
                    is_not = true;
                }
                self.expect_specific_identifier("NULL", "Expected NULL after IS [NOT]")?;

                let final_operator = if is_not { "IS NOT NULL".to_string() } else { "IS NULL".to_string() };
                return Ok(ast::ConditionTree::Comparison(Condition {
                    column,
                    operator: final_operator,
                    value: ast::AstLiteralValue::Null, // Value is irrelevant for IS NULL/IS NOT NULL
                }));
            }

            // If not "IS", then expect a standard comparison operator
            let operator = self.expect_operator_any(
                &["=", "!=", "<", ">", "<=", ">="], // "IS" removed from here
                "Expected comparison operator in condition",
            )?;

            let value = self.parse_literal_value("Expected value for condition")?;
            Ok(ast::ConditionTree::Comparison(Condition { column, operator, value }))
        }
    }

    // Parses AND conditions (higher precedence than OR)
    fn parse_condition_term(&mut self) -> Result<ast::ConditionTree, SqlParseError> {
        let mut left = self.parse_condition_factor()?;
        while self.peek_is_identifier_str("AND") {
            self.consume_any().ok_or(SqlParseError::UnexpectedEOF)?; // Consume AND
            let right = self.parse_condition_factor()?;
            left = ast::ConditionTree::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    // Parses OR conditions (lower precedence than AND)
    // This is the entry point for parsing a condition expression.
    pub(super) fn parse_condition_expr(&mut self) -> Result<ast::ConditionTree, SqlParseError> {
        let mut left = self.parse_condition_term()?;
        while self.peek_is_identifier_str("OR") {
            self.consume_any().ok_or(SqlParseError::UnexpectedEOF)?; // Consume OR
            let right = self.parse_condition_term()?;
            left = ast::ConditionTree::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
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
                    let next_token_str_for_err = format!("{:?}", self.peek().unwrap_or(&Token::EOF));
                    let current_pos_for_err = self.current_token_pos();
                    self.consume(Token::Comma).map_err(|_e| SqlParseError::UnexpectedToken {
                        expected: "comma or ']' in vector literal".to_string(),
                        found: next_token_str_for_err,
                        position: current_pos_for_err,
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
                },
            Some(other) => Err(SqlParseError::UnexpectedToken {
                expected: error_msg_context.to_string(), // "literal value (string, number, boolean, or NULL)".to_string(),
                found: format!("{:?}", other),
                position: error_pos,
            }),
            None => Err(SqlParseError::UnexpectedEOF) // Comma removed if it was here, or ensure no comma for last arm expression
        } // Closes match
    } // Closes else block for LBracket check
} // Closes fn parse_literal_value

} // Closes impl SqlParser
