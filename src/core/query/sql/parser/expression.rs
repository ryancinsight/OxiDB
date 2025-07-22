use super::core::SqlParser;
use crate::core::query::sql::ast::{self, Assignment, AstLiteralValue, Condition, SelectColumn};
use crate::core::query::sql::errors::SqlParseError;
use crate::core::query::sql::tokenizer::Token; // For matching specific tokens

impl SqlParser {
    // Helper to parse a simple or qualified identifier (e.g., column or table.column)
    pub(super) fn parse_qualified_identifier(
        &mut self,
        context_msg: &str,
    ) -> Result<String, SqlParseError> {
        let mut parts = Vec::new();
        parts.push(self.expect_identifier(context_msg)?);
        while self.match_token(Token::Dot) {
            self.consume(Token::Dot)?;
            parts.push(self.expect_identifier("Expected identifier after dot")?);
        }
        Ok(parts.join("."))
    }

    pub(super) fn parse_select_column_list(&mut self) -> Result<Vec<SelectColumn>, SqlParseError> {
        let mut columns = Vec::new();
        if self.match_token(Token::Asterisk) {
            self.consume(Token::Asterisk)?;
            // TODO: Handle qualified asterisk like table.* if needed in SelectColumn AST
            columns.push(SelectColumn::Asterisk);
            // If only an asterisk, no more columns expected unless there's a comma,
            // but standard SQL usually doesn't mix '*' with specific columns in the main list this way
            // (though some dialects might allow `SELECT *, col1 FROM ...`).
            // For now, if it's just "*", we return. If it's part of a list, the loop handles it.
            if !self.match_token(Token::Comma) {
                // If just "SELECT *", return early
                return Ok(columns);
            }
        }

        if !matches!(self.peek(), Some(Token::Identifier(_)))
            && !self.match_token(Token::Asterisk)
            && columns.is_empty()
        {
            return Err(SqlParseError::UnexpectedToken {
                expected: "column name or '*'".to_string(),
                found: format!("{:?}", self.peek().unwrap_or(&Token::EOF)),
                position: self.current_token_pos(),
            });
        }

        loop {
            if self.match_token(Token::Asterisk) {
                self.consume(Token::Asterisk)?;
                columns.push(SelectColumn::Asterisk);
            } else {
                let qualified_col_name =
                    self.parse_qualified_identifier("Expected column name or '*'")?;
                columns.push(SelectColumn::ColumnName(qualified_col_name));
            }

            if !self.match_token(Token::Comma) {
                break;
            }
            self.consume(Token::Comma)?;
            match self.peek() {
                Some(Token::Identifier(_) | Token::Asterisk) => { /* Good, loop will continue */
                }
                Some(next_token) => {
                    return Err(SqlParseError::UnexpectedToken {
                        expected: "column name or '*' after comma".to_string(),
                        found: format!("{:?}", next_token.clone()),
                        position: self.current_token_pos(),
                    });
                }
                None => {
                    return Err(SqlParseError::UnexpectedToken {
                        expected: "column name or '*' after comma".to_string(),
                        found: "EOF".to_string(),
                        position: self.current_token_pos(),
                    });
                }
            }
        }
        if columns.is_empty() {
            return Err(SqlParseError::UnexpectedToken {
                expected: "column name or '*' for select list".to_string(),
                found: format!("{:?}", self.peek().unwrap_or(&Token::EOF)),
                position: self.current_token_pos(),
            });
        }
        Ok(columns)
    }

    pub(super) fn parse_assignment_list(&mut self) -> Result<Vec<Assignment>, SqlParseError> {
        let mut assignments = Vec::new();
        if !matches!(self.peek(), Some(Token::Identifier(_))) {
            return Err(SqlParseError::UnexpectedToken {
                expected: "column name for assignment".to_string(),
                found: format!("{:?}", self.peek().unwrap_or(&Token::EOF)),
                position: self.current_token_pos(),
            });
        }
        loop {
            // LHS of assignment is typically a simple column, not qualified, but could be.
            // For now, assume simple identifier for SET column = ...
            let column = self.expect_identifier("Expected column name for assignment")?;
            self.expect_operator("=", "Expected '=' after column name in SET clause")?;
            let value = self.parse_literal_value("Expected value for assignment")?;
            assignments.push(Assignment { column, value }); // Value here is AstLiteralValue
            if !self.match_token(Token::Comma) {
                break;
            }
            self.consume(Token::Comma)?;
            match self.peek() {
                Some(Token::Identifier(_)) => { /* Good, loop will continue */ }
                Some(next_token) => {
                    return Err(SqlParseError::UnexpectedToken {
                        expected: "column name for assignment after comma".to_string(),
                        found: format!("{:?}", next_token.clone()),
                        position: self.current_token_pos(),
                    });
                }
                None => {
                    return Err(SqlParseError::UnexpectedToken {
                        expected: "column name for assignment after comma".to_string(),
                        found: "EOF".to_string(),
                        position: self.current_token_pos(),
                    });
                }
            }
        }
        if assignments.is_empty() {
            return Err(SqlParseError::UnexpectedToken {
                expected: "at least one assignment expression for SET clause".to_string(),
                found: format!("{:?}", self.peek().unwrap_or(&Token::EOF)),
                position: self.current_token_pos(),
            });
        }
        Ok(assignments)
    }

    // Helper to attempt parsing a literal value. Does not consume if it's not a clear literal start.
    // Returns Ok(None) if not a literal, Ok(Some(value)) if a literal, Err if parsing starts but fails.
    fn try_parse_literal_value(&mut self) -> Result<Option<AstLiteralValue>, SqlParseError> {
        match self.peek() {
            Some(Token::StringLiteral(_) | Token::NumericLiteral(_) |
            Token::BooleanLiteral(_) | Token::LBracket) => {
                // For vector literals
                // These are definitively literals.
                self.parse_literal_value("literal value").map(Some)
            }
            Some(Token::Identifier(ident)) => {
                // Check if it's NULL, TRUE, or FALSE (which parse_literal_value handles)
                let upper_ident = ident.to_uppercase();
                if upper_ident == "NULL" || upper_ident == "TRUE" || upper_ident == "FALSE" {
                    self.parse_literal_value("literal value (NULL, TRUE, FALSE)").map(Some)
                } else {
                    Ok(None) // It's an identifier, but not a literal keyword
                }
            }
            _ => Ok(None), // Not a token that starts a literal
        }
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
            let condition = self.parse_condition_expr()?;
            self.consume(Token::RParen)?;
            Ok(condition)
        } else {
            let column = self
                .parse_qualified_identifier("Expected column name, 'NOT', or '(' for condition")?;

            if self.peek_is_identifier_str("IS") {
                self.consume_any().ok_or(SqlParseError::UnexpectedEOF)?; // Consume IS
                let mut is_not = false;
                if self.peek_is_identifier_str("NOT") {
                    self.consume_any().ok_or(SqlParseError::UnexpectedEOF)?;
                    is_not = true;
                }
                self.expect_specific_identifier("NULL", "Expected NULL after IS [NOT]")?;
                let final_operator =
                    if is_not { "IS NOT NULL".to_string() } else { "IS NULL".to_string() };
                return Ok(ast::ConditionTree::Comparison(Condition {
                    column,
                    operator: final_operator,
                    value: ast::AstExpressionValue::Literal(ast::AstLiteralValue::Null),
                }));
            }

            let operator = self.expect_operator_any(
                &["=", "!=", "<", ">", "<=", ">="],
                "Expected comparison operator in condition",
            )?;

            // Attempt to parse RHS as literal, then as qualified identifier
            let rhs_value = match self.try_parse_literal_value()? {
                Some(literal_val) => ast::AstExpressionValue::Literal(literal_val),
                None => {
                    // Not a literal, try parsing as a qualified identifier
                    let col_ident = self.parse_qualified_identifier(
                        "Expected literal or column identifier for RHS of condition",
                    )?;
                    ast::AstExpressionValue::ColumnIdentifier(col_ident)
                }
            };

            Ok(ast::ConditionTree::Comparison(Condition { column, operator, value: rhs_value }))
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
        let error_pos = self.current_token_pos();
        if self.peek() == Some(&Token::LBracket) {
            self.consume(Token::LBracket)?;
            let mut elements = Vec::new();
            if self.peek() != Some(&Token::RBracket) {
                loop {
                    elements.push(self.parse_literal_value("Expected value in vector literal")?);
                    if self.peek() == Some(&Token::RBracket) {
                        break;
                    }
                    let next_token_str_for_err =
                        format!("{:?}", self.peek().unwrap_or(&Token::EOF));
                    let current_pos_for_err = self.current_token_pos();
                    self.consume(Token::Comma).map_err(|_e| SqlParseError::UnexpectedToken {
                        expected: "comma or ']' in vector literal".to_string(),
                        found: next_token_str_for_err,
                        position: current_pos_for_err,
                    })?;
                    if self.peek() == Some(&Token::RBracket) {
                        return Err(SqlParseError::UnexpectedToken {
                            expected: "value after comma in vector literal".to_string(),
                            found: "]".to_string(),
                            position: self.current_token_pos(),
                        });
                    }
                }
            }
            self.consume(Token::RBracket)?;
            Ok(AstLiteralValue::Vector(elements))
        } else {
            match self.consume_any() {
                Some(Token::StringLiteral(s)) => Ok(AstLiteralValue::String(s)),
                Some(Token::NumericLiteral(n)) => Ok(AstLiteralValue::Number(n)),
                Some(Token::BooleanLiteral(b)) => Ok(AstLiteralValue::Boolean(b)),
                Some(Token::Identifier(ident)) => {
                    let upper_ident = ident.to_uppercase();
                    if upper_ident == "NULL" {
                        Ok(AstLiteralValue::Null)
                    } else if upper_ident == "TRUE" {
                        Ok(AstLiteralValue::Boolean(true)) // Allow TRUE/FALSE as identifiers to become literals
                    } else if upper_ident == "FALSE" {
                        Ok(AstLiteralValue::Boolean(false))
                    } else {
                        Err(SqlParseError::UnexpectedToken {
                            expected: error_msg_context.to_string(),
                            found: format!("Identifier({})", ident),
                            position: error_pos,
                        })
                    }
                }
                Some(other) => Err(SqlParseError::UnexpectedToken {
                    expected: error_msg_context.to_string(),
                    found: format!("{:?}", other),
                    position: error_pos,
                }),
                None => Err(SqlParseError::UnexpectedEOF),
            }
        }
    }
}
