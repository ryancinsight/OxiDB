use super::core::SqlParser;
use crate::core::query::sql::ast::{
    AggregateFunction, Assignment, AstLiteralValue, AstExpressionValue, Condition, ConditionTree, 
    SelectColumn,
};
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
                // Check for aggregate functions
                let column = if let Some(Token::Identifier(name)) = self.peek() {
                    let upper_name = name.to_uppercase();
                    if matches!(upper_name.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX") {
                        self.parse_aggregate_function()?
                    } else {
                        let qualified_col_name =
                            self.parse_qualified_identifier("Expected column name or '*'")?;
                        SelectColumn::ColumnName(qualified_col_name)
                    }
                } else {
                    let qualified_col_name =
                        self.parse_qualified_identifier("Expected column name or '*'")?;
                    SelectColumn::ColumnName(qualified_col_name)
                };
                columns.push(column);
            }

            if !self.match_token(Token::Comma) {
                break;
            }
            self.consume(Token::Comma)?;
            match self.peek() {
                Some(Token::Identifier(_) | Token::Asterisk) => { /* Good, loop will continue */ }
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
            let value = self.parse_expression_value("Expected value for assignment")?;
            assignments.push(Assignment { column, value }); // Value here is AstExpressionValue
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
    // Parse an expression value (literal or parameter)
    pub(super) fn parse_expression_value(
        &mut self,
        context: &str,
    ) -> Result<AstExpressionValue, SqlParseError> {
        if self.match_token(Token::Parameter) {
            self.consume(Token::Parameter)?;
            let param_index = self.parameter_count;
            self.parameter_count += 1;
            Ok(AstExpressionValue::Parameter(param_index))
        } else if let Some(literal_val) = self.try_parse_literal_value()? {
            Ok(AstExpressionValue::Literal(literal_val))
        } else {
            // Try parsing as identifier (column reference)
            let col_ident = self.parse_qualified_identifier(context)?;
            Ok(AstExpressionValue::ColumnIdentifier(col_ident))
        }
    }

    // Returns Ok(None) if not a literal, Ok(Some(value)) if a literal, Err if parsing starts but fails.
    fn try_parse_literal_value(&mut self) -> Result<Option<AstLiteralValue>, SqlParseError> {
        match self.peek() {
            Some(
                Token::StringLiteral(_)
                | Token::NumericLiteral(_)
                | Token::BooleanLiteral(_)
                | Token::LBracket,
            ) => {
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
    fn parse_condition_factor(&mut self) -> Result<ConditionTree, SqlParseError> {
        if self.peek_is_identifier_str("NOT") {
            self.consume_any().ok_or(SqlParseError::UnexpectedEOF)?; // Consume NOT
            let condition = self.parse_condition_factor()?; // Recurse for the condition to negate
            return Ok(ConditionTree::Not(Box::new(condition)));
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
                return Ok(ConditionTree::Comparison(Condition {
                    column,
                    operator: final_operator,
                    value: AstExpressionValue::Literal(AstLiteralValue::Null),
                }));
            }

            let operator = self.expect_operator_any(
                &["=", "!=", "<", ">", "<=", ">="],
                "Expected comparison operator in condition",
            )?;

            // Attempt to parse RHS as literal, parameter, or qualified identifier
            let rhs_value = if self.match_token(Token::Parameter) {
                // Handle parameter placeholder
                self.consume(Token::Parameter)?;
                // For now, we'll use a simple counter for parameter indices
                // This will need to be enhanced to track parameter positions properly
                let param_index = self.parameter_count;
                self.parameter_count += 1;
                AstExpressionValue::Parameter(param_index)
            } else if let Some(literal_val) = self.try_parse_literal_value()? {
                AstExpressionValue::Literal(literal_val)
            } else {
                // Not a literal, try parsing as a qualified identifier
                let col_ident = self.parse_qualified_identifier(
                    "Expected literal, parameter (?), or column identifier for RHS of condition",
                )?;
                AstExpressionValue::ColumnIdentifier(col_ident)
            };

            Ok(ConditionTree::Comparison(Condition { column, operator, value: rhs_value }))
        }
    }

    // Parses AND conditions (higher precedence than OR)
    fn parse_condition_term(&mut self) -> Result<ConditionTree, SqlParseError> {
        let mut left = self.parse_condition_factor()?;
        while self.peek_is_identifier_str("AND") {
            self.consume_any().ok_or(SqlParseError::UnexpectedEOF)?; // Consume AND
            let right = self.parse_condition_factor()?;
            left = ConditionTree::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    // Parses OR conditions (lower precedence than AND)
    pub(super) fn parse_condition_expr(&mut self) -> Result<ConditionTree, SqlParseError> {
        let mut left = self.parse_condition_term()?;
        while self.peek_is_identifier_str("OR") {
            self.consume_any().ok_or(SqlParseError::UnexpectedEOF)?; // Consume OR
            let right = self.parse_condition_term()?;
            left = ConditionTree::Or(Box::new(left), Box::new(right));
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
                            found: format!("Identifier({ident})"),
                            position: error_pos,
                        })
                    }
                }
                Some(other) => Err(SqlParseError::UnexpectedToken {
                    expected: error_msg_context.to_string(),
                    found: format!("{other:?}"),
                    position: error_pos,
                }),
                None => Err(SqlParseError::UnexpectedEOF),
            }
        }
    }

    /// Parse an aggregate function like COUNT(*), SUM(column), etc.
    fn parse_aggregate_function(&mut self) -> Result<SelectColumn, SqlParseError> {
        let func_name = match self.peek() {
            Some(Token::Identifier(name)) => name.to_uppercase(),
            _ => return Err(SqlParseError::UnexpectedToken {
                expected: "aggregate function name".to_string(),
                found: format!("{:?}", self.peek().unwrap_or(&Token::EOF)),
                position: self.current_token_pos(),
            }),
        };
        
        self.consume_any().ok_or(SqlParseError::UnexpectedEOF)?; // Consume function name
        self.consume(Token::LParen)?;
        
        let function = match func_name.as_str() {
            "COUNT" => AggregateFunction::Count,
            "SUM" => AggregateFunction::Sum,
            "AVG" => AggregateFunction::Avg,
            "MIN" => AggregateFunction::Min,
            "MAX" => AggregateFunction::Max,
            _ => return Err(SqlParseError::UnexpectedToken {
                expected: "aggregate function".to_string(),
                found: func_name,
                position: self.current_token_pos(),
            }),
        };
        
        // Parse the column or * for COUNT(*)
        let column = if self.match_token(Token::Asterisk) {
            self.consume(Token::Asterisk)?;
            Box::new(SelectColumn::Asterisk)
        } else {
            let col_name = self.parse_qualified_identifier("Expected column name")?;
            Box::new(SelectColumn::ColumnName(col_name))
        };
        
        self.consume(Token::RParen)?;
        
        // Check for optional alias (AS alias_name)
        let alias = if self.peek_is_identifier_str("AS") {
            self.consume_any().ok_or(SqlParseError::UnexpectedEOF)?; // Consume AS
            Some(self.expect_identifier("Expected alias name after AS")?)
        } else {
            None
        };
        
        Ok(SelectColumn::AggregateFunction {
            function,
            column,
            alias,
        })
    }
}
