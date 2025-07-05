use super::core::SqlParser;
use crate::core::query::sql::ast::{self, Assignment, AstLiteralValue, SelectColumn}; // Removed Condition
use crate::core::query::sql::errors::SqlParseError;
use crate::core::query::sql::tokenizer::Token; // For matching specific tokens

impl SqlParser {
    // Helper to parse a simple or qualified identifier (e.g., column or table.column)
    pub(super) fn parse_qualified_identifier(&mut self, context_msg: &str) -> Result<String, SqlParseError> {
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
        loop {
            if self.match_token(Token::Asterisk) {
                // COUNT(*) is handled within parse_primary_expression's function parsing.
                // A standalone '*' here means SELECT *.
                self.consume(Token::Asterisk)?;
                columns.push(SelectColumn::Asterisk);
            } else {
                // Otherwise, parse a full expression.
                let expr = self.parse_expression()?;
                columns.push(SelectColumn::Expression(expr));
            }

            if !self.match_token(Token::Comma) {
                break; // End of column list
            }
            self.consume(Token::Comma)?; // Consume comma

            // Check for trailing comma: next token must be start of an expression or an asterisk
            if !self.is_next_token_an_expression_start() && !self.match_token(Token::Asterisk) {
                return Err(SqlParseError::UnexpectedToken {
                    expected: "expression or '*' after comma in select list".to_string(),
                    found: format!("{:?}", self.peek().unwrap_or(&Token::EOF)),
                    position: self.current_token_pos(),
                });
            }
        }

        if columns.is_empty() {
            return Err(SqlParseError::UnexpectedToken {
                expected: "select column list (expression or '*')".to_string(),
                found: format!("{:?}", self.peek().unwrap_or(&Token::EOF)),
                position: self.current_token_pos(),
            });
        }
        // Validation: if Asterisk is present, it must be the only item.
        if columns.len() > 1 && columns.iter().any(|c| matches!(c, SelectColumn::Asterisk)) {
            let asterisk_pos = self.tokens.iter().position(|t| *t == Token::Asterisk).unwrap_or_else(|| self.current_token_pos());
            return Err(SqlParseError::InvalidExpression(asterisk_pos,
                "Asterisk '*' must be the only item in the select list if used without qualification (e.g. not in COUNT(*)).".to_string()));
        }

        Ok(columns)
    }

    // Helper to check if the next token can start an expression (for trailing comma detection)
    fn is_next_token_an_expression_start(&self) -> bool {
        match self.peek() {
            Some(Token::Identifier(_)) |
            Some(Token::StringLiteral(_)) |
            Some(Token::NumericLiteral(_)) |
            Some(Token::BooleanLiteral(_)) |
            Some(Token::LParen) |
            Some(Token::LBracket) => true,
            Some(Token::Operator(op_str)) => op_str == "-" || op_str == "+",
            _ => false,
        }
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
            let value_expr = self.parse_expression()?; // Parse RHS as a full expression
            assignments.push(Assignment { column, value: value_expr });
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

    fn map_operator_str_to_ast_comparison_operator(&self, op_str: &str) -> Result<ast::AstComparisonOperator, SqlParseError> {
        match op_str {
            "=" => Ok(ast::AstComparisonOperator::Equals),
            "!=" => Ok(ast::AstComparisonOperator::NotEquals),
            "<>" => Ok(ast::AstComparisonOperator::NotEquals),
            "<" => Ok(ast::AstComparisonOperator::LessThan),
            "<=" => Ok(ast::AstComparisonOperator::LessThanOrEquals),
            ">" => Ok(ast::AstComparisonOperator::GreaterThan),
            ">=" => Ok(ast::AstComparisonOperator::GreaterThanOrEquals),
            _ => Err(SqlParseError::InvalidExpression(self.current_token_pos(), format!("Internal Error: Unmappable comparison operator string: {}", op_str))),
        }
    }

    // This function will parse the core of a comparison: expr1 op expr2 or expr1 IS [NOT] NULL
    fn parse_comparison_detail(&mut self) -> Result<ast::ConditionTree, SqlParseError> {
        let left_expr = self.parse_expression()?;

        if self.peek_is_identifier_str("IS") {
            self.consume_any().ok_or(SqlParseError::UnexpectedEOF)?; // Consume IS
            let mut is_not = false;
            if self.peek_is_identifier_str("NOT") {
                self.consume_any().ok_or(SqlParseError::UnexpectedEOF)?;
                is_not = true;
            }
            self.expect_specific_identifier("NULL", "Expected NULL after IS [NOT]")?;

            let operator = if is_not {
                ast::AstComparisonOperator::IsNotNull
            } else {
                ast::AstComparisonOperator::IsNull
            };
            return Ok(ast::ConditionTree::Comparison(ast::Condition {
                left: left_expr,
                operator,
                right: ast::AstExpression::Literal(ast::AstLiteralValue::Null), // RHS for IS NULL/IS NOT NULL
            }));
        }

        let op_str = self.expect_operator_any(
            &["=", "!=", "<>", "<", ">", "<=", ">="],
            "Expected comparison operator (e.g., =, !=, <, >, <=, >=)",
        )?;
        let comparison_op = self.map_operator_str_to_ast_comparison_operator(&op_str)?;

        let right_expr = self.parse_expression()?;

        Ok(ast::ConditionTree::Comparison(ast::Condition {
            left: left_expr,
            operator: comparison_op,
            right: right_expr,
        }))
    }

    // parse_base_condition_operand handles NOT and parentheses around a condition.
    fn parse_base_condition_operand(&mut self) -> Result<ast::ConditionTree, SqlParseError> {
        if self.peek_is_identifier_str("NOT") {
            // Check if NOT is part of "IS NOT NULL" - this should be handled by parse_comparison_detail
            // To prevent consuming NOT here if it's part of "IS NOT", we need a more careful peek.
            // A simple way: if "NOT" is followed by "IS", it's not a logical NOT here.
            // However, standard SQL `NOT (expr)` is different from `expr IS NOT NULL`.
            // The current `parse_unary_expression` handles `NOT expr`.
            // `ConditionTree::Not` should wrap a `ConditionTree`.
            // Let's assume logical NOT is handled at a higher precedence or as a unary operator on a boolean expression.
            // The current structure of `parse_condition_expr -> parse_condition_term -> parse_base_condition_operand`
            // means `parse_base_condition_operand` should return a `ConditionTree`.
            // `AstExpression::UnaryOp{op: Not, ...}` produces an `AstExpression`.
            // We need `ConditionTree::Not(Box<ConditionTree>)`.
            // So, `parse_unary_expression` handling `NOT` to produce `AstExpression` isn't directly usable
            // for `ConditionTree::Not` unless we have a way to convert that `AstExpression` back to `ConditionTree`.
            // This suggests `NOT` in conditions should be handled by the condition parsing logic itself.

            self.consume_any().ok_or(SqlParseError::UnexpectedEOF)?; // Consume NOT
            let condition = self.parse_base_condition_operand()?; // NOT applies to the next condition unit
            Ok(ast::ConditionTree::Not(Box::new(condition)))
        } else if self.match_token(Token::LParen) {
            self.consume(Token::LParen)?;
            let condition = self.parse_condition_expr()?;
            self.consume(Token::RParen)?;
            Ok(condition)
        } else {
            self.parse_comparison_detail()
        }
    }

    // Parses AND conditions (higher precedence than OR)
    fn parse_condition_term(&mut self) -> Result<ast::ConditionTree, SqlParseError> {
        let mut left = self.parse_base_condition_operand()?; // Changed from parse_condition_factor
        while self.peek_is_identifier_str("AND") {
            self.consume_any().ok_or(SqlParseError::UnexpectedEOF)?; // Consume AND
            let right = self.parse_base_condition_operand()?; // Changed from parse_condition_factor
            left = ast::ConditionTree::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    // Parses OR conditions (lower precedence than AND) - No change to signature or direct calls needed here.
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
        if self.peek() == Some(&Token::LBracket) { // Vector literal
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
                    }
                     else {
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

    // New private method for parsing primary expressions (literals, identifiers, functions, parens)
    fn parse_primary_expression(&mut self) -> Result<ast::AstExpression, SqlParseError> {
        if self.match_token(Token::LParen) {
            self.consume(Token::LParen)?;
            let expr = self.parse_expression()?; // Recursive call for parenthesized expression
            self.consume(Token::RParen)?;
            Ok(expr)
        } else if matches!(self.peek(), Some(Token::StringLiteral(_)) | Some(Token::NumericLiteral(_)) | Some(Token::BooleanLiteral(_)) | Some(Token::LBracket)) ||
                   (matches!(self.peek(), Some(Token::Identifier(_))) && (self.peek_is_identifier_str("NULL") || self.peek_is_identifier_str("TRUE") || self.peek_is_identifier_str("FALSE")))
        {
            // Attempt to parse as a literal
            let literal_val = self.parse_literal_value("Expected literal value in expression")?;
            Ok(ast::AstExpression::Literal(literal_val))
        } else if matches!(self.peek(), Some(Token::Identifier(_))) {
            // Could be a column identifier or a function call
            // Peek ahead to see if it's a function call (identifier followed by LPAREN)
            let is_function_call = if self.current + 1 < self.tokens.len() {
                self.tokens[self.current + 1] == Token::LParen
            } else {
                false
            };

            if is_function_call {
                let func_name = self.expect_identifier("Expected function name")?;
                self.consume(Token::LParen)?;
                let mut args = Vec::new();
                if !self.match_token(Token::RParen) {
                    loop {
                        if func_name.eq_ignore_ascii_case("COUNT") && self.match_token(Token::Asterisk) {
                            self.consume(Token::Asterisk)?;
                            args.push(ast::AstFunctionArg::Asterisk);
                            if !self.match_token(Token::RParen) {
                                return Err(SqlParseError::UnexpectedToken {
                                    expected: ") after * in COUNT(*)".to_string(),
                                    found: format!("{:?}", self.peek().unwrap_or(&Token::EOF)),
                                    position: self.current_token_pos(),
                                });
                            }
                            break;
                        } else if self.peek_is_identifier_str("DISTINCT") {
                            self.consume_any().ok_or(SqlParseError::UnexpectedEOF)?; // Consume DISTINCT
                            let expr = self.parse_expression()?;
                            args.push(ast::AstFunctionArg::Distinct(Box::new(expr)));
                        } else {
                            let expr = self.parse_expression()?;
                            args.push(ast::AstFunctionArg::Expression(expr));
                        }
                        if self.match_token(Token::RParen) {
                            break;
                        }
                        self.consume(Token::Comma)?;
                    }
                }
                self.consume(Token::RParen)?;
                Ok(ast::AstExpression::FunctionCall { name: func_name, args })
            } else {
                // Just a column identifier
                let col_name = self.parse_qualified_identifier("Expected column name or function call in expression")?;
                Ok(ast::AstExpression::ColumnIdentifier(col_name))
            }
        } else {
            Err(SqlParseError::UnexpectedToken {
                expected: "literal, identifier, function call, or parenthesized expression".to_string(),
                found: format!("{:?}", self.peek().unwrap_or(&Token::EOF)),
                position: self.current_token_pos(),
            })
        }
    }

    // Entry point for parsing any expression (starts with lowest precedence)
    pub(super) fn parse_expression(&mut self) -> Result<ast::AstExpression, SqlParseError> {
        self.parse_additive_expression()
    }

    // Handles additive expressions (+, -)
    fn parse_additive_expression(&mut self) -> Result<ast::AstExpression, SqlParseError> {
        let mut left = self.parse_multiplicative_expression()?; // Higher precedence

        while self.match_token(Token::Operator("+".to_string())) || self.match_token(Token::Operator("-".to_string())) {
            let op_token = self.consume_any().ok_or(SqlParseError::UnexpectedEOF)?;
            let op = match op_token {
                Token::Operator(s) if s == "+" => ast::AstArithmeticOperator::Plus,
                Token::Operator(s) if s == "-" => ast::AstArithmeticOperator::Minus,
                _ => return Err(SqlParseError::InvalidExpression(self.current_token_pos() -1, "Expected plus or minus operator".to_string())),
            };
            let right = self.parse_multiplicative_expression()?;
            left = ast::AstExpression::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    // Handles multiplicative expressions (*, /)
    fn parse_multiplicative_expression(&mut self) -> Result<ast::AstExpression, SqlParseError> {
        let mut left = self.parse_unary_expression()?; // Higher precedence

        while self.match_token(Token::Asterisk) || self.match_token(Token::Operator("/".to_string())) {
            let op_token_peek = self.peek().cloned(); // Peek before consuming

            let op = match op_token_peek {
                Some(Token::Asterisk) => {
                    self.consume(Token::Asterisk)?;
                    ast::AstArithmeticOperator::Multiply
                }
                Some(Token::Operator(ref s)) if s == "/" => {
                    self.consume(Token::Operator("/".to_string()))?;
                    ast::AstArithmeticOperator::Divide
                }
                _ => {
                    // This break is important if the token is not a multiplicative operator
                    // It means the multiplicative expression part is done.
                    break;
                }
            };

            let right = self.parse_unary_expression()?;
            left = ast::AstExpression::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    // Handles unary expressions (e.g., unary minus)
    fn parse_unary_expression(&mut self) -> Result<ast::AstExpression, SqlParseError> {
        if self.match_token(Token::Operator("-".to_string())) {
            self.consume_any().ok_or(SqlParseError::UnexpectedEOF)?; // Consume '-'
            let expr = self.parse_unary_expression()?; // Recursive call to allow for things like --value or -(-value)
            Ok(ast::AstExpression::UnaryOp {
                op: ast::AstUnaryOperator::Minus,
                expr: Box::new(expr),
            })
        } else if self.match_token(Token::Operator("+".to_string())) {
            self.consume_any().ok_or(SqlParseError::UnexpectedEOF)?; // Consume '+' (optional unary plus)
            self.parse_unary_expression() // The '+' is effectively ignored, just parse the following expression
        }
        // else if self.peek_is_identifier_str("NOT") { // Logical NOT handled by parse_base_condition_operand
        //     self.consume_any().ok_or(SqlParseError::UnexpectedEOF)?;
        //     let expr = self.parse_unary_expression()?;
        //     Ok(ast::AstExpression::UnaryOp {
        //         op: ast::AstUnaryOperator::Not, // This variant was removed
        //         expr: Box::new(expr),
        //     })
        // }
        else {
            self.parse_primary_expression()
        }
    }
}
