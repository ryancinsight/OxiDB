use super::core::SqlParser;
use crate::core::query::sql::ast::{
    self, AstLiteralValue, CreateTableStatement, DropTableStatement, OrderByExpr, OrderDirection,
    SelectStatement, Statement, UpdateStatement,
};
use crate::core::query::sql::errors::SqlParseError;
use crate::core::query::sql::tokenizer::Token; // For matching specific tokens like Token::Where

impl SqlParser {
    // Adding the new method here
    fn parse_data_type_definition(&mut self) -> Result<ast::AstDataType, SqlParseError> {
        let type_ident_token_pos = self.current_token_pos();
        let type_name_ident =
            self.expect_identifier("Expected data type name (e.g., INTEGER, TEXT, VECTOR)")?;
        let type_name_upper = type_name_ident.to_uppercase();

        // Handle types with parameters like VARCHAR(255) or DECIMAL(10,2) if needed here
        // For now, we primarily focus on VECTOR[dim] and simple types.
        // The old logic for `data_type_string.push('('); ... data_type_string.push(')');`
        // would need to be integrated if we want to capture the full string for types
        // not explicitly handled by AstDataType variants with parameters.

        match type_name_upper.as_str() {
            "INTEGER" | "INT" => Ok(ast::AstDataType::Integer),
            "TEXT" | "STRING" => Ok(ast::AstDataType::Text),
            "VARCHAR" | "CHAR" => {
                // Optionally consume (length) or (length, precision) parameters
                if self.match_token(Token::LParen) {
                    self.consume(Token::LParen)?;
                    // For now, just consume tokens until RParen, as AstDataType::Text doesn't store length/precision
                    let mut paren_depth = 1;
                    while paren_depth > 0 {
                        match self.consume_any() {
                            Some(Token::LParen) => paren_depth += 1,
                            Some(Token::RParen) => paren_depth -= 1,
                            Some(Token::EOF) => return Err(SqlParseError::UnexpectedEOF), // Unterminated type parameters
                            Some(_) => {} // Consume other tokens within parentheses
                            None => return Err(SqlParseError::UnexpectedEOF),
                        }
                    }
                }
                Ok(ast::AstDataType::Text)
            }
            "BOOLEAN" | "BOOL" => Ok(ast::AstDataType::Boolean),
            "FLOAT" | "REAL" | "DOUBLE" => Ok(ast::AstDataType::Float),
            "BLOB" => Ok(ast::AstDataType::Blob),
            "VECTOR" => {
                self.consume(Token::LBracket)?;
                let dim_token_pos = self.current_token_pos();
                let dim_str = match self.consume_any() {
                    Some(Token::NumericLiteral(s)) => s,
                    Some(other) => {
                        return Err(SqlParseError::UnexpectedToken {
                            expected: "numeric dimension for VECTOR type".to_string(),
                            found: format!("{:?}", other),
                            position: dim_token_pos,
                        })
                    }
                    None => return Err(SqlParseError::UnexpectedEOF),
                };
                let dimension = dim_str.parse::<u32>().map_err(|_| {
                    SqlParseError::InvalidDataTypeParameter {
                        type_name: "VECTOR".to_string(),
                        parameter: dim_str.clone(), // Use clone if original dim_str is needed later
                        position: dim_token_pos,
                        reason: "Dimension must be a positive integer".to_string(),
                    }
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
    pub(super) fn expect_specific_identifier(
        // Changed to pub(super)
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
            Some(Token::Drop) => self.parse_drop_table_statement(), // Added
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

        let mut distinct = false;
        if self.peek_is_identifier_str("DISTINCT") {
            self.consume_any().ok_or(SqlParseError::UnexpectedEOF)?; // Consume DISTINCT token
            distinct = true;
        }

        let columns = self.parse_select_column_list()?;
        self.consume(Token::From)?;
        let from_table_name = self.expect_identifier("Expected table name after FROM")?;
        let mut from_alias: Option<String> = None;

        if self.match_token(Token::As) {
            self.consume(Token::As)?;
            from_alias = Some(self.expect_identifier("Expected alias after AS")?);
        } else if let Some(Token::Identifier(ident_str)) = self.peek().cloned() { // Cloned to avoid double borrow
            // Check if ident_str is not a keyword that starts the next clause
            let uc_ident_str = ident_str.to_uppercase();
            if !matches!(uc_ident_str.as_str(), "JOIN" | "INNER" | "LEFT" | "RIGHT" | "FULL" | "CROSS" | "WHERE" | "ORDER" | "LIMIT" | "ON" | "GROUP" | "BY" | "SET" | "VALUES" | "TABLE" | "INTO" | "ASC" | "DESC")
                && !self.match_token(Token::Semicolon) && self.peek() != Some(&Token::EOF)
            {
                // It's likely an alias, consume it.
                // We need to be careful not to consume keywords. A simple identifier here is taken as an alias.
                from_alias = Some(self.expect_identifier("Expected alias identifier")?);
            }
        }

        let from_clause = ast::TableReference {
            name: from_table_name,
            alias: from_alias,
        };

        let mut joins = Vec::new();
        // Loop to parse JOIN clauses
        loop {
            let current_join_type: ast::JoinType;

            match self.peek().cloned() { // Cloned to allow further peeking/consuming inside match arms or after
                Some(Token::Join) => { self.consume(Token::Join)?; current_join_type = ast::JoinType::Inner; }
                Some(Token::Inner) => { self.consume(Token::Inner)?; self.consume(Token::Join)?; current_join_type = ast::JoinType::Inner; }
                Some(Token::Left) => {
                    self.consume(Token::Left)?;
                    if self.match_token(Token::Outer) { self.consume(Token::Outer)?; }
                    self.consume(Token::Join)?;
                    current_join_type = ast::JoinType::LeftOuter;
                }
                Some(Token::Right) => {
                    self.consume(Token::Right)?;
                    if self.match_token(Token::Outer) { self.consume(Token::Outer)?; }
                    self.consume(Token::Join)?;
                    current_join_type = ast::JoinType::RightOuter;
                }
                Some(Token::Full) => {
                    self.consume(Token::Full)?;
                    if self.match_token(Token::Outer) { self.consume(Token::Outer)?; }
                    self.consume(Token::Join)?;
                    current_join_type = ast::JoinType::FullOuter;
                }
                Some(Token::Cross) => { self.consume(Token::Cross)?; self.consume(Token::Join)?; current_join_type = ast::JoinType::Cross; }
                _ => { break; } // Not a JOIN keyword
            }

            let right_table_name = self.expect_identifier("Expected table name after JOIN clause")?;
            let mut right_alias: Option<String> = None;
            if self.match_token(Token::As) {
                self.consume(Token::As)?;
                right_alias = Some(self.expect_identifier("Expected alias after AS for joined table")?);
            } else if let Some(Token::Identifier(ident_str)) = self.peek().cloned() {
                let uc_ident_str = ident_str.to_uppercase();
                // Keywords that can follow a JOIN table [alias] are ON, WHERE, another JOIN, ORDER, LIMIT, etc.
                if !matches!(uc_ident_str.as_str(), "ON" | "WHERE" | "JOIN" | "INNER" | "LEFT" | "RIGHT" | "FULL" | "CROSS" | "ORDER" | "LIMIT" | "GROUP")
                    && !self.match_token(Token::Semicolon) && self.peek() != Some(&Token::EOF)
                {
                    right_alias = Some(self.expect_identifier("Expected alias for joined table")?);
                }
            }
            let right_source = ast::TableReference { name: right_table_name, alias: right_alias };

            let on_condition = if current_join_type != ast::JoinType::Cross {
                self.consume(Token::On)?;
                    Some(self.parse_condition_expr()?)
                } else {
                    None // CROSS JOIN does not have ON condition
                };

                joins.push(ast::JoinClause {
                    join_type: current_join_type,
                    right_source,
                    on_condition,
                });
            // The incorrect else block was here and has been removed.
        }

        let condition = if self.match_token(Token::Where) {
            self.consume(Token::Where)?;
            Some(self.parse_condition_expr()?)
        } else {
            None
        };

        // Parse GROUP BY
        let group_by_clause = if self.match_token(Token::Group) {
            self.consume(Token::Group)?;
            self.consume(Token::By)?;
            let mut group_exprs = Vec::new();
            if !matches!(self.peek(), Some(Token::Identifier(_))) { // Must have at least one grouping expr
                return Err(SqlParseError::UnexpectedToken {
                    expected: "column name for GROUP BY clause".to_string(),
                    found: format!("{:?}", self.peek().unwrap_or(&Token::EOF)),
                    position: self.current_token_pos(),
                });
            }
            loop {
                let expr = self.parse_expression()?; // Parse expression for GROUP BY
                group_exprs.push(expr);
                if !self.match_token(Token::Comma) {
                    break;
                }
                self.consume(Token::Comma)?;
                 // Ensure not a trailing comma before next clause
                if !matches!(self.peek(), Some(Token::Identifier(_))) {
                     return Err(SqlParseError::UnexpectedToken {
                        expected: "column name after comma in GROUP BY".to_string(),
                        found: format!("{:?}", self.peek().unwrap_or(&Token::EOF)),
                        position: self.current_token_pos(),
                    });
                }
            }
            Some(group_exprs)
        } else {
            None
        };

        // Parse HAVING
        let having_clause = if self.match_token(Token::Having) {
            if group_by_clause.is_none() && !self.dialect_allows_having_without_group_by() { // Assuming a helper or direct check
                 // Standard SQL typically requires GROUP BY if HAVING is used, though some dialects are lenient.
                 // For now, let's be strict. This check could be moved to a semantic analysis phase too.
                return Err(SqlParseError::UnexpectedToken { // Or a more specific semantic error
                    expected: "GROUP BY clause before HAVING".to_string(),
                    found: "HAVING".to_string(),
                    position: self.current_token_pos() -1, // Position of HAVING token
                });
            }
            self.consume(Token::Having)?;
            Some(self.parse_condition_expr()?)
        } else {
            None
        };

        // Parse ORDER BY
        let order_by = if self.match_token(Token::Order) {
            self.consume(Token::Order)?;
            self.consume(Token::By)?;
            Some(self.parse_order_by_list()?)
        } else {
            None
        };

        // Parse LIMIT
        let limit_val = if self.match_token(Token::Limit) {
            self.consume(Token::Limit)?;
            let current_pos = self.current_token_pos();
            match self.peek().cloned() {
                // Clone peeked token
                Some(Token::NumericLiteral(_)) => {
                    match self.parse_literal_value("Expected numeric literal for LIMIT clause")? {
                        AstLiteralValue::Number(n) => Some(AstLiteralValue::Number(n)),
                        _ => {
                            return Err(SqlParseError::UnexpectedToken {
                                expected: "numeric literal for LIMIT".to_string(),
                                found: "unexpected literal type after checking".to_string(), // Should be unreachable
                                position: current_pos,
                            });
                        }
                    }
                }
                Some(other_token_val) => {
                    // Use the cloned value
                    // If it's not a NumericLiteral, it's an error.
                    // Consume the actual token from the stream to advance the parser state.
                    self.consume_any();
                    return Err(SqlParseError::UnexpectedToken {
                        expected: "numeric literal for LIMIT".to_string(),
                        found: format!("{:?}", other_token_val), // Report the peeked token
                        position: current_pos,
                    });
                }
                None => {
                    return Err(SqlParseError::UnexpectedToken {
                        expected: "numeric literal for LIMIT".to_string(),
                        found: "EOF".to_string(),
                        position: current_pos,
                    });
                }
            }
        } else {
            None
        };

        // Semicolon handled by main parse()
        Ok(Statement::Select(SelectStatement {
            distinct, // Pass the distinct flag
            columns,
            from_clause, // Use the new from_clause
            joins,       // Add the parsed joins
            condition,
            group_by: group_by_clause, // Added group_by
            having: having_clause,     // Added having
            order_by,
            limit: limit_val,
        }))
    }

    fn parse_order_by_list(&mut self) -> Result<Vec<OrderByExpr>, SqlParseError> {
        let mut order_expressions = Vec::new();
        // Check if ORDER BY is immediately followed by something that's not an identifier
        if !matches!(self.peek(), Some(Token::Identifier(_))) {
            return Err(SqlParseError::UnexpectedToken {
                expected: "at least one column for order by".to_string(),
                found: format!("{:?}", self.peek().unwrap_or(&Token::EOF)),
                position: self.current_token_pos(),
            });
        }

        loop {
            let expr = self.parse_expression()?; // Parse expression for ORDER BY
            let mut direction: Option<OrderDirection> = None;

            if self.match_token(Token::Asc) {
                self.consume(Token::Asc)?;
                direction = Some(OrderDirection::Asc);
            } else if self.match_token(Token::Desc) {
                self.consume(Token::Desc)?;
                direction = Some(OrderDirection::Desc);
            }

            order_expressions.push(OrderByExpr { expression: expr, direction });

            if !self.match_token(Token::Comma) {
                break;
            }
            self.consume(Token::Comma)?;
            // Ensure not a trailing comma before LIMIT or Semicolon or EOF
            if self.match_token(Token::Limit)
                || self.match_token(Token::Semicolon)
                || self.is_at_end()
            {
                return Err(SqlParseError::UnexpectedToken {
                    expected: "column name after comma in ORDER BY".to_string(),
                    found: format!("{:?}", self.peek().unwrap_or(&Token::EOF)),
                    position: self.current_token_pos(),
                });
            }
        }
        if order_expressions.is_empty() {
            return Err(SqlParseError::UnexpectedToken {
                expected: "at least one column for ORDER BY clause".to_string(),
                found: format!("{:?}", self.peek().unwrap_or(&Token::EOF)),
                position: self.current_token_pos(),
            });
        }
        Ok(order_expressions)
    }

    pub(super) fn parse_update_statement(&mut self) -> Result<Statement, SqlParseError> {
        self.consume(Token::Update)?;
        let source = self.expect_identifier("Expected table name after UPDATE")?;
        self.consume(Token::Set)?;
        let assignments = self.parse_assignment_list()?;
        let condition = if self.match_token(Token::Where) {
            self.consume(Token::Where)?;
            Some(self.parse_condition_expr()?)
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
            Some(self.parse_condition_expr()?)
        } else {
            None // Or error if WHERE clause is mandatory for DELETE
        };
        // Semicolon handled by main parse()

        // ast::DeleteStatement is now used.
        Ok(Statement::Delete(ast::DeleteStatement { table_name, condition }))
    }

    fn parse_drop_table_statement(&mut self) -> Result<Statement, SqlParseError> {
        self.consume(Token::Drop)?;
        self.consume(Token::Table)?;

        let mut if_exists = false;
        if self.peek_is_identifier_str("IF") {
            self.consume_any().ok_or(SqlParseError::UnexpectedEOF)?; // Consume IF
            self.expect_specific_identifier("EXISTS", "Expected EXISTS after IF in DROP TABLE")?;
            if_exists = true;
        }

        let table_name = self.expect_identifier("Expected table name after DROP TABLE")?;

        // Semicolon handled by main parse()
        Ok(Statement::DropTable(DropTableStatement { table_name, if_exists }))
    }
}
