use super::ast::{
    Assignment, AstLiteralValue, Condition, SelectColumn, SelectStatement, Statement, UpdateStatement,
};
use super::errors::SqlParseError;
use super::tokenizer::Token;

pub struct SqlParser {
    tokens: Vec<Token>,
    current: usize,
}

impl SqlParser {
    pub fn new(tokens: Vec<Token>) -> Self {
        SqlParser { tokens, current: 0 }
    }

    pub fn parse(&mut self) -> Result<Statement, SqlParseError> {
        if self.is_at_end() || (self.peek() == Some(&Token::EOF) && self.tokens.len() == 1) {
            return Err(SqlParseError::UnexpectedEOF);
        }

        let statement = match self.peek() {
            Some(Token::Select) => self.parse_select_statement(),
            Some(Token::Update) => self.parse_update_statement(),
            Some(_other_token) => return Err(SqlParseError::UnknownStatementType(self.current_token_pos())),
            None => return Err(SqlParseError::UnexpectedEOF),
        }?;

        if !self.is_at_end() {
            return Err(SqlParseError::UnexpectedToken {
                expected: "end of statement".to_string(),
                found: format!("{:?}", self.peek().unwrap()),
                position: self.current_token_pos(),
            });
        }
        Ok(statement)
    }

    fn parse_select_statement(&mut self) -> Result<Statement, SqlParseError> {
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
        if self.peek() == Some(&Token::Semicolon) {
            self.consume(Token::Semicolon)?;
        }
        Ok(Statement::Select(SelectStatement {
            columns,
            source,
            condition,
        }))
    }

    fn parse_update_statement(&mut self) -> Result<Statement, SqlParseError> {
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
        if self.peek() == Some(&Token::Semicolon) {
            self.consume(Token::Semicolon)?;
        }
        Ok(Statement::Update(UpdateStatement {
            source,
            assignments,
            condition,
        }))
    }

    fn parse_select_column_list(&mut self) -> Result<Vec<SelectColumn>, SqlParseError> {
        let mut columns = Vec::new();
        if self.match_token(Token::Asterisk) {
            self.consume(Token::Asterisk)?;
            columns.push(SelectColumn::Asterisk);
            return Ok(columns);
        }
        loop {
            let col_name = self.expect_identifier("Expected column name or '*'")?;
            columns.push(SelectColumn::ColumnName(col_name));
            if !self.match_token(Token::Comma) {
                break;
            }
            self.consume(Token::Comma)?;
        }
        Ok(columns)
    }

    fn parse_assignment_list(&mut self) -> Result<Vec<Assignment>, SqlParseError> {
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
        }
        Ok(assignments)
    }

    fn parse_condition(&mut self) -> Result<Condition, SqlParseError> {
        let column = self.expect_identifier("Expected column name for condition")?;
        let operator = self.expect_operator_any(&["=", "!=", "<", ">", "<=", ">="], "Expected operator in condition")?;
        let value = self.parse_literal_value("Expected value for condition")?;
        Ok(Condition {
            column,
            operator,
            value,
        })
    }

    fn parse_literal_value(&mut self, _error_msg: &str) -> Result<AstLiteralValue, SqlParseError> {
        match self.consume_any() {
            Some(Token::StringLiteral(s)) => Ok(AstLiteralValue::String(s)),
            Some(Token::NumericLiteral(n)) => Ok(AstLiteralValue::Number(n)),
            Some(Token::BooleanLiteral(b)) => Ok(AstLiteralValue::Boolean(b)),
            Some(Token::Identifier(ident)) if ident.to_uppercase() == "NULL" => Ok(AstLiteralValue::Null),
            Some(other) => Err(SqlParseError::UnexpectedToken {
                expected: "literal value (string, number, boolean, or NULL)".to_string(),
                found: format!("{:?}", other),
                position: self.current_token_pos() -1,
            }),
            None => Err(SqlParseError::UnexpectedEOF),
        }
    }

    fn current_token_pos(&self) -> usize {
        self.current
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.current)
    }

    fn previous(&self) -> Option<&Token> {
        if self.current == 0 { None } else { self.tokens.get(self.current -1) }
    }

    fn is_at_end(&self) -> bool {
        self.current >= self.tokens.len() || self.peek() == Some(&Token::EOF)
    }

    fn consume(&mut self, expected_token: Token) -> Result<&Token, SqlParseError> {
        match self.peek() {
            Some(token) if *token == expected_token => {
                self.current += 1;
                Ok(self.previous().unwrap())
            }
            Some(found_token) => Err(SqlParseError::UnexpectedToken {
                expected: format!("{:?}", expected_token),
                found: format!("{:?}", found_token.clone()),
                position: self.current_token_pos(),
            }),
            None => Err(SqlParseError::UnexpectedEOF),
        }
    }

    fn consume_any(&mut self) -> Option<Token> {
        if self.is_at_end() {
            return None;
        }
        let token = self.tokens.get(self.current).cloned();
        self.current += 1;
        token
    }

    fn match_token(&self, token_type: Token) -> bool {
        match self.peek() {
            Some(token) => *token == token_type,
            None => false,
        }
    }

    fn expect_identifier(&mut self, _error_message: &str) -> Result<String, SqlParseError> {
        match self.consume_any() {
            Some(Token::Identifier(name)) => Ok(name),
            Some(other) => Err(SqlParseError::UnexpectedToken {
                expected: "Identifier".to_string(),
                found: format!("{:?}", other),
                position: self.current_token_pos() -1,
            }),
            None => Err(SqlParseError::UnexpectedEOF),
        }
    }

    fn expect_operator(&mut self, op_str: &str, _error_message: &str) -> Result<String, SqlParseError> {
        match self.consume_any() {
            Some(Token::Operator(s)) if s == op_str => Ok(s),
            Some(other) => Err(SqlParseError::UnexpectedToken {
                expected: format!("Operator '{}'", op_str),
                found: format!("{:?}", other),
                position: self.current_token_pos() -1,
            }),
            None => Err(SqlParseError::UnexpectedEOF),
        }
    }

    fn expect_operator_any(&mut self, valid_ops: &[&str], _error_message: &str) -> Result<String, SqlParseError> {
        match self.peek().cloned() {
            Some(Token::Operator(s)) if valid_ops.contains(&s.as_str()) => {
                self.consume_any();
                Ok(s)
            }
            Some(other) => Err(SqlParseError::UnexpectedToken {
                expected: format!("one of operators: {:?}", valid_ops),
                found: format!("{:?}", other),
                position: self.current_token_pos(),
            }),
            None => Err(SqlParseError::UnexpectedEOF),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::query::sql::tokenizer::Tokenizer;

    fn tokenize_str(input: &str) -> Vec<Token> {
        let mut tokenizer = Tokenizer::new(input);
        tokenizer.tokenize().unwrap_or_else(|e| panic!("Test tokenizer error: {}", e))
    }

    #[test]
    fn test_update_missing_set_keyword() {
        let tokens = tokenize_str("UPDATE table field = 'value';");
        let mut parser = SqlParser::new(tokens);
        let result = parser.parse();
        assert!(matches!(result, Err(SqlParseError::UnexpectedToken { .. })), "Result was: {:?}", result);
        if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
            assert_eq!(expected, "Set");
            assert!(found.contains("Identifier(\"field\")"));
        } else {
            panic!("Wrong error type: {:?}", result);
        }
    }

    #[test]
    fn test_update_empty_set_clause() {
        let tokens = tokenize_str("UPDATE table SET;");
        let mut parser = SqlParser::new(tokens);
        let result = parser.parse();
        assert!(matches!(result, Err(SqlParseError::UnexpectedToken { .. }) | Err(SqlParseError::UnexpectedEOF)), "Result was: {:?}", result);
        if let Err(SqlParseError::UnexpectedToken {expected, found, ..}) = result {
            assert_eq!(expected, "Identifier");
            assert_eq!(found, "Semicolon");
        } else if let Err(SqlParseError::UnexpectedEOF {}) = result {
            // also possible
        }
         else {
            panic!("Wrong error type: {:?}", result);
        }
    }

    #[test]
    fn test_update_missing_value_in_assignment() {
        let tokens = tokenize_str("UPDATE table SET field =;");
        let mut parser = SqlParser::new(tokens);
        let result = parser.parse();
        assert!(matches!(result, Err(SqlParseError::UnexpectedToken { .. }) | Err(SqlParseError::UnexpectedEOF)), "Result was: {:?}", result);
         if let Err(SqlParseError::UnexpectedToken {expected, found, ..}) = result {
            assert_eq!(expected, "literal value (string, number, boolean, or NULL)");
            assert_eq!(found, "Semicolon");
        } else if let Err(SqlParseError::UnexpectedEOF {}) = result {
            // also possible
        } else {
            panic!("Wrong error type: {:?}", result);
        }
    }

    #[test]
    fn test_update_missing_equals_in_assignment() {
        let tokens = tokenize_str("UPDATE table SET field 'value';");
        let mut parser = SqlParser::new(tokens);
        let result = parser.parse();
        assert!(matches!(result, Err(SqlParseError::UnexpectedToken { .. })), "Result was: {:?}", result);
        if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
            assert_eq!(expected, "Operator \'=\'");
            assert!(found.contains("StringLiteral(\"value\")"));
        } else {
            panic!("Wrong error type: {:?}", result);
        }
    }

    #[test]
    fn test_update_trailing_comma_in_assignment_list() {
        let tokens = tokenize_str("UPDATE table SET field = 'val', ;");
        let mut parser = SqlParser::new(tokens);
        let result = parser.parse();
        assert!(matches!(result, Err(SqlParseError::UnexpectedToken { .. }) | Err(SqlParseError::UnexpectedEOF)), "Result was: {:?}", result);
        if let Err(SqlParseError::UnexpectedToken {expected, found, ..}) = result {
            assert_eq!(expected, "Identifier");
            assert_eq!(found, "Semicolon");
        } else if let Err(SqlParseError::UnexpectedEOF {}) = result {
            // also possible
        } else {
            panic!("Wrong error type: {:?}", result);
        }
    }

    #[test]
    fn test_update_empty_where_clause() {
        let tokens = tokenize_str("UPDATE table SET field = 'val' WHERE;");
        let mut parser = SqlParser::new(tokens);
        let result = parser.parse();
        assert!(matches!(result, Err(SqlParseError::UnexpectedToken { .. }) | Err(SqlParseError::UnexpectedEOF)), "Result was: {:?}", result);
        if let Err(SqlParseError::UnexpectedToken {expected, found, ..}) = result {
            assert_eq!(expected, "Identifier");
            assert_eq!(found, "Semicolon");
        } else if let Err(SqlParseError::UnexpectedEOF {}) = result {
            // also possible
        } else {
            panic!("Wrong error type: {:?}", result);
        }
    }

    #[test]
    fn test_update_missing_value_in_condition() {
        let tokens = tokenize_str("UPDATE table SET field = 'val' WHERE id =;");
        let mut parser = SqlParser::new(tokens);
        let result = parser.parse();
        assert!(matches!(result, Err(SqlParseError::UnexpectedToken { .. }) | Err(SqlParseError::UnexpectedEOF)), "Result was: {:?}", result);
        if let Err(SqlParseError::UnexpectedToken {expected, found, ..}) = result {
            assert_eq!(expected, "literal value (string, number, boolean, or NULL)");
            assert_eq!(found, "Semicolon");
        } else if let Err(SqlParseError::UnexpectedEOF {}) = result {
            // also possible
        } else {
            panic!("Wrong error type: {:?}", result);
        }
    }

    #[test]
    fn test_update_missing_operator_in_condition() {
        let tokens = tokenize_str("UPDATE table SET field = 'val' WHERE id value;");
        let mut parser = SqlParser::new(tokens);
        let result = parser.parse();
        assert!(matches!(result, Err(SqlParseError::UnexpectedToken { .. })), "Result was: {:?}", result);
        if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
            assert!(expected.contains("one of operators"));
            assert!(found.contains("Identifier(\"value\")"));
        } else {
            panic!("Wrong error type: {:?}", result);
        }
    }

    #[test]
    fn test_update_extra_token_after_valid_statement_no_semicolon() {
        let tokens = tokenize_str("UPDATE table SET field = 'value' EXTRA_TOKEN");
        let mut parser = SqlParser::new(tokens);
        let result = parser.parse();
        assert!(matches!(result, Err(SqlParseError::UnexpectedToken { .. })), "Result was: {:?}", result);
        if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
            assert!(expected.contains("end of statement"));
            assert!(found.contains("Identifier(\"EXTRA_TOKEN\")"));
        } else {
           panic!("Wrong error type: {:?}", result);
        }
    }

    #[test]
    fn test_update_extra_token_after_semicolon() {
        let tokens = tokenize_str("UPDATE table SET field = 'value'; EXTRA_TOKEN");
        let mut parser = SqlParser::new(tokens);
        let result = parser.parse();
        assert!(matches!(result, Err(SqlParseError::UnexpectedToken { .. })), "Result was: {:?}", result);
        if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
            assert!(expected.contains("end of statement"));
            assert!(found.contains("Identifier(\"EXTRA_TOKEN\")"));
        } else {
           panic!("Wrong error type: {:?}", result);
        }
    }

    #[test]
    fn test_parse_empty_tokens() {
        let tokens = vec![Token::EOF];
        let mut parser = SqlParser::new(tokens);
        let result = parser.parse();
        assert!(matches!(result, Err(SqlParseError::UnexpectedEOF)), "Result was: {:?}", result);
    }

    #[test]
    fn test_parse_empty_tokens_no_eof() {
        let tokens = vec![];
        let mut parser = SqlParser::new(tokens);
        let result = parser.parse();
        assert!(matches!(result, Err(SqlParseError::UnexpectedEOF)), "Result was: {:?}", result);
    }

    #[test]
    fn test_parse_select_simple() {
        let tokens = tokenize_str("SELECT name FROM users;");
        let mut parser = SqlParser::new(tokens);
        let ast = parser.parse().unwrap();
        match ast {
            Statement::Select(select_stmt) => {
                assert_eq!(select_stmt.source, "users");
                assert_eq!(select_stmt.columns, vec![SelectColumn::ColumnName("name".to_string())]);
                assert!(select_stmt.condition.is_none());
            }
            _ => panic!("Expected SelectStatement"),
        }
    }

    #[test]
    fn test_parse_select_asterisk() {
        let tokens = tokenize_str("SELECT * FROM orders;");
        let mut parser = SqlParser::new(tokens);
        let ast = parser.parse().unwrap();
        match ast {
            Statement::Select(select_stmt) => {
                assert_eq!(select_stmt.source, "orders");
                assert_eq!(select_stmt.columns, vec![SelectColumn::Asterisk]);
                assert!(select_stmt.condition.is_none());
            }
            _ => panic!("Expected SelectStatement"),
        }
    }

    #[test]
    fn test_parse_select_multiple_columns() {
        let tokens = tokenize_str("SELECT id, name, email FROM customers;");
        let mut parser = SqlParser::new(tokens);
        let ast = parser.parse().unwrap();
        match ast {
            Statement::Select(select_stmt) => {
                assert_eq!(select_stmt.source, "customers");
                assert_eq!(select_stmt.columns, vec![
                    SelectColumn::ColumnName("id".to_string()),
                    SelectColumn::ColumnName("name".to_string()),
                    SelectColumn::ColumnName("email".to_string()),
                ]);
                assert!(select_stmt.condition.is_none());
            }
            _ => panic!("Expected SelectStatement"),
        }
    }

    #[test]
    fn test_parse_select_with_where_clause() {
        let tokens = tokenize_str("SELECT id FROM products WHERE price = 10.99;");
        let mut parser = SqlParser::new(tokens);
        let ast = parser.parse().unwrap();
        match ast {
            Statement::Select(select_stmt) => {
                assert_eq!(select_stmt.source, "products");
                assert_eq!(select_stmt.columns, vec![SelectColumn::ColumnName("id".to_string())]);
                assert!(select_stmt.condition.is_some());
                let cond = select_stmt.condition.unwrap();
                assert_eq!(cond.column, "price");
                assert_eq!(cond.operator, "=");
                assert_eq!(cond.value, AstLiteralValue::Number("10.99".to_string()));
            }
            _ => panic!("Expected SelectStatement"),
        }
    }

    #[test]
    fn test_parse_update_simple() {
        let tokens = tokenize_str("UPDATE users SET name = 'New Name' WHERE id = 1;");
        let mut parser = SqlParser::new(tokens);
        let ast = parser.parse().unwrap();
        match ast {
            Statement::Update(update_stmt) => {
                assert_eq!(update_stmt.source, "users");
                assert_eq!(update_stmt.assignments.len(), 1);
                assert_eq!(update_stmt.assignments[0].column, "name");
                assert_eq!(update_stmt.assignments[0].value, AstLiteralValue::String("New Name".to_string()));
                assert!(update_stmt.condition.is_some());
                let cond = update_stmt.condition.unwrap();
                assert_eq!(cond.column, "id");
                assert_eq!(cond.operator, "=");
                assert_eq!(cond.value, AstLiteralValue::Number("1".to_string()));
            }
            _ => panic!("Expected UpdateStatement"),
        }
    }

    #[test]
    fn test_parse_update_multiple_assignments() {
        let tokens = tokenize_str("UPDATE products SET price = 99.50, stock = 500 WHERE category = 'electronics';");
        let mut parser = SqlParser::new(tokens);
        let ast = parser.parse().unwrap();
        match ast {
            Statement::Update(update_stmt) => {
                assert_eq!(update_stmt.source, "products");
                assert_eq!(update_stmt.assignments.len(), 2);
                assert_eq!(update_stmt.assignments[0].column, "price");
                assert_eq!(update_stmt.assignments[0].value, AstLiteralValue::Number("99.50".to_string()));
                assert_eq!(update_stmt.assignments[1].column, "stock");
                assert_eq!(update_stmt.assignments[1].value, AstLiteralValue::Number("500".to_string()));

                assert!(update_stmt.condition.is_some());
                let cond = update_stmt.condition.unwrap();
                assert_eq!(cond.column, "category");
                assert_eq!(cond.operator, "=");
                assert_eq!(cond.value, AstLiteralValue::String("electronics".to_string()));
            }
            _ => panic!("Expected UpdateStatement"),
        }
    }

    #[test]
    fn test_parse_select_missing_from() {
        let tokens = tokenize_str("SELECT name users;");
        let mut parser = SqlParser::new(tokens);
        let result = parser.parse();
        assert!(matches!(result, Err(SqlParseError::UnexpectedToken{..})), "Result was: {:?}", result);
         if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
            assert!(expected.contains("From"));
            assert!(found.contains("Identifier(\"users\")"));
        } else {
            panic!("Wrong error type");
        }
    }

    #[test]
    fn test_parse_update_missing_set() {
        let tokens = tokenize_str("UPDATE users name = 'Test';");
        let mut parser = SqlParser::new(tokens);
        let result = parser.parse();
        assert!(matches!(result, Err(SqlParseError::UnexpectedToken{..})), "Result was: {:?}", result);
         if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
            assert!(expected.contains("Set"));
            assert!(found.contains("Identifier(\"name\")"));
        } else {
            panic!("Wrong error type");
        }
    }

    #[test]
    fn test_unexpected_token_instead_of_literal() {
        let tokens = tokenize_str("SELECT name FROM users WHERE id = SELECT;");
        let mut parser = SqlParser::new(tokens);
        let result = parser.parse();
        assert!(matches!(result, Err(SqlParseError::UnexpectedToken{..})), "Result was: {:?}", result);
        if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
            assert_eq!(expected, "literal value (string, number, boolean, or NULL)");
            assert!(found.contains("Select"));
        } else {
            panic!("Wrong error type for unexpected token: {:?}", result);
        }
    }

    #[test]
    fn test_select_missing_columns() {
        let tokens = tokenize_str("SELECT FROM table;");
        let mut parser = SqlParser::new(tokens);
        let result = parser.parse();
        assert!(matches!(result, Err(SqlParseError::UnexpectedToken { .. })), "Result was: {:?}", result);
        if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
            assert_eq!(expected, "Identifier");
            assert_eq!(found, "From");
        } else {
            panic!("Wrong error type: {:?}", result);
        }
    }

    #[test]
    fn test_select_missing_from_keyword() {
        let tokens = tokenize_str("SELECT * table;");
        let mut parser = SqlParser::new(tokens);
        let result = parser.parse();
        assert!(matches!(result, Err(SqlParseError::UnexpectedToken { .. })), "Result was: {:?}", result);
        if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
            assert_eq!(expected, "From");
            assert!(found.contains("Identifier(\"table\")"));
        } else {
            panic!("Wrong error type: {:?}", result);
        }
    }

    #[test]
    fn test_select_trailing_comma_in_column_list() {
        let tokens = tokenize_str("SELECT col, FROM table;");
        let mut parser = SqlParser::new(tokens);
        let result = parser.parse();
        assert!(matches!(result, Err(SqlParseError::UnexpectedToken { .. })), "Result was: {:?}", result);
        if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
            assert_eq!(expected, "Identifier");
            assert_eq!(found, "From");
        } else {
            panic!("Wrong error type: {:?}", result);
        }
    }

    #[test]
    fn test_select_missing_table_name() {
        let tokens = tokenize_str("SELECT col FROM;");
        let mut parser = SqlParser::new(tokens);
        let result = parser.parse();
        assert!(matches!(result, Err(SqlParseError::UnexpectedToken { .. })
            | Err(SqlParseError::UnexpectedEOF { .. })),
            "Result was: {:?}", result);

        if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
            assert_eq!(expected, "Identifier");
            assert_eq!(found, "Semicolon");
        } else if let Err(SqlParseError::UnexpectedEOF) = result {
            // This case is also possible
        }
         else {
            panic!("Wrong error type: {:?}", result);
        }
    }

    #[test]
    fn test_select_empty_where_clause() {
        let tokens = tokenize_str("SELECT col FROM table WHERE;");
        let mut parser = SqlParser::new(tokens);
        let result = parser.parse();
        assert!(matches!(result, Err(SqlParseError::UnexpectedToken { .. }) | Err(SqlParseError::UnexpectedEOF)), "Result was: {:?}", result);
         if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
            assert_eq!(expected, "Identifier");
            assert_eq!(found, "Semicolon");
        } else if let Err(SqlParseError::UnexpectedEOF) = result {
            // This case is also possible
        } else {
            panic!("Wrong error type: {:?}", result);
        }
    }

    #[test]
    fn test_select_missing_value_in_condition() {
        let tokens = tokenize_str("SELECT col FROM table WHERE field =;");
        let mut parser = SqlParser::new(tokens);
        let result = parser.parse();
        assert!(matches!(result, Err(SqlParseError::UnexpectedToken { .. }) | Err(SqlParseError::UnexpectedEOF)), "Result was: {:?}", result);
        if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
            assert_eq!(expected, "literal value (string, number, boolean, or NULL)");
            assert_eq!(found, "Semicolon");
        } else if let Err(SqlParseError::UnexpectedEOF) = result {
            // This case is also possible
        }
         else {
            panic!("Wrong error type: {:?}", result);
        }
    }

    #[test]
    fn test_select_missing_operator_in_condition() {
        let tokens = tokenize_str("SELECT col FROM table WHERE field value;");
        let mut parser = SqlParser::new(tokens);
        let result = parser.parse();
        assert!(matches!(result, Err(SqlParseError::UnexpectedToken { .. })), "Result was: {:?}", result);
        if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
            assert!(expected.contains("one of operators"));
            assert!(found.contains("Identifier(\"value\")"));
        } else {
            panic!("Wrong error type: {:?}", result);
        }
    }

    #[test]
    fn test_select_extra_token_after_valid_statement_no_semicolon() {
        let tokens = tokenize_str("SELECT col FROM table WHERE field = 1 EXTRA_TOKEN");
        let mut parser = SqlParser::new(tokens);
        let result = parser.parse();
        assert!(matches!(result, Err(SqlParseError::UnexpectedToken { .. })), "Result was: {:?}", result);
        if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
            assert!(expected.contains("end of statement"));
            assert!(found.contains("Identifier(\"EXTRA_TOKEN\")"));
        } else {
           panic!("Wrong error type: {:?}", result);
        }
    }

    #[test]
    fn test_select_extra_token_after_semicolon() {
        let tokens = tokenize_str("SELECT col FROM table WHERE field = 1; EXTRA_TOKEN");
        let mut parser = SqlParser::new(tokens);
        let result = parser.parse();
        assert!(matches!(result, Err(SqlParseError::UnexpectedToken { .. })), "Result was: {:?}", result);
        if let Err(SqlParseError::UnexpectedToken { expected, found, .. }) = result {
            assert!(expected.contains("end of statement"));
            assert!(found.contains("Identifier(\"EXTRA_TOKEN\")"));
        } else {
           panic!("Wrong error type: {:?}", result);
        }
    }

    #[test]
    fn test_select_empty_string_literal() {
        let tokens = tokenize_str("SELECT * FROM test WHERE name = '';");
        let mut parser = SqlParser::new(tokens);
        let result = parser.parse().unwrap();
        match result {
            Statement::Select(select_stmt) => {
                let cond = select_stmt.condition.unwrap();
                assert_eq!(cond.column, "name");
                assert_eq!(cond.operator, "=");
                assert_eq!(cond.value, AstLiteralValue::String("".to_string()));
            }
            _ => panic!("Expected SelectStatement"),
        }
    }

    #[test]
    fn test_update_set_null_value() {
        let tokens = tokenize_str("UPDATE test SET value = NULL WHERE id = 1;");
        let mut parser = SqlParser::new(tokens);
        let result = parser.parse().unwrap();
        match result {
            Statement::Update(update_stmt) => {
                assert_eq!(update_stmt.assignments[0].column, "value");
                assert_eq!(update_stmt.assignments[0].value, AstLiteralValue::Null);
                let cond = update_stmt.condition.unwrap();
                assert_eq!(cond.column, "id");
                assert_eq!(cond.operator, "=");
                assert_eq!(cond.value, AstLiteralValue::Number("1".to_string()));
            }
            _ => panic!("Expected UpdateStatement"),
        }
    }

    #[test]
    fn test_select_where_null_value() {
        let tokens = tokenize_str("SELECT * FROM test WHERE data = NULL;");
        let mut parser = SqlParser::new(tokens);
        let result = parser.parse().unwrap();
        match result {
            Statement::Select(select_stmt) => {
                let cond = select_stmt.condition.unwrap();
                assert_eq!(cond.column, "data");
                assert_eq!(cond.operator, "=");
                assert_eq!(cond.value, AstLiteralValue::Null);
            }
            _ => panic!("Expected SelectStatement"),
        }
    }

    #[test]
    fn test_identifier_as_substring_of_keyword() {
        let tokens = tokenize_str("SELECT selector FROM selections WHERE selector_id = 1;");
        let mut parser = SqlParser::new(tokens);
        let result = parser.parse().unwrap();
        match result {
            Statement::Select(select_stmt) => {
                assert_eq!(select_stmt.columns, vec![SelectColumn::ColumnName("selector".to_string())]);
                assert_eq!(select_stmt.source, "selections");
                let cond = select_stmt.condition.unwrap();
                assert_eq!(cond.column, "selector_id");
                assert_eq!(cond.value, AstLiteralValue::Number("1".to_string()));
            }
            _ => panic!("Expected SelectStatement"),
        }
    }

    #[test]
    fn test_mixed_case_keywords() {
        let tokens = tokenize_str("SeLeCt * FrOm my_table WhErE value = TrUe;");
        let mut parser = SqlParser::new(tokens);
        let result = parser.parse().unwrap();
        match result {
            Statement::Select(select_stmt) => {
                assert_eq!(select_stmt.columns, vec![SelectColumn::Asterisk]);
                assert_eq!(select_stmt.source, "my_table");
                let cond = select_stmt.condition.unwrap();
                assert_eq!(cond.column, "value");
                assert_eq!(cond.value, AstLiteralValue::Boolean(true));
            }
            _ => panic!("Expected SelectStatement"),
        }
    }
}
