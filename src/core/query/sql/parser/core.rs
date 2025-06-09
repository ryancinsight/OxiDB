use crate::core::query::sql::errors::SqlParseError;
use crate::core::query::sql::tokenizer::Token;

#[derive(Debug)] // Added Debug as it's useful
pub struct SqlParser {
    pub(super) tokens: Vec<Token>, // pub(super) for access from statement.rs and expression.rs
    pub(super) current: usize,     // pub(super)
}

impl SqlParser {
    pub fn new(tokens: Vec<Token>) -> Self {
        SqlParser { tokens, current: 0 }
    }

    pub(super) fn current_token_pos(&self) -> usize {
        self.current
    }

    pub(super) fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.current)
    }

    pub(super) fn previous(&self) -> Option<&Token> {
        if self.current == 0 {
            None
        } else {
            self.tokens.get(self.current - 1)
        }
    }

    pub(super) fn is_at_end(&self) -> bool {
        self.current >= self.tokens.len() || self.peek() == Some(&Token::EOF)
    }

    pub(super) fn consume(&mut self, expected_token: Token) -> Result<&Token, SqlParseError> {
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

    pub(super) fn consume_any(&mut self) -> Option<Token> {
        if self.is_at_end() {
            return None;
        }
        let token = self.tokens.get(self.current).cloned();
        self.current += 1;
        token
    }

    pub(super) fn match_token(&self, token_type: Token) -> bool {
        match self.peek() {
            Some(token) => *token == token_type,
            None => false,
        }
    }

    pub(super) fn expect_identifier(
        &mut self,
        _error_message: &str,
    ) -> Result<String, SqlParseError> {
        match self.consume_any() {
            Some(Token::Identifier(name)) => Ok(name),
            Some(other) => Err(SqlParseError::UnexpectedToken {
                expected: "Identifier".to_string(),
                found: format!("{:?}", other),
                position: self.current_token_pos() - 1, // Position of the consumed token
            }),
            None => Err(SqlParseError::UnexpectedEOF),
        }
    }

    pub(super) fn expect_operator(
        &mut self,
        op_str: &str,
        _error_message: &str,
    ) -> Result<String, SqlParseError> {
        match self.consume_any() {
            Some(Token::Operator(s)) if s == op_str => Ok(s),
            Some(other) => Err(SqlParseError::UnexpectedToken {
                expected: format!("Operator '{}'", op_str),
                found: format!("{:?}", other),
                position: self.current_token_pos() - 1, // Position of the consumed token
            }),
            None => Err(SqlParseError::UnexpectedEOF),
        }
    }

    pub(super) fn expect_operator_any(
        &mut self,
        valid_ops: &[&str],
        _error_message: &str,
    ) -> Result<String, SqlParseError> {
        // Peek first to report correct position if token is not an operator at all
        let current_pos_before_consume = self.current_token_pos();
        match self.peek().cloned() {
            // Clone to avoid borrowing issues with self.consume_any()
            Some(Token::Operator(s)) if valid_ops.contains(&s.as_str()) => {
                self.consume_any(); // Consume the matched operator
                Ok(s)
            }
            Some(other) => Err(SqlParseError::UnexpectedToken {
                expected: format!("one of operators: {:?}", valid_ops),
                found: format!("{:?}", other),
                position: current_pos_before_consume,
            }),
            None => Err(SqlParseError::UnexpectedEOF),
        }
    }
}
