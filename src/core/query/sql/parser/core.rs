use crate::core::query::sql::errors::SqlParseError;
use crate::core::query::sql::tokenizer::Token;

#[derive(Debug)] // Added Debug as it's useful
pub struct SqlParser {
    pub(super) tokens: Vec<Token>, // pub(super) for access from statement.rs and expression.rs
    pub(super) current: usize,     // pub(super)
    pub(super) parameter_count: u32, // Track parameter placeholders for indexing
}

impl SqlParser {
    #[must_use]
    pub const fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, current: 0, parameter_count: 0 }
    }

    pub(super) const fn current_token_pos(&self) -> usize {
        self.current
    }

    pub(super) fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.current)
    }

    // Helper to check if the current token is an Identifier with a specific string value (case-insensitive)
    pub(super) fn peek_is_identifier_str(&self, expected_str: &str) -> bool {
        match self.peek() {
            Some(Token::Identifier(ident)) => ident.eq_ignore_ascii_case(expected_str),
            _ => false,
        }
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
            Some(found_token) => {
                if *found_token == Token::EOF {
                    // If we found EOF but expected something else
                    Err(SqlParseError::UnexpectedEOF)
                } else {
                    Err(SqlParseError::UnexpectedToken {
                        expected: format!("{expected_token:?}"),
                        found: format!("{:?}", found_token.clone()),
                        position: self.current_token_pos(),
                    })
                }
            }
            None => Err(SqlParseError::UnexpectedEOF), // This case means tokens array is exhausted
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
        context_message: &str, // Changed from _error_message to use it
    ) -> Result<String, SqlParseError> {
        let consumed_token = self.consume_any();
        match consumed_token {
            Some(Token::Identifier(name)) => Ok(name),
            // Hack: Allow Token::Table to be treated as an identifier string "Table"
            // This is to address the immediate issue where 'table' is tokenized to Token::Table
            // and then expect_identifier fails. A proper fix might involve tokenizer changes
            // or a more sophisticated way to parse identifiers that can be keywords.
            Some(Token::Table) => {
                // Ideally, we'd get the original string ("table", "Table", etc.)
                // but Token::Table doesn't store it. For now, canonical "Table".
                Ok("Table".to_string())
            }
            Some(other) => Err(SqlParseError::UnexpectedToken {
                expected: context_message.to_string(), // Use context_message for better error
                found: format!("{other:?}"),
                position: self.current_token_pos() - 1, // Position of the consumed token
            }),
            None => Err(SqlParseError::UnexpectedEOF),
        }
    }

    pub(super) fn expect_operator(
        &mut self,
        op_str: &str,
        _error_message: &str, // _error_message is not used here, could be for consistency or future use
    ) -> Result<String, SqlParseError> {
        match self.consume_any() {
            Some(Token::Operator(s)) if s == op_str => Ok(s),
            Some(other) => Err(SqlParseError::UnexpectedToken {
                expected: format!("Operator '{op_str}'"), // This is specific enough
                found: format!("{other:?}"),
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
                expected: format!("one of operators: {valid_ops:?}"),
                found: format!("{other:?}"),
                position: current_pos_before_consume,
            }),
            None => Err(SqlParseError::UnexpectedEOF),
        }
    }
}
