use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum SqlTokenizerError {
    #[error("Unterminated string literal starting at position {0}")]
    UnterminatedString(usize),
    #[error("Invalid character '{0}' at position {1}")]
    InvalidCharacter(char, usize),
    #[error("Could not parse number at position {0}")]
    InvalidNumber(usize),
    #[error("Unexpected end of input at position {0}")]
    UnexpectedEOF(usize), // Added variant
}

#[derive(Debug, Error, PartialEq)]
pub enum SqlParseError {
    #[error("Unexpected token: expected {expected}, found {found} at position {position}")]
    UnexpectedToken { expected: String, found: String, position: usize },
    #[error("Unexpected end of input")]
    UnexpectedEOF,
    #[error("Invalid expression at position {0}: {1}")]
    InvalidExpression(usize, String),
    #[error("Tokenizer error: {0}")]
    TokenizerError(#[from] SqlTokenizerError),
    #[error("Unknown statement type at position {0}")]
    UnknownStatementType(usize),
}
