use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
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

#[derive(Debug, Error, PartialEq, Eq)]
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
    #[error("Unknown data type '{0}' at position {1}")]
    UnknownDataType(String, usize),
    #[error("Invalid parameter for data type '{type_name}' at position {position}: parameter '{parameter}', reason: {reason}")]
    InvalidDataTypeParameter {
        type_name: String,
        parameter: String,
        position: usize,
        reason: String,
    },
}
