use std::fmt;

#[derive(Debug, PartialEq, Eq)]
pub enum SqlTokenizerError {
    UnterminatedString(usize),
    InvalidCharacter(char, usize),
    InvalidNumber(usize),
    UnexpectedEOF(usize),
}

impl fmt::Display for SqlTokenizerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnterminatedString(pos) => write!(f, "Unterminated string literal starting at position {}", pos),
            Self::InvalidCharacter(ch, pos) => write!(f, "Invalid character '{}' at position {}", ch, pos),
            Self::InvalidNumber(pos) => write!(f, "Could not parse number at position {}", pos),
            Self::UnexpectedEOF(pos) => write!(f, "Unexpected end of input at position {}", pos),
        }
    }
}

impl std::error::Error for SqlTokenizerError {}

#[derive(Debug, PartialEq, Eq)]
pub enum SqlParseError {
    UnexpectedToken { expected: String, found: String, position: usize },
    UnexpectedEOF,
    InvalidExpression(usize, String),
    TokenizerError(SqlTokenizerError),
    UnknownStatementType(usize),
    UnknownDataType(String, usize),
    InvalidDataTypeParameter {
        type_name: String,
        parameter: String,
        position: usize,
        reason: String,
    },
}

impl fmt::Display for SqlParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedToken { expected, found, position } => {
                write!(f, "Unexpected token: expected {}, found {} at position {}", expected, found, position)
            }
            Self::UnexpectedEOF => write!(f, "Unexpected end of input"),
            Self::InvalidExpression(pos, msg) => write!(f, "Invalid expression at position {}: {}", pos, msg),
            Self::TokenizerError(e) => write!(f, "Tokenizer error: {}", e),
            Self::UnknownStatementType(pos) => write!(f, "Unknown statement type at position {}", pos),
            Self::UnknownDataType(name, pos) => write!(f, "Unknown data type '{}' at position {}", name, pos),
            Self::InvalidDataTypeParameter { type_name, parameter, position, reason } => {
                write!(f, "Invalid parameter for data type '{}' at position {}: parameter '{}', reason: {}", 
                    type_name, position, parameter, reason)
            }
        }
    }
}

impl std::error::Error for SqlParseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::TokenizerError(e) => Some(e),
            _ => None,
        }
    }
}

impl From<SqlTokenizerError> for SqlParseError {
    fn from(err: SqlTokenizerError) -> Self {
        Self::TokenizerError(err)
    }
}
