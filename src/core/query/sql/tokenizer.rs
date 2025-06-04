use super::errors::SqlTokenizerError;

#[derive(Debug, PartialEq, Clone)]
pub enum Token {
    // Keywords
    Select,
    From,
    Where,
    Update,
    Set,
    True,
    False,

    // Literals
    Identifier(String),
    StringLiteral(String),
    NumericLiteral(String),
    BooleanLiteral(bool),

    // Operators and Punctuation
    Operator(String), // For generic operators like =, <, >, etc.
    LParen,
    RParen,
    Comma,
    Asterisk,
    Semicolon,

    // End of File
    EOF,
}

pub struct Tokenizer<'a> {
    input: &'a str,
    chars: std::iter::Peekable<std::str::CharIndices<'a>>,
    current_pos: usize,
}

impl<'a> Tokenizer<'a> {
    pub fn new(input: &'a str) -> Self {
        Tokenizer {
            input,
            chars: input.char_indices().peekable(),
            current_pos: 0,
        }
    }

    fn skip_whitespace(&mut self) {
        while let Some((_, ch)) = self.chars.peek() {
            if ch.is_whitespace() {
                self.chars.next();
            } else {
                break;
            }
        }
    }

    fn read_identifier_or_keyword(&mut self, start_idx: usize) -> Result<Token, SqlTokenizerError> {
        let mut end_idx = start_idx;
        while let Some((idx, ch)) = self.chars.peek() {
            if ch.is_alphanumeric() || *ch == '_' {
                end_idx = *idx;
                self.chars.next();
            } else {
                break;
            }
        }
        // Need to get the slice from the original input
        // The loop consumes the last char of the identifier, so end_idx is correct.
        // We need to ensure we capture the full length of the identifier.
        let ident = &self.input[start_idx..=end_idx];
        self.current_pos = end_idx + 1;

        Ok(match ident.to_uppercase().as_str() {
            "SELECT" => Token::Select,
            "FROM" => Token::From,
            "WHERE" => Token::Where,
            "UPDATE" => Token::Update,
            "SET" => Token::Set,
            "TRUE" => Token::BooleanLiteral(true),
            "FALSE" => Token::BooleanLiteral(false),
            _ => Token::Identifier(ident.to_string()),
        })
    }

    fn read_string_literal(&mut self, quote_char: char, start_idx: usize) -> Result<Token, SqlTokenizerError> {
        let mut value = String::new();
        let mut escaped = false;
        self.chars.next(); // Consume the opening quote

        while let Some((idx, ch)) = self.chars.next() {
            self.current_pos = idx;
            if escaped {
                value.push(ch);
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == quote_char {
                return Ok(Token::StringLiteral(value));
            } else {
                value.push(ch);
            }
        }
        Err(SqlTokenizerError::UnterminatedString(start_idx))
    }

    fn read_numeric_literal(&mut self, start_idx: usize) -> Result<Token, SqlTokenizerError> {
        let mut end_idx = start_idx;
        let mut has_decimal = false;

        // The first character is consumed here. It's known to be a digit,
        // or a '.' followed by a digit (checked by the caller, `tokenize`).
        if let Some((idx, first_ch)) = self.chars.next() {
            end_idx = idx;
            if first_ch == '.' {
                has_decimal = true;
            }
        } else {
            // This case should ideally not be reached if `tokenize` calls this correctly.
            return Err(SqlTokenizerError::UnexpectedEOF(start_idx));
        }

        loop {
            // Peek at the next character. `.cloned()` is important to avoid holding a borrow.
            let next_char_info = self.chars.peek().cloned();

            if let Some((idx_peek, ch_peek)) = next_char_info {
                if ch_peek.is_digit(10) {
                    end_idx = idx_peek;
                    self.chars.next(); // Consume the digit
                } else if ch_peek == '.' && !has_decimal {
                    // It's a '.', and we haven't seen one yet. Check if it's part of the number.
                    // Clone the main iterator to look ahead without consuming from it yet.
                    let mut temp_iter = self.chars.clone();
                    temp_iter.next(); // In the temp_iter, consume the '.' character we just peeked.

                    // Now, peek at the character *after* the '.' in the temp_iter.
                    if temp_iter.peek().map_or(false, |&(_, next_next_ch)| next_next_ch.is_digit(10)) {
                        // The '.' is followed by a digit, so it's part of this number.
                        has_decimal = true;
                        end_idx = idx_peek; // The current character ('.') is part of the number.
                        self.chars.next(); // Consume the '.' from the main iterator.
                    } else {
                        // The '.' is not followed by a digit. The number ends before this '.'.
                        break;
                    }
                } else {
                    // Not a digit or not a valid decimal point (e.g., second '.'). End of number.
                    break;
                }
            } else {
                // No more characters. End of number.
                break;
            }
        }

        let num_str = &self.input[start_idx..=end_idx];
        // self.current_pos is updated by the main loop's skip_whitespace and peek logic,
        // or by read_identifier_or_keyword. Here we just return the token.
        Ok(Token::NumericLiteral(num_str.to_string()))
    }


    pub fn tokenize(&mut self) -> Result<Vec<Token>, SqlTokenizerError> {
        let mut tokens = Vec::new();

        loop {
            self.skip_whitespace();
            self.current_pos = self.chars.peek().map_or(self.input.len(), |(idx, _)| *idx);

            match self.chars.peek().cloned() {
                Some((idx, ch)) => {
                    match ch {
                        '=' | '<' | '>' | '!' => { // Potential multi-char operators later
                            self.chars.next();
                            // Basic for now, extend for !=, <=, >=
                            if ch == '!' && self.chars.peek().map_or(false, |&(_, next_ch)| next_ch == '=') {
                                self.chars.next();
                                tokens.push(Token::Operator("!=".to_string()));
                            } else {
                                tokens.push(Token::Operator(ch.to_string()));
                            }
                            self.current_pos = idx + 1; // Update position
                        }
                        '(' => {
                            self.chars.next();
                            tokens.push(Token::LParen);
                            self.current_pos = idx + 1;
                        }
                        ')' => {
                            self.chars.next();
                            tokens.push(Token::RParen);
                            self.current_pos = idx + 1;
                        }
                        ',' => {
                            self.chars.next();
                            tokens.push(Token::Comma);
                            self.current_pos = idx + 1;
                        }
                        '*' => {
                            self.chars.next();
                            tokens.push(Token::Asterisk);
                            self.current_pos = idx + 1;
                        }
                        ';' => {
                            self.chars.next();
                            tokens.push(Token::Semicolon);
                            self.current_pos = idx + 1;
                        }
                        '\'' | '"' => {
                            tokens.push(self.read_string_literal(ch, idx)?);
                        }
                        c if c.is_alphabetic() || c == '_' => {
                            tokens.push(self.read_identifier_or_keyword(idx)?);
                        }
                        c if c.is_digit(10) => {
                            tokens.push(self.read_numeric_literal(idx)?);
                        }
                        // Handle '.' not part of a number as an invalid character for now,
                        // or it could be part of a more complex identifier/operator later.
                        '.' => {
                             // Check if it's a decimal starting with '.'
                            if self.chars.clone().nth(1).map_or(false, |(_, next_ch)| next_ch.is_digit(10)) {
                                tokens.push(self.read_numeric_literal(idx)?);
                            } else {
                                return Err(SqlTokenizerError::InvalidCharacter(ch, idx));
                            }
                        }
                        _ => return Err(SqlTokenizerError::InvalidCharacter(ch, idx)),
                    }
                }
                None => {
                    tokens.push(Token::EOF);
                    break;
                }
            }
        }
        Ok(tokens)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select() {
        let mut tokenizer = Tokenizer::new("SELECT name FROM users WHERE id = 1;");
        let tokens = tokenizer.tokenize().unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Select,
                Token::Identifier("name".to_string()),
                Token::From,
                Token::Identifier("users".to_string()),
                Token::Where,
                Token::Identifier("id".to_string()),
                Token::Operator("=".to_string()),
                Token::NumericLiteral("1".to_string()),
                Token::Semicolon,
                Token::EOF,
            ]
        );
    }

    #[test]
    fn test_string_literal() {
        let mut tokenizer = Tokenizer::new("SELECT 'hello world' FROM test;");
        let tokens = tokenizer.tokenize().unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Select,
                Token::StringLiteral("hello world".to_string()),
                Token::From,
                Token::Identifier("test".to_string()),
                Token::Semicolon,
                Token::EOF,
            ]
        );
    }

    #[test]
    fn test_string_literal_with_escaped_quote() {
        let mut tokenizer = Tokenizer::new("SELECT 'hello \\'world\\'' FROM test;");
        let tokens = tokenizer.tokenize().unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Select,
                Token::StringLiteral("hello 'world'".to_string()),
                Token::From,
                Token::Identifier("test".to_string()),
                Token::Semicolon,
                Token::EOF,
            ]
        );
    }


    #[test]
    fn test_update_statement() {
        let mut tokenizer = Tokenizer::new("UPDATE products SET price = 10.50, name = \"New Name\" WHERE id = 101;");
        let tokens = tokenizer.tokenize().unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Update,
                Token::Identifier("products".to_string()),
                Token::Set,
                Token::Identifier("price".to_string()),
                Token::Operator("=".to_string()),
                Token::NumericLiteral("10.50".to_string()),
                Token::Comma,
                Token::Identifier("name".to_string()),
                Token::Operator("=".to_string()),
                Token::StringLiteral("New Name".to_string()),
                Token::Where,
                Token::Identifier("id".to_string()),
                Token::Operator("=".to_string()),
                Token::NumericLiteral("101".to_string()),
                Token::Semicolon,
                Token::EOF,
            ]
        );
    }

    #[test]
    fn test_keywords_case_insensitivity() {
        let mut tokenizer = Tokenizer::new("select Name FrOm Users whErE Id = 1;");
        let tokens = tokenizer.tokenize().unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Select,
                Token::Identifier("Name".to_string()), // Identifiers are case sensitive
                Token::From,
                Token::Identifier("Users".to_string()),
                Token::Where,
                Token::Identifier("Id".to_string()),
                Token::Operator("=".to_string()),
                Token::NumericLiteral("1".to_string()),
                Token::Semicolon,
                Token::EOF,
            ]
        );
    }

    #[test]
    fn test_numeric_literals() {
        let mut tokenizer = Tokenizer::new("123 45.67 .789 0.0");
        let tokens = tokenizer.tokenize().unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::NumericLiteral("123".to_string()),
                Token::NumericLiteral("45.67".to_string()),
                Token::NumericLiteral(".789".to_string()),
                Token::NumericLiteral("0.0".to_string()),
                Token::EOF,
            ]
        );
    }

    #[test]
    fn test_boolean_literals() {
        let mut tokenizer = Tokenizer::new("true FALSE True");
        let tokens = tokenizer.tokenize().unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::BooleanLiteral(true),
                Token::BooleanLiteral(false),
                Token::BooleanLiteral(true),
                Token::EOF,
            ]
        );
    }

    #[test]
    fn test_operators() {
        let mut tokenizer = Tokenizer::new("= , * ( ) ; != <>"); // Added <> for completeness, though not in Token enum yet
        let tokens = tokenizer.tokenize().unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Operator("=".to_string()),
                Token::Comma,
                Token::Asterisk,
                Token::LParen,
                Token::RParen,
                Token::Semicolon,
                Token::Operator("!=".to_string()),
                Token::Operator("<".to_string()), // This will be tokenized as < and then >
                Token::Operator(">".to_string()),
                Token::EOF,
            ]
        );
    }


    #[test]
    fn test_invalid_character() {
        let mut tokenizer = Tokenizer::new("SELECT name FROM users WHERE id = #;");
        let result = tokenizer.tokenize();
        assert!(matches!(result, Err(SqlTokenizerError::InvalidCharacter('#', 32))));
    }

    #[test]
    fn test_unterminated_string() {
        let mut tokenizer = Tokenizer::new("SELECT 'unterminated");
        let result = tokenizer.tokenize();
        assert!(matches!(result, Err(SqlTokenizerError::UnterminatedString(7))));
    }

    #[test]
    fn test_identifier_starting_with_underscore() {
        let mut tokenizer = Tokenizer::new("SELECT _name FROM _table;");
        let tokens = tokenizer.tokenize().unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Select,
                Token::Identifier("_name".to_string()),
                Token::From,
                Token::Identifier("_table".to_string()),
                Token::Semicolon,
                Token::EOF,
            ]
        );
    }

    #[test]
    fn test_select_asterisk() {
        let mut tokenizer = Tokenizer::new("SELECT * FROM users;");
        let tokens = tokenizer.tokenize().unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Select,
                Token::Asterisk,
                Token::From,
                Token::Identifier("users".to_string()),
                Token::Semicolon,
                Token::EOF,
            ]
        );
    }
}
