use std::fmt;
use std::vec;

use crate::error::{ParsingError, ParsingErrorMessage};

#[derive(Debug, Clone)]
pub struct Token {
    pub position_start: usize, // inclusive
    pub position_end: usize,   // exclusive, one past the last character in token

    pub info: TokenInfo,
}

#[derive(Debug, Clone)]
pub enum TokenInfo {
    Identifier(String),
    QuotedIdentifier(String),
    Number(String),
    Eof,

    Star,               // '*'
    Dot,                // '.'
    Equals,             // '='
    Comma,              // ','
    Semicolon,          // ';'
    SquareBracketStart, // '['
    SquareBracketEnd,   // ']'
    ParenthesesStart,   // '('
    ParenthesesEnd,     // ')'

    Select,    // 'SELECT'
    From,      // 'FROM'
    Count,     // 'COUNT'
    Limit,     // 'LIMIT'
    Group,     // 'GROUP'
    By,        // 'BY'
    Where,     // 'WHERE'
    And,       // 'AND'
    Bypass,    // 'BYPASS'
    Cache,     // 'CACHE'
    Distinct,  // 'DISTINCT'
    Json,      // 'JSON'
    Null,      // 'NULL'
    Allow,     // 'ALLOW'
    Filtering, // 'FILTERING'
    Contains,  // 'CONTAINS'
    In,        // 'IN'
    Key,       // 'KEY'
    Like,      // 'LIKE'
    Cast,      // 'CAST'
    As,        // 'AS'
    False,     // 'FALSE'
    True,      // 'TRUE'
    Order,     // 'ORDER'
    Desc,      // 'DESC'
    Asc,       // 'ASC'
    Per,       // 'PER'
    Partition, // 'PARTITION'
}

impl fmt::Display for TokenInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &*self {
            TokenInfo::Identifier(identifier) => write!(f, "{}", identifier),
            TokenInfo::QuotedIdentifier(identifier) => write!(f, "{}", identifier),
            TokenInfo::Number(number) => write!(f, "{}", number),
            TokenInfo::Eof => write!(f, "<end of query>"),

            TokenInfo::Star => write!(f, "*"),
            TokenInfo::Dot => write!(f, "."),
            TokenInfo::Equals => write!(f, "="),
            TokenInfo::Comma => write!(f, ","),
            TokenInfo::Semicolon => write!(f, ";"),
            TokenInfo::SquareBracketStart => write!(f, "["),
            TokenInfo::SquareBracketEnd => write!(f, "]"),
            TokenInfo::ParenthesesStart => write!(f, "("),
            TokenInfo::ParenthesesEnd => write!(f, ")"),

            TokenInfo::Select => write!(f, "SELECT"),
            TokenInfo::From => write!(f, "FROM"),
            TokenInfo::Count => write!(f, "COUNT"),
            TokenInfo::Limit => write!(f, "LIMIT"),
            TokenInfo::Group => write!(f, "GROUP"),
            TokenInfo::By => write!(f, "BY"),
            TokenInfo::Where => write!(f, "WHERE"),
            TokenInfo::And => write!(f, "AND"),
            TokenInfo::Bypass => write!(f, "BYPASS"),
            TokenInfo::Cache => write!(f, "CACHE"),
            TokenInfo::Distinct => write!(f, "DISTINCT"),
            TokenInfo::Json => write!(f, "JSON"),
            TokenInfo::Null => write!(f, "NULL"),
            TokenInfo::Allow => write!(f, "ALLOW"),
            TokenInfo::Filtering => write!(f, "FILTERING"),
            TokenInfo::Contains => write!(f, "CONTAINS"),
            TokenInfo::In => write!(f, "IN"),
            TokenInfo::Key => write!(f, "KEY"),
            TokenInfo::Like => write!(f, "LIKE"),
            TokenInfo::Cast => write!(f, "CAST"),
            TokenInfo::As => write!(f, "AS"),
            TokenInfo::False => write!(f, "FALSE"),
            TokenInfo::True => write!(f, "TRUE"),
            TokenInfo::Order => write!(f, "ORDER"),
            TokenInfo::Desc => write!(f, "DESC"),
            TokenInfo::Asc => write!(f, "ASC"),
            TokenInfo::Per => write!(f, "PER"),
            TokenInfo::Partition => write!(f, "PARTITION"),
        }
    }
}

struct LexerState {
    pub query_characters: Vec<char>,
    pub current_position: usize,
}

impl LexerState {
    fn peek_character(&self) -> char {
        self.peek_character_offset(0)
    }

    fn peek_character_offset(&self, offset: usize) -> char {
        self.query_characters
            .get(self.current_position + offset)
            .copied()
            .unwrap_or('\0')
    }

    fn eat_character(&mut self) -> char {
        let result = self.peek_character();
        self.current_position += 1;
        result
    }

    fn finished(&self) -> bool {
        self.current_position >= self.query_characters.len()
    }
}

pub fn lex(query: &str) -> Result<Vec<Token>, ParsingError> {
    let mut lexer_state = LexerState {
        query_characters: query.chars().collect(),
        current_position: 0,
    };
    let mut tokens = Vec::new();

    while !lexer_state.finished() {
        let current_character = lexer_state.peek_character();
        match current_character {
            c if c.is_whitespace() => lex_whitespace(&mut lexer_state),
            c if c.is_ascii_alphabetic() => lex_ident(&mut lexer_state, &mut tokens),
            c if c.is_ascii_digit() => lex_integer(&mut lexer_state, &mut tokens),
            '\'' => lex_quoted_string_literal(&mut lexer_state, &mut tokens),
            '*' => lex_single_character_token(&mut lexer_state, &mut tokens, TokenInfo::Star),
            '.' => lex_single_character_token(&mut lexer_state, &mut tokens, TokenInfo::Dot),
            '=' => lex_single_character_token(&mut lexer_state, &mut tokens, TokenInfo::Equals),
            ',' => lex_single_character_token(&mut lexer_state, &mut tokens, TokenInfo::Comma),
            ';' => lex_single_character_token(&mut lexer_state, &mut tokens, TokenInfo::Semicolon),
            '[' => lex_single_character_token(
                &mut lexer_state,
                &mut tokens,
                TokenInfo::SquareBracketStart,
            ),
            ']' => lex_single_character_token(
                &mut lexer_state,
                &mut tokens,
                TokenInfo::SquareBracketEnd,
            ),
            '(' => lex_single_character_token(
                &mut lexer_state,
                &mut tokens,
                TokenInfo::ParenthesesStart,
            ),
            ')' => {
                lex_single_character_token(&mut lexer_state, &mut tokens, TokenInfo::ParenthesesEnd)
            }
            _ => Err(ParsingError {
                errors: vec![ParsingErrorMessage {
                    msg: format!(
                        "Encountered invalid character '{}' in the query while lexing.",
                        current_character
                    ),
                    position_start: lexer_state.current_position,
                    position_end: lexer_state.current_position + 1,
                }],
                notes: Vec::new(),
            }),
        }?;
    }

    tokens.push(Token {
        position_start: lexer_state.current_position,
        position_end: lexer_state.current_position,
        info: TokenInfo::Eof,
    });

    Ok(tokens)
}

fn lex_whitespace(lexer_state: &mut LexerState) -> Result<(), ParsingError> {
    lexer_state.eat_character();
    Ok(())
}

fn lex_ident(lexer_state: &mut LexerState, tokens: &mut Vec<Token>) -> Result<(), ParsingError> {
    // IDENT in Cql.g
    let position_start = lexer_state.current_position;

    lexer_state.eat_character();
    while lexer_state.peek_character().is_ascii_alphabetic()
        || lexer_state.peek_character().is_ascii_digit()
        || lexer_state.peek_character() == '_'
    {
        lexer_state.eat_character();
    }

    let position_end = lexer_state.current_position;
    let slice: String = lexer_state
        .query_characters
        .get(position_start..position_end)
        .unwrap()
        .iter()
        .collect();
    let slice_lowercase = slice.to_lowercase();

    let mut push_token = |token_info: TokenInfo| {
        tokens.push(Token {
            position_start,
            position_end,
            info: token_info,
        });
    };

    match slice_lowercase.as_str() {
        "select" => push_token(TokenInfo::Select),
        "from" => push_token(TokenInfo::From),
        "count" => push_token(TokenInfo::Count),
        "limit" => push_token(TokenInfo::Limit),
        "group" => push_token(TokenInfo::Group),
        "by" => push_token(TokenInfo::By),
        "where" => push_token(TokenInfo::Where),
        "and" => push_token(TokenInfo::And),
        "bypass" => push_token(TokenInfo::Bypass),
        "cache" => push_token(TokenInfo::Cache),
        "distinct" => push_token(TokenInfo::Distinct),
        "json" => push_token(TokenInfo::Json),
        "null" => push_token(TokenInfo::Null),
        "allow" => push_token(TokenInfo::Allow),
        "filtering" => push_token(TokenInfo::Filtering),
        "contains" => push_token(TokenInfo::Contains),
        "in" => push_token(TokenInfo::In),
        "key" => push_token(TokenInfo::Key),
        "like" => push_token(TokenInfo::Like),
        "cast" => push_token(TokenInfo::Cast),
        "as" => push_token(TokenInfo::As),
        "false" => push_token(TokenInfo::False),
        "true" => push_token(TokenInfo::True),
        "order" => push_token(TokenInfo::Order),
        "desc" => push_token(TokenInfo::Desc),
        "asc" => push_token(TokenInfo::Asc),
        "per" => push_token(TokenInfo::Per),
        "partition" => push_token(TokenInfo::Partition),
        _ => {
            tokens.push(Token {
                position_start,
                position_end,
                info: TokenInfo::Identifier(slice),
            });
        }
    }
    Ok(())
}

fn lex_integer(lexer_state: &mut LexerState, tokens: &mut Vec<Token>) -> Result<(), ParsingError> {
    // INTEGER in Cql.g (but without '-')
    let position_start = lexer_state.current_position;

    lexer_state.eat_character();
    while lexer_state.peek_character().is_ascii_digit() {
        lexer_state.eat_character();
    }

    // FIXME: 123abc parses to Number(123) and Identifier(abc),
    // but 123, correctly to Number(123) and Comma.

    let position_end = lexer_state.current_position;
    tokens.push(Token {
        position_start,
        position_end,
        info: TokenInfo::Number(
            lexer_state
                .query_characters
                .get(position_start..position_end)
                .unwrap()
                .iter()
                .collect(),
        ),
    });

    Ok(())
}

fn lex_quoted_string_literal(
    lexer_state: &mut LexerState,
    tokens: &mut Vec<Token>,
) -> Result<(), ParsingError> {
    // "conventional quoted string literal" in Cql.g
    let position_start = lexer_state.current_position;

    lexer_state.eat_character(); // Eat starting quote

    let mut contents = String::new();
    loop {
        let current_character = lexer_state.peek_character();
        let next_character = lexer_state.peek_character_offset(1);

        if current_character == '\0' {
            // Reached EOF, the quoted string literal did not end...
            return Err(ParsingError {
                errors: vec![ParsingErrorMessage {
                    msg: "A quoted string literal ('example literal') was started, but never ended. Lexer reached the end of query string without being closed.".to_string(),
                    position_start: position_start,
                    position_end: lexer_state.current_position,
                }],
                notes: vec![ParsingErrorMessage {
                    msg: "The quoted string literal started here.".to_string(),
                    position_start: position_start,
                    position_end: position_start + 1,
                }],
            });
        } else if current_character == '\'' {
            // Either this is the end of quoted string literal
            // or "''" that should get parsed to "'".
            if next_character == '\'' {
                // "''"
                contents.push('\'');
                lexer_state.eat_character();
                lexer_state.eat_character();
            } else {
                // End of quoted string literal
                lexer_state.eat_character();
                break;
            }
        } else {
            lexer_state.eat_character();
            contents.push(current_character);
        }
    }

    let position_end = lexer_state.current_position;
    tokens.push(Token {
        position_start,
        position_end,
        info: TokenInfo::QuotedIdentifier(contents),
    });

    Ok(())
}

fn lex_single_character_token(
    lexer_state: &mut LexerState,
    tokens: &mut Vec<Token>,
    token_info: TokenInfo,
) -> Result<(), ParsingError> {
    tokens.push(Token {
        position_start: lexer_state.current_position,
        position_end: lexer_state.current_position + 1,
        info: token_info,
    });
    lexer_state.eat_character();
    Ok(())
}

pub fn to_column_identifier(token_info: &TokenInfo) -> TokenInfo {
    match token_info {
        TokenInfo::QuotedIdentifier(ident) => TokenInfo::Identifier(ident.clone()),
        // unreserved_keyword in Cql.g
        // TODO: list not final
        TokenInfo::Key => TokenInfo::Identifier("key".to_string()), // FIXME: we don't preserve capitalization...
        TokenInfo::Group => TokenInfo::Identifier("group".to_string()), // FIXME: we don't preserve capitalization...
        _ => token_info.clone(),
    }
}
