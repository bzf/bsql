#[derive(Debug, PartialEq)]
pub enum Token {
    OpeningParenthesis,
    ClosingParenthesis,
    Comma,
    Semicolon,

    Keyword(String),
    Identifier(String),
    NumericLiteral(String),
}

pub fn tokenize(input: &str) -> Vec<Token> {
    let mut cursor = input.chars().peekable();
    let mut tokens = vec![];

    while let Some(character) = cursor.next() {
        if character.is_whitespace() {
            continue;
        }

        if character == '(' {
            tokens.push(Token::OpeningParenthesis);
            continue;
        } else if character == ')' {
            tokens.push(Token::ClosingParenthesis);
            continue;
        } else if character == ',' {
            tokens.push(Token::Comma);
            continue;
        } else if character == ';' {
            tokens.push(Token::Semicolon);
            continue;
        }

        let mut token = String::from(character);

        while let Some(next_charcter) = cursor.peek() {
            if !next_charcter.is_ascii_whitespace()
                && *next_charcter != ';'
                && *next_charcter != ','
                && *next_charcter != ')'
            {
                token.push(cursor.next().expect("Could not read a peeked character"));
            } else {
                break;
            }
        }

        match &token[..] {
            "CREATE" => tokens.push(Token::Keyword(token)),
            "INSERT" => tokens.push(Token::Keyword(token)),

            "TABLE" => tokens.push(Token::Keyword(token)),
            "DATABASE" => tokens.push(Token::Keyword(token)),

            "VALUES" => tokens.push(Token::Keyword(token)),
            "INTO" => tokens.push(Token::Keyword(token)),
            "NOT" => tokens.push(Token::Keyword(token)),
            "NULL" => tokens.push(Token::Keyword(token)),

            "integer" => tokens.push(Token::Keyword(token)),

            _ => {
                if token.chars().all(|i| i.is_numeric()) {
                    tokens.push(Token::NumericLiteral(token));
                } else {
                    tokens.push(Token::Identifier(token))
                }
            }
        }
    }

    return tokens;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenizing_create_database_input() {
        assert_eq!(
            vec![
                Token::Keyword("CREATE".to_string()),
                Token::Keyword("DATABASE".to_string()),
                Token::Identifier("my_database".to_string()),
                Token::Semicolon
            ],
            tokenize("CREATE DATABASE my_database;"),
        )
    }

    #[test]
    fn test_tokenizing_create_table_without_any_columns_input() {
        assert_eq!(
            vec![
                Token::Keyword("CREATE".to_string()),
                Token::Keyword("TABLE".to_string()),
                Token::Identifier("my_table".to_string()),
                Token::OpeningParenthesis,
                Token::ClosingParenthesis,
                Token::Semicolon
            ],
            tokenize("CREATE TABLE my_table ();"),
        )
    }

    #[test]
    fn test_tokenizing_create_table_with_columns_input() {
        assert_eq!(
            vec![
                Token::Keyword("CREATE".to_string()),
                Token::Keyword("TABLE".to_string()),
                Token::Identifier("users".to_string()),
                Token::OpeningParenthesis,
                Token::Identifier("birth_year".to_string()),
                Token::Keyword("integer".to_string()),
                Token::Comma,
                Token::Identifier("age".to_string()),
                Token::Keyword("integer".to_string()),
                Token::Keyword("NOT".to_string()),
                Token::Keyword("NULL".to_string()),
                Token::ClosingParenthesis,
                Token::Semicolon
            ],
            tokenize("CREATE TABLE users (birth_year integer, age integer NOT NULL);"),
        )
    }

    #[test]
    fn test_tokenizing_insert_input() {
        assert_eq!(
            vec![
                Token::Keyword("INSERT".to_string()),
                Token::Keyword("INTO".to_string()),
                Token::Identifier("users".to_string()),
                Token::Keyword("VALUES".to_string()),
                Token::OpeningParenthesis,
                Token::NumericLiteral("12".to_string()),
                Token::Comma,
                Token::NumericLiteral("31".to_string()),
                Token::ClosingParenthesis,
                Token::Semicolon
            ],
            tokenize("INSERT INTO users VALUES (12, 31);"),
        )
    }
}
