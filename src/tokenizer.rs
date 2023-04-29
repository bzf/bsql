#[derive(Debug, PartialEq)]
pub enum Token {
    OpeningParenthesis,
    ClosingParenthesis,
    Comma,
    Semicolon,

    SelectKeyword,
    CreateKeyword,
    InsertKeyword,
    UseKeyword,

    TableKeyword,
    DatabaseKeyword,

    FromKeyword,
    ValuesKeyword,
    IntoKeyword,
    NotKeyword,
    NullKeyword,
    IntegerKeyword,

    Asterisk,

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
        } else if character == '*' {
            tokens.push(Token::Asterisk);
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
            "CREATE" => tokens.push(Token::CreateKeyword),
            "INSERT" => tokens.push(Token::InsertKeyword),
            "SELECT" => tokens.push(Token::SelectKeyword),
            "USE" => tokens.push(Token::UseKeyword),

            "TABLE" => tokens.push(Token::TableKeyword),
            "DATABASE" => tokens.push(Token::DatabaseKeyword),

            "FROM" => tokens.push(Token::FromKeyword),
            "VALUES" => tokens.push(Token::ValuesKeyword),
            "INTO" => tokens.push(Token::IntoKeyword),
            "NOT" => tokens.push(Token::NotKeyword),
            "NULL" => tokens.push(Token::NullKeyword),

            "integer" => tokens.push(Token::IntegerKeyword),

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
    fn test_tokenizing_special_characters() {
        assert_eq!(vec![Token::Asterisk,], tokenize("*"),)
    }

    #[test]
    fn test_tokenizing_create_database_input() {
        assert_eq!(
            vec![
                Token::CreateKeyword,
                Token::DatabaseKeyword,
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
                Token::CreateKeyword,
                Token::TableKeyword,
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
                Token::CreateKeyword,
                Token::TableKeyword,
                Token::Identifier("users".to_string()),
                Token::OpeningParenthesis,
                Token::Identifier("birth_year".to_string()),
                Token::IntegerKeyword,
                Token::Comma,
                Token::Identifier("age".to_string()),
                Token::IntegerKeyword,
                Token::NotKeyword,
                Token::NullKeyword,
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
                Token::InsertKeyword,
                Token::IntoKeyword,
                Token::Identifier("users".to_string()),
                Token::ValuesKeyword,
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

    #[test]
    fn test_tokenizing_select_all_input() {
        assert_eq!(
            vec![
                Token::SelectKeyword,
                Token::Asterisk,
                Token::FromKeyword,
                Token::Identifier("users".to_string()),
                Token::Semicolon
            ],
            tokenize("SELECT * FROM users;"),
        )
    }
}
