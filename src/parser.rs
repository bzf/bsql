use crate::data_type::DataType;
use crate::tokenizer::Token;

#[derive(Debug, PartialEq)]
pub enum Command {
    CreateDatabase(String),
    CreateTable(String, Vec<(String, DataType)>),
}

// TODO: Only reads one command at a time and ignores any tokens after that.
pub fn parse(input: &str) -> Option<Command> {
    let tokens = crate::tokenizer::tokenize(input);

    let command_tokens: Vec<Token> = tokens
        .into_iter()
        .take_while(|t| *t != Token::Semicolon)
        .collect();

    match command_tokens.first() {
        Some(Token::CreateKeyword) => parse_create_command(command_tokens),

        _ => None,
    }
}

fn parse_create_command(mut tokens: Vec<Token>) -> Option<Command> {
    tokens.reverse();
    tokens.pop()?; // Skip the first known `Token::CreateKeyword`

    let create_type_keyword = tokens.pop()?;

    let Token::Identifier(identifier) = tokens.pop()? else {
        return None;
    };

    match create_type_keyword {
        Token::DatabaseKeyword => Some(Command::CreateDatabase(identifier)),
        Token::TableKeyword => {
            tokens.reverse(); // Reverse them back to the input order
            return parse_create_table_command(identifier, tokens);
        }

        _ => None,
    }
}

fn parse_create_table_command(identifier: String, tokens: Vec<Token>) -> Option<Command> {
    Some(Command::CreateTable(
        identifier,
        parse_table_column_info_list(tokens)?,
    ))
}

fn parse_table_column_info_list(tokens: Vec<Token>) -> Option<Vec<(String, DataType)>> {
    let mut column_info_list = vec![];
    let mut tokens = tokens.into_iter().peekable();

    let Some(Token::OpeningParenthesis) = tokens.next() else {
        return None; // Expected the column list to start with an openining parenthesis
    };

    loop {
        if Some(&Token::ClosingParenthesis) == tokens.peek() {
            break;
        } else if let (Some(identifier_token), Some(data_type_token)) =
            (tokens.next(), tokens.next())
        {
            column_info_list.push(parse_table_column_info(identifier_token, data_type_token)?);

            if let Some(Token::Comma) = tokens.peek() {
                tokens.next(); // Step over the trailing comma
            }
        } else {
            println!("Got unexpected token: {:?}", tokens.peek());
            return None; // Expeceted another column info or a `ClosingParenthesis`
        }
    }

    return Some(column_info_list);
}

fn parse_table_column_info(
    identifier_token: Token,
    data_type_token: Token,
) -> Option<(String, DataType)> {
    match identifier_token {
        Token::Identifier(identifier) => {
            let data_type: Option<DataType> = data_type_token.into();
            Some((identifier, data_type?))
        }

        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parsing_create_database_expression() {
        assert_eq!(
            Some(Command::CreateDatabase("my_database".to_string())),
            parse("CREATE DATABASE my_database;"),
        );
    }

    #[test]
    fn test_parsing_create_table_expression() {
        assert_eq!(
            Some(Command::CreateTable(
                "users".to_string(),
                vec![
                    ("age".to_string(), DataType::Integer),
                    ("birthyear".to_string(), DataType::Integer),
                ]
            )),
            parse("CREATE TABLE users (age integer, birthyear integer);"),
        );
    }
}
