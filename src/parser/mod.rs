mod literal_value;
mod tokenizer;

use crate::internal::{ColumnDefinition, DataType, TableSchema};
use literal_value::LiteralValue;
use tokenizer::Token;

#[derive(Debug, PartialEq)]
pub enum Command {
    CreateDatabase {
        database_name: String,
    },

    CreateTable {
        schema: TableSchema,
    },

    InsertInto {
        table_name: String,
        values: Vec<LiteralValue>,
    },
}

// TODO: Only reads one command at a time and ignores any tokens after that.
pub fn parse(input: &str) -> Option<Command> {
    let tokens = tokenizer::tokenize(input);

    let command_tokens: Vec<Token> = tokens
        .into_iter()
        .take_while(|t| *t != Token::Semicolon)
        .collect();

    match command_tokens.first() {
        Some(Token::CreateKeyword) => parse_create_command(command_tokens),
        Some(Token::InsertKeyword) => parse_insert_command(command_tokens),

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
        Token::DatabaseKeyword => Some(Command::CreateDatabase {
            database_name: identifier,
        }),
        Token::TableKeyword => {
            tokens.reverse(); // Reverse them back to the input order
            return parse_create_table_command(identifier, tokens);
        }

        _ => None,
    }
}

fn parse_insert_command(mut tokens: Vec<Token>) -> Option<Command> {
    tokens.reverse();
    tokens.pop()?; // Skip the first known `Token::InsertKeyword`

    let Some(Token::IntoKeyword) = tokens.pop() else {
        // Expeceted `Token::IntoKeyword`
        return None;
    };

    let Token::Identifier(identifier) = tokens.pop()? else {
        // Expeceted an table identifier
        return None;
    };

    let Token::ValuesKeyword = tokens.pop()? else {
        // Expeceted `Token::ValuesKeyword`
        return None;
    };

    let Token::OpeningParenthesis = tokens.pop()? else {
        // Expeceted `Token::OpeningParenthesis`
        return None;
    };

    tokens.reverse(); // Return the list back to the input order
    let mut tokens = tokens.into_iter().peekable();
    println!("parse_insert_command: {:?}", tokens);

    let mut literal_values: Vec<LiteralValue> = vec![];

    loop {
        if Some(&Token::ClosingParenthesis) == tokens.peek() {
            break;
        } else if let Some(literal_value) = tokens.next()?.into() {
            literal_values.push(literal_value);

            if let Some(Token::Comma) = tokens.peek() {
                tokens.next(); // Step over the trailing comma
            }
        } else {
            println!("Got unexpected token: {:?}", tokens.peek());
            return None; // Expeceted another column info or a `ClosingParenthesis`
        }
    }

    return Some(Command::InsertInto {
        table_name: identifier,
        values: literal_values,
    });
}

fn parse_create_table_command(identifier: String, tokens: Vec<Token>) -> Option<Command> {
    Some(Command::CreateTable {
        schema: TableSchema::new(identifier, parse_column_definitions(tokens)?),
    })
}

fn parse_column_definitions(tokens: Vec<Token>) -> Option<Vec<ColumnDefinition>> {
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
            column_info_list.push(parse_column_definition(identifier_token, data_type_token)?);

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

fn parse_column_definition(
    identifier_token: Token,
    data_type_token: Token,
) -> Option<ColumnDefinition> {
    match identifier_token {
        Token::Identifier(identifier) => {
            let data_type: Option<DataType> = data_type_token.into();
            Some(ColumnDefinition::new(identifier, data_type?))
        }

        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use literal_value::LiteralValue;

    use super::*;

    #[test]
    fn test_parsing_create_database_expression() {
        assert_eq!(
            Some(Command::CreateDatabase {
                database_name: "my_database".to_string()
            }),
            parse("CREATE DATABASE my_database;"),
        );
    }

    #[test]
    fn test_parsing_create_table_expression() {
        assert_eq!(
            Some(Command::CreateTable {
                schema: TableSchema::new(
                    "users".to_string(),
                    vec![
                        ColumnDefinition::new("age".to_string(), DataType::Integer),
                        ColumnDefinition::new("birthyear".to_string(), DataType::Integer)
                    ]
                )
            }),
            parse("CREATE TABLE users (age integer, birthyear integer);"),
        );
    }

    #[test]
    fn test_parsing_insert_into_expression() {
        assert_eq!(
            Some(Command::InsertInto {
                table_name: "users2".to_string(),
                values: vec![LiteralValue::Integer(12)]
            }),
            parse("INSERT INTO users2 VALUES (12);"),
        );
    }

    #[test]
    fn test_parsing_insert_into_expression_with_multiple_values() {
        assert_eq!(
            Some(Command::InsertInto {
                table_name: "users2".to_string(),
                values: vec![LiteralValue::Integer(12), LiteralValue::Integer(14)]
            }),
            parse("INSERT INTO users2 VALUES (12, 14);"),
        );
    }
}
