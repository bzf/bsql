mod data_type_identifier;
mod literal_value;
mod tokenizer;

use super::Error;
use literal_value::LiteralValue;

pub use data_type_identifier::DataTypeIdentifier;
pub use tokenizer::Token;

#[macro_export]
macro_rules! expect_token {
    ( $token:expr, $expected_token:pat ) => {{
        let token = $token;

        match token {
            Some($expected_token) => Ok(token.unwrap()),
            Some(_) => Err(Error::UnexpectedToken {
                actual: token.unwrap(),
            }),

            None => Err(Error::MissingToken),
        }
    }};
}

#[derive(Debug, PartialEq)]
pub enum Command {
    CreateDatabase {
        database_name: String,
    },

    CreateTable {
        table_name: String,
        column_definitions: Vec<(String, DataTypeIdentifier)>,
    },

    InsertInto {
        table_name: String,
        values: Vec<LiteralValue>,
    },

    Select {
        identifiers: Vec<String>,
        table_name: String,
        where_conditions: Vec<ConditionExpression>,
    },
}

#[derive(Debug, PartialEq)]
pub struct ConditionExpression {
    lhs: String,
    comparison: CompareOperation,
    rhs: LiteralValue,
}

#[derive(Debug, PartialEq)]
pub enum CompareOperation {
    Equality,
}

// TODO: Only reads one command at a time and ignores any tokens after that.
pub fn parse(input: &str) -> Result<Command, Error> {
    let tokens = tokenizer::tokenize(input);

    let command_tokens: Vec<Token> = tokens
        .into_iter()
        .take_while(|t| *t != Token::Semicolon)
        .collect();

    match command_tokens.first() {
        Some(Token::CreateKeyword) => parse_create_command(command_tokens),
        Some(Token::InsertKeyword) => parse_insert_command(command_tokens),
        Some(Token::SelectKeyword) => parse_select_command(command_tokens),

        Some(token) => Err(Error::UnexpectedToken {
            actual: token.clone(),
        }),
        None => Err(Error::MissingToken),
    }
}

fn parse_create_command(mut tokens: Vec<Token>) -> Result<Command, Error> {
    tokens.reverse();
    expect_token!(tokens.pop(), Token::CreateKeyword)?;

    let create_type_keyword = tokens.pop().ok_or(Error::MissingToken)?;
    let identifier = expect_identifier(tokens.pop())?;

    match create_type_keyword {
        Token::DatabaseKeyword => Ok(Command::CreateDatabase {
            database_name: identifier,
        }),
        Token::TableKeyword => {
            tokens.reverse(); // Reverse them back to the input order
            return parse_create_table_command(identifier, tokens);
        }

        _ => Err(Error::UnexpectedToken {
            actual: create_type_keyword,
        }),
    }
}

fn parse_select_command(tokens: Vec<Token>) -> Result<Command, Error> {
    let mut tokens = tokens.into_iter().peekable();
    expect_token!(tokens.next(), Token::SelectKeyword)?;

    let mut identifiers: Vec<String> = vec![];

    loop {
        match tokens.next() {
            Some(Token::Comma) => (),
            Some(Token::Asterisk) => identifiers.push("*".to_string()),
            Some(Token::Identifier(name)) => identifiers.push(name),

            Some(Token::FromKeyword) => break,

            Some(token) => return Err(Error::UnexpectedToken { actual: token }),
            None => return Err(Error::MissingToken),
        }
    }

    let table_name = expect_identifier(tokens.next())?;

    let mut where_conditions = vec![];

    match tokens.next() {
        Some(Token::WhereKeyword) => {
            if let Some(where_expression) = parse_condition_expression(tokens.collect()) {
                where_conditions.push(where_expression);
            }
        }
        _ => (),
    }

    return Ok(Command::Select {
        identifiers,
        table_name,
        where_conditions,
    });
}

fn parse_insert_command(mut tokens: Vec<Token>) -> Result<Command, Error> {
    tokens.reverse();

    expect_token!(tokens.pop(), Token::InsertKeyword)?;
    expect_token!(tokens.pop(), Token::IntoKeyword)?;

    let identifier = expect_identifier(tokens.pop())?;

    expect_token!(tokens.pop(), Token::ValuesKeyword)?;
    expect_token!(tokens.pop(), Token::OpeningParenthesis)?;

    tokens.reverse(); // Return the list back to the input order
    let mut tokens = tokens.into_iter().peekable();

    let mut literal_values: Vec<LiteralValue> = vec![];

    loop {
        let next_token = tokens.next();

        match next_token {
            Some(Token::ClosingParenthesis) => break,

            Some(token) => {
                let literal_value: LiteralValue = {
                    let literal: Option<LiteralValue> = token.clone().into();
                    literal.ok_or(Error::UnexpectedToken { actual: token })?
                };

                literal_values.push(literal_value);

                if let Some(Token::Comma) = tokens.peek() {
                    tokens.next();
                }
            }

            None => return Err(Error::MissingToken),
        }
    }

    return Ok(Command::InsertInto {
        table_name: identifier,
        values: literal_values,
    });
}

fn parse_create_table_command(identifier: String, tokens: Vec<Token>) -> Result<Command, Error> {
    Ok(Command::CreateTable {
        table_name: identifier,
        column_definitions: parse_column_definitions(tokens)?,
    })
}

fn parse_column_definitions(
    tokens: Vec<Token>,
) -> Result<Vec<(String, DataTypeIdentifier)>, Error> {
    let mut column_info_list = vec![];
    let mut tokens = tokens.into_iter().peekable();

    expect_token!(tokens.next(), Token::OpeningParenthesis)?;

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
            match tokens.next() {
                Some(token) => return Err(Error::UnexpectedToken { actual: token }),
                _ => return Err(Error::MissingToken),
            }
        }
    }

    return Ok(column_info_list);
}

fn parse_column_definition(
    identifier_token: Token,
    data_type_token: Token,
) -> Result<(String, DataTypeIdentifier), Error> {
    match identifier_token {
        Token::Identifier(identifier) => {
            let data_type: Option<DataTypeIdentifier> = data_type_token.clone().into();
            Ok((
                identifier,
                data_type.ok_or(Error::UnexpectedToken {
                    actual: data_type_token,
                })?,
            ))
        }

        actual => Err(Error::UnexpectedToken { actual }),
    }
}

fn parse_condition_expression(tokens: Vec<Token>) -> Option<ConditionExpression> {
    let mut token_iter = tokens.into_iter();
    let Some(Token::Identifier(lhs)) = token_iter.next() else {
        return None;
    };

    let Some(Token::EqualSign) = token_iter.next() else {
        return None;
    };

    let rhs: Option<LiteralValue> = token_iter.next()?.into();

    Some(ConditionExpression {
        lhs,
        rhs: rhs?,
        comparison: CompareOperation::Equality,
    })
}

fn expect_identifier(token: Option<Token>) -> Result<String, Error> {
    match token {
        Some(Token::Identifier(identifier)) => Ok(identifier),
        Some(token) => Err(Error::UnexpectedToken { actual: token }),
        None => Err(Error::MissingToken),
    }
}

#[cfg(test)]
mod tests {
    use literal_value::LiteralValue;

    use super::*;

    #[test]
    fn test_parsing_create_database_expression() {
        assert_eq!(
            Ok(Command::CreateDatabase {
                database_name: "my_database".to_string()
            }),
            parse("CREATE DATABASE my_database;"),
        );
    }

    #[test]
    fn test_parsing_create_table_expression() {
        assert_eq!(
            Ok(Command::CreateTable {
                table_name: "users".to_string(),
                column_definitions: vec![
                    ("age".to_string(), DataTypeIdentifier::Integer),
                    ("birthyear".to_string(), DataTypeIdentifier::Integer)
                ]
            }),
            parse("CREATE TABLE users (age integer, birthyear integer);"),
        );
    }

    #[test]
    fn test_parsing_insert_into_expression() {
        assert_eq!(
            Ok(Command::InsertInto {
                table_name: "users2".to_string(),
                values: vec![LiteralValue::Integer(12)]
            }),
            parse("INSERT INTO users2 VALUES (12);"),
        );
    }

    #[test]
    fn test_parsing_insert_into_expression_with_multiple_values() {
        assert_eq!(
            Ok(Command::InsertInto {
                table_name: "users2".to_string(),
                values: vec![LiteralValue::Integer(12), LiteralValue::Integer(14)]
            }),
            parse("INSERT INTO users2 VALUES (12, 14);"),
        );
    }

    #[test]
    fn test_parsing_select_all() {
        assert_eq!(
            Ok(Command::Select {
                identifiers: vec!["*".to_string()],
                table_name: "my_table".to_string(),
                where_conditions: vec![],
            }),
            parse("SELECT * FROM my_table;"),
        );
    }

    #[test]
    fn test_parsing_select_with_where_condition() {
        assert_eq!(
            Ok(Command::Select {
                identifiers: vec!["*".to_string()],
                table_name: "my_table".to_string(),
                where_conditions: vec![ConditionExpression {
                    lhs: "favorite_number".to_string(),
                    comparison: CompareOperation::Equality,
                    rhs: LiteralValue::Integer(42),
                }]
            }),
            parse("SELECT * FROM my_table WHERE favorite_number = 42;"),
        );
    }

    #[test]
    fn test_parsing_condition_expression() {
        assert_eq!(
            Some(ConditionExpression {
                lhs: "user_id".to_string(),
                comparison: CompareOperation::Equality,
                rhs: LiteralValue::Integer(3),
            }),
            parse_condition_expression(vec![
                Token::Identifier("user_id".to_string()),
                Token::EqualSign,
                Token::NumericLiteral(3.to_string()),
            ])
        );
    }
}
