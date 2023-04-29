use crate::tokenizer::Token;

#[derive(Debug, PartialEq)]
pub enum DataType {
    Integer,
}

impl From<Token> for Option<DataType> {
    fn from(value: Token) -> Self {
        match value {
            Token::IntegerKeyword => Some(DataType::Integer),
            _ => None,
        }
    }
}
