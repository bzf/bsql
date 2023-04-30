use super::tokenizer::Token;

#[derive(Debug, PartialEq)]
pub enum DataTypeIdentifier {
    Integer,
}

impl From<Token> for Option<DataTypeIdentifier> {
    fn from(value: Token) -> Self {
        match value {
            Token::IntegerKeyword => Some(DataTypeIdentifier::Integer),
            _ => None,
        }
    }
}
