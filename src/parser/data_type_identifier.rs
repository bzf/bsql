use crate::internal::DataType;

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

impl Into<DataType> for DataTypeIdentifier {
    fn into(self) -> DataType {
        match self {
            DataTypeIdentifier::Integer => DataType::Integer,
        }
    }
}
