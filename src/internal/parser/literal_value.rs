use crate::internal::Value;

use super::tokenizer::Token;

#[derive(Debug, PartialEq)]
pub enum LiteralValue {
    Integer(u8),
}

impl From<Token> for Option<LiteralValue> {
    fn from(value: Token) -> Self {
        match value {
            Token::NumericLiteral(literal) => {
                if let Ok(value) = literal.parse::<u8>() {
                    Some(LiteralValue::Integer(value))
                } else {
                    None
                }
            }

            _ => None,
        }
    }
}

impl Into<Value> for LiteralValue {
    fn into(self) -> Value {
        match self {
            LiteralValue::Integer(value) => Value::Integer(value),
        }
    }
}
