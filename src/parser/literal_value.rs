use super::tokenizer::Token;

#[derive(Debug, PartialEq)]
pub enum LiteralValue {
    Integer(i32),
}

impl From<Token> for Option<LiteralValue> {
    fn from(value: Token) -> Self {
        match value {
            Token::NumericLiteral(literal) => {
                if let Ok(value) = literal.parse::<i32>() {
                    Some(LiteralValue::Integer(value))
                } else {
                    None
                }
            }

            _ => None,
        }
    }
}
