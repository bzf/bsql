use std::fmt::Display;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Integer(u8),
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Integer(value) => write!(f, "{}", value),
        }
    }
}
