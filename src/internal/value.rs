use std::fmt::Display;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Integer(u8),
}

impl Value {
    pub fn to_bsql_data(&self) -> Vec<u8> {
        match self {
            Value::Integer(value) => vec![*value],
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Integer(value) => write!(f, "{}", value),
        }
    }
}
