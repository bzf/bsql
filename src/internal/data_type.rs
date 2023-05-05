use super::Value;

#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Integer,
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Integer => write!(f, "integer"),
        }
    }
}

impl DataType {
    pub fn from_type_id(type_id: u8) -> Option<Self> {
        match type_id {
            1 => Some(DataType::Integer),

            _ => None,
        }
    }

    pub fn to_bsql_value(data_type: &Self, data: &[u8]) -> Option<Value> {
        match data_type {
            DataType::Integer => Some(Value::Integer(*data.first()?)),
        }
    }

    pub fn bsql_type_id(&self) -> u8 {
        match self {
            DataType::Integer => 1,
        }
    }

    pub fn bsql_size(&self) -> u8 {
        match self {
            DataType::Integer => 1,
        }
    }
}
