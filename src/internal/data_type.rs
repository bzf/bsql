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
