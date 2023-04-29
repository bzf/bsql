use super::DataType;

#[derive(Debug, PartialEq)]
pub struct ColumnDefinition {
    column_name: String,
    data_type: DataType,
}

impl ColumnDefinition {
    pub fn new(column_name: String, data_type: DataType) -> Self {
        Self {
            column_name,
            data_type,
        }
    }
}
