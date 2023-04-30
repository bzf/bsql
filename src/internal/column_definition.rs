use super::DataType;

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDefinition {
    column_index: u8,
    data_type: DataType,
}

impl ColumnDefinition {
    pub fn new(column_index: u8, data_type: DataType) -> Self {
        Self {
            column_index,
            data_type,
        }
    }
}
