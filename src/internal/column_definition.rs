use super::DataType;

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDefinition {
    column_id: u8,
    data_type: DataType,
}

impl ColumnDefinition {
    pub fn new(column_id: u8, data_type: DataType) -> Self {
        Self {
            column_id,
            data_type,
        }
    }

    pub fn column_id(&self) -> u8 {
        self.column_id
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }
}
