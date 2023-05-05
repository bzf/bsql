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

    pub fn from_raw_bytes(bytes: &[u8]) -> Option<ColumnDefinition> {
        let [column_id, data_type_id] = bytes else {
            return None;
        };

        Some(Self {
            column_id: *column_id,
            data_type: DataType::from_type_id(*data_type_id)?,
        })
    }

    pub fn to_raw_bytes(&self) -> Vec<u8> {
        let mut column_definition = Vec::with_capacity(2);
        column_definition.push(self.column_id);
        column_definition.push(self.data_type().bsql_type_id());

        return column_definition;
    }

    pub fn column_id(&self) -> u8 {
        self.column_id
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serializing_and_deserializing_into_bytes() {
        let column_definition = ColumnDefinition::new(1, DataType::Integer);

        let serialized_column: Vec<u8> = column_definition.to_raw_bytes();
        assert_eq!(vec![0x1, 0x1], serialized_column);

        let deserialized_column = ColumnDefinition::from_raw_bytes(&serialized_column);
        assert_eq!(Some(column_definition), deserialized_column);
    }
}
