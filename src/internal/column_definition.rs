use super::DataType;

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDefinition {
    column_id: u8,
    data_type: DataType,
    name: String,
}

impl ColumnDefinition {
    pub fn new(column_id: u8, data_type: DataType, name: String) -> Self {
        Self {
            column_id,
            data_type,
            name,
        }
    }

    pub fn from_raw_bytes(bytes: &[u8]) -> Option<ColumnDefinition> {
        Some(Self {
            column_id: bytes[0],
            data_type: DataType::from_type_id(bytes[1])?,
            name: String::from_utf8(bytes[2..].to_vec()).ok()?,
        })
    }

    pub fn to_raw_bytes(&self) -> Vec<u8> {
        let mut column_definition = Vec::with_capacity(2 + self.name.len());
        column_definition.push(2 + self.name.len() as u8);
        column_definition.push(self.column_id);
        column_definition.push(self.data_type().bsql_type_id());
        column_definition.extend_from_slice(self.name.as_bytes());

        return column_definition;
    }

    pub fn column_id(&self) -> u8 {
        self.column_id
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub fn name(&self) -> &String {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serializing_and_deserializing_into_bytes() {
        let column_definition = ColumnDefinition::new(1, DataType::Integer, "hello".to_string());

        let serialized_column: Vec<u8> = column_definition.to_raw_bytes();
        assert_eq!(vec![7, 1, 1, 104, 101, 108, 108, 111], serialized_column);

        let deserialized_column = ColumnDefinition::from_raw_bytes(&serialized_column[1..]);
        assert_eq!(Some(column_definition), deserialized_column);
    }
}
