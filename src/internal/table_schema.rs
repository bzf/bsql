use std::collections::HashMap;

use super::{ColumnDefinition, DataType};

#[derive(Debug, Clone, PartialEq)]
pub struct TableSchema {
    next_column_index: u8,

    column_definitions: HashMap<String, ColumnDefinition>,
}

impl TableSchema {
    pub fn new() -> Self {
        Self {
            next_column_index: 0,

            column_definitions: HashMap::new(),
        }
    }

    pub fn column_definitions_len(&self) -> usize {
        self.column_definitions.len()
    }

    pub fn add_column(&mut self, column_name: &str, data_type: DataType) -> bool {
        if !self.column_definitions.contains_key(column_name) {
            self.column_definitions.insert(
                column_name.to_string(),
                ColumnDefinition::new(self.next_column_index, data_type),
            );
            self.next_column_index += 1;

            return true;
        } else {
            return false;
        }
    }

    pub fn drop_column(&mut self, column_name: &str) -> bool {
        self.column_definitions.remove(column_name).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_adding_first_column() {
        let mut table_schema = TableSchema::new();

        let result = table_schema.add_column("foobar", DataType::Integer);

        assert!(result, "Failed to add column");
    }

    #[test]
    fn test_adding_duplicate_column() {
        let mut table_schema = TableSchema::new();
        assert!(table_schema.add_column("foobar", DataType::Integer));

        let result = table_schema.add_column("foobar", DataType::Integer);

        assert_eq!(false, result);
    }

    #[test]
    fn test_deleting_column() {
        let mut table_schema = TableSchema::new();
        assert!(table_schema.add_column("foobar", DataType::Integer));

        let result = table_schema.drop_column("foobar");

        assert!(result);
    }

    #[test]
    fn test_deleting_non_existing_column() {
        let mut table_schema = TableSchema::new();

        let result = table_schema.drop_column("foobar");

        assert_eq!(false, result);
    }
}
