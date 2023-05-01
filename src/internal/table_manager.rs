use std::collections::HashMap;

use super::{ColumnDefinition, DataType};

pub struct TableManager {
    table_id: u64,
    next_column_id: u8,

    column_definitions: HashMap<String, ColumnDefinition>,
}

impl TableManager {
    pub fn new(table_id: u64) -> Self {
        Self {
            table_id,
            next_column_id: 0,

            column_definitions: HashMap::new(),
        }
    }

    pub fn add_column(&mut self, column_name: &str, data_type: DataType) {
        self.column_definitions.insert(
            column_name.to_string(),
            ColumnDefinition::new(self.next_column_id, data_type),
        );

        self.next_column_id += 1;
    }
}
