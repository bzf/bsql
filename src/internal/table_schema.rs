use super::ColumnDefinition;

#[derive(Debug, PartialEq)]
pub struct TableSchema {
    table_name: String,
    column_definitions: Vec<ColumnDefinition>,
}

impl TableSchema {
    pub fn new(table_name: String, column_definitions: Vec<ColumnDefinition>) -> Self {
        Self {
            table_name,
            column_definitions,
        }
    }

    pub fn name(&self) -> &str {
        return &self.table_name;
    }
}
