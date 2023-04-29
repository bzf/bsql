use std::collections::HashMap;

use crate::internal::TableSchema;

pub struct Database {
    table_definitions: HashMap<String, TableSchema>,
}

impl Database {
    pub fn new() -> Self {
        Self {
            table_definitions: HashMap::new(),
        }
    }

    pub fn table_names(&self) -> Vec<String> {
        self.table_definitions.keys().cloned().collect()
    }

    pub fn create_table(&mut self, table_schema: &TableSchema) -> Option<()> {
        if !self.table_exists(table_schema.name()) {
            self.table_definitions
                .insert(table_schema.name().to_string(), table_schema.clone());
            Some(())
        } else {
            None
        }
    }

    fn table_exists(&self, table_name: &str) -> bool {
        self.table_definitions.contains_key(table_name)
    }
}

#[cfg(test)]
mod tests {
    use crate::internal::{ColumnDefinition, DataType};

    use super::*;

    #[test]
    fn test_creating_new_table_without_columns() {
        let mut database = Database::new();

        let result = database.create_table(&TableSchema::new("foobar".to_string(), vec![]));

        assert!(result.is_some(), "Failed to create table");
    }

    #[test]
    fn test_creating_new_table_with_columns() {
        let mut database = Database::new();

        let result = database.create_table(&TableSchema::new(
            "foobar".to_string(),
            vec![ColumnDefinition::new("age".to_string(), DataType::Integer)],
        ));

        assert!(result.is_some(), "Failed to create table");
    }

    #[test]
    fn test_creating_table_that_already_exists() {
        let mut database = Database::new();
        let table_schema = TableSchema::new("new_database".to_string(), vec![]);
        assert!(database.create_table(&table_schema).is_some());

        let result = database.create_table(&table_schema);

        assert!(result.is_none(), "Should fail to create duplicate database");
    }
}
