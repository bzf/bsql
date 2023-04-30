use std::collections::HashMap;

use super::TableSchema;

pub struct DatabaseDefinition {
    table_definitions: HashMap<String, TableSchema>,
}

impl DatabaseDefinition {
    pub fn new() -> Self {
        Self {
            table_definitions: HashMap::new(),
        }
    }

    pub fn table_names(&self) -> Vec<String> {
        self.table_definitions.keys().cloned().collect()
    }

    pub fn create_table(&mut self, table_name: &str) -> Option<()> {
        if !self.table_exists(table_name) {
            self.table_definitions
                .insert(table_name.to_string(), TableSchema::new());
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
    use super::*;

    #[test]
    fn test_creating_new_table_without_columns() {
        let mut database = DatabaseDefinition::new();

        let result = database.create_table("foobar");

        assert!(result.is_some(), "Failed to create table");
    }

    #[test]
    fn test_creating_new_table_with_columns() {
        let mut database = DatabaseDefinition::new();

        let result = database.create_table("foobar");

        assert!(result.is_some(), "Failed to create table");
    }

    #[test]
    fn test_creating_table_that_already_exists() {
        let mut database = DatabaseDefinition::new();
        let table_name = "new_database";
        assert!(database.create_table(table_name).is_some());

        let result = database.create_table(table_name);

        assert!(result.is_none(), "Should fail to create duplicate database");
    }
}
