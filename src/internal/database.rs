use std::collections::HashMap;

use super::TableSchema;

type TableId = u64;

pub struct Database {
    table_names: HashMap<String, TableId>,
    table_definitions: HashMap<TableId, TableSchema>,

    next_table_id: TableId,
}

impl Database {
    pub fn new() -> Self {
        Self {
            table_definitions: HashMap::new(),
            table_names: HashMap::new(),

            next_table_id: 0,
        }
    }

    pub fn table_names(&self) -> Vec<String> {
        self.table_names.keys().cloned().collect()
    }

    pub fn create_table(&mut self, table_name: &str) -> Option<TableId> {
        if !self.table_exists(table_name) {
            let table_id = self.next_table_id;
            self.next_table_id += 1;

            self.table_names.insert(table_name.to_string(), table_id);
            self.table_definitions.insert(table_id, TableSchema::new());

            Some(table_id)
        } else {
            None
        }
    }

    fn table_exists(&self, table_name: &str) -> bool {
        self.table_names.contains_key(table_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_creating_new_table_without_columns() {
        let mut database = Database::new();

        let result = database.create_table("foobar");

        assert!(result.is_some(), "Failed to create table");
    }

    #[test]
    fn test_creating_new_table_with_columns() {
        let mut database = Database::new();

        let result = database.create_table("foobar");

        assert!(result.is_some(), "Failed to create table");
    }

    #[test]
    fn test_creating_table_that_already_exists() {
        let mut database = Database::new();
        let table_name = "new_database";
        assert!(database.create_table(table_name).is_some());

        let result = database.create_table(table_name);

        assert!(result.is_none(), "Should fail to create duplicate database");
    }
}
