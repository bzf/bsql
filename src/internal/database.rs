use std::collections::HashMap;

use super::{DataType, TableSchema, TableStore, Value};

type TableId = u64;

pub struct Database {
    table_names: HashMap<String, TableId>,
    table_definitions: HashMap<TableId, TableSchema>,
    table_stores: HashMap<TableId, TableStore>,

    next_table_id: TableId,
}

impl Database {
    pub fn new() -> Self {
        Self {
            table_definitions: HashMap::new(),
            table_names: HashMap::new(),
            table_stores: HashMap::new(),

            next_table_id: 0,
        }
    }

    pub fn table_names(&self) -> Vec<String> {
        self.table_names.keys().cloned().collect()
    }

    pub fn table_schema(&self, table_name: &str) -> Option<&TableSchema> {
        let Some(table_id) = self.table_names.get(table_name) else {
            return None;
        };

        self.table_definitions.get(table_id)
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

    pub fn add_column(&mut self, table_name: &str, column_name: &str, data_type: DataType) -> bool {
        let Some(table_id) = self.table_names.get(table_name) else {
            return false;
        };

        let Some(table_schema) = self.table_definitions.get_mut(table_id) else {
            return false;
        };

        table_schema.add_column(column_name, data_type)
    }

    pub fn insert_row(&mut self, table_name: &str, values: Vec<Value>) -> bool {
        let Some(table_id) = self.table_names.get(table_name) else {
            return false;
        };

        let Some(table_schema) = self.table_definitions.get(table_id) else {
            return false;
        };

        if values.len() != table_schema.column_definitions_len() {
            return false;
        }

        let table_store = self
            .table_stores
            .entry(*table_id)
            .or_insert(TableStore::new());

        table_store.insert_record(values).is_some()
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

    #[test]
    fn test_adding_column_to_table() {
        let mut database = Database::new();
        let table_name = "new_database";
        assert!(database.create_table(table_name).is_some());

        let result = database.add_column(table_name, "age", DataType::Integer);

        assert!(result, "Failed to add column to table");
    }

    #[test]
    fn inserting_row_to_a_table() {
        let mut database = Database::new();
        let table_name = "new_table";
        assert!(database.create_table(table_name).is_some());
        assert!(database.add_column(table_name, "age", DataType::Integer));

        let result = database.insert_row(table_name, vec![Value::Integer(3)]);

        assert!(result, "Failed to insert row to table");
    }

    #[test]
    fn inserting_row_with_different_len_values() {
        let mut database = Database::new();
        let table_name = "new_table";
        assert!(database.create_table(table_name).is_some());
        assert!(database.add_column(table_name, "age", DataType::Integer));

        let result = database.insert_row(table_name, vec![Value::Integer(3), Value::Integer(5)]);

        assert_eq!(result, false);
    }
}
