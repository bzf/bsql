use std::collections::HashMap;

use super::{ColumnDefinition, DataType, TableManager, Value};

type TableId = u64;

pub struct Database {
    table_names: HashMap<String, TableId>,
    table_managers: HashMap<TableId, TableManager>,

    next_table_id: TableId,
}

impl Database {
    pub fn new() -> Self {
        Self {
            table_managers: HashMap::new(),

            table_names: HashMap::new(),
            next_table_id: 0,
        }
    }

    pub fn table_names(&self) -> Vec<String> {
        self.table_names.keys().cloned().collect()
    }

    pub fn column_definitions(&self, table_name: &str) -> Option<Vec<(String, ColumnDefinition)>> {
        let Some(table_id) = self.table_names.get(table_name) else {
            return None;
        };

        self.table_managers
            .get(table_id)
            .map(|table_manager| table_manager.column_definitions())
    }

    pub fn create_table(&mut self, table_name: &str) -> Option<TableId> {
        if !self.table_exists(table_name) {
            let table_id = self.next_table_id;
            self.next_table_id += 1;

            self.table_names.insert(table_name.to_string(), table_id);
            self.table_managers
                .insert(table_id, TableManager::new(table_id));

            Some(table_id)
        } else {
            None
        }
    }

    pub fn add_column(&mut self, table_name: &str, column_name: &str, data_type: DataType) -> bool {
        let Some(table_id) = self.table_names.get(table_name) else {
            return false;
        };

        let Some(table_manager) = self.table_managers.get_mut(table_id) else {
            return false;
        };

        table_manager.add_column(column_name, data_type);

        return true;
    }

    pub fn insert_row(&mut self, table_name: &str, values: Vec<Value>) -> bool {
        let Some(table_id) = self.table_names.get(table_name) else {
            return false;
        };

        let Some(table_manager) = self.table_managers.get_mut(table_id) else {
            return false;
        };

        if values.len() != table_manager.column_definitions().len() {
            return false;
        }

        table_manager.insert_record(values)
    }

    pub fn select_all_columns(&self, table_name: &str) -> Option<Vec<Vec<Option<Value>>>> {
        let Some(table_id) = self.table_names.get(table_name) else {
            return None;
        };

        if let Some(table_manager) = self.table_managers.get(table_id) {
            return Some(table_manager.get_records());
        } else {
            return Some(vec![]);
        };
    }

    pub fn select_columns_by_name(
        &self,
        table_name: &str,
        column_names: Vec<&str>,
    ) -> Option<Vec<Vec<Option<Value>>>> {
        let Some(table_id) = self.table_names.get(table_name) else {
            return None;
        };

        let Some(table_manager) = self.table_managers.get(table_id) else {
            return None;
        };

        return table_manager.get_records_for_columns(&column_names);
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

    #[test]
    fn select_all_from_table() {
        let mut database = Database::new();
        let table_name = "new_table";
        assert!(database.create_table(table_name).is_some());
        assert!(database.add_column(table_name, "age", DataType::Integer));
        assert!(database.insert_row(table_name, vec![Value::Integer(5)]));

        let result = database
            .select_all_columns(table_name)
            .expect("Failed to select from database");

        assert_eq!(vec![vec![Some(Value::Integer(5))]], result);
    }

    #[test]
    fn select_all_from_table_with_different_columns_over_time() {
        let mut database = Database::new();
        let table_name = "new_table";
        assert!(database.create_table(table_name).is_some());
        assert!(database.add_column(table_name, "age", DataType::Integer));
        assert!(database.insert_row(table_name, vec![Value::Integer(1)]));

        assert!(database.add_column(table_name, "month", DataType::Integer));
        assert!(database.insert_row(table_name, vec![Value::Integer(2), Value::Integer(3)]));

        // Order is not guaranteed
        let result = database
            .select_all_columns(table_name)
            .expect("Failed to select from database");

        assert!(result.contains(&vec![Some(Value::Integer(1)), None]));
        assert!(result.contains(&vec![Some(Value::Integer(1)), None]));
    }

    #[test]
    fn select_all_from_empty_table() {
        let mut database = Database::new();
        let table_name = "new_table";
        assert!(database.create_table(table_name).is_some());

        let result = database
            .select_all_columns(table_name)
            .expect("Failed to select from database");

        assert!(result.is_empty());
    }

    #[test]
    fn select_from_table() {
        let mut database = Database::new();
        let table_name = "new_table";
        assert!(database.create_table(table_name).is_some());
        assert!(database.add_column(table_name, "age", DataType::Integer));
        assert!(database.add_column(table_name, "birthday", DataType::Integer));
        assert!(database.insert_row(table_name, vec![Value::Integer(5), Value::Integer(3)]));

        let result = database
            .select_columns_by_name(table_name, vec!["birthday"])
            .expect("Failed to select from database");

        assert_eq!(vec![vec![Some(Value::Integer(3))]], result);
    }

    #[test]
    fn select_with_column_that_doesnt_exist() {
        let mut database = Database::new();
        let table_name = "new_table";
        assert!(database.create_table(table_name).is_some());
        assert!(database.add_column(table_name, "age", DataType::Integer));
        assert!(database.insert_row(table_name, vec![Value::Integer(5)]));

        let result = database.select_columns_by_name(table_name, vec!["lol123"]);

        assert_eq!(None, result);
    }
}
