use std::collections::HashMap;

use super::{ColumnDefinition, DataType, Database, QueryResult, Value};

pub struct Manager {
    database_definitions: HashMap<String, Database>,
}

impl Manager {
    pub fn new() -> Self {
        Self {
            database_definitions: HashMap::new(),
        }
    }

    pub fn database_names(&self) -> Vec<String> {
        self.database_definitions.keys().cloned().collect()
    }

    pub fn database_table_names(&self, database_name: &str) -> Option<Vec<String>> {
        self.database_definitions
            .get(database_name)
            .map(|database| database.table_names())
    }

    pub fn table_definition(
        &self,
        database_name: &str,
        table_name: &str,
    ) -> Option<Vec<(String, ColumnDefinition)>> {
        self.database_definitions
            .get(database_name)
            .and_then(|database| database.column_definitions(table_name))
    }

    pub fn database_exists(&self, key: &str) -> bool {
        self.database_definitions.contains_key(key)
    }

    pub fn create_table(&mut self, database_name: &str, table_name: &str) -> bool {
        let Some(_database) = self.database_definitions.get_mut(database_name) else {
            return false;
        };

        self.database_definitions
            .get_mut(database_name)
            .and_then(|database| database.create_table(table_name))
            .is_some()
    }

    pub fn add_column(
        &mut self,
        database_name: &str,
        table_name: &str,
        column_name: &str,
        data_type: DataType,
    ) -> bool {
        let Some(database) = self.database_definitions.get_mut(database_name) else {
            return false;
        };

        database.add_column(table_name, column_name, data_type)
    }

    pub fn create_database(&mut self, name: &str) -> Option<()> {
        if !self.database_definitions.contains_key(name) {
            self.database_definitions
                .insert(name.to_string(), Database::new());

            println!("CREATE DATABASE");
            return Some(());
        } else {
            println!("error: Database '{}' already exists.", name);
            return None;
        }
    }

    pub fn insert_row(
        &mut self,
        database_name: &str,
        table_name: &str,
        values: Vec<Value>,
    ) -> bool {
        let Some(database) = self.database_definitions.get_mut(database_name) else {
            return false;
        };

        if database.insert_row(table_name, values) {
            println!("INSERT INTO");
            return true;
        } else {
            println!("ERROR: INSERT failed.");
            return false;
        }
    }

    pub fn select_all(&self, database_name: &str, table_name: &str) -> Option<QueryResult> {
        let Some(database) = self.database_definitions.get(database_name) else {
            return None;
        };

        return database.select_all_columns(table_name);
    }

    pub fn select(
        &self,
        database_name: &str,
        table_name: &str,
        columns: Vec<String>,
    ) -> Option<QueryResult> {
        let Some(database) = self.database_definitions.get(database_name) else {
            return None;
        };

        return database.select_columns_by_name(
            table_name,
            columns.iter().map(|i| i.as_str()).collect::<Vec<&str>>(),
        );
    }
}
