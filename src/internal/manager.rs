use std::collections::HashMap;

use super::{ColumnDefinition, DataType, Database, Error, QueryResult, Value};

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

    pub fn database_table_names(&self, database_name: &str) -> Result<Vec<String>, Error> {
        self.database_definitions
            .get(database_name)
            .ok_or(Error::DatabaseDoesNotExist(database_name.to_string()))
            .map(|database| database.table_names())
    }

    pub fn table_definition(
        &self,
        database_name: &str,
        table_name: &str,
    ) -> Result<Vec<(String, ColumnDefinition)>, Error> {
        self.database_definitions
            .get(database_name)
            .ok_or(Error::DatabaseDoesNotExist(database_name.to_string()))
            .and_then(|database| database.column_definitions(table_name))
    }

    pub fn database_exists(&self, key: &str) -> bool {
        self.database_definitions.contains_key(key)
    }

    pub fn create_table(
        &mut self,
        database_name: &str,
        table_name: &str,
    ) -> Result<QueryResult, Error> {
        let Some(database) = self.database_definitions.get_mut(database_name) else {
            return Err(Error::DatabaseDoesNotExist(database_name.to_string()));
        };

        database
            .create_table(table_name)
            .map(|_table_id| QueryResult::CommandSuccessMessage("CREATE TABLE".to_string()))
    }

    pub fn add_column(
        &mut self,
        database_name: &str,
        table_name: &str,
        column_name: &str,
        data_type: DataType,
    ) -> Result<QueryResult, Error> {
        let database = self
            .database_definitions
            .get_mut(database_name)
            .ok_or(Error::DatabaseDoesNotExist(database_name.to_string()))?;

        database
            .add_column(table_name, column_name, data_type)
            .map(|command| QueryResult::CommandSuccessMessage(command))
    }

    pub fn create_database(&mut self, name: &str) -> Result<QueryResult, Error> {
        if !self.database_definitions.contains_key(name) {
            self.database_definitions
                .insert(name.to_string(), Database::new());

            Ok(QueryResult::CommandSuccessMessage(
                "CREATE DATABASE".to_string(),
            ))
        } else {
            Err(Error::DatabaseAlreadyExists(name.to_string()))
        }
    }

    pub fn insert_row(
        &mut self,
        database_name: &str,
        table_name: &str,
        values: Vec<Value>,
    ) -> Result<QueryResult, Error> {
        let database = self
            .database_definitions
            .get_mut(database_name)
            .ok_or(Error::DatabaseDoesNotExist(database_name.to_string()))?;

        database
            .insert_row(table_name, values)
            .map(|_record_id| QueryResult::CommandSuccessMessage("INSERT INTO".to_string()))
    }

    pub fn select_all(&self, database_name: &str, table_name: &str) -> Result<QueryResult, Error> {
        let database = self
            .database_definitions
            .get(database_name)
            .ok_or(Error::DatabaseDoesNotExist(database_name.to_string()))?;

        return Ok(QueryResult::RowResult(
            database.select_all_columns(table_name)?,
        ));
    }

    pub fn select(
        &self,
        database_name: &str,
        table_name: &str,
        columns: Vec<String>,
    ) -> Result<QueryResult, Error> {
        let database = self
            .database_definitions
            .get(database_name)
            .ok_or(Error::DatabaseDoesNotExist(database_name.to_string()))?;

        return database
            .select_columns_by_name(
                table_name,
                columns.iter().map(|i| i.as_str()).collect::<Vec<&str>>(),
            )
            .map(|rows| QueryResult::RowResult(rows));
    }
}
