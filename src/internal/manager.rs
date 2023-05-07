use super::{
    parse, ColumnDefinition, Command, DataType, Database, Error, PageId, QueryResult, Value,
};
use crate::internal::SharedInternalPage;

pub struct Manager {
    page: SharedInternalPage,
    database_page_ids: Vec<PageId>,
}

impl Manager {
    pub fn new() -> Self {
        let mut page_manager = super::page_manager().write().unwrap();
        let (_page_id, shared_page) = page_manager.create_page();

        Self {
            page: shared_page,
            database_page_ids: Vec::new(),
        }
    }

    pub fn execute(&mut self, database_name: &str, query: &str) -> Result<QueryResult, Error> {
        match parse(query)? {
            Command::CreateDatabase { database_name } => self.create_database(&database_name),

            Command::CreateTable {
                table_name,
                column_definitions,
            } => {
                let columns: Vec<(String, DataType)> = column_definitions
                    .into_iter()
                    .map(|(c, dt)| (c, dt.into()))
                    .collect();

                self.create_table(database_name, &table_name, columns)
            }

            Command::InsertInto { table_name, values } => self.insert_row(
                database_name,
                &table_name,
                values.into_iter().map(|value| value.into()).collect(),
            ),

            Command::Select {
                identifiers,
                table_name,
                ..
            } => {
                match &identifiers
                    .iter()
                    .map(|i| i.as_str())
                    .collect::<Vec<&str>>()[..]
                {
                    ["*"] => self.select_all(database_name, &table_name),

                    _ => self.select(database_name, &table_name, identifiers.clone()),
                }
            }
        }
    }

    pub fn database_names(&self) -> Vec<String> {
        self.databases()
            .iter()
            .map(|d| d.name().to_string())
            .collect()
    }

    pub fn database_table_names(&self, database_name: &str) -> Result<Vec<String>, Error> {
        self.databases()
            .iter()
            .find(|d| d.name() == database_name)
            .ok_or(Error::DatabaseDoesNotExist(database_name.to_string()))
            .map(|database| database.table_names())
    }

    pub fn table_definition(
        &self,
        database_name: &str,
        table_name: &str,
    ) -> Result<Vec<ColumnDefinition>, Error> {
        self.databases()
            .iter()
            .find(|d| d.name() == database_name)
            .ok_or(Error::DatabaseDoesNotExist(database_name.to_string()))
            .and_then(|database| database.column_definitions(table_name))
    }

    pub fn database_exists(&self, key: &str) -> bool {
        self.databases().iter().find(|d| d.name() == key).is_some()
    }

    fn create_table(
        &mut self,
        database_name: &str,
        table_name: &str,
        columns: Vec<(String, DataType)>,
    ) -> Result<QueryResult, Error> {
        let mut databases = self.databases();
        let Some(database) = databases.iter_mut().find(|d| d.name() == database_name) else {
            return Err(Error::DatabaseDoesNotExist(database_name.to_string()));
        };

        database
            .create_table(table_name, columns)
            .map(|_table_id| QueryResult::CommandSuccessMessage("CREATE TABLE".to_string()))
    }

    fn add_column(
        &mut self,
        database_name: &str,
        table_name: &str,
        column_name: &str,
        data_type: DataType,
    ) -> Result<QueryResult, Error> {
        let mut databases = self.databases();
        let database = databases
            .iter_mut()
            .find(|d| d.name() == database_name)
            .ok_or(Error::DatabaseDoesNotExist(database_name.to_string()))?;

        database
            .add_column(table_name, column_name, data_type)
            .map(|command| QueryResult::CommandSuccessMessage(command))
    }

    fn create_database(&mut self, name: &str) -> Result<QueryResult, Error> {
        if !self.database_exists(name) {
            let mut page_manager = super::page_manager().write().unwrap();
            let (page_id, shared_page) = page_manager.create_page();

            Database::initialize(shared_page.clone(), name)?;

            self.database_page_ids.push(page_id);

            Ok(QueryResult::CommandSuccessMessage(
                "CREATE DATABASE".to_string(),
            ))
        } else {
            Err(Error::DatabaseAlreadyExists(name.to_string()))
        }
    }

    fn insert_row(
        &mut self,
        database_name: &str,
        table_name: &str,
        values: Vec<Value>,
    ) -> Result<QueryResult, Error> {
        let mut databases = self.databases();
        let database = databases
            .iter_mut()
            .find(|d| d.name() == database_name)
            .ok_or(Error::DatabaseDoesNotExist(database_name.to_string()))?;

        database
            .insert_row(table_name, values)
            .map(|_record_id| QueryResult::InsertSuccess { count: 1 })
    }

    fn select_all(&self, database_name: &str, table_name: &str) -> Result<QueryResult, Error> {
        let databases = self.databases();
        let database = databases
            .iter()
            .find(|d| d.name() == database_name)
            .ok_or(Error::DatabaseDoesNotExist(database_name.to_string()))?;

        return Ok(QueryResult::RowResult(
            database.select_all_columns(table_name)?,
        ));
    }

    fn select(
        &self,
        database_name: &str,
        table_name: &str,
        columns: Vec<String>,
    ) -> Result<QueryResult, Error> {
        let databases = self.databases();
        let database = databases
            .iter()
            .find(|d| d.name() == database_name)
            .ok_or(Error::DatabaseDoesNotExist(database_name.to_string()))?;

        return database
            .select_columns_by_name(
                table_name,
                columns.iter().map(|i| i.as_str()).collect::<Vec<&str>>(),
            )
            .map(|rows| QueryResult::RowResult(rows));
    }

    fn databases(&self) -> Vec<Database> {
        let mut databases: Vec<Database> = Vec::new();

        for database_page_id in &self.database_page_ids {
            let database = {
                let page_manager = super::page_manager().read().unwrap();
                let shared_page = page_manager.fetch_page(*database_page_id as u32).unwrap();
                Database::load(shared_page).unwrap()
            };

            databases.push(database);
        }

        return databases;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_databases() {
        let mut manager = Manager::new();
        manager.create_database("hello").unwrap();
        manager.create_database("world").unwrap();

        assert_eq!(
            vec!["hello".to_string(), "world".to_string()],
            manager.database_names()
        );
    }
}
