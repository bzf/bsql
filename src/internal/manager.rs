use std::rc::Rc;
use std::sync::RwLock;

use super::{
    parse, ColumnDefinition, Command, DataType, Database, Error, PageId, PageManager, QueryResult,
    Value,
};
use crate::internal::SharedInternalPage;

pub struct Manager {
    page_manager: Rc<RwLock<PageManager>>,
    page: SharedInternalPage,
}

impl Manager {
    pub fn new(page_manager: Rc<RwLock<PageManager>>) -> Self {
        let (_page_id, shared_page) = {
            let mut page_manager = page_manager.write().unwrap();
            page_manager.create_page()
        };

        Self::write_metadata_page(shared_page.clone(), vec![]);

        Self {
            page_manager,
            page: shared_page,
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
            let mut page_manager = self.page_manager.write().unwrap();
            let (page_id, shared_page) = page_manager.create_page();

            Database::initialize(self.page_manager.clone(), shared_page.clone(), name)?;

            let mut database_page_ids = self.database_page_ids();
            database_page_ids.push(page_id);
            Self::write_metadata_page(self.page.clone(), database_page_ids);

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
        let database_page_ids = self.database_page_ids();
        let mut databases = Vec::new();

        for database_page_id in &database_page_ids {
            let page_manager = self.page_manager.read().unwrap();
            let page = page_manager.fetch_page(*database_page_id).unwrap();

            let database = Database::load(self.page_manager.clone(), page).unwrap();
            databases.push(database);
        }

        return databases;
    }

    fn database_page_ids(&self) -> Vec<PageId> {
        let page = self.page.read().unwrap();
        let number_of_database_page_ids = page.metadata[0] as usize;
        let mut database_page_ids: Vec<PageId> = Vec::new();

        for index in 0..number_of_database_page_ids {
            let start_index = index as usize * 4 + 1;
            let end_index = start_index + 4;
            let page_id_bytes: &[u8; 4] =
                &page.metadata[start_index..end_index].try_into().unwrap();

            let database_page_id = PageId::from_le_bytes(*page_id_bytes);
            database_page_ids.push(database_page_id);
        }

        return database_page_ids;
    }

    fn write_metadata_page(shared_page: SharedInternalPage, database_page_ids: Vec<PageId>) {
        let mut page = shared_page.write().unwrap();
        let number_of_database_page_ids = database_page_ids.len();
        page.metadata[0] = number_of_database_page_ids as u8;

        let database_page_ids_bytes: Vec<u8> = database_page_ids
            .iter()
            .map(|dpid| dpid.to_le_bytes())
            .flatten()
            .collect();

        page.metadata[1..database_page_ids_bytes.len() + 1]
            .copy_from_slice(&database_page_ids_bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_databases() {
        let page_manager = Rc::new(RwLock::new(PageManager::new()));
        let mut manager = Manager::new(page_manager);
        manager.create_database("hello").unwrap();
        manager.create_database("world").unwrap();

        assert_eq!(
            vec!["hello".to_string(), "world".to_string()],
            manager.database_names()
        );
    }
}
