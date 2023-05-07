use super::{
    ColumnDefinition, DataType, Error, PageId, RowResult, SharedInternalPage, TableManager, Value,
};

type TableId = u64;

const TABLE_MANAGER_PAGE_IDS_OFFSET: usize = 64;

pub struct Database {
    page: SharedInternalPage,
    next_table_id: TableId,
}

impl Database {
    pub fn new(name: &str) -> Result<Self, Error> {
        let mut page_manager = super::page_manager().write().unwrap();
        let (_page_id, shared_page) = page_manager.create_page();

        if name.len() >= 63 {
            return Err(Error::DatabaseNameTooLong);
        }

        Self::write_metadata_page(shared_page.clone(), name, vec![]);

        Ok(Self {
            page: shared_page,
            next_table_id: 0,
        })
    }

    pub fn name(&self) -> String {
        let page = self.page.read().ok().unwrap();

        let name_length = page.metadata[0] as usize;
        let name_bytes = &page.metadata[1..name_length + 1];
        String::from_utf8(name_bytes.to_vec()).unwrap()
    }

    pub fn table_names(&self) -> Vec<String> {
        self.table_managers()
            .iter()
            .map(|t| t.name().clone())
            .collect()
    }

    pub fn column_definitions(&self, table_name: &str) -> Result<Vec<ColumnDefinition>, Error> {
        Ok(self
            .table_managers()
            .iter()
            .find(|t| t.name() == table_name)
            .ok_or(Error::TableDoesNotExist(table_name.to_string()))?
            .column_definitions())
    }

    pub fn create_table(
        &mut self,
        table_name: &str,
        columns: Vec<(String, DataType)>,
    ) -> Result<TableId, Error> {
        if !self.table_exists(table_name) {
            let table_id = self.next_table_id;

            let mut page_manager = super::page_manager().write().unwrap();
            let (page_id, shared_page) = page_manager.create_page();

            let mut table_manager = TableManager::initialize(shared_page, table_name)?;
            for (column_name, data_type) in columns.iter() {
                table_manager.add_column(column_name, data_type.clone())?;
            }

            let mut table_manager_page_ids = self.table_manager_page_ids();
            table_manager_page_ids.push(page_id);

            Self::write_metadata_page(self.page.clone(), &self.name(), table_manager_page_ids);

            self.next_table_id += 1;

            Ok(table_id)
        } else {
            Err(Error::TableAlreadyExists(table_name.to_string()))
        }
    }

    pub fn add_column(
        &mut self,
        table_name: &str,
        column_name: &str,
        data_type: DataType,
    ) -> Result<String, Error> {
        let mut table_managers = self.table_managers();
        let table_manager = table_managers
            .iter_mut()
            .find(|t| t.name() == table_name)
            .ok_or(Error::TableDoesNotExist(table_name.to_string()))?;

        table_manager.add_column(column_name, data_type)?;

        return Ok("ALTER TABLE".to_string());
    }

    pub fn insert_row(&mut self, table_name: &str, values: Vec<Value>) -> Result<u64, Error> {
        let mut table_managers = self.table_managers();

        table_managers
            .iter_mut()
            .find(|t| t.name() == table_name)
            .ok_or(Error::TableDoesNotExist(table_name.to_string()))?
            .insert_record(values)
            .ok_or(Error::InsertFailed)
    }

    pub fn select_all_columns(&self, table_name: &str) -> Result<RowResult, Error> {
        Ok(self
            .table_managers()
            .iter()
            .find(|t| t.name() == table_name)
            .ok_or(Error::TableDoesNotExist(table_name.to_string()))?
            .get_records())
    }

    pub fn select_columns_by_name(
        &self,
        table_name: &str,
        column_names: Vec<&str>,
    ) -> Result<RowResult, Error> {
        self.table_managers()
            .iter()
            .find(|t| t.name() == table_name)
            .ok_or(Error::TableDoesNotExist(table_name.to_string()))?
            .get_records_for_columns(&column_names)
    }

    fn table_exists(&self, table_name: &str) -> bool {
        self.table_managers()
            .iter()
            .find(|t| t.name() == table_name)
            .is_some()
    }

    fn table_managers(&self) -> Vec<TableManager> {
        let page_manager = super::page_manager().read().unwrap();
        let mut table_managers = Vec::new();

        for page_id in &self.table_manager_page_ids() {
            let shared_page = page_manager.fetch_page(*page_id).unwrap();

            let table_manager = TableManager::load(shared_page).unwrap();
            table_managers.push(table_manager);
        }

        return table_managers;
    }

    fn table_manager_page_ids(&self) -> Vec<PageId> {
        let page = self.page.read().unwrap();
        let number_of_databases = page.metadata[TABLE_MANAGER_PAGE_IDS_OFFSET] as u8;

        let mut page_ids = vec![];

        for i in 0..number_of_databases {
            let start_index = (TABLE_MANAGER_PAGE_IDS_OFFSET as usize) + (i as usize) * 4 + 1;
            let end_index = start_index + 4;

            let bytes: &[u8; 4] = &page.metadata[start_index..end_index].try_into().unwrap();

            page_ids.push(PageId::from_le_bytes(*bytes));
        }

        return page_ids;
    }

    fn write_metadata_page(shared_page: SharedInternalPage, name: &str, page_ids: Vec<PageId>) {
        let mut page = shared_page.write().unwrap();

        let name_length = name.len();
        page.metadata[0] = name_length as u8;
        page.metadata[1..name_length + 1].copy_from_slice(name.as_bytes());

        // Write the page ids to the metadata.
        let number_of_page_ids = page_ids.len();

        page.metadata[TABLE_MANAGER_PAGE_IDS_OFFSET] = number_of_page_ids as u8;
        let page_ids_bytes: Vec<u8> = page_ids
            .iter()
            .map(|pids| pids.to_le_bytes())
            .flatten()
            .collect();

        page.metadata[TABLE_MANAGER_PAGE_IDS_OFFSET + 1
            ..TABLE_MANAGER_PAGE_IDS_OFFSET + 1 + page_ids_bytes.len()]
            .copy_from_slice(&page_ids_bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_creating_new_table_without_columns() {
        let mut database = Database::new("test").unwrap();

        let result = database.create_table("foobar", vec![]);

        assert!(result.is_ok(), "Failed to create table");
    }

    #[test]
    fn test_creating_new_table_with_columns() {
        let mut database = Database::new("test").unwrap();

        let result =
            database.create_table("foobar", vec![("hello".to_string(), DataType::Integer)]);

        assert!(result.is_ok(), "Failed to create table");
    }

    #[test]
    fn test_creating_table_that_already_exists() {
        let mut database = Database::new("test").unwrap();
        let table_name = "new_database";
        assert!(database.create_table(table_name, vec![]).is_ok());

        let result = database.create_table(table_name, vec![]);

        assert_eq!(
            Err(Error::TableAlreadyExists(table_name.to_string())),
            result
        );
    }

    #[test]
    fn test_adding_column_to_table() {
        let mut database = Database::new("test").unwrap();
        let table_name = "new_database";
        assert!(database.create_table(table_name, vec![]).is_ok());

        let result = database.add_column(table_name, "age", DataType::Integer);

        assert!(result.is_ok(), "Failed to add column to table");
    }

    #[test]
    fn inserting_row_to_a_table() {
        let mut database = Database::new("test").unwrap();
        let table_name = "new_table";
        assert!(database
            .create_table(table_name, vec![("age".to_string(), DataType::Integer)])
            .is_ok());

        let result = database.insert_row(table_name, vec![Value::Integer(3)]);

        assert!(result.is_ok(), "Failed to insert row to table");
    }

    #[test]
    fn inserting_row_with_different_len_values() {
        let mut database = Database::new("test").unwrap();
        let table_name = "new_table";
        assert!(database
            .create_table(table_name, vec![("age".to_string(), DataType::Integer)])
            .is_ok());

        let result = database.insert_row(table_name, vec![Value::Integer(3), Value::Integer(5)]);

        assert_eq!(result.is_ok(), false);
    }

    #[test]
    fn select_all_from_table() {
        let mut database = Database::new("test").unwrap();
        let table_name = "new_table";
        assert!(database
            .create_table(table_name, vec![("age".to_string(), DataType::Integer)])
            .is_ok());
        assert!(database
            .insert_row(table_name, vec![Value::Integer(5)])
            .is_ok());

        let result = database
            .select_all_columns(table_name)
            .expect("Failed to select from database");

        assert_eq!(1, *result.count());
        assert_eq!(vec![vec![Some(Value::Integer(5))]], result.rows());
    }

    #[test]
    fn select_all_from_table_with_different_columns_over_time() {
        let mut database = Database::new("test").unwrap();
        let table_name = "new_table";
        assert!(database
            .create_table(table_name, vec![("age".to_string(), DataType::Integer)])
            .is_ok());
        assert!(database
            .insert_row(table_name, vec![Value::Integer(1)])
            .is_ok());

        assert!(database
            .add_column(table_name, "month", DataType::Integer)
            .is_ok());
        assert!(database
            .insert_row(table_name, vec![Value::Integer(2), Value::Integer(3)])
            .is_ok());

        // Order is not guaranteed
        let result = database
            .select_all_columns(table_name)
            .expect("Failed to select from database");

        assert_eq!(2, *result.count());
        assert!(result.rows().contains(&vec![Some(Value::Integer(1)), None]));
        assert!(result.rows().contains(&vec![Some(Value::Integer(1)), None]));
    }

    #[test]
    fn select_all_from_empty_table() {
        let mut database = Database::new("test").unwrap();
        let table_name = "new_table";
        assert!(database.create_table(table_name, vec![]).is_ok());

        let result = database
            .select_all_columns(table_name)
            .expect("Failed to select from database");

        assert_eq!(0, *result.count());
        assert!(result.rows().is_empty());
    }

    #[test]
    fn select_from_table() {
        let mut database = Database::new("test").unwrap();
        let table_name = "new_table";
        assert!(database
            .create_table(
                table_name,
                vec![
                    ("age".to_string(), DataType::Integer),
                    ("birthday".to_string(), DataType::Integer),
                ]
            )
            .is_ok());
        assert!(database
            .insert_row(table_name, vec![Value::Integer(5), Value::Integer(3)])
            .is_ok());

        let result = database
            .select_columns_by_name(table_name, vec!["birthday"])
            .expect("Failed to select from database");

        assert_eq!(1, *result.count());
        assert_eq!(vec![vec![Some(Value::Integer(3))]], result.rows());
    }

    #[test]
    fn select_with_column_that_doesnt_exist() {
        let mut database = Database::new("test").unwrap();
        let table_name = "new_table";
        assert!(database.create_table(table_name, vec![]).is_ok());

        let result = database.select_columns_by_name(table_name, vec!["lol123"]);

        assert_eq!(Err(Error::ColumnDoesNotExist("lol123".to_string())), result);
    }

    #[test]
    fn test_inserting_record_and_getting_it_back_out() {
        let mut table_manager = TableManager::new("test").unwrap();
        table_manager.add_column("day", DataType::Integer).unwrap();

        let record_id = table_manager
            .insert_record(vec![Value::Integer(29)])
            .expect("Failed to insert a row");

        let record = table_manager
            .get_record(record_id)
            .expect("Failed to retrive record by interal record id");

        assert_eq!(1, *record.count());
        assert_eq!(
            vec![Some(Value::Integer(29))],
            *record
                .rows()
                .first()
                .expect("Failed to fetch the first record")
        );
    }
}
