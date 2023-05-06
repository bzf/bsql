use super::{ColumnDefinition, DataType, Error, RowResult, TableManager, Value};

type TableId = u64;

pub struct Database {
    table_managers: Vec<TableManager>,
    next_table_id: TableId,
}

impl Database {
    pub fn new() -> Self {
        Self {
            table_managers: Vec::new(),
            next_table_id: 0,
        }
    }

    pub fn table_names(&self) -> Vec<String> {
        self.table_managers
            .iter()
            .map(|t| t.name().clone())
            .collect()
    }

    pub fn column_definitions(&self, table_name: &str) -> Result<Vec<ColumnDefinition>, Error> {
        Ok(self
            .table_managers
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

            let mut table_manager = TableManager::new(table_name)?;
            for (column_name, data_type) in columns.iter() {
                table_manager.add_column(column_name, data_type.clone())?;
            }

            self.table_managers.push(table_manager);
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
        let table_manager = self
            .table_managers
            .iter_mut()
            .find(|t| t.name() == table_name)
            .ok_or(Error::TableDoesNotExist(table_name.to_string()))?;

        table_manager.add_column(column_name, data_type)?;

        return Ok("ALTER TABLE".to_string());
    }

    pub fn insert_row(&mut self, table_name: &str, values: Vec<Value>) -> Result<u64, Error> {
        self.table_managers
            .iter_mut()
            .find(|t| t.name() == table_name)
            .ok_or(Error::TableDoesNotExist(table_name.to_string()))?
            .insert_record(values)
            .ok_or(Error::InsertFailed)
    }

    pub fn select_all_columns(&self, table_name: &str) -> Result<RowResult, Error> {
        Ok(self
            .table_managers
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
        self.table_managers
            .iter()
            .find(|t| t.name() == table_name)
            .ok_or(Error::TableDoesNotExist(table_name.to_string()))?
            .get_records_for_columns(&column_names)
    }

    fn table_exists(&self, table_name: &str) -> bool {
        self.table_managers
            .iter()
            .find(|t| t.name() == table_name)
            .is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_creating_new_table_without_columns() {
        let mut database = Database::new();

        let result = database.create_table("foobar", vec![]);

        assert!(result.is_ok(), "Failed to create table");
    }

    #[test]
    fn test_creating_new_table_with_columns() {
        let mut database = Database::new();

        let result =
            database.create_table("foobar", vec![("hello".to_string(), DataType::Integer)]);

        assert!(result.is_ok(), "Failed to create table");
    }

    #[test]
    fn test_creating_table_that_already_exists() {
        let mut database = Database::new();
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
        let mut database = Database::new();
        let table_name = "new_database";
        assert!(database.create_table(table_name, vec![]).is_ok());

        let result = database.add_column(table_name, "age", DataType::Integer);

        assert!(result.is_ok(), "Failed to add column to table");
    }

    #[test]
    fn inserting_row_to_a_table() {
        let mut database = Database::new();
        let table_name = "new_table";
        assert!(database
            .create_table(table_name, vec![("age".to_string(), DataType::Integer)])
            .is_ok());

        let result = database.insert_row(table_name, vec![Value::Integer(3)]);

        assert!(result.is_ok(), "Failed to insert row to table");
    }

    #[test]
    fn inserting_row_with_different_len_values() {
        let mut database = Database::new();
        let table_name = "new_table";
        assert!(database
            .create_table(table_name, vec![("age".to_string(), DataType::Integer)])
            .is_ok());

        let result = database.insert_row(table_name, vec![Value::Integer(3), Value::Integer(5)]);

        assert_eq!(result.is_ok(), false);
    }

    #[test]
    fn select_all_from_table() {
        let mut database = Database::new();
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
        let mut database = Database::new();
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
        let mut database = Database::new();
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
        let mut database = Database::new();
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
        let mut database = Database::new();
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
