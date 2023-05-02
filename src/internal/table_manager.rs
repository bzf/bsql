use std::{collections::HashMap, rc::Rc, sync::RwLock};

use super::{ColumnDefinition, DataType, Error, RowResult, TablePage, Value};

type ColumnId = u8;

type LockedTablePage = Rc<RwLock<TablePage>>;

pub struct TableManager {
    table_id: u64,
    next_column_id: ColumnId,

    pages: Vec<LockedTablePage>,
    column_pages: HashMap<Vec<ColumnId>, Vec<LockedTablePage>>,

    column_names: HashMap<String, ColumnId>,
    column_definitions: Vec<ColumnDefinition>,
}

impl TableManager {
    pub fn new(table_id: u64) -> Self {
        Self {
            table_id,
            next_column_id: 0,

            column_names: HashMap::new(),
            column_definitions: Vec::new(),

            pages: Vec::new(),
            column_pages: HashMap::new(),
        }
    }

    pub fn column_definitions(&self) -> Vec<(String, ColumnDefinition)> {
        self.column_names
            .iter()
            .map(|(column_name, column_id)| {
                (
                    column_name.to_string(),
                    self.column_definitions
                        .get(*column_id as usize)
                        .unwrap()
                        .clone(),
                )
            })
            .collect()
    }

    pub fn add_column(&mut self, column_name: &str, data_type: DataType) -> Result<(), Error> {
        println!("table_manager.add_column {} {:?}", column_name, data_type);

        if !self.column_names.contains_key(column_name) {
            self.column_names
                .insert(column_name.to_string(), self.next_column_id as u8);
            self.column_definitions
                .push(ColumnDefinition::new(self.next_column_id as u8, data_type));

            self.next_column_id += 1;

            Ok(())
        } else {
            Err(Error::ColumnAlreadyExist(column_name.to_string()))
        }
    }

    pub fn insert_record(&mut self, values: Vec<Value>) -> Option<u64> {
        // Check that we have the same amount of `values` as we have `column_definitions`.
        if values.len() != self.column_definitions.len() {
            return None;
        }

        let (page_index, writable_page) = self.get_writable_page();
        let mut active_table_page = writable_page.write().ok()?;

        let record_slot = active_table_page.insert_record(values.clone())?;

        Some((page_index << 32) as u64 | record_slot as u64)
    }

    pub fn get_record(&self, record_id: u64) -> Option<RowResult> {
        let page_index = (record_id >> 32) & 0xFFFF_FFFF;
        let record_slot = record_id & 0xFFFF_FFFF;

        let page = self.pages.get(page_index as usize)?.read().ok()?;

        let page_columns = page.column_definitions();
        let row_data = page
            .get_record(record_slot as u8)
            .map(|record| self.normalize_page_record(&page_columns, record))?;

        Some(RowResult::new(
            self.column_names.keys().cloned().collect(),
            vec![row_data],
        ))
    }

    pub fn get_records(&self) -> RowResult {
        let mut rows: Vec<Vec<Option<Value>>> = Vec::new();

        for locked_page in self.column_pages.values().flatten() {
            let page = locked_page.read().unwrap();

            let page_columns = page.column_definitions();
            let page_records = page.get_records();

            // For every column that we want to return (all might not exist), figure out how to
            // transform the order of the record we retrieved into what we expect.
            for page_record in page_records.into_iter() {
                rows.push(self.normalize_page_record(page_columns, page_record));
            }
        }

        RowResult::new(self.column_names.keys().cloned().collect(), rows)
    }

    pub fn get_records_for_columns(&self, column_names: &Vec<&str>) -> Result<RowResult, Error> {
        let sorted_column_indices: Vec<ColumnId> = column_names
            .into_iter()
            .map(|column_name| {
                self.column_names
                    .get(*column_name)
                    .ok_or(Error::ColumnDoesNotExist(column_name.to_string()))
                    .map(|column_id| *column_id)
            })
            .collect::<Result<Vec<ColumnId>, Error>>()?;

        let records = self.get_records();
        let sorted_rows = records
            .rows()
            .into_iter()
            .map(|row| {
                return sorted_column_indices
                    .iter()
                    .map(|column_index| row.get(*column_index as usize).unwrap().clone())
                    .collect();
            })
            .collect();

        Ok(RowResult::new(
            column_names.iter().map(|name| name.to_string()).collect(),
            sorted_rows,
        ))
    }

    fn get_writable_page(&mut self) -> (usize, &LockedTablePage) {
        let column_ids: Vec<ColumnId> = self
            .column_definitions
            .iter()
            .map(|c| c.column_id())
            .collect();

        let page_vec: &mut Vec<LockedTablePage> =
            self.column_pages.entry(column_ids).or_insert(Vec::new());

        // Check if the last page, if any, is full and if so create a new one.
        // If there are no pages, create one.
        let last_table_page_locked = page_vec.last_mut();
        if let Some(last_table_page_locked) = last_table_page_locked {
            let is_page_full = {
                let last_table_page = last_table_page_locked.write().unwrap();
                last_table_page.is_full()
            };

            if is_page_full {
                let next_table_page =
                    Rc::new(RwLock::new(TablePage::new(self.column_definitions.clone())));

                self.pages.push(next_table_page.clone());
                page_vec.push(next_table_page);
            }
        } else {
            let next_table_page =
                Rc::new(RwLock::new(TablePage::new(self.column_definitions.clone())));

            self.pages.push(next_table_page.clone());
            page_vec.push(next_table_page);
        }

        let page_index = page_vec
            .iter()
            .position(|i| Rc::ptr_eq(i, page_vec.last().unwrap()))
            .unwrap();

        return (page_index, page_vec.last_mut().unwrap());
    }

    // Takes a `Vec<Value>` and transforms it into a `Vec<Option<Value>>` where any missing columns
    // in the input gets casted to `None`.
    fn normalize_page_record(
        &self,
        page_columns: &Vec<ColumnDefinition>,
        page_record: Vec<Value>,
    ) -> Vec<Option<Value>> {
        // TODO: Make sure that `self.column_names.iter()` always returns the same order.

        // For every column that we want to return (all might not exist), figure out how to
        // transform the order of the record we retrieved into what we expect.
        let value_index_order: Vec<Option<ColumnId>> = self
            .column_definitions
            .iter()
            .map(|expected_column| {
                page_columns
                    .iter()
                    .find(|page_column| expected_column.column_id() == page_column.column_id())
                    .map(|page_column| page_column.column_id())
            })
            .collect();

        return value_index_order
            .clone()
            .into_iter()
            .map(|value_index| {
                value_index.and_then(|index| page_record.get(index as usize).map(|i| i.clone()))
            })
            .collect();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_records_for_pages_with_different_columns() {
        let mut table_manager = TableManager::new(1);
        table_manager.add_column("day", DataType::Integer).unwrap();
        assert!(table_manager
            .insert_record(vec![Value::Integer(31)])
            .is_some());

        table_manager
            .add_column("month", DataType::Integer)
            .unwrap();
        assert!(table_manager
            .insert_record(vec![Value::Integer(1), Value::Integer(5)])
            .is_some());

        // Returns records for the _current_ columns. Order is not guaranteed.
        let records = table_manager.get_records();
        assert!(
            records
                .rows()
                .contains(&vec![Some(Value::Integer(31)), None]),
            "Missing record with only first column"
        );
        assert!(
            records
                .rows()
                .contains(&vec![Some(Value::Integer(1)), Some(Value::Integer(5))]),
            "Missing record both column"
        );
    }

    #[test]
    fn test_get_records_with_specific_columns() {
        let mut table_manager = TableManager::new(1);
        table_manager.add_column("day", DataType::Integer).unwrap();
        assert!(table_manager
            .insert_record(vec![Value::Integer(13)])
            .is_some());

        table_manager
            .add_column("month", DataType::Integer)
            .unwrap();
        assert!(table_manager
            .insert_record(vec![Value::Integer(2), Value::Integer(4)])
            .is_some());

        // Returns records for the _given_ columns. Order is not guaranteed.
        let records = table_manager
            .get_records_for_columns(&vec!["month", "day"])
            .expect("Failed to get the result");

        assert_eq!(2, *records.count());
        assert!(
            records
                .rows()
                .contains(&vec![None, Some(Value::Integer(13))]),
            "Missing record with only first column"
        );
        assert!(
            records
                .rows()
                .contains(&vec![Some(Value::Integer(4)), Some(Value::Integer(2))]),
            "Missing record both column"
        );
    }

    #[test]
    fn test_get_records_with_specific_columns_with_invalid_columns() {
        let mut table_manager = TableManager::new(1);
        table_manager.add_column("day", DataType::Integer).unwrap();

        assert_eq!(
            Err(Error::ColumnDoesNotExist("month".to_string())),
            table_manager.get_records_for_columns(&vec!["month", "day"])
        )
    }
}
