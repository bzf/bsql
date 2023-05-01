use std::{collections::HashMap, rc::Rc, sync::RwLock};

use super::{ColumnDefinition, DataType, TablePage, Value};

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

    pub fn add_column(&mut self, column_name: &str, data_type: DataType) {
        self.column_names
            .insert(column_name.to_string(), self.next_column_id as u8);
        self.column_definitions
            .push(ColumnDefinition::new(self.next_column_id as u8, data_type));

        self.next_column_id += 1;
    }

    pub fn insert_record(&mut self, values: Vec<Value>) -> bool {
        // Check that we have the same amount of `values` as we have `column_definitions`.
        if values.len() != self.column_definitions.len() {
            return false;
        }

        let mut active_table_page = self.get_writable_page().write().unwrap();
        return active_table_page.insert_record(values.clone()).is_some();
    }

    pub fn get_records(&self) -> Vec<Vec<Option<Value>>> {
        let mut records: Vec<Vec<Option<Value>>> = Vec::new();

        for locked_page in self.column_pages.values().flatten() {
            let page = locked_page.read().unwrap();

            let page_columns = page.column_definitions();
            let page_records = page.get_records();

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

            for page_record in page_records.into_iter() {
                let normalized_record: Vec<Option<Value>> = value_index_order
                    .clone()
                    .into_iter()
                    .map(|value_index| {
                        value_index
                            .and_then(|index| page_record.get(index as usize).map(|i| i.clone()))
                    })
                    .collect();

                records.push(normalized_record);
            }
        }

        return records;
    }

    pub fn get_records_for_columns(
        &self,
        column_names: &Vec<&str>,
    ) -> Option<Vec<Vec<Option<Value>>>> {
        let sorted_column_indices: Vec<ColumnId> = column_names
            .iter()
            .map(|column_name| self.column_names.get(&**column_name))
            .flat_map(|i| i.map(|t| *t))
            .collect();

        if sorted_column_indices.len() != column_names.len() {
            return None;
        }

        let sorted_records = self
            .get_records()
            .into_iter()
            .map(|row| {
                return sorted_column_indices
                    .iter()
                    .map(|column_index| row.get(*column_index as usize).unwrap().clone())
                    .collect();
            })
            .collect();

        return Some(sorted_records);
    }

    fn get_writable_page(&mut self) -> &LockedTablePage {
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

        return page_vec.last_mut().unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_records_for_pages_with_different_columns() {
        let mut table_manager = TableManager::new(1);
        table_manager.add_column("day", DataType::Integer);
        assert!(table_manager.insert_record(vec![Value::Integer(31)]));

        table_manager.add_column("month", DataType::Integer);
        assert!(table_manager.insert_record(vec![Value::Integer(1), Value::Integer(5)]));

        // Returns records for the _current_ columns. Order is not guaranteed.
        let records = table_manager.get_records();
        assert!(
            records.contains(&vec![Some(Value::Integer(31)), None]),
            "Missing record with only first column"
        );
        assert!(
            records.contains(&vec![Some(Value::Integer(1)), Some(Value::Integer(5))]),
            "Missing record both column"
        );
    }

    #[test]
    fn test_get_records_with_specific_columns() {
        let mut table_manager = TableManager::new(1);
        table_manager.add_column("day", DataType::Integer);
        assert!(table_manager.insert_record(vec![Value::Integer(13)]));

        table_manager.add_column("month", DataType::Integer);
        assert!(table_manager.insert_record(vec![Value::Integer(2), Value::Integer(4)]));

        // Returns records for the _given_ columns. Order is not guaranteed.
        let records = table_manager
            .get_records_for_columns(&vec!["month", "day"])
            .expect("Failed to get the result");

        assert!(
            records.contains(&vec![None, Some(Value::Integer(13))]),
            "Missing record with only first column"
        );
        assert!(
            records.contains(&vec![Some(Value::Integer(4)), Some(Value::Integer(2))]),
            "Missing record both column"
        );
    }

    #[test]
    fn test_get_records_with_specific_columns_with_invalid_columns() {
        let mut table_manager = TableManager::new(1);
        table_manager.add_column("day", DataType::Integer);

        assert_eq!(
            None,
            table_manager.get_records_for_columns(&vec!["month", "day"])
        )
    }
}
