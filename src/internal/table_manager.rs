use std::collections::HashMap;

use super::{ColumnDefinition, DataType, TablePage, Value};

type ColumnId = u8;

pub struct TableManager {
    table_id: u64,
    next_column_id: ColumnId,

    pages: HashMap<Vec<ColumnId>, Vec<TablePage>>,

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

            pages: HashMap::new(),
        }
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

        let active_table_page = self.get_writable_page();
        return active_table_page.insert_record(values.clone()).is_some();
    }

    fn get_writable_page(&mut self) -> &mut TablePage {
        let column_ids: Vec<ColumnId> = self
            .column_definitions
            .iter()
            .map(|c| c.column_id())
            .collect();

        let page_vec: &mut Vec<TablePage> = self.pages.entry(column_ids).or_insert(Vec::new());

        // Check if the last page, if any, is full and if so create a new one.
        // If there are no pages, create one.
        if let Some(last_table_page) = page_vec.last_mut() {
            if last_table_page.is_full() {
                page_vec.push(TablePage::new(self.column_definitions.clone()));
            }
        } else {
            let next_table_page = TablePage::new(self.column_definitions.clone());
            page_vec.push(next_table_page);
        }

        return page_vec.last_mut().unwrap();
    }
}
