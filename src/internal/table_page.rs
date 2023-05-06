use std::cell::RefCell;
use std::rc::Rc;

use super::{BitmapIndex, ColumnDefinition, DataType, PageId, Value};

/// A `TablePage` is a struct that represents a full page of data + metadata of records (and their
/// columns) that are stored in a table.
/// A `TablePage` has a immutable header which consists of 32 bytes for the bitmap index (for knowing
/// which free slots there are in the page) followed by the the serialized column definitions.

/// The `TablePage` consists of two parts:
/// - A `metadata_page` which stores information about which columns are present in the page, as
/// well as a lookup table for finding records with a certain `InternalId`.
/// - A `data_page` which stores all data for the records.

pub struct TablePage {
    column_definitions: Vec<ColumnDefinition>,
    slots_index: BitmapIndex<255>,

    page_id: PageId,
}

impl TablePage {
    pub fn new(column_definitions: Vec<ColumnDefinition>) -> Self {
        let mut page_manager = super::page_manager().write().unwrap();
        let (page_id, shared_page) = page_manager.create_page();
        let mut page = shared_page.write().unwrap();

        let slots_index: BitmapIndex<255> = {
            let bytes = Rc::new(RefCell::new(page.metadata[0..32].try_into().unwrap()));
            BitmapIndex::from_raw(bytes).unwrap()
        };

        let column_definition_bytes: Vec<u8> = column_definitions
            .iter()
            .map(|cd| cd.to_raw_bytes())
            .flatten()
            .collect();

        // Store the length of the column definitions in the metadata page after the
        page.metadata[32..40].copy_from_slice(&column_definition_bytes.len().to_be_bytes());
        page.metadata[40..40 + column_definition_bytes.len()]
            .copy_from_slice(&column_definition_bytes);

        Self {
            column_definitions,
            page_id,
            slots_index,
        }
    }

    /// Checks that the record columns matches what's stored in the `TablePage` and returns the
    /// relative index of the record in the page.
    /// Return `None` when the page is full.
    pub fn insert_record(&mut self, record_data: Vec<Value>) -> Option<u8> {
        let page_manager = super::page_manager().write().ok()?;
        let mut page = page_manager.fetch_page(self.page_id)?.write().ok()?;

        if record_data.len() != self.column_definitions.len() {
            return None;
        }

        let record_index = self.slots_index.consume()?;

        let record_data: Vec<u8> = record_data
            .into_iter()
            .flat_map(|value| value.to_bsql_data())
            .collect();

        let record_size: usize = self.record_size() as usize;
        let start_index: usize = (record_index as usize * record_size) as usize;
        page.data[start_index..(start_index + record_size)].copy_from_slice(&record_data);

        Some(record_index)
    }

    pub fn get_records(&self) -> Vec<Vec<Value>> {
        let mut records = Vec::with_capacity(self.record_count());

        for record_index in self.slots_index.indices() {
            records.push(self.get_record(record_index).unwrap());
        }

        return records;
    }

    pub fn get_record(&self, record_index: u8) -> Option<Vec<Value>> {
        if !self.slots_index.is_set(record_index) {
            return None;
        }

        let page_manager = super::page_manager().read().ok()?;
        let page = page_manager.fetch_page(self.page_id)?.read().ok()?;

        let start_index: usize = (record_index as usize) * (self.record_size() as usize);
        let end_index: usize = start_index + self.record_size() as usize;
        let record_data = page.data.get(start_index..end_index)?;

        let mut value_offset: usize = 0;
        let mut values = vec![];

        for column_definition in &self.column_definitions {
            let value_size = column_definition.data_type().bsql_size() as usize;
            let value_data = &record_data[value_offset..(value_offset + value_size)];

            values.push(DataType::to_bsql_value(
                &column_definition.data_type(),
                value_data,
            )?);

            value_offset += value_size;
        }

        return Some(values);
    }

    pub fn delete_record(&mut self, record_index: u8) {
        self.slots_index.unset(record_index);
    }

    pub fn column_definitions(&self) -> &Vec<ColumnDefinition> {
        &self.column_definitions
    }

    pub fn serialize_page_header(&self) -> Vec<u8> {
        let mut page_header: Vec<u8> = Vec::with_capacity(self.page_header_size().into());
        page_header.push(self.column_definitions.len() as u8);

        self.column_definitions
            .iter()
            .for_each(|column_definition| {
                page_header.push(column_definition.column_id());
                page_header.push(column_definition.data_type().bsql_type_id());
            });

        return page_header;
    }

    pub fn is_full(&self) -> bool {
        self.slots_index.is_full()
    }

    fn record_count(&self) -> usize {
        self.slots_index.count().into()
    }

    fn page_header_size(&self) -> u8 {
        1 + (self.column_definitions.len() as u8 * 2)
    }

    fn record_size(&self) -> u8 {
        self.column_definitions
            .iter()
            .map(|c| c.data_type().bsql_size())
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use crate::internal::DataType;

    use super::*;

    #[test]
    fn test_inserting_and_reading_record_with_one_column() {
        let column_definition = ColumnDefinition::new(1, DataType::Integer, "day".to_string());
        let mut table_page = TablePage::new(vec![column_definition.clone()]);

        let record_id = table_page.insert_record(vec![Value::Integer(3)]);
        assert!(
            record_id.is_some(),
            "Failed to insert the record into the page."
        );

        let record_data = table_page.get_record(record_id.unwrap());
        assert_eq!(Some(vec![Value::Integer(3)]), record_data);
    }

    #[test]
    fn test_inserting_and_reading_record_with_multiple_columns() {
        let mut table_page = TablePage::new(vec![
            ColumnDefinition::new(1, DataType::Integer, "day".to_string()),
            ColumnDefinition::new(2, DataType::Integer, "month".to_string()),
        ]);

        let record_id = table_page.insert_record(vec![Value::Integer(3), Value::Integer(5)]);
        assert!(
            record_id.is_some(),
            "Failed to insert the record into the page."
        );

        let record_data = table_page.get_record(record_id.unwrap());
        assert_eq!(
            Some(vec![Value::Integer(3), Value::Integer(5)]),
            record_data
        );
    }

    #[test]
    fn test_inserting_record_when_the_page_is_full() {
        let column_definition = ColumnDefinition::new(1, DataType::Integer, "day".to_string());

        let mut table_page = TablePage::new(vec![column_definition.clone()]);
        for _ in 0..=u8::MAX {
            table_page
                .insert_record(vec![Value::Integer(3)])
                .expect("Failed to insert record while filling the page");
        }

        let record_id = table_page.insert_record(vec![Value::Integer(3)]);
        assert_eq!(None, record_id);
    }

    #[test]
    fn test_inserting_record_with_other_column_definitions() {
        let column_definition = ColumnDefinition::new(1, DataType::Integer, "day".to_string());
        let mut table_page = TablePage::new(vec![column_definition.clone()]);

        let record_id = table_page.insert_record(vec![Value::Integer(3), Value::Integer(1)]);
        assert_eq!(None, record_id);
    }

    #[test]
    fn test_inserting_and_deleting_record() {
        let mut table_page = TablePage::new(vec![ColumnDefinition::new(
            1,
            DataType::Integer,
            "day".to_string(),
        )]);

        let record_id = table_page.insert_record(vec![Value::Integer(3)]);
        assert!(record_id.is_some());
        assert_eq!(1, table_page.record_count());

        table_page.delete_record(record_id.unwrap());
        assert_eq!(0, table_page.record_count());
    }

    #[test]
    fn test_record_size() {
        {
            let table_page = TablePage::new(vec![ColumnDefinition::new(
                1,
                DataType::Integer,
                "day".to_string(),
            )]);
            assert_eq!(1, table_page.record_size());
        }

        {
            let table_page = TablePage::new(vec![
                ColumnDefinition::new(1, DataType::Integer, "day".to_string()),
                ColumnDefinition::new(2, DataType::Integer, "month".to_string()),
            ]);

            assert_eq!(2, table_page.record_size());
        }
    }

    #[test]
    fn test_serialize_page_header() {
        let table_page = TablePage::new(vec![
            ColumnDefinition::new(23, DataType::Integer, "day".to_string()),
            ColumnDefinition::new(11, DataType::Integer, "month".to_string()),
        ]);

        let page_header = table_page.serialize_page_header();

        assert_eq!(
            [
                2,                                // two columns
                23,                               // column id
                DataType::Integer.bsql_type_id(), // (column_id, type_id)
                11,                               // column id
                DataType::Integer.bsql_type_id(), // (column_id, type_id)
            ],
            page_header.as_slice()
        );
        assert_eq!(page_header.len() as u8, table_page.page_header_size());
        assert_eq!(
            2,
            DataType::Integer.bsql_size() + DataType::Integer.bsql_size()
        );
    }
}
