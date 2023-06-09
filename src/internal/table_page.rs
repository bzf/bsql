use std::cell::RefCell;
use std::rc::Rc;
use std::sync::RwLock;

use super::{BitmapIndex, ColumnDefinition, DataType, PageManager, SharedInternalPage, Value};

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
    page: SharedInternalPage,
}

impl TablePage {
    fn new(
        page_manager: Rc<RwLock<PageManager>>,
        column_definitions: Vec<ColumnDefinition>,
    ) -> Self {
        let (_page_id, shared_page) = {
            let mut page_manager = page_manager.write().unwrap();
            page_manager.create_page()
        };

        return Self::initialize(page_manager, shared_page, column_definitions);
    }

    /// Initialize a `SharedInternalPage` for this `TablePage`
    pub fn initialize(
        page_manager: Rc<RwLock<PageManager>>,
        shared_page: SharedInternalPage,
        column_definitions: Vec<ColumnDefinition>,
    ) -> Self {
        {
            let mut page = shared_page.write().unwrap();

            let column_definitions_bytes: Vec<u8> = column_definitions
                .iter()
                .map(|cd| cd.to_raw_bytes())
                .flatten()
                .collect();

            // Store the length of the column definitions in the metadata page after the
            page.metadata[32..40].copy_from_slice(&column_definitions_bytes.len().to_be_bytes());
            page.metadata[40..40 + column_definitions_bytes.len()]
                .copy_from_slice(&column_definitions_bytes);
        }

        Self {
            column_definitions,
            page: shared_page,
        }
    }

    /// Load a `TablePage` with the data from the `SharedInternalPage`
    pub fn load(page_manager: Rc<RwLock<PageManager>>, shared_page: SharedInternalPage) -> Self {
        let mut column_definitions = Vec::new();

        {
            let page = shared_page.read().unwrap();
            let column_definitions_byte_length =
                usize::from_be_bytes(page.metadata[32..40].try_into().unwrap());

            let column_definitions_slice = &page.metadata[40..40 + column_definitions_byte_length];

            let mut start_cursor = 0;

            while start_cursor < column_definitions_slice.len() {
                let column_definition_byte_length = column_definitions_slice[start_cursor] as usize;
                start_cursor += 1;

                let column_definition_byte_slice = &column_definitions_slice
                    [start_cursor..start_cursor + column_definition_byte_length];
                start_cursor += column_definition_byte_length;

                let column_definition =
                    ColumnDefinition::from_raw_bytes(column_definition_byte_slice).unwrap();

                column_definitions.push(column_definition);
            }
        }

        Self {
            column_definitions,
            page: shared_page,
        }
    }

    /// Checks that the record columns matches what's stored in the `TablePage` and returns the
    /// relative index of the record in the page.
    /// Return `None` when the page is full.
    pub fn insert_record(&mut self, record_data: Vec<Value>) -> Option<u8> {
        let mut page = self.page.write().ok()?;

        if record_data.len() != self.column_definitions.len() {
            return None;
        }

        let mut slots_index: BitmapIndex<255> =
            BitmapIndex::from_raw(&mut page.metadata[0..32]).unwrap();

        let record_index = slots_index.consume()?;

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

        let slots_indices = {
            let mut page = self.page.write().unwrap();
            BitmapIndex::<255>::from_raw(&mut page.metadata[0..32])
                .unwrap()
                .indices()
        };

        for record_index in slots_indices {
            records.push(self.get_record(record_index).unwrap());
        }

        return records;
    }

    pub fn get_record(&self, record_index: u8) -> Option<Vec<Value>> {
        let mut page = self.page.write().unwrap();
        let slots_index: BitmapIndex<255> =
            BitmapIndex::from_raw(&mut page.metadata[0..32]).unwrap();

        if !slots_index.is_set(record_index) {
            return None;
        }

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
        let mut page = self.page.write().unwrap();
        let mut slots_index: BitmapIndex<255> =
            BitmapIndex::from_raw(&mut page.metadata[0..32]).unwrap();

        slots_index.unset(record_index);
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
        let mut page = self.page.write().unwrap();
        let slots_index: BitmapIndex<255> =
            BitmapIndex::from_raw(&mut page.metadata[0..32]).unwrap();

        slots_index.is_full()
    }

    fn record_count(&self) -> usize {
        let mut page = self.page.write().unwrap();
        let slots_index: BitmapIndex<255> =
            BitmapIndex::from_raw(&mut page.metadata[0..32]).unwrap();

        slots_index.count().into()
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
    use std::sync::RwLock;

    use super::DataType;
    use crate::internal::InternalPage;

    use super::*;

    #[test]
    fn test_inserting_and_reading_record_with_one_column() {
        let page_manager = Rc::new(RwLock::new(PageManager::new(":memory:")));
        let column_definition = ColumnDefinition::new(1, DataType::Integer, "day".to_string());
        let mut table_page = TablePage::new(page_manager, vec![column_definition.clone()]);

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
        let page_manager = Rc::new(RwLock::new(PageManager::new(":memory:")));
        let mut table_page = TablePage::new(
            page_manager,
            vec![
                ColumnDefinition::new(1, DataType::Integer, "day".to_string()),
                ColumnDefinition::new(2, DataType::Integer, "month".to_string()),
            ],
        );

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
        let page_manager = Rc::new(RwLock::new(PageManager::new(":memory:")));
        let column_definition = ColumnDefinition::new(1, DataType::Integer, "day".to_string());

        let mut table_page = TablePage::new(page_manager, vec![column_definition.clone()]);
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
        let page_manager = Rc::new(RwLock::new(PageManager::new(":memory:")));
        let column_definition = ColumnDefinition::new(1, DataType::Integer, "day".to_string());
        let mut table_page = TablePage::new(page_manager, vec![column_definition.clone()]);

        let record_id = table_page.insert_record(vec![Value::Integer(3), Value::Integer(1)]);
        assert_eq!(None, record_id);
    }

    #[test]
    fn test_inserting_and_deleting_record() {
        let page_manager = Rc::new(RwLock::new(PageManager::new(":memory:")));
        let mut table_page = TablePage::new(
            page_manager,
            vec![ColumnDefinition::new(
                1,
                DataType::Integer,
                "day".to_string(),
            )],
        );

        let record_id = table_page.insert_record(vec![Value::Integer(3)]);
        assert!(record_id.is_some());
        assert_eq!(1, table_page.record_count());

        table_page.delete_record(record_id.unwrap());
        assert_eq!(0, table_page.record_count());
    }

    #[test]
    fn test_record_size() {
        let page_manager = Rc::new(RwLock::new(PageManager::new(":memory:")));

        {
            let table_page = TablePage::new(
                page_manager.clone(),
                vec![ColumnDefinition::new(
                    1,
                    DataType::Integer,
                    "day".to_string(),
                )],
            );
            assert_eq!(1, table_page.record_size());
        }

        {
            let table_page = TablePage::new(
                page_manager,
                vec![
                    ColumnDefinition::new(1, DataType::Integer, "day".to_string()),
                    ColumnDefinition::new(2, DataType::Integer, "month".to_string()),
                ],
            );

            assert_eq!(2, table_page.record_size());
        }
    }

    #[test]
    fn test_initialize_and_load() {
        let page_manager = Rc::new(RwLock::new(PageManager::new(":memory:")));
        let page = Rc::new(RwLock::new(InternalPage::new()));
        let column_definitions = vec![
            ColumnDefinition::new(23, DataType::Integer, "day".to_string()),
            ColumnDefinition::new(11, DataType::Integer, "month".to_string()),
        ];

        {
            let mut table_page = TablePage::initialize(
                page_manager.clone(),
                page.clone(),
                column_definitions.clone(),
            );
            table_page.insert_record(vec![Value::Integer(13), Value::Integer(12)]);
        }

        let table_page = TablePage::load(page_manager, page.clone());
        assert_eq!(&column_definitions, table_page.column_definitions());
        assert_eq!(1, table_page.record_count());
    }

    #[test]
    fn test_serialize_page_header() {
        let page_manager = Rc::new(RwLock::new(PageManager::new(":memory:")));
        let table_page = TablePage::new(
            page_manager,
            vec![
                ColumnDefinition::new(23, DataType::Integer, "day".to_string()),
                ColumnDefinition::new(11, DataType::Integer, "month".to_string()),
            ],
        );

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
