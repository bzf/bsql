use std::{cell::RefCell, collections::HashMap, rc::Rc, sync::RwLock};

use super::{BitmapIndex, ColumnDefinition, DataType, Error, PageId, RowResult, TablePage, Value};

type ColumnId = u8;

type LockedTablePage = Rc<RwLock<TablePage>>;

const COLUMN_BITMAP_RANGE: std::ops::Range<usize> = 0..32;
const COLUMN_DEFINITION_START_OFFSET: usize = 32;

pub struct TableManager {
    pages: Vec<LockedTablePage>,
    column_pages: HashMap<Vec<ColumnId>, Vec<LockedTablePage>>,

    page_id: PageId,
    column_index: BitmapIndex<255>,
}

impl TableManager {
    pub fn new() -> Self {
        let mut page_manager = super::page_manager().write().unwrap();
        let (page_id, shared_page) = page_manager.create_page();
        let page = shared_page.write().unwrap();

        let ref_cell: RefCell<[u8; 32]> =
            RefCell::new(page.metadata[COLUMN_BITMAP_RANGE].try_into().unwrap());
        let column_index = BitmapIndex::from_raw(ref_cell).expect("Failed to build BitmapIndex");

        Self {
            pages: Vec::new(),
            column_pages: HashMap::new(),

            page_id,
            column_index,
        }
    }

    pub fn column_definitions(&self) -> Vec<ColumnDefinition> {
        let page_manager = super::page_manager().write().ok().unwrap();
        let page = page_manager
            .fetch_page(self.page_id)
            .unwrap()
            .read()
            .ok()
            .unwrap();

        let mut column_definitions = Vec::new();
        let number_of_columns: u8 = page.metadata[COLUMN_DEFINITION_START_OFFSET];

        let mut start_cursor = COLUMN_DEFINITION_START_OFFSET + 1;

        for _ in 0..number_of_columns {
            let column_size_in_bytes: u8 = page.metadata[start_cursor];
            start_cursor += 1;

            let column_definition_bytes: &[u8] =
                &page.metadata[start_cursor..start_cursor + (column_size_in_bytes as usize)];

            start_cursor += column_size_in_bytes as usize;

            let column_definition =
                ColumnDefinition::from_raw_bytes(column_definition_bytes).unwrap();

            column_definitions.push(column_definition);
        }

        return column_definitions;
    }

    pub fn add_column(&mut self, column_name: &str, data_type: DataType) -> Result<(), Error> {
        println!("table_manager.add_column {} {:?}", column_name, data_type);
        let mut column_definitions = self.column_definitions();

        if !self.column_exists(column_name) {
            column_definitions.push(ColumnDefinition::new(
                self.column_index
                    .consume()
                    .ok_or(Error::TooManyColumnsInUse)?,
                data_type,
                column_name.to_string(),
            ));

            self.write_metadata_page(&column_definitions);

            Ok(())
        } else {
            Err(Error::ColumnAlreadyExist(column_name.to_string()))
        }
    }

    pub fn insert_record(&mut self, values: Vec<Value>) -> Option<u64> {
        // Check that we have the same amount of `values` as we have `column_definitions`.
        if values.len() != self.column_definitions().len() {
            return None;
        }

        let (page_index, record_slot) = {
            let (page_index, writable_page) = self.get_writable_page();
            let mut active_table_page = writable_page.write().ok()?;

            let record_slot = active_table_page.insert_record(values.clone())?;

            (page_index, record_slot)
        };

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

        Some(RowResult::new(self.column_names(), vec![row_data]))
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

        RowResult::new(self.column_names(), rows)
    }

    pub fn get_records_for_columns(&self, column_names: &Vec<&str>) -> Result<RowResult, Error> {
        let sorted_column_indices: Vec<ColumnId> = column_names
            .into_iter()
            .map(|column_name| {
                self.column_definitions()
                    .iter()
                    .find(|cd| cd.name() == column_name)
                    .ok_or(Error::ColumnDoesNotExist(column_name.to_string()))
                    .map(|cd| cd.column_id())
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
        let column_definitions = self.column_definitions();
        let column_ids: Vec<ColumnId> = self
            .column_definitions()
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
                let next_table_page = Rc::new(RwLock::new(TablePage::new(column_definitions)));

                self.pages.push(next_table_page.clone());
                page_vec.push(next_table_page);
            }
        } else {
            let next_table_page = Rc::new(RwLock::new(TablePage::new(column_definitions)));

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
            .column_definitions()
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

    fn column_exists(&self, column_name: &str) -> bool {
        self.column_definitions()
            .iter()
            .find(|cd| cd.name() == column_name)
            .is_some()
    }

    fn column_names(&self) -> Vec<String> {
        self.column_definitions()
            .iter()
            .map(|cd| cd.name().clone())
            .collect()
    }

    fn write_metadata_page(&self, column_definitions: &Vec<ColumnDefinition>) {
        let mut column_definition_data: Vec<u8> = Vec::new();

        column_definition_data.push(column_definitions.len() as u8);

        for column_definition in column_definitions.iter() {
            column_definition_data.extend_from_slice(&column_definition.to_raw_bytes());
        }

        // Add the total column definition size as the first 4 bytes
        let mut metadata: Vec<u8> = Vec::with_capacity(column_definition_data.len());
        metadata.extend_from_slice(&column_definition_data);

        let page_manager = super::page_manager().write().ok().unwrap();
        let mut page = page_manager
            .fetch_page(self.page_id)
            .unwrap()
            .write()
            .ok()
            .unwrap();

        page.metadata
            [COLUMN_DEFINITION_START_OFFSET..COLUMN_DEFINITION_START_OFFSET + metadata.len()]
            .copy_from_slice(&metadata);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_records_for_pages_with_different_columns() {
        let mut table_manager = TableManager::new();
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
        let mut table_manager = TableManager::new();
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
    fn test_getting_a_single_record_works() {
        let mut table_manager = TableManager::new();
        table_manager.add_column("day", DataType::Integer).unwrap();

        let record_id = table_manager
            .insert_record(vec![Value::Integer(13)])
            .expect("Failed to insert record");

        let record = table_manager
            .get_record(record_id)
            .expect("Failed to fetch inserted record.");

        assert_eq!(
            &vec![Some(Value::Integer(13))],
            record.rows().first().unwrap()
        );
    }

    #[test]
    fn test_get_records_with_specific_columns_with_invalid_columns() {
        let mut table_manager = TableManager::new();
        table_manager.add_column("day", DataType::Integer).unwrap();

        assert_eq!(
            Err(Error::ColumnDoesNotExist("month".to_string())),
            table_manager.get_records_for_columns(&vec!["month", "day"])
        )
    }
}
