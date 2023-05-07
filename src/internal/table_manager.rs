use super::{
    page_manager::SharedInternalPage, BitmapIndex, ColumnDefinition, DataType, Error, PageId,
    RowResult, TablePage, Value,
};

type ColumnId = u8;

const COLUMN_BITMAP_RANGE: std::ops::Range<usize> = 0..32;
const COLUMN_TABLE_NAME_RANGE: std::ops::Range<usize> = 32..96;
const COLUMN_DEFINITION_START_OFFSET: usize = 96;

pub struct TableManager {
    page: SharedInternalPage,
    page_ids: Vec<PageId>,
}

impl TableManager {
    pub fn new(table_name: &str) -> Result<Self, Error> {
        if table_name.len() >= COLUMN_TABLE_NAME_RANGE.len() - 1 {
            return Err(Error::TableNameTooLong);
        }

        let mut page_manager = super::page_manager().write().unwrap();
        let (_page_id, shared_page) = page_manager.create_page();

        {
            let mut page = shared_page.write().unwrap();

            page.metadata[COLUMN_TABLE_NAME_RANGE.start] = table_name.len() as u8;
            page.metadata[COLUMN_TABLE_NAME_RANGE.start + 1
                ..COLUMN_TABLE_NAME_RANGE.start + 1 + table_name.len()]
                .copy_from_slice(table_name.as_bytes());
        }

        Ok(Self {
            page_ids: Vec::new(),
            page: shared_page,
        })
    }

    pub fn name(&self) -> String {
        let page = self.page.read().ok().unwrap();

        let name_length = page.metadata[COLUMN_TABLE_NAME_RANGE.start] as usize;
        let name_bytes = &page.metadata
            [COLUMN_TABLE_NAME_RANGE.start + 1..COLUMN_TABLE_NAME_RANGE.start + 1 + name_length];

        return String::from_utf8(name_bytes.to_vec()).unwrap();
    }

    pub fn column_definitions(&self) -> Vec<ColumnDefinition> {
        let page = self.page.read().ok().unwrap();

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
        let mut column_definitions = self.column_definitions();

        if !self.column_exists(column_name) {
            let column_id = {
                let mut page = self.page.write().unwrap();
                let mut column_index: BitmapIndex<255> =
                    BitmapIndex::from_raw(&mut page.metadata[COLUMN_BITMAP_RANGE]).unwrap();
                column_index.consume().ok_or(Error::TooManyColumnsInUse)?
            };

            column_definitions.push(ColumnDefinition::new(
                column_id,
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

        let (page_id, record_slot) = {
            let (page_id, mut active_table_page) = self.get_writable_page();

            let record_slot = active_table_page.insert_record(values.clone())?;

            (page_id, record_slot)
        };

        println!(
            "TableManager.insert_record -> ({}, {})",
            page_id, record_slot
        );

        Some((page_id << 32) as u64 | record_slot as u64)
    }

    pub fn get_record(&self, record_id: u64) -> Option<RowResult> {
        let page_id = (record_id >> 32) & 0xFFFF_FFFF;
        let record_slot = record_id & 0xFFFF_FFFF;

        println!("TableManager.get_record({}, {})", page_id, record_slot);

        let table_page = {
            let page_manager = super::page_manager().read().unwrap();
            let shared_page = page_manager.fetch_page(page_id as u32).unwrap();
            TablePage::load(shared_page)
        };

        let page_columns = table_page.column_definitions();
        let row_data = table_page
            .get_record(record_slot as u8)
            .map(|record| self.normalize_page_record(&page_columns, record))?;

        Some(RowResult::new(self.column_names(), vec![row_data]))
    }

    pub fn get_records(&self) -> RowResult {
        let mut rows: Vec<Vec<Option<Value>>> = Vec::new();

        println!("TableManager.get_records page_ids={:?}", self.page_ids);

        for page_id in &self.page_ids {
            let table_page = {
                let page_manager = super::page_manager().read().unwrap();
                let shared_page = page_manager.fetch_page(*page_id as u32).unwrap();
                TablePage::load(shared_page)
            };

            let page_columns = table_page.column_definitions();
            let page_records = table_page.get_records();

            println!("page_records for page_id={} -> {:?}", page_id, page_records);

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

    fn get_writable_page(&mut self) -> (usize, TablePage) {
        for page_id in &self.page_ids {
            // Load the `TablePage` from the `page_id`
            let page_manager = super::page_manager().read().unwrap();
            let page = page_manager.fetch_page(*page_id).unwrap();
            let table_page = TablePage::load(page);

            // If this `TablePage` have different columns than us, skip to the next one.
            if *table_page.column_definitions() != self.column_definitions() {
                continue;
            }

            // Check if the page is full, otherwise return it.
            if table_page.is_full() {
                // Create a new page and return that.
                let mut page_manager = super::page_manager().write().unwrap();
                let (page_id, shared_page) = page_manager.create_page();

                let table_page = TablePage::initialize(shared_page, self.column_definitions());
                self.page_ids.push(page_id);

                return (page_id as usize, table_page);
            } else {
                return (*page_id as usize, table_page);
            }
        }

        // Create a new page and return that.
        let mut page_manager = super::page_manager().write().unwrap();
        let (page_id, shared_page) = page_manager.create_page();

        let table_page = TablePage::initialize(shared_page, self.column_definitions());
        self.page_ids.push(page_id);

        return (page_id as usize, table_page);
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

        let mut page = self.page.write().ok().unwrap();

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
        let mut table_manager = TableManager::new("test").unwrap();
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
        let mut table_manager = TableManager::new("test").unwrap();
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
        let mut table_manager = TableManager::new("test").unwrap();
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
        let mut table_manager = TableManager::new("test").unwrap();
        table_manager.add_column("day", DataType::Integer).unwrap();

        assert_eq!(
            Err(Error::ColumnDoesNotExist("month".to_string())),
            table_manager.get_records_for_columns(&vec!["month", "day"])
        )
    }
}
