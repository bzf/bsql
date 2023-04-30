use std::collections::HashMap;

use super::TableSchema;

type InternalId = u64;
type RecordData = Vec<u8>;

#[derive(Debug, PartialEq)]
pub struct TableStore {
    /// The column definition for records in this store.
    table_schema: TableSchema,

    /// Each record with an internal id that points to the data for the record.
    records: HashMap<InternalId, RecordData>,

    /// The next primary key to use for inserting the next record.
    next_internal_id: InternalId,
}

impl TableStore {
    pub fn new(table_schema: TableSchema) -> Self {
        Self {
            table_schema,

            records: HashMap::new(),
            next_internal_id: 1,
        }
    }

    pub fn insert_record(&mut self, record_data: RecordData) -> Option<InternalId> {
        if self.table_schema.column_definitions_len() == record_data.len() {
            let record_id = self.next_internal_id;
            self.next_internal_id += 1;

            self.records.insert(record_id, record_data);
            Some(record_id)
        } else {
            None
        }
    }

    pub fn get_record(&self, internal_id: InternalId) -> Option<&RecordData> {
        self.records.get(&internal_id)
    }

    pub fn count(&self) -> usize {
        self.records.len()
    }
}

#[cfg(test)]
mod tests {
    use crate::internal::data_type::DataType;

    use super::*;

    #[test]
    fn test_storing_single_column_record() {
        let mut table_schema = TableSchema::new();
        table_schema.add_column("age", DataType::Integer);
        let mut table_store = TableStore::new(table_schema);

        table_store.insert_record(vec![28]);
        table_store.insert_record(vec![13]);

        assert_eq!(2, table_store.count());
    }

    #[test]
    fn test_storing_record_with_different_size() {
        let mut table_schema = TableSchema::new();
        table_schema.add_column("age", DataType::Integer);
        let mut table_store = TableStore::new(table_schema);

        table_store.insert_record(vec![28, 29]);

        assert_eq!(0, table_store.count());
    }

    #[test]
    fn test_retriving_record_with_internal_id() {
        let mut table_schema = TableSchema::new();
        table_schema.add_column("age", DataType::Integer);
        let mut table_store = TableStore::new(table_schema);

        let internal_id = table_store
            .insert_record(vec![28])
            .expect("Failed to insert record into store");

        let record_data = table_store.get_record(internal_id);

        assert!(record_data.is_some());
    }
}
