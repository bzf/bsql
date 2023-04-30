use std::collections::HashMap;

use super::Value;

type InternalId = u64;
type RecordData = Vec<Value>;

#[derive(Debug, PartialEq)]
pub struct TableStore {
    /// Each record with an internal id that points to the data for the record.
    records: HashMap<InternalId, RecordData>,

    /// The next primary key to use for inserting the next record.
    next_internal_id: InternalId,
}

impl TableStore {
    pub fn new() -> Self {
        Self {
            records: HashMap::new(),
            next_internal_id: 1,
        }
    }

    pub fn insert_record(&mut self, record_data: RecordData) -> Option<InternalId> {
        let record_id = self.next_internal_id;
        self.next_internal_id += 1;

        self.records.insert(record_id, record_data);
        Some(record_id)
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
    use super::*;

    #[test]
    fn test_storing_single_column_record() {
        let mut table_store = TableStore::new();

        table_store.insert_record(vec![Value::Integer(28)]);
        table_store.insert_record(vec![Value::Integer(13)]);

        assert_eq!(2, table_store.count());
    }

    #[test]
    fn test_retriving_record_with_internal_id() {
        let mut table_store = TableStore::new();

        let internal_id = table_store
            .insert_record(vec![Value::Integer(28)])
            .expect("Failed to insert record into store");

        let record_data = table_store.get_record(internal_id);

        assert!(record_data.is_some());
    }
}
