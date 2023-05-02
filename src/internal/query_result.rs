use super::Value;

pub type RowResult = Vec<Option<Value>>;

#[derive(Debug, PartialEq)]
pub struct QueryResult {
    count: usize,

    columns: Vec<String>,
    rows: Vec<RowResult>,
}

impl QueryResult {
    pub fn new(columns: Vec<String>, rows: Vec<RowResult>) -> Self {
        Self {
            count: rows.len(),

            columns,
            rows,
        }
    }

    pub fn columns(&self) -> &Vec<String> {
        &self.columns
    }

    pub fn rows(&self) -> &[RowResult] {
        &self.rows
    }

    pub fn count(&self) -> &usize {
        &self.count
    }
}
