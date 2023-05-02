use super::Value;

pub type RowValues = Vec<Option<Value>>;

#[derive(Debug, PartialEq)]
pub struct RowResult {
    count: usize,

    columns: Vec<String>,
    rows: Vec<RowValues>,
}

impl RowResult {
    pub fn new(columns: Vec<String>, rows: Vec<RowValues>) -> Self {
        Self {
            count: rows.len(),

            columns,
            rows,
        }
    }

    pub fn columns(&self) -> &Vec<String> {
        &self.columns
    }

    pub fn rows(&self) -> &[RowValues] {
        &self.rows
    }

    pub fn count(&self) -> &usize {
        &self.count
    }
}
