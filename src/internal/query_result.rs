use super::RowResult;

#[derive(Debug, PartialEq)]
pub enum QueryResult {
    CommandSuccessMessage(String),
    InsertSuccess { count: usize },
    RowResult(RowResult),
}
