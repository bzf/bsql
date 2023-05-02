use super::RowResult;

#[derive(Debug, PartialEq)]
pub enum QueryResult {
    CommandSuccessMessage(String),
    RowResult(RowResult),
}
