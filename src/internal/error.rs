use crate::Token;

#[derive(Debug, PartialEq)]
pub enum Error {
    MissingToken,
    UnexpectedToken { actual: Token },

    DatabaseDoesNotExist(String),
    DatabaseAlreadyExists(String),
    TableDoesNotExist(String),
    TableAlreadyExists(String),
    ColumnDoesNotExist(String),
    InsertFailed,
}
