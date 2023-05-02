use super::Token;

#[derive(Debug, PartialEq)]
pub enum Error {
    MissingToken,
    UnexpectedToken { actual: Token },
    MissingDataType,
}
