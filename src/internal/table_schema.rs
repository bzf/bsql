#[derive(Debug, Clone, PartialEq)]
pub struct TableSchema {
    table_name: String,
}

impl TableSchema {
    pub fn new(table_name: String) -> Self {
        Self { table_name }
    }
}
