pub struct DatabaseManager {}

impl DatabaseManager {
    pub fn new() -> Self {
        println!("DatabaseManager::new()");
        Self {}
    }

    pub fn create_database(&mut self, name: &str) -> Option<()> {
        println!("DatabaseManager::create_database({})", name);
        None
    }

    pub fn create_table(&mut self, database_name: &str, table_name: &str) -> Option<()> {
        println!(
            "DatabaseManager::create_table({}, {})",
            database_name, table_name
        );
        None
    }
}
