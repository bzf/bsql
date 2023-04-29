use std::collections::HashMap;

mod database;

pub struct DatabaseManager {
    databases: HashMap<String, database::Database>,
}

impl DatabaseManager {
    pub fn new() -> Self {
        println!("DatabaseManager::new()");
        Self {
            databases: HashMap::new(),
        }
    }

    pub fn create_database(&mut self, name: &str) -> Option<()> {
        if !self.databases.contains_key(name) {
            self.databases
                .insert(name.to_string(), database::Database::new());

            println!("CREATE DATABASE");
            return Some(());
        } else {
            println!("error: Database '{}' already exists.", name);
            return None;
        }
    }
}
