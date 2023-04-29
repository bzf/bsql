use std::collections::HashMap;

mod database;

pub struct DatabaseManager {
    databases: HashMap<String, database::Database>,
}

impl DatabaseManager {
    pub fn new() -> Self {
        Self {
            databases: HashMap::new(),
        }
    }

    pub fn database_names(&self) -> Vec<String> {
        self.databases.keys().cloned().collect()
    }

    pub fn database_exists(&self, key: &str) -> bool {
        self.databases.contains_key(key)
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
