use std::collections::HashMap;

mod database_definition;

pub struct DatabaseManager {
    database_definitions: HashMap<String, database_definition::DatabaseDefinition>,
}

impl DatabaseManager {
    pub fn new() -> Self {
        Self {
            database_definitions: HashMap::new(),
        }
    }

    pub fn database_names(&self) -> Vec<String> {
        self.database_definitions.keys().cloned().collect()
    }

    pub fn database_table_names(&self, database_name: &str) -> Option<Vec<String>> {
        self.database_definitions
            .get(database_name)
            .map(|database| database.table_names())
    }

    pub fn database_exists(&self, key: &str) -> bool {
        self.database_definitions.contains_key(key)
    }

    pub fn create_table(&mut self, database_name: &str, table_name: &str) -> bool {
        let Some(_database) = self.database_definitions.get_mut(database_name) else {
            return false;
        };

        self.database_definitions
            .get_mut(database_name)
            .and_then(|database| database.create_table(table_name))
            .is_some()
    }

    pub fn create_database(&mut self, name: &str) -> Option<()> {
        if !self.database_definitions.contains_key(name) {
            self.database_definitions.insert(
                name.to_string(),
                database_definition::DatabaseDefinition::new(),
            );

            println!("CREATE DATABASE");
            return Some(());
        } else {
            println!("error: Database '{}' already exists.", name);
            return None;
        }
    }
}
