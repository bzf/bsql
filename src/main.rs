use std::io::Write;

use crate::parser::Command;

mod database_manager;
mod internal;
mod literal_value;
mod parser;
mod tokenizer;

fn main() {
    let mut database_manager = database_manager::DatabaseManager::new();
    let mut active_database: Option<String> = None;

    loop {
        let expression = if let Some(database_name) = &active_database {
            prompt(&format!("{}> ", database_name))
        } else {
            prompt("> ")
        };

        let command_parts: Vec<&str> = expression.split(" ").collect();

        match &command_parts[..] {
            ["\\c", database_name] => {
                active_database = Some(database_name.to_string());
                println!("You are now connected to database \"{}\".", database_name);
                continue;
            }

            ["exit"] => {
                break;
            }

            _ => {
                let command = parser::parse(&expression);

                match command {
                    Some(Command::CreateDatabase { database_name }) => {
                        database_manager.create_database(&database_name);
                    }

                    Some(Command::CreateTable { schema }) => {
                        let Some(ref database_name) = active_database else {
                            println!("No active database selected.");
                            continue;
                        };

                        todo!(
                            "create table '{}' in database '{}'",
                            schema.name(),
                            database_name
                        )
                    }

                    Some(Command::InsertInto {
                        table_name: _,
                        values: _,
                    }) => (),
                    None => (),
                }
            }
        }
    }
}

fn prompt(name: &str) -> String {
    let mut line = String::new();
    print!("{}", name);

    std::io::stdout().flush().unwrap();
    std::io::stdin()
        .read_line(&mut line)
        .expect("Error: Could not read a line");

    return line.trim().to_string();
}
