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
                if database_manager.database_exists(database_name) {
                    active_database = Some(database_name.to_string());
                    println!("You are now connected to database \"{}\".", database_name);
                } else {
                    println!("FATAL: database \"{}\" does not exist", database_name);
                }
            }

            ["\\l"] => list_databases(database_manager.database_names()),
            ["\\list"] => list_databases(database_manager.database_names()),

            ["\\dt"] => {
                let Some(database_name) = &active_database else {
                    println!("No active database selected.");
                    continue;
                };

                if let Some(table_names) = database_manager.database_table_names(&database_name) {
                    list_tables(table_names)
                } else {
                    println!("FATAL: Active database no longer exists.");
                }
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

                        if database_manager.create_table(database_name, schema) {
                            println!("CREATE TABLE");
                        } else {
                            println!("ERROR: Table \"{}\" already exists.", database_name);
                        }
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

fn list_databases(database_names: Vec<String>) {
    database_names
        .iter()
        .for_each(|database_name| println!("{}", database_name));

    println!("({} rows)", database_names.len());
}

fn list_tables(table_names: Vec<String>) {
    table_names
        .iter()
        .for_each(|table_name| println!("{}", table_name));

    println!("({} rows)", table_names.len());
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
