use std::io::Write;

mod internal;
mod print_table;

use internal::{parse, ColumnDefinition, Command, Error, QueryResult, Token};
use print_table::{print_row_result, print_table};

fn main() {
    let mut database_manager = internal::Manager::new();
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

            ["\\l"] => print_databases(database_manager.database_names()),
            ["\\list"] => print_databases(database_manager.database_names()),

            ["\\dt"] => {
                let Some(database_name) = &active_database else {
                    println!("FATAL: No active database selected.");
                    continue;
                };

                match database_manager.database_table_names(&database_name) {
                    Ok(table_names) => print_tables(table_names),
                    Err(error) => print_error(&error),
                }
            }

            ["\\d+", table_name] => {
                let Some(database_name) = &active_database else {
                    println!("No active database selected.");
                    continue;
                };

                match database_manager.table_definition(database_name, table_name) {
                    Ok(table_definition) => print_table_definition(table_definition),
                    Err(error) => print_error(&error),
                }
            }

            ["exit"] => {
                break;
            }

            _ => {
                let command = parse(&expression);

                match command {
                    Ok(Command::CreateDatabase { database_name }) => {
                        match database_manager.create_database(&database_name) {
                            Ok(query_result) => print_query_result(&query_result),
                            Err(error) => print_error(&error),
                        }
                    }

                    Ok(Command::CreateTable {
                        table_name,
                        column_definitions,
                    }) => {
                        let Some(ref database_name) = active_database else {
                            println!("No active database selected.");
                            continue;
                        };

                        match database_manager.create_table(database_name, &table_name) {
                            Ok(query_result) => {
                                for column in column_definitions {
                                    database_manager.add_column(
                                        database_name,
                                        &table_name,
                                        &column.0,
                                        column.1.into(),
                                    );
                                }

                                print_query_result(&query_result)
                            }

                            Err(error) => print_error(&error),
                        }
                    }

                    Ok(Command::InsertInto { table_name, values }) => {
                        let Some(ref database_name) = active_database else {
                            println!("No active database selected.");
                            continue;
                        };

                        match database_manager.insert_row(
                            database_name,
                            &table_name,
                            values.into_iter().map(|value| value.into()).collect(),
                        ) {
                            Ok(query_result) => print_query_result(&query_result),
                            Err(error) => print_error(&error),
                        }
                    }

                    Ok(Command::Select {
                        identifiers,
                        table_name,
                        ..
                    }) => {
                        let Some(ref database_name) = active_database else {
                            println!("No active database selected.");
                            continue;
                        };

                        match &identifiers
                            .iter()
                            .map(|i| i.as_str())
                            .collect::<Vec<&str>>()[..]
                        {
                            ["*"] => {
                                let query_result =
                                    database_manager.select_all(database_name, &table_name);

                                match query_result {
                                    Ok(query_result) => print_query_result(&query_result),
                                    Err(error) => print_error(&error),
                                }
                            }

                            _ => {
                                let query_result = database_manager.select(
                                    database_name,
                                    &table_name,
                                    identifiers.clone(),
                                );

                                match query_result {
                                    Ok(query_result) => print_query_result(&query_result),
                                    Err(error) => print_error(&error),
                                }
                            }
                        }
                    }

                    Err(error) => eprintln!("{:?}", error),
                }
            }
        }
    }
}

fn print_databases(database_names: Vec<String>) {
    print_table(
        vec!["Database name"],
        database_names.into_iter().map(|name| vec![name]).collect(),
    );
}

fn print_tables(table_names: Vec<String>) {
    print_table(
        vec!["Table name"],
        table_names.into_iter().map(|name| vec![name]).collect(),
    );
}

fn print_table_definition(column_definitions: Vec<(String, ColumnDefinition)>) {
    print_table(
        vec!["Column name", "Data type"],
        column_definitions
            .into_iter()
            .map(|(column_name, definition)| vec![column_name, definition.data_type().to_string()])
            .collect(),
    );
}

fn print_query_result(query_result: &QueryResult) {
    match query_result {
        QueryResult::CommandSuccessMessage(message) => println!("{}", message),
        QueryResult::RowResult(row_result) => print_row_result(row_result),
    }
}

fn print_error(error: &Error) {
    eprintln!("ERROR: {:?}", error);
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
