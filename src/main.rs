#![allow(incomplete_features)]
#![feature(adt_const_params, generic_const_exprs)]

use std::{io::Write, rc::Rc, sync::RwLock};

mod print_table;

use bsql::{ColumnDefinition, Error, Manager, PageManager, QueryResult};
use print_table::{print_row_result, print_table};

fn main() {
    let page_manager = Rc::new(RwLock::new(PageManager::new("bsql.db")));

    let mut database_manager = Manager::new(page_manager);
    let mut active_database = String::new();

    loop {
        let expression = prompt(&format!("{}> ", active_database));
        let command_parts: Vec<&str> = expression.split(" ").collect();

        match &command_parts[..] {
            ["\\c", database_name] => {
                if database_manager.database_exists(database_name) {
                    active_database = database_name.to_string();
                    println!("You are now connected to database \"{}\".", database_name);
                } else {
                    println!("FATAL: database \"{}\" does not exist", database_name);
                }
            }

            ["\\l"] => print_databases(database_manager.database_names()),
            ["\\list"] => print_databases(database_manager.database_names()),

            ["\\dt"] => match database_manager.database_table_names(&active_database) {
                Ok(table_names) => print_tables(table_names),
                Err(error) => print_error(&error),
            },

            ["\\d+", table_name] => {
                match database_manager.table_definition(&active_database, table_name) {
                    Ok(table_definition) => print_table_definition(&table_definition),
                    Err(error) => print_error(&error),
                }
            }

            ["exit"] => {
                break;
            }

            _ => match database_manager.execute(&active_database, &expression) {
                Ok(query_result) => print_query_result(&query_result),
                Err(error) => print_error(&error),
            },
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

fn print_table_definition(column_definitions: &Vec<ColumnDefinition>) {
    print_table(
        vec!["Column name", "Data type"],
        column_definitions
            .into_iter()
            .map(|definition| {
                vec![
                    definition.name().clone(),
                    definition.data_type().to_string(),
                ]
            })
            .collect(),
    );
}

fn print_query_result(query_result: &QueryResult) {
    match query_result {
        QueryResult::CommandSuccessMessage(message) => println!("{}", message),
        QueryResult::InsertSuccess { count } => println!("INSERT 0 {}", count),
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
