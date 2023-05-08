use std::{rc::Rc, sync::RwLock};

use bsql::{Manager, PageManager, QueryResult};

#[test]
fn test_creating_database_table_and_inserting_rows() {
    let page_manager = Rc::new(RwLock::new(PageManager::new()));
    let mut manager = Manager::new(page_manager);

    let database_name = "drinkr";
    manager
        .execute("", &format!("CREATE DATABASE {};", database_name))
        .expect("Failed to create database");

    let table_name = "brands";
    manager
        .execute(
            database_name,
            &format!("CREATE TABLE {} (brand_id integer);", table_name),
        )
        .expect("Failed to create table");

    let number_of_brands = 32;
    for brand_id in 0..number_of_brands {
        manager
            .execute(
                database_name,
                &format!("INSERT INTO {} VALUES ({});", table_name, brand_id),
            )
            .expect("Failed to insert rows into the table");
    }

    let Ok(QueryResult::RowResult(row_result)) = manager.execute(database_name, &format!("SELECT * FROM {};", table_name)) else {
        return assert!(false, "Did not get the expected result");
    };

    assert_eq!(row_result.rows().len(), number_of_brands);
}
