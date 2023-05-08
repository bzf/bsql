bsql
====

An experiment to implement a generic SQL-database in Rust without any external
dependencies for learning about database internals.

It stores its data and metadata into pages (sized 4096 bytes each) in the
`bsql.db` file.


## Build

```sh
$ cargo build
```


## Run

```sh
$ cargo run
```


## Example use

```SQL
# Create a new database named `test`.
> CREATE DATABASE test;
CREATE DATABASE

# Set `test` as our active database.
> \c test
You are now connected to database "test".

# Create a new table `drivers` with a single column, `number`.
test> CREATE TABLE drivers (number integer);
CREATE TABLE

# Insert some rows into the table.
test> INSERT INTO drivers VALUES (44);
INSERT 0 1
test> INSERT INTO drivers VALUES (4);
INSERT 0 1
test> INSERT INTO drivers VALUES (11);
INSERT 0 1

# Select all rows from the `drivers` table.
test> SELECT * FROM drivers;
 number |
--------+
 44     |
 4      |
 11     |
```
