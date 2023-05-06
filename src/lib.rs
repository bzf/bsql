#![allow(incomplete_features)]
#![feature(adt_const_params, generic_const_exprs)]

mod internal;

pub use internal::{ColumnDefinition, Error, Manager, QueryResult, RowResult};
