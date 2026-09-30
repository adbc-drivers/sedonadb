// Copyright (c) 2026 ADBC Drivers Contributors
// Licensed under the Apache License, Version 2.0.

use std::error::Error;

use adbc_core::{Connection, Database, Driver, Statement};
use adbc_driver_sedonadb::SedonaDriver;

#[test]
fn spatial_functions_are_registered() -> Result<(), Box<dyn Error>> {
    let mut driver = SedonaDriver::default();
    let database = driver.new_database()?;
    let mut connection = database.new_connection()?;
    let mut statement = connection.new_statement()?;

    statement.set_sql_query("SELECT ST_AsText(ST_Point(30, 10)) AS geometry")?;
    let batches = statement.execute()?.collect::<Result<Vec<_>, _>>()?;

    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        1
    );
    Ok(())
}
