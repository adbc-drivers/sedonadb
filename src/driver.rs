// Copyright (c) 2026 ADBC Drivers Contributors
// Licensed under the Apache License, Version 2.0.

use adbc_core::{
    Driver, Optionable,
    error::Result,
    options::{OptionDatabase, OptionValue},
};

use crate::database::SedonaDatabase;

#[derive(Default)]
pub struct SedonaDriver {}

impl Driver for SedonaDriver {
    type DatabaseType = SedonaDatabase;

    fn new_database(&mut self) -> Result<Self::DatabaseType> {
        Ok(Self::DatabaseType {})
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> Result<Self::DatabaseType> {
        let mut database = Self::DatabaseType {};
        for (key, value) in opts {
            database.set_option(key, value)?;
        }
        Ok(database)
    }
}
