// Copyright (c) 2026 ADBC Drivers Contributors
// Licensed under the Apache License, Version 2.0.

use adbc_core::{
    Driver,
    error::Result,
    options::{OptionDatabase, OptionValue},
};

use crate::database::SedonaDatabase;

#[derive(Default)]
pub struct SedonaDriver {}

impl Driver for SedonaDriver {
    type DatabaseType = SedonaDatabase;

    fn new_database(&mut self) -> Result<Self::DatabaseType> {
        SedonaDatabase::try_new([])
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> Result<Self::DatabaseType> {
        SedonaDatabase::try_new(opts)
    }
}
