// Copyright (c) 2026 ADBC Drivers Contributors
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use adbc_core::{
    Driver,
    error::Result,
    options::{OptionDatabase, OptionValue},
};
use adbc_driver_datafusion::{ContextInit, DataFusionDatabase, DataFusionDriver};
use sedona::context::SedonaContext;

/// An ADBC driver backed by DataFusion and initialized with Apache SedonaDB.
pub struct SedonaDriver {
    inner: DataFusionDriver,
}

impl SedonaDriver {
    /// Create a SedonaDB driver, optionally reusing an existing Tokio runtime.
    pub fn new(handle: Option<tokio::runtime::Handle>) -> Self {
        let context_init: ContextInit = Arc::new(|context, _options| {
            // DataFusion's downstream-driver hook owns session initialization.
            // Replacing the context here ensures all connections created from
            // this ADBC database share Sedona's functions and optimizer rules.
            *context = SedonaContext::new().ctx;
            Ok(())
        });

        Self {
            inner: DataFusionDriver::new_with_context_init(handle, context_init),
        }
    }
}

impl Default for SedonaDriver {
    fn default() -> Self {
        Self::new(None)
    }
}

impl Driver for SedonaDriver {
    type DatabaseType = DataFusionDatabase;

    fn new_database(&mut self) -> Result<Self::DatabaseType> {
        self.inner.new_database()
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> Result<Self::DatabaseType> {
        self.inner.new_database_with_opts(opts)
    }
}

#[cfg(feature = "ffi")]
adbc_ffi::export_driver!(AdbcSedonadbDriverInit, SedonaDriver);
