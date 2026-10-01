// Copyright (c) 2026 ADBC Drivers Contributors
// Licensed under the Apache License, Version 2.0.

mod utils;

#[cfg(feature = "bundled-proj-data")]
mod bundled_proj;

pub mod connection;
pub mod database;
pub mod driver;
pub mod statement;

use driver::SedonaDriver;

adbc_ffi::export_driver!(AdbcSedonadbDriverInit, SedonaDriver);
