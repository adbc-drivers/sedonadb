// Copyright (c) 2026 ADBC Drivers Contributors
// Licensed under the Apache License, Version 2.0.

mod utils;

pub mod connection;
pub mod database;
pub mod driver;
pub mod statement;

use driver::SedonaDriver;

adbc_ffi::export_driver!(AdbcSedonadbDriverInit, SedonaDriver);
