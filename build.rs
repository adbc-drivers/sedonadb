// Copyright (c) 2026 ADBC Drivers Contributors
// Licensed under the Apache License, Version 2.0.

use std::{env, fs, path::PathBuf};

fn main() {
    const ARCHIVE_ENV: &str = "PROJ_DATA_ARCHIVE";

    println!("cargo:rerun-if-env-changed={ARCHIVE_ENV}");
    if env::var_os("CARGO_FEATURE_BUNDLED_PROJ_DATA").is_none() {
        return;
    }

    let source = PathBuf::from(env::var_os(ARCHIVE_ENV).unwrap_or_else(|| {
        panic!("{ARCHIVE_ENV} must point to a .tar.gz archive when bundled-proj-data is enabled")
    }));
    if !source.is_file() {
        panic!(
            "{ARCHIVE_ENV} does not point to a file: {}",
            source.display()
        );
    }

    println!("cargo:rerun-if-changed={}", source.display());
    let destination = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR is set by Cargo"))
        .join("proj-data.tar.gz");
    fs::copy(&source, &destination).unwrap_or_else(|error| {
        panic!(
            "failed to copy {} to {}: {error}",
            source.display(),
            destination.display()
        )
    });
}
