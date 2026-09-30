// Copyright (c) 2026 ADBC Drivers Contributors
// Licensed under the Apache License, Version 2.0.

use std::{io::Cursor, sync::LazyLock};

use adbc_core::error::{Error, Result, Status};
use flate2::read::GzDecoder;
use sedona_proj::register::{ProjCrsEngineBuilder, configure_global_proj_engine};
use tar::Archive;
use tempfile::TempDir;

const PROJ_DATA_ARCHIVE: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/proj-data.tar.gz"));

static PROJ_DATA_DIR: LazyLock<std::result::Result<TempDir, String>> = LazyLock::new(|| {
    let directory = tempfile::tempdir()
        .map_err(|error| format!("failed to create a temporary PROJ data directory: {error}"))?;
    let compressed = GzDecoder::new(Cursor::new(PROJ_DATA_ARCHIVE));
    Archive::new(compressed)
        .unpack(directory.path())
        .map_err(|error| format!("failed to unpack bundled PROJ data: {error}"))?;

    let database = directory.path().join("proj/proj.db");
    if !database.is_file() {
        return Err("bundled PROJ data does not contain proj/proj.db".to_string());
    }

    Ok(directory)
});

static PROJ_CONFIGURATION: LazyLock<std::result::Result<(), String>> = LazyLock::new(|| {
    let directory = PROJ_DATA_DIR.as_ref().map_err(|message| message.clone())?;
    let data_directory = directory.path().join("proj");
    let builder = ProjCrsEngineBuilder::default()
        .with_database_path(data_directory.join("proj.db"))
        .with_search_paths(vec![data_directory]);

    configure_global_proj_engine(builder)
        .map_err(|error| format!("failed to configure bundled PROJ data: {error}"))
});

pub(crate) fn configure() -> Result<()> {
    PROJ_CONFIGURATION
        .as_ref()
        .map_err(|message| Error::with_message_and_status(message.clone(), Status::Internal))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use sedona_proj::transform::with_global_proj_engine;

    #[test]
    fn bundled_data_initializes_proj() {
        super::configure().unwrap();
        with_global_proj_engine(|_| Ok(())).unwrap();
    }
}
