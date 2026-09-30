// Copyright (c) 2026 ADBC Drivers Contributors
// Licensed under the Apache License, Version 2.0.

use std::{collections::HashMap, sync::Arc};

use adbc_core::{
    Database, Optionable,
    error::{Error, Result, Status},
    options::{OptionConnection, OptionDatabase, OptionValue},
};
use sedona::{context::SedonaContext, context_builder::SedonaContextBuilder};
use sedona_extension::runtime::RuntimeHandle;

use crate::{connection::SedonaConnection, err_unrecognized_option, utils::from_datafusion_error};

/// Maximum query-execution memory, as bytes or a human-readable size such as `"4gb"`.
pub const OPTION_MEMORY_LIMIT: &str = "memory_limit";
/// Directory used for temporary spill files.
pub const OPTION_TEMP_DIRECTORY: &str = "temp_directory";
/// Memory pool implementation: `"fair"` or `"greedy"`.
pub const OPTION_MEMORY_POOL_TYPE: &str = "memory_pool_type";
/// Fraction of a fair memory pool reserved for unspillable consumers.
pub const OPTION_UNSPILLABLE_RESERVE_RATIO: &str = "unspillable_reserve_ratio";

const SEDONA_OPTION_TEMP_DIR: &str = "temp_dir";

#[derive(Debug)]
struct DatabaseOptions {
    memory_limit: String,
    temp_directory: Option<String>,
    memory_pool_type: String,
    unspillable_reserve_ratio: f64,
}

impl Default for DatabaseOptions {
    fn default() -> Self {
        Self {
            memory_limit: "unlimited".to_string(),
            temp_directory: None,
            memory_pool_type: "fair".to_string(),
            unspillable_reserve_ratio: 0.2,
        }
    }
}

impl DatabaseOptions {
    fn parse(opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>) -> Result<Self> {
        let mut parsed = Self::default();

        for (key, value) in opts {
            match key.as_ref() {
                OPTION_MEMORY_LIMIT => {
                    parsed.memory_limit = match value {
                        OptionValue::String(value) => value,
                        OptionValue::Int(value) if value >= 0 => value.to_string(),
                        _ => {
                            return Err(invalid_option(
                                &key,
                                "expected a non-negative integer or string",
                            ));
                        }
                    };
                }
                OPTION_TEMP_DIRECTORY => {
                    parsed.temp_directory = Some(option_string(&key, value)?);
                }
                OPTION_MEMORY_POOL_TYPE => {
                    parsed.memory_pool_type = option_string(&key, value)?;
                }
                OPTION_UNSPILLABLE_RESERVE_RATIO => {
                    parsed.unspillable_reserve_ratio = match value {
                        OptionValue::Double(value) => value,
                        OptionValue::String(value) => value.parse().map_err(|_| {
                            invalid_option(&key, "expected a floating-point number")
                        })?,
                        _ => return Err(invalid_option(&key, "expected a double or string")),
                    };
                }
                _ => return err_unrecognized_option!(key),
            }
        }

        Ok(parsed)
    }

    fn runtime_options(&self) -> HashMap<String, String> {
        let mut opts = HashMap::from([
            (OPTION_MEMORY_LIMIT.to_string(), self.memory_limit.clone()),
            (
                OPTION_MEMORY_POOL_TYPE.to_string(),
                self.memory_pool_type.clone(),
            ),
            (
                OPTION_UNSPILLABLE_RESERVE_RATIO.to_string(),
                self.unspillable_reserve_ratio.to_string(),
            ),
        ]);
        if let Some(temp_directory) = &self.temp_directory {
            opts.insert(SEDONA_OPTION_TEMP_DIR.to_string(), temp_directory.clone());
        }
        opts
    }
}

fn option_string(key: &OptionDatabase, value: OptionValue) -> Result<String> {
    match value {
        OptionValue::String(value) => Ok(value),
        _ => Err(invalid_option(key, "expected a string")),
    }
}

fn invalid_option(key: &OptionDatabase, message: &str) -> Error {
    Error::with_message_and_status(
        format!("Invalid value for option {:?}: {message}", key.as_ref()),
        Status::InvalidArguments,
    )
}

pub struct SedonaDatabase {
    runtime: Arc<RuntimeHandle>,
    ctx: Arc<SedonaContext>,
    options: DatabaseOptions,
}

impl SedonaDatabase {
    pub(crate) fn try_new(
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> Result<Self> {
        let options = DatabaseOptions::parse(opts)?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| {
                Error::with_message_and_status(
                    format!("Failed to build multithreaded runtime: {e}"),
                    Status::Internal,
                )
            })?;

        let builder = SedonaContextBuilder::from_options(&options.runtime_options())
            .map_err(from_datafusion_error)?;
        let ctx = runtime
            .block_on(builder.build())
            .map_err(from_datafusion_error)?;

        Ok(Self {
            runtime: Arc::new(RuntimeHandle::new(runtime)),
            ctx: Arc::new(ctx),
            options,
        })
    }
}

impl Optionable for SedonaDatabase {
    type Option = OptionDatabase;

    fn set_option(&mut self, key: Self::Option, _value: OptionValue) -> Result<()> {
        match key.as_ref() {
            OPTION_MEMORY_LIMIT
            | OPTION_TEMP_DIRECTORY
            | OPTION_MEMORY_POOL_TYPE
            | OPTION_UNSPILLABLE_RESERVE_RATIO => Err(Error::with_message_and_status(
                format!(
                    "Option {:?} cannot be changed after database initialization",
                    key.as_ref()
                ),
                Status::InvalidState,
            )),
            _ => err_unrecognized_option!(key),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match key.as_ref() {
            OPTION_MEMORY_LIMIT => Ok(self.options.memory_limit.clone()),
            OPTION_TEMP_DIRECTORY => self
                .options
                .temp_directory
                .clone()
                .ok_or_else(|| invalid_option(&key, "option is not set")),
            OPTION_MEMORY_POOL_TYPE => Ok(self.options.memory_pool_type.clone()),
            OPTION_UNSPILLABLE_RESERVE_RATIO => {
                Ok(self.options.unspillable_reserve_ratio.to_string())
            }
            _ => err_unrecognized_option!(key),
        }
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        err_unrecognized_option!(key)
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        err_unrecognized_option!(key)
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        match key.as_ref() {
            OPTION_UNSPILLABLE_RESERVE_RATIO => Ok(self.options.unspillable_reserve_ratio),
            _ => err_unrecognized_option!(key),
        }
    }
}

impl Database for SedonaDatabase {
    type ConnectionType = SedonaConnection;

    fn new_connection(&self) -> Result<SedonaConnection> {
        self.new_connection_with_opts([])
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = (OptionConnection, OptionValue)>,
    ) -> Result<SedonaConnection> {
        SedonaConnection::new(self.runtime.clone(), self.ctx.clone(), opts)
    }
}

#[cfg(test)]
mod test {
    use adbc_core::Driver;
    use datafusion::execution::memory_pool::MemoryLimit;

    use crate::driver::SedonaDriver;

    use super::*;

    fn option(name: &str) -> OptionDatabase {
        OptionDatabase::Other(name.to_string())
    }

    #[test]
    fn initializes_context_with_database_options() {
        let database = SedonaDriver::default()
            .new_database_with_opts([
                (option(OPTION_MEMORY_LIMIT), OptionValue::from("32mb")),
                (option(OPTION_TEMP_DIRECTORY), OptionValue::from("/tmp")),
                (option(OPTION_MEMORY_POOL_TYPE), OptionValue::from("greedy")),
                (
                    option(OPTION_UNSPILLABLE_RESERVE_RATIO),
                    OptionValue::from(0.1),
                ),
            ])
            .unwrap();

        assert!(matches!(
            database.ctx.ctx.runtime_env().memory_pool.memory_limit(),
            MemoryLimit::Finite(limit) if limit == 32 * 1024 * 1024
        ));
        assert_eq!(
            database
                .options
                .runtime_options()
                .get(SEDONA_OPTION_TEMP_DIR),
            Some(&"/tmp".to_string())
        );
    }

    #[test]
    fn rejects_invalid_database_options() {
        let error = SedonaDriver::default()
            .new_database_with_opts([(option(OPTION_MEMORY_LIMIT), OptionValue::from(-1_i64))])
            .err()
            .unwrap();
        assert_eq!(error.status, Status::InvalidArguments);

        let error = SedonaDriver::default()
            .new_database_with_opts([(
                option(OPTION_UNSPILLABLE_RESERVE_RATIO),
                OptionValue::from(1.1),
            )])
            .err()
            .unwrap();
        assert_eq!(error.status, Status::InvalidArguments);
    }
}
