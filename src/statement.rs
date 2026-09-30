// Copyright (c) 2026 ADBC Drivers Contributors
// Licensed under the Apache License, Version 2.0.

use adbc_core::{PartitionedResult, constants};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::Schema;
use sedona::context::SedonaContext;
use sedona_extension::runtime::RuntimeHandle;
use sedona_extension::streaming::StreamingRecordBatchReader;
use std::sync::Arc;

use adbc_core::{
    Optionable, Statement,
    error::{Error, Result, Status},
    options::{IngestMode, OptionStatement, OptionValue},
};

use crate::{err_not_implemented, err_unrecognized_option, utils::from_datafusion_error};

pub struct SedonaStatement {
    runtime: Arc<RuntimeHandle>,
    ctx: Arc<SedonaContext>,
    sql_query: Option<String>,
    ingest: IngestOptions,
}

#[derive(Debug)]
struct IngestOptions {
    target_table: Option<String>,
    target_catalog: Option<String>,
    target_schema: Option<String>,
    mode: IngestMode,
    temporary: bool,
}

impl Default for IngestOptions {
    fn default() -> Self {
        Self {
            target_table: None,
            target_catalog: None,
            target_schema: None,
            mode: IngestMode::Create,
            temporary: false,
        }
    }
}

impl SedonaStatement {
    pub(crate) fn new(runtime: Arc<RuntimeHandle>, ctx: Arc<SedonaContext>) -> SedonaStatement {
        Self {
            runtime,
            ctx,
            sql_query: None,
            ingest: IngestOptions::default(),
        }
    }
}

impl Optionable for SedonaStatement {
    type Option = OptionStatement;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        match &key {
            OptionStatement::TargetTable => {
                self.ingest.target_table = Some(option_string(&key, value)?);
                Ok(())
            }
            OptionStatement::TargetCatalog => {
                self.ingest.target_catalog = Some(option_string(&key, value)?);
                Ok(())
            }
            OptionStatement::TargetDbSchema => {
                self.ingest.target_schema = Some(option_string(&key, value)?);
                Ok(())
            }
            OptionStatement::IngestMode => {
                self.ingest.mode = IngestMode::try_from(&value)?;
                Ok(())
            }
            OptionStatement::Temporary => match value {
                OptionValue::String(value) if value == constants::ADBC_OPTION_VALUE_DISABLED => {
                    self.ingest.temporary = false;
                    Ok(())
                }
                _ => Err(Error::with_message_and_status(
                    "Temporary tables are not supported",
                    Status::NotImplemented,
                )),
            },
            _ => err_unrecognized_option!(key),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match &key {
            OptionStatement::TargetTable => option_value(&key, &self.ingest.target_table),
            OptionStatement::TargetCatalog => option_value(&key, &self.ingest.target_catalog),
            OptionStatement::TargetDbSchema => option_value(&key, &self.ingest.target_schema),
            OptionStatement::IngestMode => Ok(self.ingest.mode.into()),
            OptionStatement::Temporary => Ok(if self.ingest.temporary {
                constants::ADBC_OPTION_VALUE_ENABLED
            } else {
                constants::ADBC_OPTION_VALUE_DISABLED
            }
            .to_string()),
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
        err_unrecognized_option!(key)
    }
}

fn option_value(key: &OptionStatement, value: &Option<String>) -> Result<String> {
    value.clone().ok_or_else(|| {
        Error::with_message_and_status(
            format!("Option {:?} has not been set", key.as_ref()),
            Status::NotFound,
        )
    })
}

fn option_string(key: &OptionStatement, value: OptionValue) -> Result<String> {
    match value {
        OptionValue::String(value) => Ok(value),
        _ => Err(Error::with_message_and_status(
            format!("Option {:?} must be a string", key.as_ref()),
            Status::InvalidArguments,
        )),
    }
}

impl Statement for SedonaStatement {
    fn set_sql_query(&mut self, query: impl AsRef<str>) -> Result<()> {
        self.sql_query = Some(query.as_ref().to_string());
        Ok(())
    }

    fn prepare(&mut self) -> Result<()> {
        Ok(())
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        if let Some(query) = self.sql_query.clone() {
            self.runtime.block_on(async {
                let df = self.ctx.sql(&query).await.map_err(from_datafusion_error)?;
                Ok(df.schema().as_arrow().clone())
            })
        } else {
            Err(Error::with_message_and_status(
                "query not set yet",
                Status::InvalidState,
            ))
        }
    }

    fn execute(&mut self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        if let Some(query) = self.sql_query.clone() {
            self.runtime.block_on(async {
                let df = self.ctx.sql(&query).await.map_err(from_datafusion_error)?;
                let stream = df.execute_stream().await.map_err(from_datafusion_error)?;
                let reader = StreamingRecordBatchReader::new(stream, self.runtime.clone())
                    .with_skip_empty_batches(true);
                Ok(Box::new(reader) as Box<dyn RecordBatchReader + Send + 'static>)
            })
        } else {
            Err(Error::with_message_and_status(
                "query not set yet",
                Status::InvalidState,
            ))
        }
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        err_not_implemented!()
    }

    fn execute_partitions(&mut self) -> Result<PartitionedResult> {
        err_not_implemented!()
    }

    fn bind(&mut self, _batch: RecordBatch) -> Result<()> {
        err_not_implemented!()
    }

    fn bind_stream(&mut self, _reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        err_not_implemented!()
    }

    fn get_parameter_schema(&self) -> Result<Schema> {
        err_not_implemented!()
    }

    fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> Result<()> {
        err_not_implemented!()
    }

    fn cancel(&mut self) -> Result<()> {
        err_not_implemented!()
    }
}

#[cfg(test)]
mod test {
    use std::ops::Deref;

    use adbc_core::{
        Connection, Database, Driver, Optionable, Statement, constants,
        error::Status,
        options::{IngestMode, OptionStatement, OptionValue},
    };
    use arrow_array::RecordBatch;
    use arrow_schema::{Field, Schema};
    use datafusion::assert_batches_eq;

    use crate::driver::SedonaDriver;

    #[test]
    fn statement() {
        let mut statement = SedonaDriver::default()
            .new_database()
            .unwrap()
            .new_connection()
            .unwrap()
            .new_statement()
            .unwrap();

        // Can't execute_schema() or execute() before setting a query
        let maybe_err = statement.execute_schema();
        assert_eq!(maybe_err.err().unwrap().message, "query not set yet");
        let maybe_err = statement.execute();
        assert_eq!(maybe_err.err().unwrap().message, "query not set yet");

        statement
            .set_sql_query("SELECT ST_AsText(ST_Point(1, 2)) AS geom")
            .unwrap();

        statement.prepare().unwrap();

        assert_eq!(
            statement.execute_schema().unwrap(),
            Schema::new(vec![Field::new("geom", arrow_schema::DataType::Utf8, true)])
        );

        let batches: Result<Vec<RecordBatch>, _> = statement.execute().unwrap().collect();

        assert_batches_eq!(
            [
                "+------------+",
                "| geom       |",
                "+------------+",
                "| POINT(1 2) |",
                "+------------+",
            ],
            batches.unwrap().deref()
        );
    }

    #[test]
    fn ingest_options() {
        let mut statement = SedonaDriver::default()
            .new_database()
            .unwrap()
            .new_connection()
            .unwrap()
            .new_statement()
            .unwrap();

        assert_eq!(statement.ingest.mode, IngestMode::Create);
        assert!(!statement.ingest.temporary);
        assert_eq!(
            statement
                .get_option_string(OptionStatement::TargetTable)
                .unwrap_err()
                .status,
            Status::NotFound
        );

        statement
            .set_option(
                OptionStatement::TargetTable,
                OptionValue::from("observations"),
            )
            .unwrap();
        statement
            .set_option(
                OptionStatement::TargetCatalog,
                OptionValue::from("datafusion"),
            )
            .unwrap();
        statement
            .set_option(OptionStatement::TargetDbSchema, OptionValue::from("public"))
            .unwrap();
        statement
            .set_option(
                OptionStatement::IngestMode,
                OptionValue::from(IngestMode::CreateAppend),
            )
            .unwrap();
        statement
            .set_option(OptionStatement::Temporary, OptionValue::from(false))
            .unwrap();

        assert_eq!(
            statement.ingest.target_table.as_deref(),
            Some("observations")
        );
        assert_eq!(
            statement.ingest.target_catalog.as_deref(),
            Some("datafusion")
        );
        assert_eq!(statement.ingest.target_schema.as_deref(), Some("public"));
        assert_eq!(statement.ingest.mode, IngestMode::CreateAppend);
        assert!(!statement.ingest.temporary);
        assert_eq!(
            statement
                .get_option_string(OptionStatement::TargetTable)
                .unwrap(),
            "observations"
        );
        assert_eq!(
            statement
                .get_option_string(OptionStatement::TargetCatalog)
                .unwrap(),
            "datafusion"
        );
        assert_eq!(
            statement
                .get_option_string(OptionStatement::TargetDbSchema)
                .unwrap(),
            "public"
        );
        assert_eq!(
            statement
                .get_option_string(OptionStatement::IngestMode)
                .unwrap(),
            String::from(IngestMode::CreateAppend)
        );
        assert_eq!(
            statement
                .get_option_string(OptionStatement::Temporary)
                .unwrap(),
            constants::ADBC_OPTION_VALUE_DISABLED
        );

        let error = statement
            .set_option(OptionStatement::TargetTable, OptionValue::from(1_i64))
            .unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);

        let error = statement
            .set_option(
                OptionStatement::IngestMode,
                OptionValue::from("unsupported"),
            )
            .unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);

        let error = statement
            .set_option(OptionStatement::Temporary, OptionValue::from(true))
            .unwrap_err();
        assert_eq!(error.status, Status::NotImplemented);
    }
}
