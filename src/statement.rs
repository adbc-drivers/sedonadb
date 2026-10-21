// Copyright (c) 2026 ADBC Drivers Contributors
// Licensed under the Apache License, Version 2.0.

use adbc_core::{PartitionedResult, constants};
use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use datafusion::{
    arrow::compute::CastOptions,
    common::{ParamValues, ScalarValue, metadata::ScalarAndMetadata},
    dataframe::DataFrame,
};
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
    bound: Option<Box<dyn RecordBatchReader + Send>>,
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
            bound: None,
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
                let df = if let Some(bound) = &self.bound {
                    bind_parameters(df, &bound.schema(), None)?
                } else {
                    df
                };
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
                if let Some(bound) = &self.bound {
                    let schema = Arc::new(
                        bind_parameters(df.clone(), &bound.schema(), None)?
                            .schema()
                            .as_arrow()
                            .clone(),
                    );
                    return Ok(Box::new(ParameterReader {
                        runtime: self.runtime.clone(),
                        df,
                        bound: self.bound.take().unwrap(),
                        schema,
                        batch: None,
                        row: 0,
                        results: None,
                        finished: false,
                    }) as Box<dyn RecordBatchReader + Send>);
                }
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

    fn bind(&mut self, batch: RecordBatch) -> Result<()> {
        let schema = batch.schema();
        self.bind_stream(Box::new(RecordBatchIterator::new([Ok(batch)], schema)))
    }

    fn bind_stream(&mut self, reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        self.bound = Some(reader);
        Ok(())
    }

    fn get_parameter_schema(&self) -> Result<Schema> {
        let query = self.sql_query.as_deref().ok_or_else(|| {
            Error::with_message_and_status("query not set yet", Status::InvalidState)
        })?;
        self.runtime.block_on(async {
            let plan = self
                .ctx
                .ctx
                .state()
                .create_logical_plan(query)
                .await
                .map_err(from_datafusion_error)?;
            let mut parameters: Vec<_> = plan
                .get_parameter_fields()
                .map_err(from_datafusion_error)?
                .into_iter()
                .collect();
            parameters.sort_by(|(a, _), (b, _)| {
                let ordinal = |name: &str| name[1..].parse::<usize>().unwrap_or(usize::MAX);
                ordinal(a).cmp(&ordinal(b)).then_with(|| a.cmp(b))
            });
            Ok(Schema::new(
                parameters
                    .into_iter()
                    .map(|(name, field)| {
                        field
                            .map(|field| field.as_ref().clone().with_name(name.clone()))
                            .unwrap_or_else(|| Field::new(name, DataType::Null, true))
                    })
                    .collect::<Vec<_>>(),
            ))
        })
    }

    fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> Result<()> {
        err_not_implemented!()
    }

    fn cancel(&mut self) -> Result<()> {
        err_not_implemented!()
    }
}

// Bind by ordinal for $1, $2, ... and by Arrow field name for $name.
// A typed null supplies schema information without consuming the input stream.
fn bind_parameters(
    df: DataFrame,
    schema: &Schema,
    row: Option<(&RecordBatch, usize)>,
) -> Result<DataFrame> {
    let mut values = std::collections::HashMap::new();
    for (id, expected) in df
        .logical_plan()
        .get_parameter_fields()
        .map_err(from_datafusion_error)?
    {
        let name = &id[1..];
        let index = if let Ok(position) = name.parse::<usize>() {
            position
                .checked_sub(1)
                .filter(|i| *i < schema.fields().len())
        } else {
            schema.index_of(name).ok()
        }
        .ok_or_else(|| {
            Error::with_message_and_status(
                format!("No bound value for parameter {id}"),
                Status::InvalidArguments,
            )
        })?;
        let field = schema.field(index);
        let mut value = match row {
            Some((batch, row)) => ScalarValue::try_from_array(batch.column(index), row),
            None => ScalarValue::try_from(field.data_type()),
        }
        .map_err(from_datafusion_error)?;
        if let Some(expected) = expected
            && expected.data_type() != &DataType::Null
        {
            value = value
                .cast_to_with_options(
                    expected.data_type(),
                    &CastOptions {
                        safe: false,
                        ..Default::default()
                    },
                )
                .map_err(|err| {
                    Error::with_message_and_status(err.to_string(), Status::InvalidArguments)
                })?;
        }
        values.insert(
            name.to_string(),
            ScalarAndMetadata::new(value, Some(field.metadata().clone().into())),
        );
    }
    df.with_param_values(ParamValues::Map(values))
        .map_err(from_datafusion_error)
}

// Consume parameter rows and their result streams lazily, retaining only one
// input batch and one query result reader at a time.
struct ParameterReader {
    runtime: Arc<RuntimeHandle>,
    df: DataFrame,
    bound: Box<dyn RecordBatchReader + Send>,
    schema: SchemaRef,
    batch: Option<RecordBatch>,
    row: usize,
    results: Option<StreamingRecordBatchReader>,
    finished: bool,
}

impl ParameterReader {
    fn advance(&mut self) -> std::result::Result<Option<RecordBatch>, ArrowError> {
        loop {
            if let Some(results) = &mut self.results {
                if let Some(batch) = results.next() {
                    let batch = batch?;
                    // Literal substitution can change field names and nullability.
                    return RecordBatch::try_new_with_options(
                        self.schema.clone(),
                        batch.columns().to_vec(),
                        &arrow_array::RecordBatchOptions::new()
                            .with_row_count(Some(batch.num_rows())),
                    )
                    .map(Some);
                }
                self.results = None;
            }
            if self
                .batch
                .as_ref()
                .is_none_or(|batch| self.row == batch.num_rows())
            {
                match self.bound.next() {
                    Some(batch) => {
                        self.batch = Some(batch?);
                        self.row = 0;
                    }
                    None => return Ok(None),
                }
                continue;
            }
            let batch = self.batch.as_ref().unwrap();
            let df = bind_parameters(self.df.clone(), &batch.schema(), Some((batch, self.row)))
                .map_err(|err| ArrowError::ExternalError(Box::new(err)))?;
            self.row += 1;
            let stream = self.runtime.block_on(df.execute_stream())?;
            self.results = Some(
                StreamingRecordBatchReader::new(stream, self.runtime.clone())
                    .with_skip_empty_batches(true),
            );
        }
    }
}

impl Iterator for ParameterReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        match self.advance() {
            Ok(Some(batch)) => Some(Ok(batch)),
            result => {
                self.finished = true;
                result.err().map(Err)
            }
        }
    }
}

impl RecordBatchReader for ParameterReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
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
    use arrow_array::{Float64Array, RecordBatch};
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

        assert_eq!(
            statement.get_parameter_schema().unwrap_err().status,
            Status::InvalidState
        );

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

    fn parameter_statement(sql: &str) -> super::SedonaStatement {
        let mut statement = SedonaDriver::default()
            .new_database()
            .unwrap()
            .new_connection()
            .unwrap()
            .new_statement()
            .unwrap();
        statement.set_sql_query(sql).unwrap();
        statement.prepare().unwrap();
        statement
    }

    fn parameter_batch(name: &str, values: Vec<Option<i64>>) -> RecordBatch {
        RecordBatch::try_from_iter(vec![(
            name,
            std::sync::Arc::new(arrow_array::Int64Array::from(values)) as arrow_array::ArrayRef,
        )])
        .unwrap()
    }

    #[test]
    fn bind_positional_and_named_parameters() {
        for (sql, name) in [
            ("SELECT $1 + $1 AS value", "ignored"),
            ("SELECT $value + $value AS value", "value"),
        ] {
            let mut statement = parameter_statement(sql);
            statement
                .bind(parameter_batch(name, vec![Some(21), None, Some(5)]))
                .unwrap();
            let schema = statement.execute_schema().unwrap();
            assert_eq!(schema.field(0).data_type(), &arrow_schema::DataType::Int64);
            let reader = statement.execute().unwrap();
            assert_eq!(reader.schema().as_ref(), &schema);
            let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
            assert!(
                batches
                    .iter()
                    .all(|batch| batch.schema().as_ref() == &schema)
            );
            assert_batches_eq!(
                [
                    "+-------+",
                    "| value |",
                    "+-------+",
                    "| 42    |",
                    "|       |",
                    "| 10    |",
                    "+-------+"
                ],
                &batches
            );
            statement
                .bind(parameter_batch(name, vec![Some(4)]))
                .unwrap();
            let batches = statement
                .execute()
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_batches_eq!(
                [
                    "+-------+",
                    "| value |",
                    "+-------+",
                    "| 8     |",
                    "+-------+"
                ],
                &batches
            );
        }
    }

    #[test]
    fn bind_multiple_parameters_to_spatial_query() {
        for sql in [
            "SELECT ST_AsText(ST_Point($1, $2)) AS geom",
            "SELECT ST_AsText(ST_Point($x, $y)) AS geom",
        ] {
            let mut statement = parameter_statement(sql);
            let batch = RecordBatch::try_from_iter(vec![
                (
                    "x",
                    std::sync::Arc::new(Float64Array::from(vec![1., 3.])) as arrow_array::ArrayRef,
                ),
                (
                    "y",
                    std::sync::Arc::new(Float64Array::from(vec![2., 4.])) as arrow_array::ArrayRef,
                ),
            ])
            .unwrap();
            statement.bind(batch).unwrap();
            let batches = statement
                .execute()
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_batches_eq!(
                [
                    "+------------+",
                    "| geom       |",
                    "+------------+",
                    "| POINT(1 2) |",
                    "| POINT(3 4) |",
                    "+------------+",
                ],
                &batches
            );
        }
    }

    #[test]
    fn bind_stream_multiple_and_empty_batches() {
        let mut statement = parameter_statement("SELECT $1 AS value");
        let batch = parameter_batch("", vec![Some(7)]);
        let schema = batch.schema();
        let empty = RecordBatch::new_empty(schema.clone());
        statement
            .bind_stream(Box::new(arrow_array::RecordBatchIterator::new(
                vec![Ok(empty.clone()), Ok(batch.clone()), Ok(empty), Ok(batch)],
                schema.clone(),
            )))
            .unwrap();
        let batches = statement
            .execute()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_batches_eq!(
            [
                "+-------+",
                "| value |",
                "+-------+",
                "| 7     |",
                "| 7     |",
                "+-------+"
            ],
            &batches
        );
        statement
            .bind_stream(Box::new(arrow_array::RecordBatchIterator::new(
                Vec::<Result<RecordBatch, arrow_schema::ArrowError>>::new(),
                schema,
            )))
            .unwrap();
        let mut reader = statement.execute().unwrap();
        assert_eq!(
            reader.schema().field(0).data_type(),
            &arrow_schema::DataType::Int64
        );
        assert!(reader.next().is_none());
    }

    #[test]
    fn bind_errors() {
        let mut statement = parameter_statement("SELECT $2 AS value");
        statement.bind(parameter_batch("", vec![Some(1)])).unwrap();
        assert_eq!(
            statement.execute().err().unwrap().status,
            Status::InvalidArguments
        );
        statement.set_sql_query("SELECT $1 AS value").unwrap();
        // A failed execution does not consume the binding.
        assert_eq!(
            statement
                .execute()
                .unwrap()
                .next()
                .unwrap()
                .unwrap()
                .num_rows(),
            1
        );
        let schema = parameter_batch("", vec![Some(1)]).schema();
        statement
            .bind_stream(Box::new(arrow_array::RecordBatchIterator::new(
                vec![Err(arrow_schema::ArrowError::ComputeError(
                    "input failed".into(),
                ))],
                schema,
            )))
            .unwrap();
        let mut reader = statement.execute().unwrap();
        assert!(
            reader
                .next()
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("input failed")
        );
        assert!(reader.next().is_none());

        statement.set_sql_query("SELECT $1 + 1 AS value").unwrap();
        let batch = RecordBatch::try_from_iter(vec![(
            "",
            std::sync::Arc::new(arrow_array::StringArray::from(vec!["not an integer"]))
                as arrow_array::ArrayRef,
        )])
        .unwrap();
        statement.bind(batch).unwrap();
        let mut reader = statement.execute().unwrap();
        assert!(reader.next().unwrap().is_err());
        assert!(reader.next().is_none());
    }

    #[test]
    fn parameter_schema_does_not_execute_ddl() {
        let mut statement =
            parameter_statement("CREATE TABLE parameter_schema_test (value BIGINT)");
        assert!(
            statement
                .get_parameter_schema()
                .unwrap()
                .fields()
                .is_empty()
        );
        statement
            .set_sql_query("SELECT * FROM parameter_schema_test")
            .unwrap();
        assert!(statement.execute().is_err());
    }

    #[test]
    fn parameter_schema_and_inferred_cast() {
        let statement = parameter_statement("SELECT $10, $2, $1 + 1, $1 + 2");
        let schema = statement.get_parameter_schema().unwrap();
        assert_eq!(
            schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            vec!["$1", "$2", "$10"]
        );
        assert_eq!(schema.field(0).data_type(), &arrow_schema::DataType::Int64);
        assert_eq!(schema.field(1).data_type(), &arrow_schema::DataType::Null);
        assert!(
            parameter_statement("SELECT 1")
                .get_parameter_schema()
                .unwrap()
                .fields()
                .is_empty()
        );

        let mut statement = parameter_statement("SELECT $1 + 1 AS value");
        statement
            .bind(
                RecordBatch::try_from_iter(vec![(
                    "",
                    std::sync::Arc::new(arrow_array::Int32Array::from(vec![Some(41), None]))
                        as arrow_array::ArrayRef,
                )])
                .unwrap(),
            )
            .unwrap();
        let batches = statement
            .execute()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_batches_eq!(
            [
                "+-------+",
                "| value |",
                "+-------+",
                "| 42    |",
                "|       |",
                "+-------+"
            ],
            &batches
        );
    }

    #[test]
    fn transforms_crs84_to_crs27() {
        let mut statement = SedonaDriver::default()
            .new_database()
            .unwrap()
            .new_connection()
            .unwrap()
            .new_statement()
            .unwrap();

        statement
            .set_sql_query(
                "SELECT ST_X(transformed) AS longitude, ST_Y(transformed) AS latitude \
                 FROM (SELECT ST_Transform(ST_Point(-100, 40), \
                     'OGC:CRS84', 'OGC:CRS27') AS transformed)",
            )
            .unwrap();

        let batches: Vec<RecordBatch> = statement
            .execute()
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);

        let longitude = batches[0].column(0);
        let longitude = longitude.as_any().downcast_ref::<Float64Array>().unwrap();
        let latitude = batches[0].column(1);
        let latitude = latitude.as_any().downcast_ref::<Float64Array>().unwrap();

        assert!((longitude.value(0) - -99.999_584_426_821).abs() < 1e-7);
        assert!((latitude.value(0) - 40.000_003_116_098).abs() < 1e-7);
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
