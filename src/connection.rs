// Copyright (c) 2026 ADBC Drivers Contributors
// Licensed under the Apache License, Version 2.0.

use adbc_core::{
    Connection,
    options::{InfoCode, ObjectDepth},
};
use adbc_core_0_23::options::InfoCode as DriverbaseInfoCode;
use arrow_array::RecordBatchReader;
use datafusion::{
    catalog::{CatalogProvider, CatalogProviderList, MemoryCatalogProviderList},
    execution::runtime_env::RuntimeEnv,
};
use sedona::context::SedonaContext;
use sedona_extension::runtime::RuntimeHandle;
use std::sync::Arc;

use adbc_core::{
    Optionable,
    error::{Error, Result, Status},
    options::{OptionConnection, OptionValue},
};

use crate::{
    err_not_implemented, err_unrecognized_option, statement::SedonaStatement,
    utils::OptionValueExt, utils::from_datafusion_error,
};

/// Exposes database-wide catalogs through Sedona's connection-local catalog
/// wrapper so object-store discovery still uses the calling session's state.
#[derive(Debug)]
struct ConnectionCatalogProviderList {
    shared: Arc<MemoryCatalogProviderList>,
    session: Arc<dyn CatalogProviderList>,
}

impl CatalogProviderList for ConnectionCatalogProviderList {
    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        self.shared.register_catalog(name, catalog)
    }

    fn catalog_names(&self) -> Vec<String> {
        self.shared.catalog_names()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        let catalog = self.shared.catalog(name)?;
        // Install the shared provider into this session's original Sedona
        // wrapper, then return its session-aware view of that provider.
        self.session.register_catalog(name.to_string(), catalog);
        self.session.catalog(name)
    }
}

pub struct SedonaConnection {
    runtime: Arc<RuntimeHandle>,
    ctx: Arc<SedonaContext>,
    autocommit_on: bool,
}

impl SedonaConnection {
    pub(crate) fn try_new(
        runtime: Arc<RuntimeHandle>,
        runtime_env: Arc<RuntimeEnv>,
        catalogs: Arc<MemoryCatalogProviderList>,
        opts: impl IntoIterator<Item = (OptionConnection, OptionValue)>,
    ) -> Result<Self> {
        let ctx = runtime
            .block_on(SedonaContext::new_local_interactive_with_runtime_env(
                runtime_env,
            ))
            .map_err(from_datafusion_error)?;
        let session_catalogs = ctx.ctx.state().catalog_list().clone();
        ctx.ctx
            .register_catalog_list(Arc::new(ConnectionCatalogProviderList {
                shared: catalogs,
                session: session_catalogs,
            }));

        let mut connection = Self {
            runtime,
            ctx: Arc::new(ctx),
            autocommit_on: true,
        };

        let mut current_catalog = None;
        let mut current_schema = None;

        for (key, value) in opts {
            match &key {
                OptionConnection::CurrentCatalog => match value {
                    OptionValue::String(value) => current_catalog = Some(value),
                    _ => return Err(invalid_string_option(&key)),
                },
                OptionConnection::CurrentSchema => match value {
                    OptionValue::String(value) => current_schema = Some(value),
                    _ => return Err(invalid_string_option(&key)),
                },
                _ => connection.set_option(key, value)?,
            }
        }

        if current_catalog.is_some() || current_schema.is_some() {
            connection.set_current_catalog_and_schema(current_catalog, current_schema)?;
        }

        Ok(connection)
    }

    fn set_current_catalog_and_schema(
        &self,
        catalog_name: Option<String>,
        schema_name: Option<String>,
    ) -> Result<()> {
        let state = self.ctx.ctx.state_ref();
        let mut state = state.write();
        let catalog_name =
            catalog_name.unwrap_or_else(|| state.config_options().catalog.default_catalog.clone());
        let schema_name =
            schema_name.unwrap_or_else(|| state.config_options().catalog.default_schema.clone());
        let catalog = state.catalog_list().catalog(&catalog_name).ok_or_else(|| {
            Error::with_message_and_status(
                format!("Catalog {catalog_name:?} does not exist"),
                Status::NotFound,
            )
        })?;

        if catalog.schema(&schema_name).is_none() {
            return Err(Error::with_message_and_status(
                format!("Schema {schema_name:?} does not exist in catalog {catalog_name:?}"),
                Status::NotFound,
            ));
        }

        let options = &mut state.config_mut().options_mut().catalog;
        options.default_catalog = catalog_name;
        options.default_schema = schema_name;
        Ok(())
    }

    fn set_current_catalog(&self, catalog_name: String) -> Result<()> {
        self.set_current_catalog_and_schema(Some(catalog_name), None)
    }

    fn set_current_schema(&self, schema_name: String) -> Result<()> {
        self.set_current_catalog_and_schema(None, Some(schema_name))
    }
}

impl Optionable for SedonaConnection {
    type Option = OptionConnection;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        match &key {
            OptionConnection::AutoCommit => {
                self.autocommit_on = value.as_bool()?;
                Ok(())
            }
            OptionConnection::CurrentCatalog => match value {
                OptionValue::String(value) => self.set_current_catalog(value),
                _ => Err(invalid_string_option(&key)),
            },
            OptionConnection::CurrentSchema => match value {
                OptionValue::String(value) => self.set_current_schema(value),
                _ => Err(invalid_string_option(&key)),
            },
            _ => err_unrecognized_option!(key),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match &key {
            OptionConnection::AutoCommit => Ok(if self.autocommit_on {
                "true".to_string()
            } else {
                "false".to_string()
            }),
            OptionConnection::CurrentCatalog => Ok(self
                .ctx
                .ctx
                .state()
                .config_options()
                .catalog
                .default_catalog
                .clone()),
            OptionConnection::CurrentSchema => Ok(self
                .ctx
                .ctx
                .state()
                .config_options()
                .catalog
                .default_schema
                .clone()),
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

fn invalid_string_option(key: &OptionConnection) -> Error {
    Error::with_message_and_status(
        format!("Option {:?} must be a string", key.as_ref()),
        Status::InvalidArguments,
    )
}

impl Connection for SedonaConnection {
    type StatementType = SedonaStatement;

    fn new_statement(&mut self) -> Result<SedonaStatement> {
        Ok(SedonaStatement::new(self.runtime.clone(), self.ctx.clone()))
    }

    fn cancel(&mut self) -> Result<()> {
        err_not_implemented!()
    }

    fn get_info(
        &self,
        codes: Option<std::collections::HashSet<InfoCode>>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let codes = codes.map(|codes| {
            codes
                .into_iter()
                .filter_map(|code| DriverbaseInfoCode::try_from(u32::from(&code)).ok())
                .collect::<std::collections::HashSet<_>>()
        });
        let info = get_info_codes();
        Ok(Box::new(info.get_info(codes).build()))
    }

    fn get_objects(
        &self,
        _depth: ObjectDepth,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _table_type: Option<Vec<&str>>,
        _column_name: Option<&str>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        err_not_implemented!()
    }

    fn get_table_schema(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: &str,
    ) -> Result<arrow_schema::Schema> {
        err_not_implemented!()
    }

    fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        err_not_implemented!()
    }

    fn get_statistic_names(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        err_not_implemented!()
    }

    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _approximate: bool,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        err_not_implemented!()
    }

    fn commit(&mut self) -> Result<()> {
        err_not_implemented!()
    }

    fn rollback(&mut self) -> Result<()> {
        err_not_implemented!()
    }

    fn read_partition(
        &self,
        _partition: impl AsRef<[u8]>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        err_not_implemented!()
    }
}

static INFO_CODES: std::sync::OnceLock<driverbase::InfoRegistry> = std::sync::OnceLock::new();

fn get_info_codes() -> &'static driverbase::InfoRegistry {
    INFO_CODES.get_or_init(|| {
        let mut registry = driverbase::InfoRegistry::new();
        registry.add_string(
            DriverbaseInfoCode::DriverName,
            "ADBC Driver Foundry Driver for Apache SedonaDB",
        );
        registry.add_string(
            DriverbaseInfoCode::DriverVersion,
            concat!("v", env!("CARGO_PKG_VERSION")),
        );
        registry.add_string(DriverbaseInfoCode::VendorName, "Apache SedonaDB");
        registry.add_string(DriverbaseInfoCode::VendorVersion, "0.5.0");
        registry.add_string(
            DriverbaseInfoCode::VendorArrowVersion,
            format!("arrow-rs v{}", datafusion::arrow::ARROW_VERSION),
        );
        registry.add_string(
            DriverbaseInfoCode::DriverArrowVersion,
            format!("arrow-rs v{}", datafusion::arrow::ARROW_VERSION),
        );
        registry
    })
}

#[cfg(test)]
mod test {

    use std::collections::{HashMap, HashSet};

    use adbc_core::{Database, Driver, Statement};
    use arrow_array::{StringArray, UInt32Array, UnionArray};
    use datafusion::assert_batches_eq;
    use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider};

    use crate::driver::SedonaDriver;

    use super::*;

    #[test]
    fn autocommit() {
        let mut connection = SedonaDriver::default()
            .new_database()
            .unwrap()
            .new_connection()
            .unwrap();

        // Turn autocommit on
        connection
            .set_option(
                OptionConnection::AutoCommit,
                OptionValue::String("true".to_string()),
            )
            .unwrap();
        assert_eq!(
            connection
                .get_option_string(OptionConnection::AutoCommit)
                .unwrap(),
            "true"
        );

        // Turn autocommit off
        connection
            .set_option(
                OptionConnection::AutoCommit,
                OptionValue::String("false".to_string()),
            )
            .unwrap();
        assert_eq!(
            connection
                .get_option_string(OptionConnection::AutoCommit)
                .unwrap(),
            "false"
        );

        // Try to set autocommit with an in appropriate value
        let err = connection
            .set_option(OptionConnection::AutoCommit, OptionValue::Bytes(vec![]))
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "InvalidArguments: Expected boolean option (sqlstate: 00000, vendor_code: 0)"
        );
    }

    #[test]
    fn current_catalog_and_schema() {
        let mut connection = SedonaDriver::default()
            .new_database()
            .unwrap()
            .new_connection()
            .unwrap();

        assert_eq!(
            connection
                .get_option_string(OptionConnection::CurrentCatalog)
                .unwrap(),
            "datafusion"
        );
        assert_eq!(
            connection
                .get_option_string(OptionConnection::CurrentSchema)
                .unwrap(),
            "public"
        );

        let catalog = Arc::new(MemoryCatalogProvider::new());
        catalog
            .register_schema("public", Arc::new(MemorySchemaProvider::new()))
            .unwrap();
        catalog
            .register_schema("analytics", Arc::new(MemorySchemaProvider::new()))
            .unwrap();
        connection.ctx.ctx.register_catalog("secondary", catalog);

        connection
            .set_option(
                OptionConnection::CurrentCatalog,
                OptionValue::from("secondary"),
            )
            .unwrap();
        connection
            .set_option(
                OptionConnection::CurrentSchema,
                OptionValue::from("analytics"),
            )
            .unwrap();

        assert_eq!(
            connection
                .get_option_string(OptionConnection::CurrentCatalog)
                .unwrap(),
            "secondary"
        );
        assert_eq!(
            connection
                .get_option_string(OptionConnection::CurrentSchema)
                .unwrap(),
            "analytics"
        );

        let error = connection
            .set_option(
                OptionConnection::CurrentCatalog,
                OptionValue::from("missing"),
            )
            .unwrap_err();
        assert_eq!(error.status, Status::NotFound);

        let error = connection
            .set_option(
                OptionConnection::CurrentSchema,
                OptionValue::from("missing"),
            )
            .unwrap_err();
        assert_eq!(error.status, Status::NotFound);
        let catalog = Arc::new(MemoryCatalogProvider::new());
        catalog
            .register_schema("other", Arc::new(MemorySchemaProvider::new()))
            .unwrap();
        connection.ctx.ctx.register_catalog("incompatible", catalog);
        let error = connection
            .set_option(
                OptionConnection::CurrentCatalog,
                OptionValue::from("incompatible"),
            )
            .unwrap_err();
        assert_eq!(error.status, Status::NotFound);
        assert_eq!(
            connection
                .get_option_string(OptionConnection::CurrentCatalog)
                .unwrap(),
            "secondary"
        );
        assert_eq!(
            connection
                .get_option_string(OptionConnection::CurrentSchema)
                .unwrap(),
            "analytics"
        );

        let error = connection
            .set_option(OptionConnection::CurrentCatalog, OptionValue::from(1_i64))
            .unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
    }

    #[test]
    fn connection_options_apply_catalog_and_schema_atomically() {
        let database = SedonaDriver::default().new_database().unwrap();
        let connection = database.new_connection().unwrap();
        let catalog = Arc::new(MemoryCatalogProvider::new());
        catalog
            .register_schema("analytics", Arc::new(MemorySchemaProvider::new()))
            .unwrap();
        connection.ctx.ctx.register_catalog("secondary", catalog);

        let error = database
            .new_connection_with_opts([
                (
                    OptionConnection::CurrentCatalog,
                    OptionValue::from("secondary"),
                ),
                (
                    OptionConnection::CurrentSchema,
                    OptionValue::from("missing"),
                ),
            ])
            .err()
            .unwrap();
        assert_eq!(error.status, Status::NotFound);
        assert_eq!(
            connection
                .get_option_string(OptionConnection::CurrentCatalog)
                .unwrap(),
            "datafusion"
        );

        let connection = database
            .new_connection_with_opts([
                (
                    OptionConnection::CurrentSchema,
                    OptionValue::from("analytics"),
                ),
                (
                    OptionConnection::CurrentCatalog,
                    OptionValue::from("secondary"),
                ),
            ])
            .unwrap();
        assert_eq!(
            connection
                .get_option_string(OptionConnection::CurrentCatalog)
                .unwrap(),
            "secondary"
        );
        assert_eq!(
            connection
                .get_option_string(OptionConnection::CurrentSchema)
                .unwrap(),
            "analytics"
        );
    }

    #[test]
    fn connections_share_runtime_and_tables_but_not_session_state() {
        let database = SedonaDriver::default().new_database().unwrap();
        let mut first = database.new_connection().unwrap();
        let mut second = database.new_connection().unwrap();

        assert!(Arc::ptr_eq(
            &first.ctx.ctx.runtime_env(),
            &second.ctx.ctx.runtime_env()
        ));
        assert!(!Arc::ptr_eq(
            &first.ctx.ctx.state_ref(),
            &second.ctx.ctx.state_ref()
        ));

        let catalog = Arc::new(MemoryCatalogProvider::new());
        catalog
            .register_schema("public", Arc::new(MemorySchemaProvider::new()))
            .unwrap();
        catalog
            .register_schema("analytics", Arc::new(MemorySchemaProvider::new()))
            .unwrap();
        first.ctx.ctx.register_catalog("secondary", catalog);
        first
            .set_option(
                OptionConnection::CurrentCatalog,
                OptionValue::from("secondary"),
            )
            .unwrap();
        first
            .set_option(
                OptionConnection::CurrentSchema,
                OptionValue::from("analytics"),
            )
            .unwrap();

        assert_eq!(
            second
                .get_option_string(OptionConnection::CurrentCatalog)
                .unwrap(),
            "datafusion"
        );
        assert_eq!(
            second
                .get_option_string(OptionConnection::CurrentSchema)
                .unwrap(),
            "public"
        );

        let mut create = second.new_statement().unwrap();
        create
            .set_sql_query("CREATE TABLE shared_table AS SELECT 1 AS value")
            .unwrap();
        let _: Vec<_> = create
            .execute()
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap();

        let mut query = database.new_connection().unwrap().new_statement().unwrap();
        query
            .set_sql_query("SELECT value FROM shared_table")
            .unwrap();
        let batches = query
            .execute()
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_batches_eq!(
            [
                "+-------+",
                "| value |",
                "+-------+",
                "| 1     |",
                "+-------+",
            ],
            &batches
        );
    }

    fn get_info_values(codes: Option<HashSet<InfoCode>>) -> HashMap<u32, String> {
        let connection = SedonaDriver::default()
            .new_database()
            .unwrap()
            .new_connection()
            .unwrap();
        let batches = connection
            .get_info(codes)
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(batches.len(), 1);

        let batch = &batches[0];
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<UnionArray>()
            .unwrap();

        (0..batch.num_rows())
            .map(|index| {
                assert_eq!(values.type_id(index), 0);
                let value = values.value(index);
                let value = value.as_any().downcast_ref::<StringArray>().unwrap();
                (names.value(index), value.value(0).to_string())
            })
            .collect()
    }

    #[test]
    fn get_info() {
        let values = get_info_values(None);
        assert_eq!(values.len(), 6);
        assert_eq!(
            values[&u32::from(&InfoCode::DriverName)],
            "ADBC Driver Foundry Driver for Apache SedonaDB"
        );
        assert_eq!(
            values[&u32::from(&InfoCode::DriverVersion)],
            concat!("v", env!("CARGO_PKG_VERSION"))
        );
        assert_eq!(values[&u32::from(&InfoCode::VendorName)], "Apache SedonaDB");
        assert_eq!(values[&u32::from(&InfoCode::VendorVersion)], "0.5.0");
        assert_eq!(
            values[&u32::from(&InfoCode::VendorArrowVersion)],
            format!("arrow-rs v{}", datafusion::arrow::ARROW_VERSION)
        );
        assert_eq!(
            values[&u32::from(&InfoCode::DriverArrowVersion)],
            format!("arrow-rs v{}", datafusion::arrow::ARROW_VERSION)
        );

        let values = get_info_values(Some(HashSet::from([
            InfoCode::VendorName,
            InfoCode::Other(42),
        ])));
        assert_eq!(
            values,
            HashMap::from([(
                u32::from(&InfoCode::VendorName),
                "Apache SedonaDB".to_string()
            )])
        );
    }
}
