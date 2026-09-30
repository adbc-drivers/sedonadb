// Copyright (c) 2026 ADBC Drivers Contributors
// Licensed under the Apache License, Version 2.0.

use adbc_core::{
    Connection,
    options::{InfoCode, ObjectDepth},
};
use arrow_array::RecordBatchReader;
use sedona::context::SedonaContext;
use sedona_extension::runtime::RuntimeHandle;
use std::sync::Arc;

use adbc_core::{
    Optionable,
    error::{Error, Result, Status},
    options::{OptionConnection, OptionValue},
};

use crate::{
    err_not_implemented, err_unrecognized_option, statement::SedonaStatement, utils::OptionValueExt,
};

pub struct SedonaConnection {
    runtime: Arc<RuntimeHandle>,
    ctx: Arc<SedonaContext>,
    autocommit_on: bool,
}

impl SedonaConnection {
    pub(crate) fn new(
        runtime: Arc<RuntimeHandle>,
        ctx: Arc<SedonaContext>,
        opts: impl IntoIterator<Item = (OptionConnection, OptionValue)>,
    ) -> Result<Self> {
        let mut connection = Self {
            runtime,
            ctx,
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
        let state = self.ctx.ctx.state_ref();
        let mut state = state.write();

        if state.catalog_list().catalog(&catalog_name).is_none() {
            return Err(Error::with_message_and_status(
                format!("Catalog {catalog_name:?} does not exist"),
                Status::NotFound,
            ));
        }

        state.config_mut().options_mut().catalog.default_catalog = catalog_name;
        Ok(())
    }

    fn set_current_schema(&self, schema_name: String) -> Result<()> {
        let state = self.ctx.ctx.state_ref();
        let mut state = state.write();
        let catalog_name = state.config_options().catalog.default_catalog.clone();
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

        state.config_mut().options_mut().catalog.default_schema = schema_name;
        Ok(())
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
        _codes: Option<std::collections::HashSet<InfoCode>>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        err_not_implemented!()
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

#[cfg(test)]
mod test {

    use adbc_core::{Database, Driver};
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
}
