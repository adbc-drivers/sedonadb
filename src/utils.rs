// Copyright (c) 2026 ADBC Drivers Contributors
// Licensed under the Apache License, Version 2.0.

use adbc_core::{
    error::{Error, Result, Status},
    options::OptionValue,
};
use datafusion::error::DataFusionError;

pub(crate) fn from_datafusion_error(value: DataFusionError) -> Error {
    let status = match &value {
        DataFusionError::IoError(_) => Status::IO,
        DataFusionError::NotImplemented(_) => Status::NotImplemented,
        DataFusionError::Configuration(_) => Status::InvalidArguments,
        _ => Status::Internal,
    };

    Error::with_message_and_status(value.message(), status)
}

#[macro_export]
macro_rules! err_not_implemented {
    () => {
        Err(Error::with_message_and_status(
            "not implemented",
            Status::NotImplemented,
        ))
    };
    ($msg:expr) => {
        Err(Error::with_message_and_status($msg, Status::NotImplemented))
    };
}

#[macro_export]
macro_rules! err_unrecognized_option {
    ($key:expr) => {
        Err(Error::with_message_and_status(
            format!("Unrecognized option: {:?}", $key),
            Status::NotFound,
        ))
    };
}

pub(crate) trait OptionValueExt {
    fn as_bool(&self) -> Result<bool>;
}

impl OptionValueExt for OptionValue {
    fn as_bool(&self) -> Result<bool> {
        match self {
            OptionValue::Int(value) => Ok(*value != 0),
            OptionValue::String(value) => Ok(value == "true"),
            _ => Err(Error::with_message_and_status(
                "Expected boolean option".to_string(),
                Status::InvalidArguments,
            )),
        }
    }
}
