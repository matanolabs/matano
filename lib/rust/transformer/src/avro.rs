use std::collections::HashMap;

use crate::arrow::{coerce_data_type, type_to_schema};
use ::value::value::{timestamp_to_string, Value};
use anyhow::{anyhow, Result};

pub(crate) trait TryIntoAvro<T>: Sized {
    /// The type returned in the event of a conversion error.
    type Error;

    /// Performs the conversion from a generic VRL Value to an Avro Value.
    fn try_into(self) -> Result<T, Self::Error>;
}
impl TryIntoAvro<apache_avro::types::Value> for Value {
    type Error = apache_avro::Error;

    fn try_into(self) -> Result<apache_avro::types::Value, Self::Error> {
        match self {
            Self::Boolean(v) => Ok(apache_avro::types::Value::from(v)),
            Self::Integer(v) => Ok(apache_avro::types::Value::from(v)),
            Self::Float(v) => Ok(apache_avro::types::Value::from(v.into_inner())),
            Self::Bytes(v) => Ok(apache_avro::types::Value::from(
                String::from_utf8(v.to_vec()).map_err(Self::Error::ConvertToUtf8)?,
            )),
            Self::Regex(regex) => Ok(apache_avro::types::Value::from(regex.as_str().to_string())),
            Self::Object(v) => {
                let items: Result<HashMap<String, _>, _> = v
                    .into_iter()
                    .map(|(key, value)| {
                        TryIntoAvro::try_into(value).and_then(|value| Ok((key, value)))
                    })
                    .collect();
                Ok(apache_avro::types::Value::Map(items?))
            }
            Self::Array(v) => {
                let items: Result<Vec<_>, _> = v.into_iter().map(TryIntoAvro::try_into).collect();
                Ok(apache_avro::types::Value::Array(items?))
            }
            Self::Null => Ok(apache_avro::types::Value::Null),
            Self::Timestamp(v) => Ok(apache_avro::types::Value::from(v.timestamp_micros())),
        }
    }
}
