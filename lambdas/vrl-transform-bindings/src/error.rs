use serde::{de, ser};
use std::{convert::From, fmt::Display};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("String too long for nodejs len: {0}")]
    StringTooLong(usize),
    #[error("Unable to coerce value to type: {0}")]
    UnableToCoerce(&'static str),
    #[error("Empty string")]
    EmptyString,
    #[error("String too long to be a char expected len: 1 got len: {0}")]
    StringTooLongForChar(usize),
    #[error("Expecting null")]
    ExpectingNull,
    #[error("Invalid key type: {0}")]
    InvalidKeyType(String),
    #[error("Array index out of bounds: index: {0}, size: {1}")]
    ArrayIndexOutOfBounds(u32, u32),
    #[error("Not implemented")]
    NotImplemented(&'static str),
    #[error("JS exception")]
    Js(neon::result::Throw),
    #[error("Cast error")]
    Cast,
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Deserialization error: {0}")]
    Deserialization(String),
}

impl ser::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::Serialization(msg.to_string())
    }
}

impl de::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::Deserialization(msg.to_string())
    }
}

impl From<neon::result::Throw> for Error {
    fn from(throw: neon::result::Throw) -> Self {
        Error::Js(throw)
    }
}