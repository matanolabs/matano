
use thiserror::Error;

/// Main error type of the library.
#[derive(Error, Debug)]
pub enum MatanoError {
    #[error("Failed to decode with base64")]
    Base64Decode(#[from] base64::DecodeError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

/// Error validating TMC params values.
#[derive(Debug, Error)]
pub enum ParamError {
    // #[error("Parameter key/value was empty")]
    // Empty,
    // #[error("Invalid character found in key/value: {0}")]
    // InvalidChar(char),
}

#[derive(Debug, Error)]
enum NodeError {
    #[error(transparent)]
    Langs(#[from] MatanoError),
    // #[error("Invalid token")]
    // InvalidTokenError(#[source] MatanoError),
}
