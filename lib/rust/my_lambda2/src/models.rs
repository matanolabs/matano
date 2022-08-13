//! Project-specific model definitions
//!
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// Commenting this out because we're using `anyhow` library here instead, for
// error handling purposes.
//
// #[derive(Debug, Serialize)]
// pub(crate) struct FailureResponse {
//     pub body: String,
// }
//
// // Implement Display for the Failure response so that we can then implement Error.
// impl std::fmt::Display for FailureResponse {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(f, "{}", self.body)
//     }
// }
//
// // Implement Error for the FailureResponse so that we can `?` (try) the Response
// // returned by `lambda_runtime::run(func).await` in `fn main`.
// impl std::error::Error for FailureResponse {}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Person {
    pub first_name: String,
    pub last_name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PersonResponse {
    pub data: String,
    pub method: String,
    pub headers: HashMap<String, String>,
}
