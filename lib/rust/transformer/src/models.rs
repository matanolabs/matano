//! Project-specific model definitions
//!
use serde::{Deserialize, Serialize};

//todo: handle key as urlencoded_string, plus AWS encodes spaces as `+` rather than `%20`
#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct DataBatcherRequestItem {
    pub bucket: String,
    pub key: String,
    pub size: i32,
    pub sequencer: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LogSourceConfiguration {
    pub name: String,
    pub transform: Option<String>,
    pub ingest: Option<IngestConfig>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IngestConfig {
    pub expand_records_from_payload: Option<String>,
    pub s3_source: Option<S3SourceConfig>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct S3SourceConfig {
}

#[derive(Debug, Serialize)]
pub(crate) struct FailureResponse {
    pub body: String,
}

// Implement Display for the Failure response so that we can then implement Error.
impl std::fmt::Display for FailureResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.body)
    }
}

// Implement Error for the FailureResponse so that we can `?` (try) the Response
// returned by `lambda_runtime::run(func).await` in `fn main`.
impl std::error::Error for FailureResponse {}
