use anyhow::Result;
use log::error;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct SQSLambdaError {
    pub msg: String,
    pub id: String,
}

impl SQSLambdaError {
    pub fn new(msg: String, id: String) -> Self {
        SQSLambdaError { msg, id }
    }
}

impl std::error::Error for SQSLambdaError {}

impl std::fmt::Display for SQSLambdaError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Failure for SQS message ID:{} - {} ", self.id, self.msg)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SQSBatchResponseItemFailure {
    itemIdentifier: String,
}
impl SQSBatchResponseItemFailure {
    pub fn new(id: String) -> SQSBatchResponseItemFailure {
        SQSBatchResponseItemFailure { itemIdentifier: id }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SQSBatchResponse {
    pub batchItemFailures: Vec<SQSBatchResponseItemFailure>,
}

impl SQSBatchResponse {
    pub fn new(ids: Vec<String>) -> SQSBatchResponse {
        SQSBatchResponse {
            batchItemFailures: ids
                .into_iter()
                .map(SQSBatchResponseItemFailure::new)
                .collect(),
        }
    }
}

/// Converts a list of SQSLambdaError's into a SQS batch response for Lambda
pub fn sqs_errors_to_response(errors: Vec<SQSLambdaError>) -> Result<Option<SQSBatchResponse>> {
    if errors.is_empty() {
        Ok(None)
    } else {
        error!(
            "Encountered {} errors processing messages, returning to SQS",
            errors.len()
        );
        let ids = errors
            .into_iter()
            .map(|e| {
                error!("Encountered error: {}", e.msg);
                e.id
            })
            .collect::<Vec<_>>();
        Ok(Some(SQSBatchResponse::new(ids)))
    }
}
