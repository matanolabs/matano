use config::Config;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// todo: handle key as urlencoded_string, plus AWS encodes spaces as `+` rather than `%20`
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DataBatcherOutputRecord {
    pub bucket: String,
    pub key: String,
    pub size: i64,
    pub sequencer: String,
    #[serde(skip)]
    pub log_source: String,
    #[serde(default)]
    pub retry_depth: Option<i64>,
}

impl DataBatcherOutputRecord {
    pub fn increment_retry_depth(&mut self) {
        self.retry_depth = match self.retry_depth {
            Some(depth) => Some(depth + 1),
            None => Some(1),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LogSourceConfiguration {
    pub base: Config,
    pub tables: HashMap<String, Config>,
}
