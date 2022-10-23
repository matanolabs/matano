use serde::{Deserialize, Serialize};

// todo: handle key as urlencoded_string, plus AWS encodes spaces as `+` rather than `%20`
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DataBatcherOutputRecord {
    pub bucket: String,
    pub key: String,
    pub size: i64,
    pub sequencer: String,
    #[serde(skip)]
    pub log_source: String,
}
