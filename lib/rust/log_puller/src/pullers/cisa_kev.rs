use anyhow::{anyhow, Context as AnyhowContext, Error, Result};
use async_trait::async_trait;
use chrono::{DateTime, FixedOffset};
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    io::{Read, Write},
};

use super::{PullLogs, PullLogsContext};

#[derive(Clone)]
pub struct CisaKevPuller;


const CISA_KEV_URL: &str = "https://www.cisa.gov/sites/default/files/csv/known_exploited_vulnerabilities.csv";
const CISA_KEV_HEADERS: [&str; 9] = [
    "cveID",
    "vendorProject",
    "product",
    "vulnerabilityName",
    "dateAdded",
    "shortDescription",
    "requiredAction",
    "dueDate",
    "notes",
];

#[async_trait]
impl PullLogs for CisaKevPuller {
    async fn pull_logs(
        self,
        client: reqwest::Client,
        ctx: &PullLogsContext,
        start_dt: DateTime<FixedOffset>,
        end_dt: DateTime<FixedOffset>,
    ) -> Result<Vec<u8>> {
        info!("Pulling CISA KEV...");
        let resp = client.get(CISA_KEV_URL).send().await?.bytes().await?;

        let mut json_bytes = vec![];

        let mut csv_reader = csv::ReaderBuilder::new()
            .comment(Some(b'#'))
            .from_reader(resp);

        csv_reader.set_headers(csv::StringRecord::from(CISA_KEV_HEADERS.to_vec()));
        for result in csv_reader.deserialize() {
            let record: HashMap<String, String> = result?;
            let bytes = serde_json::to_vec(&record)?;
            json_bytes.write(bytes.as_slice())?;
            json_bytes.write(b"\n")?;
        }

        return Ok(json_bytes);
    }
}
