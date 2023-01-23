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
pub struct AbuseChUrlhausPuller;
#[derive(Clone)]
pub struct AbuseChMalwareBazaarPuller;
#[derive(Clone)]
pub struct AbuseChThreatfoxPuller;

const URLHAUS_URL: &str = "https://urlhaus.abuse.ch/downloads/csv";
const URLHAUS_HEADERS: [&str; 9] = [
    "id",
    "dateadded",
    "url",
    "url_status",
    "last_online",
    "threat",
    "tags",
    "urlhaus_link",
    "reporter",
];
const MALWAREBAZAAR_URL: &str = "https://mb-api.abuse.ch/api/v1/";
const THREATFOX_URL: &str = "https://threatfox-api.abuse.ch/api/v1/";

#[async_trait]
impl PullLogs for AbuseChUrlhausPuller {
    async fn pull_logs(
        self,
        client: reqwest::Client,
        ctx: &PullLogsContext,
        start_dt: DateTime<FixedOffset>,
        end_dt: DateTime<FixedOffset>,
    ) -> Result<Vec<u8>> {
        info!("Pulling URLhaus...");
        let resp = client.get(URLHAUS_URL).send().await?.bytes().await?;
        let mut zipfile = zip::ZipArchive::new(std::io::Cursor::new(resp))?;
        let csvfile = zipfile.by_name("csv.txt")?;

        let mut json_bytes = vec![];

        let mut csv_reader = csv::ReaderBuilder::new()
            .comment(Some(b'#'))
            .from_reader(csvfile);

        csv_reader.set_headers(csv::StringRecord::from(URLHAUS_HEADERS.to_vec()));
        for result in csv_reader.deserialize() {
            let record: HashMap<String, String> = result?;
            let bytes = serde_json::to_vec(&record)?;
            json_bytes.write(bytes.as_slice())?;
            json_bytes.write(b"\n")?;
        }

        return Ok(json_bytes);
    }
}

#[async_trait]
impl PullLogs for AbuseChMalwareBazaarPuller {
    async fn pull_logs(
        self,
        client: reqwest::Client,
        ctx: &PullLogsContext,
        start_dt: DateTime<FixedOffset>,
        end_dt: DateTime<FixedOffset>,
    ) -> Result<Vec<u8>> {
        info!("Pulling malware bazaar");
        let resp = client
            .post(MALWAREBAZAAR_URL)
            .form(&[("query", "get_recent"), ("selector", "time")])
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;

        let query_status = resp
            .get("query_status")
            .context("must exist")?
            .as_str()
            .context("must be str")?;
        if query_status == "no_results" {
            info!("Malware Bazaar: no results, returning.");
            return Ok(vec![]);
        } else if query_status != "ok" {
            return Err(anyhow!("Malware bazaar got query_status: {}", query_status));
        }

        let data = resp
            .get("data")
            .context("data must exist")?
            .as_array()
            .context("must be array")?;

        let mut json_writer = std::io::Cursor::new(vec![]);
        for record in data {
            json_writer.write(serde_json::to_vec(&record)?.as_slice())?;
            json_writer.write(b"\n")?;
        }
        let json_bytes = json_writer.into_inner();

        Ok(json_bytes)
    }
}

#[async_trait]
impl PullLogs for AbuseChThreatfoxPuller {
    async fn pull_logs(
        self,
        client: reqwest::Client,
        ctx: &PullLogsContext,
        start_dt: DateTime<FixedOffset>,
        end_dt: DateTime<FixedOffset>,
    ) -> Result<Vec<u8>> {
        info!("Pulling Threatfox...");
        let is_initial_run = ctx.is_initial_run();

        // This is a simple implementation. There's also a full export available
        // that we could technically retrieve on the initial run, but keep it simple for now.
        let days_to_retrieve = if is_initial_run {
            "7" // maximum
        } else {
            "1"
        };
        let body = format!(
            "{{\"query\":\"get_iocs\",\"days\":\"{}\"}}",
            days_to_retrieve
        );
        let resp = client
            .post(THREATFOX_URL)
            .body(body)
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;

        let data = resp
            .get("data")
            .context("data must exist")?
            .as_array()
            .context("data must be array")?;

        let mut json_bytes = vec![];
        for record in data {
            json_bytes.write(serde_json::to_vec(&record)?.as_slice())?;
            json_bytes.write(b"\n")?;
        }

        Ok(json_bytes)
    }
}
