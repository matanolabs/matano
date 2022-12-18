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
const THREATFOX_FULL_URL: &str = "https://threatfox.abuse.ch/export/json/full/";

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

        let mut json_writer = std::io::Cursor::new(vec![]);

        let mut csv_reader = csv::ReaderBuilder::new()
            .comment(Some(b'#'))
            .from_reader(csvfile);

        csv_reader.set_headers(csv::StringRecord::from(URLHAUS_HEADERS.to_vec()));
        for result in csv_reader.deserialize() {
            let record: HashMap<String, String> = result?;
            let bytes = serde_json::to_vec(&record)?;
            json_writer.write(bytes.as_slice())?;
            json_writer.write(b"\n")?;
        }
        let json_bytes = json_writer.into_inner();

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

#[derive(Clone, Serialize, Deserialize)]
pub struct ThreatfoxRow {
    pub id: Option<String>,
    pub ioc_value: Option<String>,
    pub ioc_type: Option<String>,
    pub threat_type: Option<String>,
    pub malware: Option<String>,
    pub malware_alias: Option<String>,
    pub malware_printable: Option<String>,
    pub first_seen_utc: Option<String>,
    pub last_seen_utc: Option<String>,
    pub confidence_level: Option<i64>,
    pub reference: Option<String>,
    pub tags: Option<String>,
    pub anonymous: Option<String>,
    pub reporter: Option<String>,
}
type ThreatFoxDoc = HashMap<String, [ThreatfoxRow; 1]>;

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

        let resp = client.get(THREATFOX_FULL_URL).send().await?.bytes().await?;
        let mut zipfile = zip::ZipArchive::new(std::io::Cursor::new(resp))?;
        let mut jsonfile = zipfile.by_name("full.json")?;

        let mut buf = vec![];
        jsonfile.read_to_end(&mut buf)?;
        let json_obj: ThreatFoxDoc = serde_json::from_slice(buf.as_slice())?;

        let mut json_writer = std::io::Cursor::new(vec![]);
        for (id, record) in json_obj {
            // for loop but irl seem to be all one element arrays.
            for mut row in record {
                row.id = Some(id.to_owned());
                json_writer.write(serde_json::to_vec(&row)?.as_slice())?;
                json_writer.write(b"\n")?;
            }
        }
        let json_bytes = json_writer.into_inner();

        Ok(json_bytes)
    }
}
