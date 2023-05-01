use std::{collections::HashMap, sync::atomic::AtomicBool};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use aws_sdk_s3::types::ByteStream;
use chrono::{DateTime, FixedOffset};
use enum_dispatch::enum_dispatch;
use log::{debug, error, info};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::Mutex;

use shared::secrets::load_secret;

mod abusech;
mod amazon_inspector;
mod duo;
mod google_workspace;
mod msft;
mod o365;
mod okta;
mod onepassword;
mod otx;
mod snyk;

#[derive(Clone)]
pub struct PullerCache {
    cache: HashMap<String, (String, i64)>,
}
impl PullerCache {
    pub fn new() -> PullerCache {
        PullerCache {
            cache: HashMap::new(),
        }
    }

    pub fn get(&self, k: &str) -> Option<&String> {
        self.cache
            .get(k)
            .and_then(|(v, expiry)| (chrono::Utc::now().timestamp() < *expiry).then_some(v))
    }

    pub fn set(&mut self, k: &str, v: String, duration: Option<chrono::Duration>) {
        let duration = duration.unwrap_or(chrono::Duration::minutes(10));
        let expiry = (chrono::Utc::now() + duration).timestamp();
        self.cache.insert(k.to_string(), (v, expiry));
    }
}

pub struct PullLogsContext {
    pub log_source_name: String,
    secret_cache: Arc<Mutex<Option<HashMap<String, String>>>>,
    secret_arn: Option<String>,
    pub log_source_type: LogSource,
    config: HashMap<String, String>,
    tables_config: HashMap<String, config::Config>,
    cache: Arc<Mutex<PullerCache>>,
    s3: aws_sdk_s3::Client,
    pub checkpoint_json: Arc<Mutex<Option<Value>>>,
}

impl PullLogsContext {
    pub fn new(
        log_source_name: String,
        secret_arn: Option<String>,
        log_source_type: LogSource,
        config: HashMap<String, String>,
        tables_config: HashMap<String, config::Config>,
        s3: aws_sdk_s3::Client,
    ) -> PullLogsContext {
        PullLogsContext {
            log_source_name,
            secret_cache: Arc::new(Mutex::new(None)),
            secret_arn,
            log_source_type,
            config,
            tables_config,
            cache: Arc::new(Mutex::new(PullerCache::new())),
            s3,
            checkpoint_json: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn get_secret_field(&self, key: &str) -> Result<Option<String>> {
        if self.secret_arn.is_none() {
            return Ok(None);
        }
        let secret_arn = self.secret_arn.as_ref().unwrap();

        let secret_cache_ref = self.secret_cache.clone();
        let mut secret_cache_opt = secret_cache_ref.lock().await;
        let secrets_val = if secret_cache_opt.as_ref().is_none() {
            let secrets = load_secret(secret_arn.clone()).await?;
            let sec_val = secrets.get(key).cloned();
            if let Some(v) = sec_val.as_ref() {
                if !v.contains("placeholder") {
                    *secret_cache_opt = Some(secrets);
                }
            }
            sec_val
        } else {
            secret_cache_opt
                .as_ref()
                .and_then(|v| v.get(key).map(|v| v.clone()))
        };

        Ok(secrets_val)
    }

    /// Returns true if a checkpoint was loaded, false if this is the initial run. Useful for e.g. pulling more logs on first run.
    pub async fn load_checkpoint(&self) -> Result<bool> {
        let bucket = std::env::var("INGESTION_BUCKET_NAME").context("need bucket!")?;

        let initial_run_key = "__puller_last_run_checkpoint__";
        let s3_key = format!("{}/{}.json", initial_run_key, self.log_source_name);

        let res = self
            .s3
            .get_object()
            .bucket(&bucket)
            .key(&s3_key)
            .send()
            .await;
        let checkpoint_json = match res {
            Ok(output) => Ok(Some(
                serde_json::from_slice(&output.body.collect().await?.into_bytes())
                    .context("failed to parse last checkpoint file as json")?,
            )),
            Err(e) => {
                let se = e.into_service_error();
                match se.kind {
                    aws_sdk_s3::error::GetObjectErrorKind::NoSuchKey(_) => Ok(None),
                    _ => Err(se),
                }
            }
        }?;

        if checkpoint_json.is_none() {
            info!(
                "No checkpoint found for {}, assuming initial run",
                self.log_source_name
            );
        }

        *self.checkpoint_json.lock().await = checkpoint_json.clone();

        Ok(checkpoint_json.is_some())
    }

    pub async fn upload_checkpoint(&self, checkpoint_json: &Value) -> Result<()> {
        let bucket = std::env::var("INGESTION_BUCKET_NAME").context("need bucket!")?;

        let initial_run_key = "__puller_last_run_checkpoint__";
        let s3_key = format!("{}/{}.json", initial_run_key, self.log_source_name);

        // write checkpoint to s3
        self.s3
            .put_object()
            .bucket(&bucket)
            .key(&s3_key)
            .body(ByteStream::from(serde_json::to_vec(checkpoint_json)?))
            .send()
            .await?;

        // sync local checkpoint state
        *self.checkpoint_json.lock().await = Some(checkpoint_json.clone());

        Ok(())
    }

    pub fn config(&self) -> &HashMap<String, String> {
        &self.config
    }

    pub fn tables_config(&self) -> &HashMap<String, config::Config> {
        &self.tables_config
    }

    pub fn cache(&self) -> Arc<Mutex<PullerCache>> {
        self.cache.clone()
    }
}

#[async_trait]
#[enum_dispatch]
pub trait PullLogs {
    async fn pull_logs(
        self,
        client: reqwest::Client,
        ctx: &PullLogsContext,
        start_dt: DateTime<FixedOffset>,
        end_dt: DateTime<FixedOffset>,
    ) -> Result<Vec<u8>>;
}

#[derive(Clone)]
#[enum_dispatch(PullLogs)]
pub enum LogSource {
    AmazonInspectorPuller(amazon_inspector::AmazonInspectorPuller),
    O365Puller(o365::O365Puller),
    MicrosoftGraphPuller(msft::MicrosoftGraphPuller),
    GoogleWorkspacePuller(google_workspace::GoogleWorkspacePuller),
    DuoPuller(duo::DuoPuller),
    OktaPuller(okta::OktaPuller),
    OnePasswordPuller(onepassword::OnePasswordPuller),
    Otx(otx::OtxPuller),
    Snyk(snyk::SnykPuller),
    AbuseChUrlhausPuller(abusech::AbuseChUrlhausPuller),
    AbuseChMalwareBazaarPuller(abusech::AbuseChMalwareBazaarPuller),
    AbuseChThreatfoxPuller(abusech::AbuseChThreatfoxPuller),
}

impl LogSource {
    pub fn from_str(s: &str) -> Option<LogSource> {
        match s.to_lowercase().as_str() {
            "aws_inspector" => Some(LogSource::AmazonInspectorPuller(
                amazon_inspector::AmazonInspectorPuller {},
            )),
            "o365" => Some(LogSource::O365Puller(o365::O365Puller {})),
            "msft" => Some(LogSource::MicrosoftGraphPuller(
                msft::MicrosoftGraphPuller {},
            )),
            "duo" => Some(LogSource::DuoPuller(duo::DuoPuller {})),
            "okta" => Some(LogSource::OktaPuller(okta::OktaPuller {})),
            "onepassword" => Some(LogSource::OnePasswordPuller(
                onepassword::OnePasswordPuller {},
            )),
            "google_workspace" => Some(LogSource::GoogleWorkspacePuller(
                google_workspace::GoogleWorkspacePuller {},
            )),
            "otx" => Some(LogSource::Otx(otx::OtxPuller {})),
            "snyk" => Some(LogSource::Snyk(snyk::SnykPuller {})),
            "abusech_urlhaus" => Some(LogSource::AbuseChUrlhausPuller(
                abusech::AbuseChUrlhausPuller {},
            )),
            "abusech_malwarebazaar" => Some(LogSource::AbuseChMalwareBazaarPuller(
                abusech::AbuseChMalwareBazaarPuller {},
            )),
            "abusech_threatfox" => Some(LogSource::AbuseChThreatfoxPuller(
                abusech::AbuseChThreatfoxPuller {},
            )),
            _ => None,
        }
    }
    pub fn to_str(&self) -> &str {
        match self {
            LogSource::AmazonInspectorPuller(_) => "aws_inspector",
            LogSource::DuoPuller(_) => "duo",
            LogSource::OktaPuller(_) => "okta",
            LogSource::O365Puller(_) => "o365",
            LogSource::OnePasswordPuller(_) => "onepassword",
            LogSource::MicrosoftGraphPuller(_) => "msft",
            LogSource::GoogleWorkspacePuller(_) => "google_workspace",
            LogSource::Otx(_) => "otx",
            LogSource::Snyk(_) => "snyk",
            LogSource::AbuseChUrlhausPuller(_) => "abusech_urlhaus",
            LogSource::AbuseChMalwareBazaarPuller(_) => "abusech_malwarebazaar",
            LogSource::AbuseChThreatfoxPuller(_) => "abusech_threatfox",
        }
    }
}
