use std::{collections::HashMap, sync::atomic::AtomicBool};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use aws_sdk_s3::types::ByteStream;
use chrono::{DateTime, FixedOffset};
use enum_dispatch::enum_dispatch;
use log::{debug, error, info};

use std::sync::Arc;
use tokio::sync::Mutex;

use shared::secrets::load_secret;

mod abusech;
mod amazon_inspector;
mod duo;
mod o365;
mod okta;
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
    secret_cache: Arc<Mutex<Option<HashMap<String, String>>>>,
    secret_arn: String,
    pub log_source_type: LogSource,
    config: HashMap<String, String>,
    tables_config: HashMap<String, config::Config>,
    cache: Arc<Mutex<PullerCache>>,
    s3: aws_sdk_s3::Client,
    is_initial_run: AtomicBool,
}

impl PullLogsContext {
    pub fn new(
        secret_arn: String,
        log_source_type: LogSource,
        config: HashMap<String, String>,
        tables_config: HashMap<String, config::Config>,
        s3: aws_sdk_s3::Client,
    ) -> PullLogsContext {
        PullLogsContext {
            secret_cache: Arc::new(Mutex::new(None)),
            secret_arn,
            log_source_type,
            config,
            tables_config,
            cache: Arc::new(Mutex::new(PullerCache::new())),
            s3,
            is_initial_run: AtomicBool::new(false),
        }
    }

    pub async fn get_secret_field(&self, key: &str) -> Result<Option<String>> {
        let secret_cache_ref = self.secret_cache.clone();
        let mut secret_cache_opt = secret_cache_ref.lock().await;
        let secrets_val = match secret_cache_opt.as_ref() {
            Some(v) => v,
            None => {
                let secrets = load_secret(self.secret_arn.clone()).await?;
                *secret_cache_opt = Some(secrets);
                secret_cache_opt.as_ref().unwrap()
            }
        };

        Ok(secrets_val.get(key).map(|s| s.to_owned()))
    }

    /// Returns true if this is the first time the puller has run. Useful for e.g. pulling more logs on first run.
    pub async fn load_is_initial_run(&self) -> Result<bool> {
        let bucket = std::env::var("INGESTION_BUCKET_NAME").context("need bucket!")?;

        let initial_run_key = "__puller_initial_run__";
        let s3_key = format!("{}/{}", initial_run_key, self.log_source_type.to_str());

        let res = self
            .s3
            .head_object()
            .bucket(&bucket)
            .key(&s3_key)
            .send()
            .await;
        let is_initial = match res {
            Ok(_) => Ok(false),
            Err(aws_sdk_s3::types::SdkError::ServiceError {
                err:
                    aws_sdk_s3::error::HeadObjectError {
                        kind: aws_sdk_s3::error::HeadObjectErrorKind::NotFound(_),
                        ..
                    },
                ..
            }) => Ok(true),
            Err(e) => Err(e),
        }?;

        if is_initial {
            self.is_initial_run
                .swap(true, std::sync::atomic::Ordering::Relaxed);
        }

        Ok(is_initial)
    }

    pub async fn mark_initial_run_complete(&self) -> Result<()> {
        let bucket = std::env::var("INGESTION_BUCKET_NAME").context("need bucket!")?;

        let initial_run_key = "__puller_initial_run__";
        let s3_key = format!("{}/{}", initial_run_key, self.log_source_type.to_str());

        self.s3
            .put_object()
            .bucket(&bucket)
            .key(&s3_key)
            .body(ByteStream::from(Vec::with_capacity(0)))
            .send()
            .await?;

        Ok(())
    }

    pub fn is_initial_run(&self) -> bool {
        self.is_initial_run
            .load(std::sync::atomic::Ordering::Relaxed)
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
    DuoPuller(duo::DuoPuller),
    OktaPuller(okta::OktaPuller),
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
            "duo" => Some(LogSource::DuoPuller(duo::DuoPuller {})),
            "okta" => Some(LogSource::OktaPuller(okta::OktaPuller {})),
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
            LogSource::Otx(_) => "otx",
            LogSource::Snyk(_) => "snyk",
            LogSource::AbuseChUrlhausPuller(_) => "abusech_urlhaus",
            LogSource::AbuseChMalwareBazaarPuller(_) => "abusech_malwarebazaar",
            LogSource::AbuseChThreatfoxPuller(_) => "abusech_threatfox",
        }
    }
}
