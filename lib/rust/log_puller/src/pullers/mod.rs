use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, FixedOffset};
use enum_dispatch::enum_dispatch;
use log::{debug, error, info};

use std::sync::Arc;
use tokio::sync::Mutex;

use shared::secrets::load_secret;

mod abusech;
mod o365;

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
    cache: Arc<Mutex<PullerCache>>,
}

impl PullLogsContext {
    pub fn new(
        secret_arn: String,
        log_source_type: LogSource,
        config: HashMap<String, String>,
    ) -> PullLogsContext {
        PullLogsContext {
            secret_cache: Arc::new(Mutex::new(None)),
            secret_arn,
            log_source_type,
            config,
            cache: Arc::new(Mutex::new(PullerCache::new())),
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

    pub fn config(&self) -> &HashMap<String, String> {
        &self.config
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
    O365Puller(o365::O365Puller),
    AbuseChUrlhausPuller(abusech::AbuseChUrlhausPuller),
    AbuseChMalwareBazaarPuller(abusech::AbuseChMalwareBazaarPuller),
    AbuseChThreatfoxPuller(abusech::AbuseChThreatfoxPuller),
}

impl LogSource {
    pub fn from_str(s: &str) -> Option<LogSource> {
        match s.to_lowercase().as_str() {
            "o365" => Some(LogSource::O365Puller(o365::O365Puller {})),
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
}
