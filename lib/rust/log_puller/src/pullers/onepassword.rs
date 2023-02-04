use regex::Regex;
use std::sync::Arc;
use tokio::sync::Mutex;

use anyhow::{anyhow, Context as AnyhowContext, Error, Result};
use async_trait::async_trait;
use chrono::{DateTime, FixedOffset, Local, NaiveDateTime, Utc};
use log::{debug, error, info};

use super::{PullLogs, PullLogsContext};
use reqwest::header;

#[derive(Clone)]
pub struct OnePasswordPuller;

fn api_headers(auth_token: &Option<String>) -> Result<header::HeaderMap> {
    let mut headers = header::HeaderMap::new();
    headers.insert(
        header::USER_AGENT,
        "rust-reqwest/matano".parse().expect("invalid user-agent"),
    );

    if let Some(token) = auth_token {
        headers.insert(
            header::AUTHORIZATION,
            format!("Bearer {}", token)
                .parse()
                .map_err(|err| anyhow!("Failed to parse auth token: {}", err))?,
        );
    };

    Ok(headers)
}

#[async_trait]
impl PullLogs for OnePasswordPuller {
    async fn pull_logs(
        self,
        client: reqwest::Client,
        ctx: &PullLogsContext,
        start_dt: DateTime<FixedOffset>,
        end_dt: DateTime<FixedOffset>,
    ) -> Result<Vec<u8>> {
        info!("Pulling 1Password logs....");

        let config = ctx.config();
        let tables_config = ctx.tables_config();
        let cache = ctx.cache();

        let checkpoint_json = ctx.checkpoint_json.lock().await;
        let is_initial_run = checkpoint_json.is_none();

        let lookback_days_start = if is_initial_run { 30 } else { 2 };

        // collect logs from the last complete day? (current day - 2) to (current day - 1)
        let start_day = start_dt
            .checked_sub_signed(chrono::Duration::days(lookback_days_start))
            .unwrap()
            .format("%Y-%m-%d")
            .to_string();
        let yesterday = start_dt
            .checked_sub_signed(chrono::Duration::days(1))
            .unwrap()
            .format("%Y-%m-%d")
            .to_string();

        let events_api_url = config
            .get("events_api_url")
            .context("Missing Events API URL")?;
        // strip https:// or http:// from the url prefix
        let events_api_url = Regex::new(r"(?i)^https?://")?
            .replace_all(events_api_url, "")
            .to_string();
        // remove trailing slash if present
        let events_api_url = Regex::new(r"/$")?
            .replace_all(&events_api_url, "")
            .to_string();

        let api_token = ctx
            .get_secret_field("api_token")
            .await?
            .context("Missing 1Password api token")?;

        // skip early if api_token is equal <placeholder>
        if api_token == "<placeholder>" {
            return Ok(vec![]);
        }

        println!(
            "Collecting Logs from Start: {} - End: {}",
            start_day, yesterday
        );

        let mut next_page = 1;
        let mut ret: Vec<u8> = Vec::new();
        let headers = api_headers(&Some(api_token))?;
        let newline_u8 = "\n".to_string().into_bytes();

        // Remove last newline
        if ret.last() == Some(&b'\n') {
            ret.pop();
        }

        Ok(ret)
    }
}
