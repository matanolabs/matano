use regex::Regex;
use serde_json::json;
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

        let mut checkpoint_json = ctx.checkpoint_json.lock().await;
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
            info!("Skipping onepassword because secret is still <placeholder>");
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

        let limit = 1000;

        let mut cursor_itemusages = checkpoint_json
            .as_ref()
            .and_then(|json| json["cursor_itemusages"].as_str().map(|v| v.to_string()));
        let url = format!("https://{}/api/v1/itemusages", events_api_url);
        loop {
            let body = match cursor_itemusages {
                Some(ref cursor) => json!({
                    "cursor": cursor,
                }),
                None => json!({
                    "limit": limit,
                }),
            };

            // POST
            let response = client
                .post(&url)
                .headers(headers.clone())
                .json(&body)
                .send()
                .await
                .context("Failed to send request")?;

            let status = response.status();
            if !status.is_success() {
                return Err(anyhow!("Failed to get logs: {}", status));
            }

            let mut body_json: serde_json::Value = response
                .json()
                .await
                .context("Failed to parse response body")?;

            let items = body_json["items"].as_array_mut().unwrap();

            for item in items {
                item["_table"] = "item_usages".into();
                let item_str = serde_json::to_string(item)?;
                ret.extend(item_str.into_bytes());
                ret.extend(newline_u8.clone());
            }

            if let Some(cursor_json) = body_json["cursor"].as_str() {
                cursor_itemusages = Some(cursor_json.to_string());
            }

            if let Some(has_more) = body_json["has_more"].as_bool() {
                if !has_more {
                    break;
                }
            }
        }

        let mut cursor_signinattempts = checkpoint_json.as_ref().and_then(|json| {
            json["cursor_signinattempts"]
                .as_str()
                .map(|v| v.to_string())
        });
        let url = format!("https://{}/api/v1/signinattempts", events_api_url);
        loop {
            let body = match cursor_signinattempts {
                Some(ref cursor) => json!({
                    "cursor": cursor,
                }),
                None => json!({
                    "limit": limit,
                }),
            };

            // POST
            let response = client
                .post(&url)
                .headers(headers.clone())
                .json(&body)
                .send()
                .await
                .context("Failed to send request")?;

            let status = response.status();
            if !status.is_success() {
                return Err(anyhow!("Failed to get logs: {}", status));
            }

            let mut body_json: serde_json::Value = response
                .json()
                .await
                .context("Failed to parse response body")?;

            let items = body_json["items"].as_array_mut().unwrap();

            for item in items {
                item["_table"] = "signin_attempts".into();
                let item_str = serde_json::to_string(item)?;
                ret.extend(item_str.into_bytes());
                ret.extend(newline_u8.clone());
            }

            if let Some(cursor_json) = body_json["cursor"].as_str() {
                cursor_signinattempts = Some(cursor_json.to_string());
            }

            if let Some(has_more) = body_json["has_more"].as_bool() {
                if !has_more {
                    break;
                }
            }
        }

        // Remove last newline
        if ret.last() == Some(&b'\n') {
            ret.pop();
        }

        // update checkpoint
        *checkpoint_json = Some(json!({
            "cursor_itemusages": cursor_itemusages,
            "cursor_signinattempts": cursor_signinattempts,
        }));

        Ok(ret)
    }
}
