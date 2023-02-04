use std::sync::Arc;
use tokio::sync::Mutex;

use anyhow::{anyhow, Context as AnyhowContext, Error, Result};
use async_trait::async_trait;
use chrono::{DateTime, FixedOffset, Local, NaiveDateTime, Utc};
use log::{debug, error, info};

use reqwest::header;

use super::{PullLogs, PullLogsContext};

#[derive(Clone)]
pub struct SnykPuller;

fn api_headers(auth_token: &Option<String>) -> Result<header::HeaderMap> {
    let mut headers = header::HeaderMap::new();
    headers.insert(
        header::USER_AGENT,
        "rust-reqwest/matano".parse().expect("invalid user-agent"),
    );

    if let Some(token) = auth_token {
        headers.insert(
            header::AUTHORIZATION,
            format!("token {}", token)
                .parse()
                .map_err(|err| anyhow!("Failed to parse auth token: {}", err))?,
        );
    };

    Ok(headers)
}

#[async_trait]
impl PullLogs for SnykPuller {
    async fn pull_logs(
        self,
        client: reqwest::Client,
        ctx: &PullLogsContext,
        start_dt: DateTime<FixedOffset>,
        end_dt: DateTime<FixedOffset>,
    ) -> Result<Vec<u8>> {
        info!("Pulling snyk logs....");

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

        let group_id = config.get("group_id").context("Missing group_id").ok();
        let org_id = config.get("org_id").context("Missing org_id").ok();

        let api_token = ctx
            .get_secret_field("api_token")
            .await?
            .context("Missing snyk api token")?;

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

        if tables_config.get("audit").is_some() {
            // Collect Group Level Audit Logs
            while next_page != -1 && group_id.is_some() {
                let page = next_page;
                let group_id = group_id.unwrap();

                let url = format!(
                    "https://api.snyk.io/api/v1/group/{}/audit?from={}&to={}&page={}&sortOrder=ASC",
                    start_day, yesterday, group_id, page
                );
                info!("requesting url: {}", &url);

                let response = client
                    .get(url.clone())
                    .headers(headers.clone())
                    .send()
                    .await?;

                let response_json: Vec<serde_json::Value> = response.json().await?;
                let length = response_json.len();

                for mut value in response_json {
                    value["_table"] = "audit".into();
                    let value = serde_json::to_vec(&value)?;
                    ret.extend_from_slice(&value);
                    ret.extend_from_slice(&newline_u8);
                }

                // determine if there are more pages to collect
                if length == 0 {
                    // if this request returned 0, we're done
                    next_page = -1;
                } else {
                    next_page = page + 1;
                }
            }

            // Collect Group Level Audit Logs
            next_page = 1;
            while next_page != -1 && org_id.is_some() {
                let page = next_page;
                let org_id = org_id.unwrap();

                let url = format!(
                    "https://api.snyk.io/api/v1/org/{}/audit?from={}&to={}&page={}&sortOrder=ASC",
                    start_day, yesterday, org_id, page
                );
                info!("requesting url: {}", &url);

                let response = client
                    .get(url.clone())
                    .headers(headers.clone())
                    .send()
                    .await?;

                let response_json: Vec<serde_json::Value> = response.json().await?;
                let length = response_json.len();

                for mut value in response_json {
                    value["_table"] = "audit".into();
                    let value = serde_json::to_vec(&value)?;
                    ret.extend_from_slice(&value);
                    ret.extend_from_slice(&newline_u8);
                }

                // determine if there are more pages to collect
                if length == 0 {
                    // if this request returned 0, we're done
                    next_page = -1;
                } else {
                    next_page = page + 1;
                }
            }
        }

        if tables_config.get("vulnerabilities").is_some() {
            // Get vulnerability issues
            next_page = 1;
            while next_page != -1 {
                let page = next_page;

                let url = format!(
                    "https://api.snyk.io/reporting/issues/?from={}&to={}page={}&perPage=100&sortBy=issueTitle&order=asc&groupBy=issue",
                    start_day,
                    yesterday,
                    page
                );
                info!("requesting url: {}", &url);

                let response = client
                    .get(url.clone())
                    .headers(headers.clone())
                    .send()
                    .await?;

                let response_json: Vec<serde_json::Value> = response.json().await?;
                let length = response_json.len();

                for mut value in response_json {
                    value["_table"] = "vulnerabilities".into();
                    let value = serde_json::to_vec(&value)?;
                    ret.extend_from_slice(&value);
                    ret.extend_from_slice(&newline_u8);
                }

                // determine if there are more pages to collect
                if length == 0 {
                    // if this request returned 0, we're done
                    next_page = -1;
                } else {
                    next_page = page + 1;
                }
            }
        }

        // Remove last newline
        if ret.last() == Some(&b'\n') {
            ret.pop();
        }

        Ok(ret)
    }
}
