use std::sync::Arc;
use tokio::sync::Mutex;

use anyhow::{anyhow, Context as AnyhowContext, Error, Result};
use async_trait::async_trait;
use chrono::{DateTime, FixedOffset, Local, NaiveDateTime, Utc};
use lazy_static::lazy_static;
use log::{debug, error, info};
use regex::Regex;

use reqwest::header;

use super::{PullLogs, PullLogsContext};
use shared::JsonValueExt;

#[derive(Clone)]
pub struct OktaPuller;

lazy_static! {
    // cache the current checkpoint url, and reuse it in warm container
    static ref CHECKPOINT_URL: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
}

/// Search for the first "rel" link-header uri in a full link header string.
/// Seems like reqwest/hyper threw away their link-header parser implementation...
///
/// ex:
/// `Link: <https://api.github.com/resource?page=2>; rel="next"`
/// `Link: <https://gitlab.com/api/v4/projects/13083/releases?id=13083&page=2&per_page=20>; rel="next"`
///
/// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Link
/// header values may contain multiple values separated by commas
/// `Link: <https://place.com>; rel="next", <https://wow.com>; rel="next"`
pub(crate) fn find_rel_next_link(link_str: &str) -> Option<&str> {
    for link in link_str.split(',') {
        let mut uri = None;
        let mut is_rel_next = false;
        for part in link.split(';') {
            let part = part.trim();
            if part.starts_with('<') && part.ends_with('>') {
                uri = Some(part.trim_start_matches('<').trim_end_matches('>'));
            } else if part.starts_with("rel=") {
                let part = part
                    .trim_start_matches("rel=")
                    .trim_end_matches('"')
                    .trim_start_matches('"');
                if part == "next" {
                    is_rel_next = true;
                }
            }

            if is_rel_next && uri.is_some() {
                return uri;
            }
        }
    }
    None
}

fn api_headers(auth_token: &Option<String>) -> Result<header::HeaderMap> {
    let mut headers = header::HeaderMap::new();
    headers.insert(
        header::USER_AGENT,
        "rust-reqwest/matano".parse().expect("invalid user-agent"),
    );

    if let Some(token) = auth_token {
        headers.insert(
            header::AUTHORIZATION,
            format!("SSWS {}", token)
                .parse()
                .map_err(|err| anyhow!("Failed to parse auth token: {}", err))?,
        );
    };

    Ok(headers)
}

#[async_trait]
impl PullLogs for OktaPuller {
    async fn pull_logs(
        self,
        client: reqwest::Client,
        ctx: &PullLogsContext,
        start_dt: DateTime<FixedOffset>,
        end_dt: DateTime<FixedOffset>,
    ) -> Result<Vec<u8>> {
        info!("Pulling okta logs....");

        let config = ctx.config();
        let tables_config = ctx.tables_config();
        let cache = ctx.cache();
        let mut cache = cache.lock().await;

        let limit = 1000;

        let okta_base_url = config.get("base_url").context("Missing okta_base_url")?;
        // strip https:// or http:// from the url prefix
        let okta_base_url = Regex::new(r"(?i)^https?://")?
            .replace_all(okta_base_url, "")
            .to_string();
        // remove trailing slash if present
        let okta_base_url = Regex::new(r"/$")?
            .replace_all(&okta_base_url, "")
            .to_string();

        let api_token = ctx
            .get_secret_field("api_token")
            .await?
            .context("Missing okta api token")?;

        // skip early if api_token is equal <placeholder>
        if api_token == "<placeholder>" {
            return Ok(vec![]);
        }

        println!(
            "Collecting Logs from Start: {} - End: {}",
            start_dt.format("%Y-%m-%dT%H:%M:%S"),
            end_dt.format("%Y-%m-%dT%H:%M:%S")
        );

        let mut checkpoint_url = CHECKPOINT_URL.lock().await;

        let request_url = match *checkpoint_url {
            Some(ref url) => {
                println!("Using cached checkpoint url: {}", url);
                url.clone()
            }
            None => {
                println!("No cached checkpoint url found, creating from params");
                let url = format!(
                    "https://{}/api/v1/logs?since={}&limit={}",
                    okta_base_url,
                    start_dt.format("%Y-%m-%dT%H:%M:%S"),
                    limit
                );
                url
            }
        };

        let mut url = request_url;
        let mut ret: Vec<u8> = Vec::new();
        let headers = api_headers(&Some(api_token))?;
        let newline_u8 = "\n".to_string().into_bytes();

        while url != "" {
            let response = client
                .get(url.clone())
                .headers(headers.clone())
                .send()
                .await?;

            let headers = response.headers().clone();
            let response_json: Vec<serde_json::Value> = response.json().await?;

            // handle Okta paged responses containing `Link` header for 'self' and 'next'
            let links = headers.get_all(reqwest::header::LINK);

            let next_link = links
                .iter()
                .filter_map(|link| {
                    if let Ok(link) = link.to_str() {
                        find_rel_next_link(link)
                    } else {
                        None
                    }
                })
                .next();

            for value in &response_json {
                let value = serde_json::to_vec(&value)?;
                ret.extend_from_slice(&value);
                ret.extend_from_slice(&newline_u8);
            }

            // determine if there are more pages to collect
            if response_json.len() == limit && next_link.is_some() {
                // extract the next link from the response
                url = next_link.unwrap().to_string();
            } else if response_json.len() == 0 {
                // if this request returned 0 results there is no next link contained
                // use the same as the checkpointUrl for the next iteration
                *checkpoint_url = Some(url);
                url = "".to_string();
            } else {
                // if this request returned fewer results than requested
                // use the next link contained in the response for the next iteration
                *checkpoint_url = Some(next_link.unwrap().to_string());
                url = "".to_string();
            }
        }

        // Remove last newline
        if ret.last() == Some(&b'\n') {
            ret.pop();
        }

        Ok(ret)
    }
}
