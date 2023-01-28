use std::{pin::Pin, sync::Arc};
use tokio::sync::Mutex;

use anyhow::{anyhow, Context as AnyhowContext, Error, Result};
use async_stream::{stream, try_stream};
use async_trait::async_trait;
use chrono::{DateTime, FixedOffset, Local, NaiveDateTime, Utc};
use futures::{future::join_all, FutureExt, Stream};
use futures_util::pin_mut;
use futures_util::stream::StreamExt;
use lazy_static::lazy_static;
use log::{debug, error, info};
use regex::Regex;
use std::collections::HashMap;
use std::time::Duration;

use reqwest::{Method, RequestBuilder, StatusCode};
use ring::hmac;
use serde::{de::DeserializeOwned, Deserialize, Deserializer};

use super::{PullLogs, PullLogsContext};
use shared::JsonValueExt;

#[derive(Clone)]
pub struct DuoPuller;

#[async_trait]
impl PullLogs for DuoPuller {
    async fn pull_logs(
        self,
        client: reqwest::Client,
        ctx: &PullLogsContext,
        start_dt: DateTime<FixedOffset>,
        end_dt: DateTime<FixedOffset>,
    ) -> Result<Vec<u8>> {
        info!("Pulling duo logs....");

        let config = ctx.config();
        let tables_config = ctx.tables_config();
        let cache = ctx.cache();
        let mut cache = cache.lock().await;

        let api_hostname = config.get("api_hostname").context("Missing api_hostname")?;
        let integration_key = config
            .get("integration_key")
            .context("Missing integration_key")?;
        let secret_key = ctx
            .get_secret_field("secret_key")
            .await?
            .context("Missing admin secret key")?;

        let mut ret: Vec<u8> = vec![];

        // skip early if secret_key is equal <placeholder>
        if secret_key == "<placeholder>" {
            return Ok(ret);
        }

        println!(
            "Collecting Logs from Start: {} - End: {} (or -2)",
            start_dt.format("%Y-%m-%dT%H:%M:%S"),
            end_dt.format("%Y-%m-%dT%H:%M:%S")
        );
        let start_time_minus_two_minutes = start_dt
            .checked_sub_signed(chrono::Duration::minutes(2))
            .unwrap();
        let end_time_minus_two_minutes = end_dt
            .checked_sub_signed(chrono::Duration::minutes(2))
            .unwrap();

        // for v2, lag by 2 minutes to avoid missing logs
        let mintime_minus_two_minutes = start_time_minus_two_minutes.timestamp_millis();
        let maxtime_minus_two_minutes = end_time_minus_two_minutes.timestamp_millis();

        // for v1, just pull the latest
        let mintime = start_dt.timestamp_millis();
        let maxtime = end_dt.timestamp_millis();

        let duo = DuoClient::new(api_hostname, integration_key, secret_key, client.clone())?;
        let newline_u8 = "\n".to_string().into_bytes();

        // Authentication logs (v2)
        if tables_config.get("auth").is_some() {
            let auth_logs_stream = get_duo_logs_stream(
                &duo,
                "authentication",
                2,
                2,
                mintime_minus_two_minutes,
                maxtime_minus_two_minutes,
            )
            .await;
            pin_mut!(auth_logs_stream);
            while let Some(item) = auth_logs_stream.next().await {
                match item {
                    Ok(mut item) => {
                        item["_table"] = "auth".into();
                        let item_vec = serde_json::to_vec(&item)?;
                        ret.extend(item_vec);
                        ret.extend(newline_u8.clone());
                    }
                    Err(err) => {
                        error!("Failed to get authlog item: {}", err);
                    }
                }
            }
        }

        // Telephony logs (v1)
        if tables_config.get("telephony").is_some() {
            let telephony_logs_stream =
                get_duo_logs_stream(&duo, "telephony", 1, 1, mintime, maxtime).await;
            pin_mut!(telephony_logs_stream);
            while let Some(item) = telephony_logs_stream.next().await {
                match item {
                    Ok(mut item) => {
                        item["_table"] = "telephony".into();
                        let item_vec = serde_json::to_vec(&item)?;
                        ret.extend(item_vec);
                        ret.extend(newline_u8.clone());
                    }
                    Err(err) => {
                        error!("Failed to get telephony item: {}", err);
                    }
                }
            }
        }

        // Admin logs
        if tables_config.get("admin").is_some() {
            let admin_logs_stream =
                get_duo_logs_stream(&duo, "administrator", 1, 1, mintime, maxtime).await;
            pin_mut!(admin_logs_stream);
            while let Some(item) = admin_logs_stream.next().await {
                match item {
                    Ok(mut item) => {
                        item["_table"] = "admin".into();
                        let item_vec = serde_json::to_vec(&item)?;
                        ret.extend(item_vec);
                        ret.extend(newline_u8.clone());
                    }
                    Err(err) => {
                        error!("Failed to get admin item: {}", err);
                    }
                }
            }
        }

        // Offline enrollment logs (v1)
        if tables_config.get("offline_enrollment").is_some() {
            let offline_enrollment_logs_stream =
                get_duo_logs_stream(&duo, "offline_enrollment", 1, 1, mintime, maxtime).await;
            pin_mut!(offline_enrollment_logs_stream);
            while let Some(item) = offline_enrollment_logs_stream.next().await {
                match item {
                    Ok(mut item) => {
                        item["_table"] = "offline_enrollment".into();
                        let item_vec = serde_json::to_vec(&item)?;
                        ret.extend(item_vec);
                        ret.extend(newline_u8.clone());
                    }
                    Err(err) => {
                        error!("Failed to get offline_enrollment item: {}", err);
                    }
                }
            }
        }

        // Summary info
        if tables_config.get("summary").is_some() {
            let mut res = duo
                .get("/admin/v1/info/summary", StatusCode::OK, HashMap::new())
                .await?;
            let obj = &mut res["response"];
            if obj.is_object() {
                obj["_table"] = "summary".into();
                let obj_vec = serde_json::to_vec(&obj)?;
                ret.extend(obj_vec);
                ret.extend(newline_u8.clone());
            }
        }

        // Activity logs (v2)
        if tables_config.get("activity").is_some() {
            let activity_logs_stream = get_duo_logs_stream(
                &duo,
                "activity",
                2,
                2,
                mintime_minus_two_minutes,
                maxtime_minus_two_minutes,
            )
            .await;
            pin_mut!(activity_logs_stream);
            while let Some(item) = activity_logs_stream.next().await {
                match item {
                    Ok(mut item) => {
                        item["_table"] = "activity".into();
                        let item_vec = serde_json::to_vec(&item)?;
                        ret.extend(item_vec);
                        ret.extend(newline_u8.clone());
                    }
                    Err(err) => {
                        error!("Failed to get activity item: {}", err);
                    }
                }
            }
        }

        // Trust monitor events (v1 path but v2 integration)
        if tables_config.get("trust_monitor").is_some() {
            let trust_monitor_logs_stream = get_duo_logs_stream(
                &duo,
                "trust_monitor",
                2,
                1,
                mintime_minus_two_minutes,
                maxtime_minus_two_minutes,
            )
            .await;
            pin_mut!(trust_monitor_logs_stream);
            while let Some(item) = trust_monitor_logs_stream.next().await {
                match item {
                    Ok(mut item) => {
                        item["_table"] = "trust_monitor".into();
                        let item_vec = serde_json::to_vec(&item)?;
                        ret.extend(item_vec);
                        ret.extend(newline_u8.clone());
                    }
                    Err(err) => {
                        error!("Failed to get trust_monitor item: {}", err);
                    }
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

// DuoClient implementation

/// Encapsulates Duo server connection.
#[derive(Debug)]
pub struct DuoClient {
    api_hostname: String,
    integration_key: String,
    secret_key: String,
    client: reqwest::Client,
}

impl DuoClient {
    /// Creates a new DuoClient.
    pub fn new<S: Into<String>, T: Into<String>, U: Into<String>>(
        api_hostname: S,
        integration_key: T,
        secret_key: U,
        client: reqwest::Client,
    ) -> Result<DuoClient> {
        Ok(DuoClient {
            api_hostname: api_hostname.into(),
            integration_key: integration_key.into(),
            secret_key: secret_key.into(),
            client: client,
        })
    }

    async fn get(
        &self,
        path: &str,
        expected: StatusCode,
        params: HashMap<String, String>,
    ) -> Result<serde_json::Value> {
        let params_str = encode_params(&params);
        let uri = format!("https://{}{}?{}", self.api_hostname, path, params_str);

        let req = self.client.get(&uri);
        let req = self.sign_req(req, Method::GET, path, &params);

        let res = req.send().await;
        match res {
            Ok(response) if response.status() == expected => {
                let text = response.text().await?;
                let data = serde_json::from_str::<serde_json::Value>(&text)?;
                Ok(data)
            }
            Ok(response) => Err(anyhow!("failed req status: {}", response.status())),
            Err(err) => Err(anyhow!("failed send req: {}", err)),
        }
    }

    fn sign_req(
        &self,
        req: RequestBuilder,
        method: Method,
        path: &str,
        params: &HashMap<String, String>,
    ) -> RequestBuilder {
        let now = Local::now().to_rfc2822();
        let method = method.as_str();
        let api_hostname = self.api_hostname.to_lowercase();
        let params = encode_params(params);
        let canon = [
            now.as_str(),
            method,
            api_hostname.as_str(),
            path,
            params.as_str(),
        ];

        let basic_auth = basic_auth_for_canon(&self.integration_key, &self.secret_key, &canon);

        req.header("Date", &now)
            .header("Authorization", &basic_auth)
    }
}

fn encode_params(params: &HashMap<String, String>) -> String {
    let mut sorted_keys: Vec<_> = params.keys().collect();
    sorted_keys.sort();
    let mut encoder = url::form_urlencoded::Serializer::new(String::new());
    for k in sorted_keys {
        encoder.append_pair(k, params[k].as_str()); // safe
    }

    encoder.finish()
}

fn basic_auth_for_canon(integration_key: &str, secret_key: &str, canon: &[&str]) -> String {
    let canon = canon.join("\n");

    let s_key = hmac::Key::new(hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY, secret_key.as_bytes());
    let mut s_ctx = hmac::Context::with_key(&s_key);
    s_ctx.update(canon.as_bytes());
    let sig = s_ctx.sign();
    let auth = format!("{}:{}", integration_key, hex::encode(sig.as_ref()));

    let basic_auth = format!("Basic {}", base64::encode(&auth));

    basic_auth
}

async fn get_duo_logs_stream<'a>(
    duo: &'a DuoClient,
    integration_name: &'a str,
    integration_version: usize,
    integration_path_version: usize,
    mintime: i64,
    maxtime: i64,
    // maybe_last_timestamp: &'a mut Option<i64>,
    // maybe_next_token: &'a mut Option<String>,
) -> Pin<Box<dyn Stream<Item = Result<serde_json::Value>> + Send + 'a>> {
    let mut maybe_last_timestamp: Option<i64> = None;
    let mut maybe_next_token: Option<String> = None;

    let mintime = if integration_version == 1 {
        mintime / 1000
    } else {
        mintime
    };
    let maxtime = if integration_version == 1 {
        maxtime / 1000
    } else {
        maxtime
    };

    Box::pin(try_stream! {
        let mut is_first = true;
        while is_first || maybe_next_token.is_some() || maybe_last_timestamp.is_some() {
            let mintime_string = if is_first || maybe_next_token.is_some() {
                Some(mintime.to_string())
            } else {
                maybe_last_timestamp.clone().map(|t| (t+1).to_string())
            };

            let query_params = vec![
                ("sort", Some("ts:asc".to_string())),
                ("limit", Some(if integration_name == "trust_monitor" { "500".to_string() } else { "1000".to_string() })),
                ("mintime", mintime_string),
                ("maxtime", Some(maxtime.to_string())),
                ("next_token", maybe_next_token.as_ref().map(|s| s.as_str().to_string())),
            ]
                .into_iter()
                .filter(|(k, v)| v.is_some() && !(integration_version == 1 && (k == &"maxtime" || k == &"limit" || k == &"sort")))
                .map(|(k, v)| (k.to_string(), v.unwrap()))
                .collect::<HashMap<String, String>>();

            let res = if integration_name == "trust_monitor" {
                duo.get(
                    &format!(
                        "/admin/v{}/{}/events",
                        integration_path_version,
                        integration_name,
                    ),
                    StatusCode::OK,
                    query_params,
                ).await?
            } else {
                duo.get(
                    &format!(
                        "/admin/v{}/logs/{}",
                        integration_path_version,
                        integration_name,
                    ),
                    StatusCode::OK,
                    query_params,
                ).await?
            };

            let mut new_next_token: Option<String> = None;
            let mut new_last_timestamp: Option<i64> = None;
            if integration_version == 1 {
                let rows = &res["response"].as_array().ok_or(anyhow!("missing response"))?;
                if rows.len() == 1000 { // more pages
                    new_last_timestamp = rows.last().map(|row| row["timestamp"].as_i64().unwrap());
                }
                maybe_last_timestamp = new_last_timestamp;
            } else {
                new_next_token = match &res["response"]["metadata"]["next_offset"] {
                    serde_json::Value::String(s) => Some(s.to_owned()),
                    serde_json::Value::Array(a) => Some(a.iter().map(|v| v.as_str().unwrap()).collect::<Vec<_>>().join(",")),
                    serde_json::Value::Null => None,
                    _ => unimplemented!(),
                };
                maybe_next_token = new_next_token.clone();
            }

            is_first = false;

            if integration_version == 1 {
                for row in res["response"].to_owned().into_array().ok_or(anyhow!("missing rows response"))? {
                    yield row;
                }
            } else if integration_name == "authentication" {
                for row in res["response"]["authlogs"].to_owned().into_array().ok_or(anyhow!("missing authlogs response"))?.into_iter() {
                    yield row;
                }
            } else if integration_name == "activity" {
                for row in res["response"]["items"].to_owned().into_array().ok_or(anyhow!("missing activity response"))? {
                    yield row;
                }
            } else if integration_name == "trust_monitor" {
                for row in res["response"]["events"].to_owned().into_array().ok_or(anyhow!("missing trust_events response"))? {
                    yield row;
                }
            }
        }
    })
}
