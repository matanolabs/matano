use std::io::{Cursor, Write};

use anyhow::{anyhow, Context as AnyhowContext, Error, Result};
use async_trait::async_trait;
use chrono::{DateTime, FixedOffset};
use futures::future::join_all;
use futures_util::stream::StreamExt;
use lazy_static::lazy_static;
use log::{debug, error, info};

use super::{PullLogs, PullLogsContext};
use shared::JsonValueExt;

#[derive(Clone)]
pub struct OtxPuller;

//modified_since

const OTX_API_URL: &str = "https://otx.alienvault.com/api/v1/pulses/subscribed";

#[async_trait]
impl PullLogs for OtxPuller {
    async fn pull_logs(
        self,
        client: reqwest::Client,
        ctx: &PullLogsContext,
        start_dt: DateTime<FixedOffset>,
        end_dt: DateTime<FixedOffset>,
    ) -> Result<Vec<u8>> {
        info!("Pulling OTX...");
        let api_key = ctx
            .get_secret_field("api_key")
            .await?
            .context("Missing OTX API Key")?;

        if api_key == "<placeholder>" {
            return Ok(vec![]);
        }

        let mut handles = vec![];
        let mut next_url: Option<String> = None;
        let mut is_first = true;

        while is_first || next_url.is_some() {
            info!("Getting OTX page...");

            let url = if is_first {
                format!(
                    "{}?modified_since={}&limit=1000",
                    OTX_API_URL,
                    start_dt.format("%Y-%m-%dT%H:%M:%S").to_string()
                )
            } else {
                next_url.unwrap()
            };

            let res = client
                .get(url)
                .header("X-OTX-API-KEY", &api_key)
                .send()
                .await?;

            let body: serde_json::Value = res.json().await?;

            next_url = body
                .as_object()
                .and_then(|o| o.get("next"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            let results = body
                .into_object()
                .and_then(|mut o| o.remove("results"))
                .and_then(|v| v.into_array())
                .context("failed to get OTX results")?;

            let handle = tokio::task::spawn_blocking(move || process_results(results));
            handles.push(handle);
            is_first = false;
        }

        let mut writer = Cursor::new(Vec::<u8>::new());

        join_all(handles)
            .await
            .into_iter()
            .flatten()
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(|data| writer.write(data.as_slice()).map_err(|e| anyhow!(e)))
            .collect::<Result<Vec<_>>>()?;

        Ok(writer.into_inner())
    }
}

fn process_results(results: Vec<serde_json::Value>) -> Result<Vec<u8>> {
    info!("Processing OTX results...");
    let mut writer = Cursor::new(Vec::<u8>::new());

    results
        .into_iter()
        .filter_map(|result| {
            let mut result = result.into_object()?;
            let pulse_id = result.remove("id")?;
            let mut indicators = result.remove("indicators")?.into_array()?;

            for indicator in indicators.iter_mut() {
                let indicator = indicator.as_object_mut()?;
                for (top_level_key, top_level_val) in result.iter() {
                    indicator.insert(top_level_key.to_string(), top_level_val.clone());
                }
                indicator.insert("pulse_id".to_string(), pulse_id.clone());
            }
            Some(indicators)
        })
        .flatten()
        .map(|v| {
            let bytes = serde_json::to_vec(&v)?;
            writer.write(bytes.as_slice())?;
            writer.write(b"\n")?;
            Ok(())
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(writer.into_inner())
}
