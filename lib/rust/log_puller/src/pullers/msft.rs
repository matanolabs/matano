use aws_config::imds::client;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

use anyhow::{anyhow, Context as AnyhowContext, Error, Result};
use async_stream::{stream, try_stream};
use async_trait::async_trait;
use chrono::{DateTime, FixedOffset};
use futures::{future::join_all, Stream};
use futures_util::pin_mut;
use futures_util::stream::StreamExt;
use lazy_static::lazy_static;
use log::{debug, error, info};
use regex::Regex;

use super::{PullLogs, PullLogsContext};
use shared::{convert_json_array_str_to_ndjson, JsonValueExt};

#[derive(Clone)]
pub struct MicrosoftGraphPuller;

const GRAPH_ENDPOINT: &str = "https://graph.microsoft.com/v1.0";
const INITIAL_INTERVAL_DAYS: i64 = 14;

struct GraphResourceProps {
    resource: String,
    filter_key: String,
}

fn table_resource_map() -> HashMap<String, GraphResourceProps> {
    [
        (
            "aad_signinlogs",
            GraphResourceProps {
                resource: "auditLogs/signIns".to_string(),
                filter_key: "createdDateTime".to_string(),
            },
        ),
        (
            "aad_auditlogs",
            GraphResourceProps {
                resource: "auditLogs/directoryAudits".to_string(),
                filter_key: "activityDateTime".to_string(),
            },
        ),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect::<HashMap<_, _>>()
}

lazy_static! {
    static ref TABLE_RESOURCE_MAP: HashMap<String, GraphResourceProps> = table_resource_map();
}

#[async_trait]
impl PullLogs for MicrosoftGraphPuller {
    async fn pull_logs(
        self,
        client: reqwest::Client,
        ctx: &PullLogsContext,
        start_dt: DateTime<FixedOffset>,
        end_dt: DateTime<FixedOffset>,
    ) -> Result<Vec<u8>> {
        info!("Pulling Microsoft Graph logs....");

        let config = ctx.config();
        let cache = ctx.cache();

        let tenant_id = config.get("tenant_id").context("Missing tenant_id")?;
        let client_id = config.get("client_id").context("Missing client_id")?;
        let client_secret = ctx
            .get_secret_field("client_secret")
            .await?
            .context("Missing client secret")?;

        // skip early if client_secret is equal <placeholder>
        if client_secret == "<placeholder>" {
            return Ok(vec![]);
        }

        let access_token = {
            let mut cache = cache.lock().await;
            match cache.get("access_token") {
                Some(token) => token.to_owned(),
                None => {
                    let token =
                        get_access_token(&client, tenant_id, client_id, &client_secret).await?;
                    cache.set("access_token", token.clone(), None);
                    token
                }
            }
        };

        let checkpoint_json = ctx.checkpoint_json.lock().await;
        let is_initial_run = checkpoint_json.is_none();

        let start_dt = if is_initial_run {
            start_dt - chrono::Duration::days(INITIAL_INTERVAL_DAYS)
        } else {
            start_dt
        };

        let start_time = start_dt.format("%Y-%m-%dT%H:%M:%SZ").to_string();
        let end_time = end_dt.format("%Y-%m-%dT%H:%M:%SZ").to_string();

        let tables = ctx.tables_config.keys().collect::<Vec<_>>();
        if tables.is_empty() {
            return Ok(vec![]);
        }

        let table_resources = tables
            .into_iter()
            .filter_map(|t| Some((t, TABLE_RESOURCE_MAP.get(t)?)));

        let futs = table_resources
            .map(|(table, resource)| {
                get_graph_results(
                    client.clone(),
                    &tenant_id,
                    table,
                    resource,
                    &access_token,
                    &start_time,
                    &end_time,
                )
            })
            .collect::<Vec<_>>();

        let results = join_all(futs)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        Ok(results)
    }
}

async fn get_access_token(
    client: &reqwest::Client,
    tenant_id: &str,
    client_id: &str,
    client_secret: &str,
) -> Result<String> {
    info!("Getting access token");
    let url = format!(
        "https://login.microsoftonline.com/{}/oauth2/v2.0/token",
        tenant_id
    );
    let authres = client
        .post(url)
        .form(&[
            ("scope", "https://graph.microsoft.com/.default"),
            ("grant_type", "client_credentials"),
            ("client_id", client_id),
            ("client_secret", client_secret),
        ])
        .send()
        .await?
        .error_for_status()?;
    let auth_body = authres.json::<serde_json::Value>().await?;
    let access_token = auth_body
        .as_object()
        .and_then(|o| o.get("access_token")?.as_str())
        .context("Missing access token")?
        .to_string();
    Ok(access_token)
}

async fn get_graph_results(
    client: reqwest::Client,
    tenant_id: &str,
    table: &str,
    resource: &GraphResourceProps,
    access_token: &str,
    start_time: &str,
    end_time: &str,
) -> Result<Vec<u8>> {
    let query_str = format!(
        "$filter={} ge {} and {} lt {}",
        resource.filter_key, start_time, resource.filter_key, end_time
    );
    let url = format!("{}/{}?{}", GRAPH_ENDPOINT, resource.resource, query_str);
    info!("Getting results for {} from {}", table, url);

    let mut next_url = None;
    let mut first = true;

    let mut ret = vec![];

    while first || next_url.is_some() {
        let url = next_url.unwrap_or(url.clone());
        let res = client.get(&url).bearer_auth(access_token).send().await?;
        let is_failure = !res.status().is_success();

        let body = res.bytes().await?;
        if is_failure {
            let body = std::str::from_utf8(&body).unwrap_or_default();
            let msg = format!("Error calling url: {}, response: {}", &url, body);
            return Err(anyhow!(msg));
        }

        let mut body = serde_json::from_slice::<serde_json::Value>(&body)?;

        let records = body
            .get_mut("value")
            .and_then(|v| v.take().into_array())
            .context("Missing value array")?
            .into_iter()
            .filter_map(|v| v.into_object());

        for mut record in records {
            record.insert("_table".to_string(), table.into());
            record.insert("tenant_id".to_string(), tenant_id.into());

            let rec_enc = serde_json::to_vec(&record)?;
            ret.extend(rec_enc);
            ret.extend(b"\n");
        }

        next_url = body
            .get_mut("@odata.nextLink")
            .and_then(|v| v.take().into_str());
        first = false;
    }

    Ok(ret)
}
