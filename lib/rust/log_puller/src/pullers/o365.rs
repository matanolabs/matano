use std::sync::Arc;
use tokio::sync::Mutex;

use anyhow::{anyhow, Context as AnyhowContext, Error, Result};
use async_stream::{stream, try_stream};
use async_trait::async_trait;
use futures::{future::join_all, Stream};
use futures_util::pin_mut;
use futures_util::stream::StreamExt;
use lazy_static::lazy_static;
use log::{debug, error, info};
use regex::Regex;

use super::{PullLogs, PullLogsContext};
use shared::{convert_json_array_str_to_ndjson, JsonValueExt};

#[derive(Clone)]
pub struct O365Puller;

#[async_trait]
impl PullLogs for O365Puller {
    async fn pull_logs(self, client: reqwest::Client, ctx: &PullLogsContext) -> Result<Vec<u8>> {
        info!("Pulling o365 logs....");

        let config = ctx.config();
        let cache = ctx.cache();
        let mut cache = cache.lock().await;

        let tenant_id = config.get("tenant_id").context("Missing tenant_id")?;
        let client_id = config.get("client_id").context("Missing client_id")?;
        let client_secret = ctx
            .get_secret_field("client_secret")
            .await?
            .context("Missing client secret")?;

        let access_token = match cache.get("access_token") {
            Some(token) => token.to_owned(),
            None => {
                let token = get_access_token(&client, tenant_id, client_id, &client_secret).await?;
                cache.set("access_token", token.clone(), None);
                token
            }
        };

        let now = chrono::Utc::now();
        let one_min_ago = now - chrono::Duration::minutes(1);
        let start_time = one_min_ago.format("%Y-%m-%dT%H:%M:%S").to_string();
        let end_time = now.format("%Y-%m-%dT%H:%M:%S").to_string();

        let mut maybe_next_token: Option<String> = None;

        let stream = list_entries_stream(
            client.clone(),
            &access_token,
            tenant_id,
            &start_time,
            &end_time,
            &mut maybe_next_token,
        );

        pin_mut!(stream);

        let client_ref = Arc::new(Mutex::new(client.clone()));

        let mut handles = vec![];
        while let Some(uri_result) = stream.next().await {
            let uri = uri_result?;
            let handle = tokio::spawn(pull_entry(client_ref.clone(), uri, access_token.clone()));
            handles.push(handle);
        }

        let results = join_all(handles).await;
        let results = results
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

        let chunks = results
            .into_iter()
            .map(|v| {
                let s = String::from_utf8(v)?;
                convert_json_array_str_to_ndjson(&s)
            })
            .collect::<Result<Vec<_>>>()?;

        if chunks.is_empty() {
            Ok(vec![])
        } else {
            let final_ret = chunks.join("\n");
            Ok(final_ret.as_bytes().to_vec())
        }
    }
}

async fn get_access_token(
    client: &reqwest::Client,
    tenant_id: &str,
    client_id: &str,
    client_secret: &str,
) -> Result<String> {
    info!("Getting access token");
    let url = format!("https://login.windows.net/{}/oauth2/token", tenant_id);
    let authres = client
        .post(url)
        .form(&[
            ("resource", "https://manage.office.com"),
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
        .and_then(|o| o.get("access_token"))
        .and_then(|v| v.as_str())
        .context("Missing access token")?
        .to_owned();
    Ok(access_token)
}

async fn list_entries(
    client: reqwest::Client,
    access_token: &str,
    tenant_id: &str,
    start_time: &str,
    end_time: &str,
    next_page: Option<&str>,
) -> Result<(Option<Vec<String>>, Option<String>)> {
    let url = format!(
        "https://manage.office.com/api/v1.0/{}/activity/feed/subscriptions/content",
        tenant_id
    );
    let mut query = vec![
        ("contentType", "Audit.General"),
        ("startTime", start_time),
        ("endTime", end_time),
    ];
    if let Some(n_page) = next_page {
        query.push(("nextPage", n_page));
    }
    let res = client
        .get(url)
        .query(&query)
        .bearer_auth(&access_token)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .send()
        .await?
        .error_for_status()?;

    lazy_static! {
        static ref URI_RE: Regex = Regex::new(r"nextPage=([\w\d]+)").unwrap();
    }

    let maybe_next_token = res
        .headers()
        .get("NextPageUri")
        .map(|s| s.to_str().unwrap())
        .and_then(|s| URI_RE.captures(s)?.get(1).map(|c| c.as_str().to_owned()));

    let resp = res.json::<serde_json::Value>().await?;
    let ret = resp.into_array().and_then(|arr| {
        arr.into_iter()
            .map(|v| {
                v.into_object()
                    .and_then(|o| o.get("contentUri").cloned())
                    .and_then(|v| v.into_str())
            })
            .collect::<Option<Vec<_>>>()
    });

    Ok((ret, maybe_next_token))
}

fn list_entries_stream<'a>(
    client: reqwest::Client,
    access_token: &'a str,
    tenant_id: &'a str,
    start_time: &'a str,
    end_time: &'a str,
    maybe_next_token: &'a mut Option<String>,
) -> impl Stream<Item = Result<String>> + 'a {
    try_stream! {
        let mut is_first = true;
        while is_first || maybe_next_token.is_some() {
            let token = maybe_next_token.as_ref().clone().map(|s| s.as_str());
            let (new_uris, new_next_token) = list_entries(
                client.clone(),
                &access_token,
                tenant_id,
                &start_time,
                &end_time,
                token,
            ).await.unwrap();
            *maybe_next_token = new_next_token;
            is_first = false;
            if let Some(uris) = new_uris {
                for uri in uris {
                    yield uri
                }
            }
        }
    }
}

async fn pull_entry(
    client: Arc<Mutex<reqwest::Client>>,
    uri: String,
    access_token: String,
) -> Result<Vec<u8>> {
    let client = client.lock().await;
    let res = client
        .get(&uri)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .bearer_auth(access_token)
        .send()
        .await?
        .error_for_status()?;
    let resp = res.bytes().await?.to_vec();
    Ok(resp)
}
