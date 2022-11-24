use anyhow::Result;
use log::{debug, error, info};
use serde_json::Value;
use std::collections::HashMap;

async fn post(api_token: &str, body: HashMap<&str, &str>, uri: &str) -> Result<serde_json::Value> {
    debug!("{:?}", body);
    let client = reqwest::Client::new()
        .post(uri)
        .header(
            reqwest::header::CONTENT_TYPE,
            "application/json; charset=utf-8",
        )
        .header(
            reqwest::header::AUTHORIZATION,
            "Bearer ".to_owned() + api_token,
        )
        .body(serde_json::to_vec(&body)?)
        .send()
        .await?;
    let text = client.text().await?;
    let v: serde_json::Value = text.parse()?;
    println!("{}", serde_json::to_string_pretty(&v)?);
    Ok(v)
}

pub async fn post_message(api_token: &str, channel: &str, blocks: &str) -> Result<Value> {
    let mut body = HashMap::new();
    body.insert("channel", channel);
    body.insert("blocks", blocks);
    // body.insert("as_user", "true");
    post(api_token, body, "https://slack.com/api/chat.postMessage").await
}

pub async fn post_message_to_thread(
    api_token: &str,
    channel: &str,
    ts: &str,
    blocks: &str,
) -> Result<Value> {
    let mut body = HashMap::new();
    body.insert("channel", channel);
    body.insert("blocks", blocks);
    // body.insert("as_user", "true");
    body.insert("thread_ts", ts);
    post(api_token, body, "https://slack.com/api/chat.postMessage").await
}

pub async fn post_ephemeral_attachments(
    api_token: &str,
    channel: &str,
    user: &str,
    attachments: serde_json::Value,
) -> Result<Value> {
    let attachments_str = attachments.to_string();
    let mut body = HashMap::new();
    body.insert("channel", channel);
    body.insert("attachments", &attachments_str);
    body.insert("user", user);
    // body.insert("as_user", "true");
    post(api_token, body, "https://slack.com/api/chat.postEphemeral").await
}

pub async fn add_reaction(
    api_token: &str,
    channel: &str,
    ts: &str,
    reaction: &str,
) -> Result<Value> {
    let mut body = HashMap::new();
    body.insert("name", reaction);
    body.insert("channel", channel);
    body.insert("timestamp", ts);
    post(api_token, body, "https://slack.com/api/reactions.add").await
}
