use crate::{get_secret_for_destination, AlertCDCPayload};
use ::value::Value;
use anyhow::{anyhow, Context, Ok, Result};
use log::{debug, error, info};
use serde_json::json;
use shared::vrl_util::vrl;
use std::collections::HashMap;

use async_trait::async_trait;

use super::SendAlert;

#[derive(Clone)]
pub struct SlackForwarder {}

#[async_trait]

impl SendAlert for SlackForwarder {
    async fn send_alert(
        self,
        alert_payload: AlertCDCPayload,
        destination_name: String,
        config: serde_json::Value,
        dest_info: Option<serde_json::Value>,
    ) -> Result<serde_json::Value> {
        let mut alert_payload = alert_payload.clone();
        let channel = config["properties"]["channel"]
            .as_str()
            .context("channel not found")?;
        let client_id = config["properties"]["client_id"]
            .as_str()
            .context("client_id not found")?;

        let res = publish_alert_to_slack(
            &mut alert_payload,
            &destination_name,
            dest_info,
            channel,
            client_id,
        )
        .await?;
        Ok(res)
    }
}

// Slack formatting helpers
const CONTEXT_TO_STR_FMT: &str = r#"
key_to_label = {
    "related.ip": ":mag: IP",
    "related.user": ":bust_in_silhouette: User",
    "related.hosts": ":globe_with_meridians: Host",
    "related.hash": ":hash: Hash",
}

context = flatten(.)
ret = {}

for_each(context) -> |k, v| {
    label = get(key_to_label, [k]) ?? null
    values = array!(v)
    value_str_prefix = if label != null { label } else { k }

    value_str_prefix, err = "*" + to_string(value_str_prefix) + ":* "
    vals, err = map_values(values) -> |v| {
        "`" + to_string(v) + "`"
    }
    more_count_short = length(vals) - 5
    values_short = slice!(vals, 0, 5)
    value_short_str = value_str_prefix + join!(values_short, "  ")
    if more_count_short > 0 {
        value_short_str = value_short_str + " +" + to_string(more_count_short) + " more..."
    }

    more_count_long = length(vals) - 25
    values_long = slice!(vals, 0, 25)
    value_long_str = value_str_prefix + join!(values_long, "  ")
    if more_count_long > 0 {
        value_long_str = value_long_str + " +" + to_string(more_count_long) + " more..."
    }

    k_parts = split(k, ".")

    ret.long_fmt = set!(object!(ret.long_fmt || {}), k_parts, value_long_str)
    ret.short_fmt = set!(object!(ret.short_fmt || {}), k_parts, value_short_str)
}

ret
"#;

// Slack API helpers
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
    Ok(v)
}

pub async fn post_message(
    api_token: &str,
    channel: &str,
    blocks: &str,
) -> Result<serde_json::Value> {
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
) -> Result<serde_json::Value> {
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
) -> Result<serde_json::Value> {
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
) -> Result<serde_json::Value> {
    let mut body = HashMap::new();
    body.insert("name", reaction);
    body.insert("channel", channel);
    body.insert("timestamp", ts);
    post(api_token, body, "https://slack.com/api/reactions.add").await
}

// Main Slack alert publishing function
pub async fn publish_alert_to_slack(
    alert_payload: &mut AlertCDCPayload,
    dest_name: &str,
    existing_dest_info: Option<serde_json::Value>,
    channel: &str,
    client_id: &str,
) -> Result<serde_json::Value> {
    info!("Publishing alert to slack...");
    let api_token = &get_secret_for_destination(dest_name).await?["bot_user_oauth_token"];

    let mut res = json!(null);
    let alert = &alert_payload.updated_alert;

    let alert_title = &alert.title;
    let alert_severity = &alert.severity;
    let title_fmt = match alert_severity.as_str() {
        "critical" => format!("💥 🚨  [{}] {}", alert_severity.to_uppercase(), alert_title),
        "high" => format!("🚨  [{}] {}", alert_severity.to_uppercase(), alert_title),
        "notice" | "info" => format!("📢  {}", alert_title),
        _ => format!("{}", alert_title),
    };

    if alert.update_count > 0 {
        info!("Slack: processing existing alert");

        let existing_dest_info = existing_dest_info.context("no existing_dest_info")?;
        let thread_ts = existing_dest_info["ts"].as_str().context("no thread_ts")?;

        let mut context_diff_fmt = vrl(CONTEXT_TO_STR_FMT, &mut alert_payload.context_diff)?.0;

        let new_context_strs = vrl("flatten(.long_fmt) ?? {}", &mut context_diff_fmt)?.0;
        let new_context_strs = match new_context_strs {
            Value::Object(context_strs) => context_strs
                .into_values()
                .map(|v| v.as_str().map(|s| s.to_string()))
                .collect::<Option<Vec<_>>>()
                .context("new_context_strs")?,
            _ => vec![],
        };
        // let new_context_values = vrl(".values || {}", &mut new_context)?.0;
        // let new_context_values_json: serde_json::Value = new_context_values.try_into().unwrap();
        // let new_context_values_json_str = serde_json::to_string_pretty(&new_context_values_json).unwrap();
        let mut blocks = json!([
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "emoji": true,
                    "text": format!("➕ {} new rule matches", alert.match_count)
                }
            },
            {
                "type": "divider"
            }
        ]);

        let blocks_mut = blocks.as_array_mut().context("blocks")?;

        if new_context_strs.len() > 0 {
            blocks_mut.insert(
                2,
                json!({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "*New context*"
                    },
                }),
            );
            blocks_mut.insert(
                3,
                json!({
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": new_context_strs.join("\n\n")
                        }
                    ]
                }),
            );
        } else {
            blocks_mut.insert(
                2,
                json!({
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": "No new context"
                        }
                    ]
                }),
            );
        }

        let blocks_str = serde_json::to_string(&blocks).context("blocks_str")?;

        res = post_message_to_thread(api_token, channel, thread_ts, &blocks_str).await?;

        if !res["ok"].as_bool().context("no ok")? {
            return Err(anyhow!(
                "Failed to publish alert context to Slack: {}",
                res["error"].as_str().unwrap_or_default()
            ));
        }
    } else {
        let alert_creation_time_str = format!(
            "<!date^{}^{{date_long_pretty}} {{time}} (local time)|{}> ",
            alert.activated_creation_time().timestamp(),
            alert.activated_creation_time().to_rfc2822()
        );

        let mut context_fmt = vrl(
            CONTEXT_TO_STR_FMT,
            &mut alert_payload.incoming_rule_matches_context,
        )?
        .0;

        let related_strs = vrl(".short_fmt.related", &mut context_fmt)?.0;
        let related_strs = match related_strs {
            Value::Object(related_strs) => related_strs
                .into_values()
                .map(|v| v.as_str().map(|s| s.to_string()))
                .collect::<Option<Vec<_>>>()
                .context("related_strs")?,
            _ => vec![],
        };
        let context_values = vrl(".values", &mut context_fmt)?.0;

        let context_strs = vrl("flatten!(.long_fmt)", &mut context_fmt)?.0;
        let context_strs = match context_strs {
            Value::Object(context_strs) => context_strs
                .into_values()
                .map(|v| v.as_str().map(|s| s.to_string()))
                .collect::<Option<Vec<_>>>()
                .context("context_strs")?,
            _ => vec![],
        };

        let mut blocks = json!([
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "emoji": true,
                        "text": title_fmt,
                    }
                },
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "image",
                            "image_url": alert.severity_icon_url,
                            "alt_text": &alert.severity
                        },
                        {
                            "type": "mrkdwn",
                            "text": format!("Severity: *{}*", &alert.severity)
                        },
                        {
                            "type": "mrkdwn",
                            "text": format!("Match count: *{}*", alert.match_count)
                        },
                        {
                            "type": "mrkdwn",
                            "text": format!("Table: *{}*", alert.tables.join(","))
                        }
                    ]
                },
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": format!("*Alert ID:* {}", alert.id)
                        },
                        {
                            "type": "mrkdwn",
                            "text": format!("*Created:* {}", alert_creation_time_str)
                        }
                    ]
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": format!("*Runbook:* {}", &alert.runbook)
                    }
                },
                {
                    "type": "divider"
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "*Context*"
                    },
                    // "accessory": {
                    //     "type": "button",
                    //     "text": {
                    //         "type": "plain_text",
                    //         "emoji": true,
                    //         "text": "View alert details"
                    //     },
                    //     "value": "click_me_123"
                    // }
                },
        ]);

        let blocks_mut = blocks.as_array_mut().context("blocks")?;
        if related_strs.len() > 0 {
            blocks_mut.push(json!({
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": related_strs.join("\n\n")
                    }
                ]
            }));
        }

        let mut alert_false_positives_value: Value = alert.false_positives.clone().into();
        let alert_false_positives_str = vrl(
            r#"
                fps = array(.) ?? []
                fps_str = join!(fps, "\n• ")
                if fps_str != "" && length(fps) > 1 {
                    fps_str = "\n• " + fps_str
                }
                fps_str
                "#,
            &mut alert_false_positives_value,
        )?
        .0
        .as_str()
        .context("missing fp's")?
        .to_string();

        if alert_false_positives_str != "" {
            blocks_mut.insert(
                5,
                json!({
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": format!("*False positives:* {}", alert_false_positives_str)
                        }
                    ]
                }),
            );
        }

        let blocks_str = serde_json::to_string(&blocks).context("blocks_str")?;

        res = post_message(api_token, channel, &blocks_str).await?;

        if !res["ok"].as_bool().context("ok")? {
            return Err(anyhow!(
                "Failed to publish alert to Slack: {}",
                res["error"].as_str().unwrap_or_default()
            ));
        }

        let context_values_json: serde_json::Value = alert
            .context
            .to_owned()
            .try_into()
            .map_err(|_| anyhow!("alert_context"))
            .context("")?;
        let context_values_json_str = serde_json::to_string_pretty(&context_values_json)
            .context("context_values_json_str")?;

        let blocks = json!([
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "emoji": true,
                        "text": format!("ℹ️ Context details for initial rule matches")
                    }
                },
                // {
                //     "type": "divider"
                // },
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": context_strs.join("\n\n")
                        }
                    ]
                }
        ]);

        let blocks_str = serde_json::to_string(&blocks).context("blocks_str")?;

        let res2 = post_message_to_thread(
            api_token,
            channel,
            res["ts"].as_str().context("ts")?,
            &blocks_str,
        )
        .await?;

        if !res2["ok"].as_bool().context("ok")? {
            return Err(anyhow!(
                "Failed to publish alert context to Slack: {}",
                res2["error"].as_str().unwrap_or_default()
            ));
        }
    }

    Ok(res)
}
