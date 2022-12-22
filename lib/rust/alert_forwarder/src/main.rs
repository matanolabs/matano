use aws_sdk_dynamodb::model::AttributeValue;
use bytes::Bytes;
use serde_dynamo::aws_sdk_dynamodb_0_21::{from_item, to_item};
use std::collections::HashMap;
use std::env::var;
use std::pin::Pin;

use anyhow::{anyhow, Context, Result};
use async_once::AsyncOnce;
use async_stream::stream;
use aws_lambda_events::event::s3::{S3Event, S3EventRecord};
use aws_lambda_events::event::sqs::SqsEvent;
use aws_sdk_sqs::model::SendMessageBatchRequestEntry;
use base64::decode;
use futures::future::join_all;
use futures::{Stream, StreamExt, TryStreamExt};
use lambda_runtime::{
    run, service_fn, Context as LambdaContext, Error as LambdaError, LambdaEvent,
};
use lazy_static::lazy_static;
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use serde_json::json;

use std::cell::RefCell;
use std::collections::BTreeMap;
use tokio::io::AsyncReadExt;
use tokio_util::codec::{FramedRead, LinesCodec};
use tokio_util::io::StreamReader;
// use chrono_tz::Tz;
use chrono::{DateTime, NaiveDateTime, Utc};

use async_compression::tokio::bufread::ZstdDecoder;

use ::value::Value;
use shared::secrets::load_secret;
use shared::vrl_util::vrl;
use shared::{load_log_sources_configuration_map, setup_logging, LogSourceConfiguration};
use vrl::{state, value};

pub mod slack;
use slack::{post_message, post_message_to_thread};

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

lazy_static! {
    static ref AWS_CONFIG: AsyncOnce<aws_config::SdkConfig> =
        AsyncOnce::new(async { aws_config::load_from_env().await });
    static ref SQS_CLIENT: AsyncOnce<aws_sdk_sqs::Client> =
        AsyncOnce::new(async { aws_sdk_sqs::Client::new(AWS_CONFIG.get().await) });
    static ref DYNAMODB_CLIENT: AsyncOnce<aws_sdk_dynamodb::Client> =
        AsyncOnce::new(async { aws_sdk_dynamodb::Client::new(AWS_CONFIG.get().await) });
    static ref DESTINATION_TO_CONFIGURATION_MAP: HashMap<String, serde_json::Value> = {
        let dest_config_map_str = var("DESTINATION_TO_CONFIGURATION_MAP").unwrap();
        let dest_config_map: HashMap<String, serde_json::Value> =
            serde_json::from_str(&dest_config_map_str).unwrap();
        dest_config_map
    };
    static ref DESTINATION_TO_SECRET_ARN_MAP: HashMap<String, String> = {
        let dest_secret_arn_map_str = var("DESTINATION_TO_SECRET_ARN_MAP").unwrap();
        let dest_secret_arn_map: HashMap<String, String> =
            serde_json::from_str(&dest_secret_arn_map_str).unwrap();
        dest_secret_arn_map
    };
}

thread_local! {
    pub static LOG_SOURCES_CONFIG: RefCell<BTreeMap<String, LogSourceConfiguration>> = {
        let log_sources_configuration_map = load_log_sources_configuration_map();
        RefCell::new(log_sources_configuration_map)
    };
}

const FLATTENED_CONTEXT_EXPANDER: &str = r#"
key_to_label = {
    "related.ip": ":mag: IP",
    "related.user": ":bust_in_silhouette: User",
    "related.hosts": ":globe_with_meridians: Host",
    "related.hash": ":hash: Hash",
}

context = .
. = {}

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

    .long_fmt = set!(object!(.long_fmt || {}), k_parts, value_long_str)
    .short_fmt = set!(object!(.short_fmt || {}), k_parts, value_short_str)
    .values = set!(object!(.values || {}), k_parts, values)
}
"#;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Alert {
    // pub alert_info: Value,
    pub id: String,

    pub creation_time: DateTime<Utc>,
    pub title: String,
    pub title_fmt: String,
    pub severity: String,
    pub severity_icon_url: String,
    pub runbook: String,
    pub false_positives_str: String,
    pub destinations: Vec<String>,
    pub related_strs: Vec<String>,
    pub context_strs: Vec<String>,
    pub context_values: Value,
    pub tables: Vec<String>,

    pub match_count: i64,
    pub update_count: i64,
    pub destination_to_alert_info: HashMap<String, serde_json::Value>,
}

#[tokio::main]
async fn main() -> Result<(), LambdaError> {
    setup_logging();

    let func = service_fn(handler);
    run(func).await?;

    Ok(())
}

/// Checks if object is definitely compressed (false negatives)
fn is_compressed(key: &str) -> bool {
    key.ends_with("gz") || key.ends_with("gzip") || key.ends_with("zst") || key.ends_with("zstd")
}

async fn get_secret_for_destination(destination_name: &str) -> Result<HashMap<String, String>> {
    let secret_arn = DESTINATION_TO_SECRET_ARN_MAP
        .get(destination_name)
        .unwrap()
        .to_owned();
    let secret = load_secret(secret_arn).await;
    secret
}

async fn handler(event: LambdaEvent<SqsEvent>) -> Result<()> {
    let bytes = event
        .payload
        .records
        .into_iter()
        .flat_map(|record| {
            let bytes = decode(
                record
                    .body
                    .unwrap_or("SQS message body is required".to_string()),
            )
            .unwrap();
            bytes
        })
        .collect::<Vec<_>>();

    let stream: tokio_stream::Iter<std::vec::IntoIter<Result<_, std::io::Error>>> =
        tokio_stream::iter(vec![Ok(bytes::Bytes::from(bytes))]);
    let reader = StreamReader::new(stream);

    let mut reader = ZstdDecoder::new(reader);
    reader.multiple_members(true);

    let lines = FramedRead::new(reader, LinesCodec::new()).map_err(|e| anyhow!(e));

    let _ = lines.try_for_each_concurrent(50, |l| async move {
        let mut ecs_alert_info_agg: Value = serde_json::from_str::<Value>(l.as_str())?.into();

        let wrapped_prog = r#"
            . = compact(.)

            combined_context = {}

            rule_matches, err = map_values(array!(.rule_matches.new)) -> |v| {
                context = del(v.matano.alert.context)
                # del(v.matano.alert.original_event)
                # del(v.matano.alert.original_event)
                # del(v.matano.alert.rule.match.id)
                # del(v.matano.alert.rule.destinations)
                # del(v.matano.alert.rule.deduplication_window)
                # del(v.matano.alert.dedupe)
                # del(v.matano.alert.breached)
                # del(matano.alert.first_matched_at)
                matano_alert_info = del(v.matano.alert)
                del(v.ts)
                del(v.event.id)
                del(v.event.created)
                del(v.event.kind)
                del(v.event.duration)
                # del(v.event.created)

                flatten(v)
            }

            for_each(rule_matches) -> |_i, m| {
                for_each(object!(m)) -> |k,v| {
                    merged = get(combined_context, [k]) ?? []
                    merged = flatten([merged, v])
                    combined_context = set!(combined_context, [k], merged)
                }
            }

            combined_context = map_values(combined_context) -> |v| {
                v = unique(v)
            }

            combined_context = compact(combined_context, recursive: true)

            . = combined_context

            matano_alert_info.match_count = length(rule_matches)
            matano_alert_info
            "#;
        let mut matano_alert_info= vrl(&wrapped_prog, &mut ecs_alert_info_agg)?.0;
        vrl(FLATTENED_CONTEXT_EXPANDER, &mut ecs_alert_info_agg)?;

        let alert_id = vrl(".id", &mut matano_alert_info)?.0.as_str().context("missing id")?.to_string();

        let alert_creation_time = vrl(r#"to_timestamp!(int!(.created) * 1000, "nanoseconds")"#, &mut matano_alert_info)?.0.as_timestamp_unwrap().to_owned();
        let alert_title = vrl(".title", &mut matano_alert_info)?.0.as_str().context("missing title")?.to_string();
        let alert_severity = vrl(".severity", &mut matano_alert_info)?.0.as_str().context("missing severity")?.to_string();
        let alert_runbook = vrl(".runbook || \"\"", &mut matano_alert_info)?.0.as_str().context("missing runbook")?.to_string();
        let alert_false_positives_str = vrl(
            r#"
            fps = array(.rule.false_positives) ?? []
            fps_str = join!(fps, "\n• ")
            if fps_str != "" && length(fps) > 1 {
                fps_str = "\n• " + fps_str
            }
            fps_str
            "#,
            &mut matano_alert_info
        )?.0.as_str().context("missing fp's")?.to_string();
        let alert_destinations = vrl(".destinations || []", &mut matano_alert_info)?.0.as_array_unwrap().into_iter().map(|v| v.as_str().unwrap().to_string()).collect::<Vec<_>>();
        let match_count = vrl(".match_count", &mut matano_alert_info)?.0.as_integer().unwrap();

        let title_fmt = match alert_severity.as_str() {
            "critical" => format!("💥 🚨  [{}] {}", alert_severity.to_uppercase(), alert_title),
            "high" => format!("🚨  [{}] {}", alert_severity.to_uppercase(), alert_title),
            "notice" | "info" => format!("📢  {}", alert_title),
            _ =>  format!("{}", alert_title),
        };

        let severity_icon_label = match alert_severity.as_str() {
            "critical" => "high",
            "notice" => "info",
            s => &s,
        };
        // TODO(shaeq): host these images properly
        let severity_icon_url = format!("https://gist.githubusercontent.com/shaeqahmed/6c38fc5f0c3adb7e1a3fe6c5f78bbc4f/raw/9a12ff8d23592b31f224f9e27503e77b843b075c/apple-sev-{}-icon.png", severity_icon_label);

        let related_strs = vrl(".short_fmt.related", &mut ecs_alert_info_agg)?.0;
        let related_strs = match related_strs {
            Value::Object(related_strs) => related_strs.into_values().map(|v| v.as_str().unwrap().to_string()).collect::<Vec<String>>(),
            _ => vec![],
        };
        let mut context_values = vrl(".values", &mut ecs_alert_info_agg)?.0;
        let context_strs = vrl("flatten!(.long_fmt)", &mut ecs_alert_info_agg)?.0;
        let context_strs = match context_strs {
            Value::Object(context_strs) => context_strs.into_values().map(|v| v.as_str().unwrap().to_string()).collect::<Vec<String>>(),
            _ => vec![],
        };

        let alert_tables = vrl(".matano.table", &mut context_values)?.0
            .as_array()
            .context("missing matano.table")?
            .into_iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect::<Vec<String>>();

        let dynamodb = DYNAMODB_CLIENT.get().await;

        let alert_tracker_table_name = var("ALERT_TRACKER_TABLE_NAME")?;
        let existing_alert_item = dynamodb.get_item().table_name(&alert_tracker_table_name).key("id",  AttributeValue::S(alert_id.clone())).send().await?.item;
        let mut existing_alert = match existing_alert_item {
            Some(existing_alert_item) => {
                let existing_alert = from_item::<Alert>(existing_alert_item).context("failed to deserialize existing alert")?;
                Some(existing_alert)
            },
            None => None,
        };
        let mut new_alert = Alert {
            // alert_info: matano_alert_info,
            id: alert_id,
            creation_time: alert_creation_time,
            title: alert_title,
            title_fmt: title_fmt,
            severity: alert_severity,
            severity_icon_url: severity_icon_url,
            runbook: alert_runbook,
            false_positives_str: alert_false_positives_str,
            destinations: alert_destinations,
            tables: alert_tables,
            match_count: match_count,
            update_count: 1,
            related_strs: related_strs,
            context_strs: context_strs,
            context_values: context_values,
            destination_to_alert_info: HashMap::new(),
        };

        println!("destinations: {:?}", new_alert.destinations);

        for destination in &new_alert.destinations {
            let dest_config = DESTINATION_TO_CONFIGURATION_MAP.get(destination);
            if dest_config.is_none() {
                continue;
            }
            let dest_config = dest_config.unwrap();
            let dest_type = dest_config["type"].as_str().unwrap();
            let dest_name = dest_config["name"].as_str().unwrap();
            println!("processing alert");

            if dest_type == "slack" {
                println!("sending slack alert");
                let channel = dest_config["properties"]["channel"].as_str().unwrap();
                let client_id = dest_config["properties"]["client_id"].as_str().unwrap();
                let res = publish_alert_to_slack(dest_name, &existing_alert, &new_alert, channel, client_id).await.unwrap();
                println!("published to slack: {:?}", res);
                new_alert.destination_to_alert_info.entry(destination.to_string()).or_insert(res);
            }
        }

        if existing_alert.is_none() {
            existing_alert = Some(new_alert.clone());
        } else {
            existing_alert = existing_alert.map(|mut a| {
                a.match_count += new_alert.match_count;
                a.update_count += 1;
                a
            });
        }

        let existing_alert_item = Some(to_item(&existing_alert)?);
        let res = dynamodb.put_item().table_name(&alert_tracker_table_name).set_item(existing_alert_item).send().await?;

        Ok(())
    }).await?;

    println!("DONE");

    Ok(())
}

async fn publish_alert_to_slack(
    dest_name: &str,
    existing_alert: &Option<Alert>,
    alert: &Alert,
    channel: &str,
    client_id: &str,
) -> Result<serde_json::Value> {
    let api_token = &get_secret_for_destination(dest_name).await?["bot_user_oauth_token"];

    let mut res = json!(null);

    match existing_alert {
        Some(existing_alert) => {
            println!("existing alert");

            let existing_dest_info = existing_alert
                .destination_to_alert_info
                .get(dest_name)
                .unwrap();
            let thread_ts = existing_dest_info["ts"].as_str().unwrap();

            let compute_new_context = r#"
            new_context = {}

            .a = flatten(object!(.a))
            .b = flatten(object!(.b))

            for_each(.b) -> |k, v| {
                a_v = array!(get(.a, [k]) ?? [])
                a_v_set = {}
                for_each(a_v) -> |_i, f| {
                  a_v_set = set!(a_v_set, [f], true)
                }
                v_new = []
                for_each(array!(v)) -> |_i, f| {
                  exists = get(a_v_set, [f]) ?? false
                  exists = if exists == true {
                    true
                  } else {
                    false
                  }
                  if !exists {
                    v_new = push(v_new, f)
                  }
                }
                new_context = set!(new_context, [k], v_new)
            }
            compact(new_context)"#;
            let existing_context = existing_alert.context_values.to_owned();
            let incoming_context = alert.context_values.to_owned();
            let mut new_context = vrl(
                compute_new_context,
                &mut value!({
                    "a": existing_context,
                    "b": incoming_context,
                }),
            )?
            .0;
            vrl(FLATTENED_CONTEXT_EXPANDER, &mut new_context)?;
            let new_context_strs = vrl("flatten(.long_fmt) ?? {}", &mut new_context)?.0;
            let new_context_strs = match new_context_strs {
                Value::Object(context_strs) => context_strs
                    .into_values()
                    .map(|v| v.as_str().unwrap().to_string())
                    .collect::<Vec<String>>(),
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

            if new_context_strs.len() > 0 {
                blocks.as_array_mut().unwrap().insert(
                    2,
                    json!({
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "*New context*"
                        },
                    }),
                );
                blocks.as_array_mut().unwrap().insert(
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
                blocks.as_array_mut().unwrap().insert(
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

            let blocks_str = serde_json::to_string(&blocks).unwrap();

            println!("{}", blocks.to_string());

            res = post_message_to_thread(api_token, channel, thread_ts, &blocks_str).await?;

            if !res["ok"].as_bool().unwrap() {
                return Err(anyhow!(
                    "Failed to publish alert context to Slack: {}",
                    res["error"].as_str().unwrap()
                ));
            }
        }
        None => {
            let alert_creation_time_str = format!(
                "<!date^{}^{{date_long_pretty}} {{time}} (local time)|{}> ",
                alert.creation_time.timestamp(),
                alert.creation_time.to_rfc2822()
            );

            println!("new alert");

            let mut blocks = json!([
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "emoji": true,
                            "text": alert.title_fmt,
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
                    {
                        "type": "context",
                        "elements": [
                            {
                                "type": "mrkdwn",
                                "text": alert.related_strs.join("\n\n")
                            }
                        ]
                    }
            ]);

            if alert.false_positives_str != "" {
                blocks.as_array_mut().unwrap().insert(
                    5,
                    json!({
                        "type": "context",
                        "elements": [
                            {
                                "type": "mrkdwn",
                                "text": format!("*False positives:* {}", alert.false_positives_str)
                            }
                        ]
                    }),
                );
            }

            let blocks_str = serde_json::to_string(&blocks).unwrap();

            println!("{}", blocks.to_string());

            res = post_message(api_token, channel, &blocks_str).await?;

            if !res["ok"].as_bool().unwrap() {
                return Err(anyhow!(
                    "Failed to publish alert to Slack: {}",
                    res["error"].as_str().unwrap()
                ));
            }

            let context_values_json: serde_json::Value =
                alert.context_values.to_owned().try_into().unwrap();
            let context_values_json_str =
                serde_json::to_string_pretty(&context_values_json).unwrap();

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
                                "text": alert.context_strs.join("\n\n")
                            }
                        ]
                    }
            ]);

            let blocks_str = serde_json::to_string(&blocks).unwrap();

            println!("{}", blocks.to_string());

            let res2 = post_message_to_thread(
                api_token,
                channel,
                res["ts"].as_str().unwrap(),
                &blocks_str,
            )
            .await?;

            if !res2["ok"].as_bool().unwrap() {
                return Err(anyhow!(
                    "Failed to publish alert context to Slack: {}",
                    res2["error"].as_str().unwrap()
                ));
            }
        }
    }

    Ok(res)
}
