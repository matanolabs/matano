use aws_lambda_events::sqs::SqsEvent;
use aws_sdk_dynamodb::model::AttributeValue;
use serde_dynamo::aws_sdk_dynamodb_0_21::{from_item, to_item};
use serde_json::json;
use std::collections::HashMap;
use std::env::var;
use std::sync::Arc;

use anyhow::{anyhow, Context, Ok, Result};
use async_once::AsyncOnce;
use base64::decode;
use futures::{Stream, StreamExt, TryStreamExt};
use lambda_runtime::{
    run, service_fn, Context as LambdaContext, Error as LambdaError, LambdaEvent,
};
use lazy_static::lazy_static;
use log::{debug, error, info};
use serde::{Deserialize, Serialize};

use std::collections::BTreeMap;
use tokio_util::codec::{FramedRead, LinesCodec};
use tokio_util::io::StreamReader;
// use chrono_tz::Tz;
use chrono::{DateTime, NaiveDateTime, Utc};

use async_compression::tokio::bufread::ZstdDecoder;

use ::value::Value;
use shared::secrets::load_secret;
use shared::setup_logging;
use shared::vrl_util::vrl;
use vrl::{state, value};

pub mod slack;
use slack::publish_alert_to_slack;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

lazy_static! {
    static ref AWS_CONFIG: AsyncOnce<aws_config::SdkConfig> =
        AsyncOnce::new(async { aws_config::load_from_env().await });
    static ref SQS_CLIENT: AsyncOnce<aws_sdk_sqs::Client> =
        AsyncOnce::new(async { aws_sdk_sqs::Client::new(AWS_CONFIG.get().await) });
    static ref SNS_CLIENT: AsyncOnce<aws_sdk_sns::Client> =
        AsyncOnce::new(async { aws_sdk_sns::Client::new(AWS_CONFIG.get().await) });
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

const FLATTENED_CONTEXT_EXPANDER: &str = r#"
context = .
. = {}

for_each(context) -> |k, v| {
    values = array!(v)

    k_parts = split(k, ".")

    .values = set!(object!(.values || {}), k_parts, values)
}
"#;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Alert {
    pub id: String,

    pub creation_time: DateTime<Utc>,
    pub title: String,
    pub severity: String,
    pub severity_icon_url: String,
    pub runbook: String,
    pub false_positives: Vec<String>,
    pub destinations: Vec<String>,
    pub context: Value,
    pub tables: Vec<String>,

    pub match_count: i64,
    pub update_count: i64,
    pub destination_to_alert_info: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AlertCDCPayload {
    pub updated_alert: Alert,
    pub incoming_rule_matches_context: Value,
    pub context_diff: Value,
}

#[tokio::main]
async fn main() -> Result<(), LambdaError> {
    setup_logging();

    let func = service_fn(handler);
    run(func).await?;

    core::result::Result::Ok(())
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
        tokio_stream::iter(vec![Ok(bytes::Bytes::from(bytes))
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))]);
    let reader = StreamReader::new(stream);

    let mut reader = ZstdDecoder::new(reader);
    reader.multiple_members(true);

    let lines = FramedRead::new(reader, LinesCodec::new()).map_err(|e| anyhow!(e));

    let _ = lines
        .try_for_each_concurrent(50, |l| async move {
            let mut ecs_alert_info_agg: Value = serde_json::from_str::<Value>(l.as_str())?.into();

            let mut alert_cdc_payload =
                merge_incoming_rule_matches_to_alert_and_compute_cdc(&mut ecs_alert_info_agg)
                    .await?;
            // arc it

            let mut updated_alert = alert_cdc_payload.updated_alert.clone();

            println!("destinations: {:?}", updated_alert.destinations);

            for destination in &updated_alert.destinations {
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
                    let res = publish_alert_to_slack(
                        &mut alert_cdc_payload,
                        dest_name,
                        channel,
                        client_id,
                    )
                    .await
                    .unwrap();

                    updated_alert
                        .destination_to_alert_info
                        .entry(destination.to_string())
                        .or_insert(res);
                }
            }

            alert_cdc_payload.updated_alert = updated_alert;

            // update alert in dynamodb
            let dynamodb = DYNAMODB_CLIENT.get().await;
            let alert_tracker_table_name = var("ALERT_TRACKER_TABLE_NAME")?;
            let updated_alert_item = Some(to_item(&alert_cdc_payload.updated_alert)?);
            let res = dynamodb
                .put_item()
                .table_name(&alert_tracker_table_name)
                .set_item(updated_alert_item)
                .send()
                .await?;

            // cast each Value to String in alert_cdc_payload.updated_alert.destination_to_alert_info
            let mut destination_to_alert_info = HashMap::new();
            for (k, v) in alert_cdc_payload.updated_alert.destination_to_alert_info {
                destination_to_alert_info.insert(k, json!(v.to_string()));
            }
            let updated_alert = Alert {
                destination_to_alert_info,
                ..alert_cdc_payload.updated_alert
            };
            let alert_cdc_payload = AlertCDCPayload {
                updated_alert,
                ..alert_cdc_payload
            };
            // send alert change stream payload to alerting sns topic
            let sns = SNS_CLIENT.get().await;
            let alerting_sns_topic_arn = var("ALERTING_SNS_TOPIC_ARN")?;
            let res = sns
                .publish()
                .topic_arn(&alerting_sns_topic_arn)
                .message(serde_json::to_string(&alert_cdc_payload)?)
                .send()
                .await?;

            info!("published to sns: {:?}", res);

            Ok(())
        })
        .await?;

    println!("DONE");

    Ok(())
}

// returns AlertCDCPayload { merged_alert, incoming_rule_matches_context, context_diff }
async fn merge_incoming_rule_matches_to_alert_and_compute_cdc(
    incoming_ecs_alert_info_agg: &mut Value,
) -> Result<AlertCDCPayload> {
    let mut merged_ecs_alert_context: Value = incoming_ecs_alert_info_agg.clone();

    // TODO(shaeq): make this cleaner by not modifying the input value
    let wrapped_prog = r#"
        . = compact(.)

        combined_context = flatten(.existing_context) ?? {}

        rule_matches, err = map_values(array!(.rule_matches.new)) -> |v| {
            context = del(v.matano.alert.context)
            # del(v.matano.alert.original_event)
            # del(v.matano.alert.original_event)
            # del(v.matano.alert.rule.match.id)
            # del(v.matano.alert.rule.destinations)
            # del(v.matano.alert.rule.deduplication_window)
            # del(v.matano.alert.dedupe)
            # del(v.matano.alert.activated)
            # del(matano.alert.first_matched_at)
            matano_alert_info = del(v.matano.alert)
            del(v.ts)
            del(v.event.id)
            del(v.event.created)
            del(v.event.kind)
            del(v.event.duration)

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
            v = unique(array!(v))
        }

        combined_context = compact(combined_context, recursive: true)

        . = combined_context

        matano_alert_info.match_count = length(rule_matches)
        matano_alert_info
        "#;
    let mut matano_alert_info = vrl(&wrapped_prog, incoming_ecs_alert_info_agg)?.0;
    vrl(FLATTENED_CONTEXT_EXPANDER, incoming_ecs_alert_info_agg)?;

    let alert_id = vrl(".id", &mut matano_alert_info)?
        .0
        .as_str()
        .context("missing id")?
        .to_string();

    // lookup existing alert
    let dynamodb = DYNAMODB_CLIENT.get().await;

    let alert_tracker_table_name = var("ALERT_TRACKER_TABLE_NAME")?;
    let merged_alert_item = dynamodb
        .get_item()
        .table_name(&alert_tracker_table_name)
        .key("id", AttributeValue::S(alert_id.clone()))
        .send()
        .await?
        .item;
    let mut merged_alert = match merged_alert_item {
        Some(merged_alert_item) => {
            let merged_alert = from_item::<Alert>(merged_alert_item)
                .context("failed to deserialize existing alert")?;
            Some(merged_alert)
        }
        None => None,
    };

    // then computed merged context
    merged_ecs_alert_context
        .as_object_mut_unwrap()
        .entry("combined_context".to_owned())
        .or_insert(match merged_alert {
            Some(ref merged_alert) => merged_alert.context.clone(),
            None => Value::Null,
        });
    vrl(&wrapped_prog, &mut merged_ecs_alert_context)?.0;
    vrl(FLATTENED_CONTEXT_EXPANDER, &mut merged_ecs_alert_context)?;
    let merged_context_values = vrl(".values", &mut merged_ecs_alert_context)?.0;

    // now we have the alert info, the new context, and the merged context
    let alert_creation_time = vrl(
        r#"to_timestamp!(int!(.created) * 1000, "nanoseconds")"#,
        &mut matano_alert_info,
    )?
    .0
    .as_timestamp_unwrap()
    .to_owned();
    let alert_title = vrl(".title", &mut matano_alert_info)?
        .0
        .as_str()
        .context("missing title")?
        .to_string();
    let alert_severity = vrl(".severity", &mut matano_alert_info)?
        .0
        .as_str()
        .context("missing severity")?
        .to_string();
    let alert_runbook = vrl(".runbook || \"\"", &mut matano_alert_info)?
        .0
        .as_str()
        .context("missing runbook")?
        .to_string();
    let alert_false_positives = vrl(".false_positives || []", &mut matano_alert_info)?
        .0
        .as_array_unwrap()
        .into_iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    let alert_false_positives_str = vrl(
        r#"
        fps = array(.rule.false_positives) ?? []
        fps_str = join!(fps, "\n• ")
        if fps_str != "" && length(fps) > 1 {
            fps_str = "\n• " + fps_str
        }
        fps_str
        "#,
        &mut matano_alert_info,
    )?
    .0
    .as_str()
    .context("missing fp's")?
    .to_string();
    let alert_destinations = vrl(".destinations || []", &mut matano_alert_info)?
        .0
        .as_array_unwrap()
        .into_iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    let match_count = vrl(".match_count", &mut matano_alert_info)?
        .0
        .as_integer()
        .unwrap();

    let severity_icon_label = match alert_severity.as_str() {
        "critical" => "high",
        "notice" => "info",
        s => &s,
    };
    // TODO(shaeq): host these images properly
    let severity_icon_url = format!("https://gist.githubusercontent.com/shaeqahmed/6c38fc5f0c3adb7e1a3fe6c5f78bbc4f/raw/9a12ff8d23592b31f224f9e27503e77b843b075c/apple-sev-{}-icon.png", severity_icon_label);

    let mut context_values = vrl(".values", incoming_ecs_alert_info_agg)?.0;

    let alert_tables = vrl(".matano.table", &mut context_values)?
        .0
        .as_array()
        .context("missing matano.table")?
        .into_iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect::<Vec<String>>();

    let new_alert = Alert {
        id: alert_id,
        creation_time: alert_creation_time,
        title: alert_title,
        severity: alert_severity,
        severity_icon_url: severity_icon_url,
        runbook: alert_runbook,
        false_positives: alert_false_positives,
        destinations: alert_destinations,
        tables: alert_tables,
        match_count: match_count,
        update_count: 0,
        context: context_values,
        destination_to_alert_info: HashMap::new(),
    };

    // compute a contextual diff
    let compute_context_diff_prog = r#"
    context_diff = {}

    .a = flatten(object!(.a))
    .b = flatten(object!(.b))

    for_each(.b) -> |k, v| {
        a_v = array!( (get(.a, [k]) ?? []) || [] )
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
        context_diff = set!(context_diff, [k], v_new)
    }
    compact(context_diff)"#;
    let existing_context = if merged_alert.is_some() {
        merged_alert.as_ref().unwrap().context.clone()
    } else {
        Value::Object(BTreeMap::new())
    };
    let incoming_context = new_alert.context.clone();
    let mut context_diff = vrl(
        compute_context_diff_prog,
        &mut value!({
            "a": existing_context,
            "b": incoming_context,
        }),
    )?
    .0;
    vrl(FLATTENED_CONTEXT_EXPANDER, &mut context_diff)?;
    let context_diff_values = vrl(".values || {}", &mut context_diff)?.0;

    // merge the new rule matches into the existing alert to get a merged alert
    if merged_alert.is_none() {
        merged_alert = Some(new_alert.clone());
    } else {
        merged_alert = merged_alert.map(|mut a| {
            a.match_count += new_alert.match_count;
            a.update_count += 1;
            a.context = merged_context_values;
            a
        });
    }

    let merged_alert = merged_alert.unwrap();

    Ok(AlertCDCPayload {
        updated_alert: merged_alert,
        incoming_rule_matches_context: new_alert.context,
        context_diff: context_diff_values,
    })
}
