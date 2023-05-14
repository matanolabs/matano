use aws_lambda_events::sqs::SqsEvent;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_sns::types::PublishBatchRequestEntry;
use bytes::Bytes;
use futures_util::future::{join_all, try_join, try_join_all};
use futures_util::{FutureExt, TryFutureExt};
use serde_dynamo::aws_sdk_dynamodb_0_25::{from_item, to_item};
use serde_json::json;
use shared::alert_util::encode_alerts_for_publish;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Context, Result};
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

use async_compression::tokio::bufread::ZstdDecoder;

use ::value::Value;
use shared::setup_logging;
use shared::vrl_util::vrl;
use shared::{alert_util::*, ArcMutexExt};
use vrl::{state, value};

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
    static ref RULE_MATCHES_QUEUE_URL: String = std::env::var("RULE_MATCHES_QUEUE_URL").unwrap();
    static ref RULE_MATCHES_DLQ_URL: String = std::env::var("RULE_MATCHES_DLQ_URL").unwrap();
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

#[tokio::main]
async fn main() -> Result<(), LambdaError> {
    setup_logging();

    let func = service_fn(handler);
    run(func).await?;

    Ok(())
}

async fn get_cdc_payload(l: String) -> Result<AlertCDCPayload> {
    let mut ecs_alert_info_agg = serde_json::from_str::<Value>(l.as_str())?;

    let cdc_payload =
        merge_incoming_rule_matches_to_alert_and_compute_cdc(&mut ecs_alert_info_agg).await?;

    Ok(cdc_payload)
}

async fn write_alert_ddb(alert: &Alert) -> Result<()> {
    let dynamodb = DYNAMODB_CLIENT.get().await;
    let alert_tracker_table_name = std::env::var("ALERT_TRACKER_TABLE_NAME")?;
    let alert_item = to_item(alert)?;

    let mut expr_values: HashMap<String, AttributeValue> =
        [(":alert".to_string(), AttributeValue::M(alert_item))]
            .into_iter()
            .collect();
    let mut update_expr = "SET alert = :alert".to_string();
    // set empty alert info column if create (so can set nested later)
    if alert.update_count == 0 {
        info!("Initial create of alert: {}", alert.id);
        update_expr += ", destination_alert_info = :info";
        expr_values.insert(":info".to_string(), AttributeValue::M(HashMap::new()));
    } else {
        info!("Updating alert: {}", alert.id);
    }

    dynamodb
        .update_item()
        .table_name(&alert_tracker_table_name)
        .key("id", AttributeValue::S(alert.id.to_string()))
        .update_expression(update_expr)
        .set_expression_attribute_values(Some(expr_values))
        .send()
        .await?;
    Ok(())
}

async fn publish_sns_batch(
    topic_arn: String,
    entries: Vec<PublishBatchRequestEntry>,
) -> Result<()> {
    let sns = SNS_CLIENT.get().await;

    let futs = entries
        .chunks(10)
        .into_iter()
        .map(|chunk| {
            sns.publish_batch()
                .topic_arn(&topic_arn)
                .set_publish_batch_request_entries(Some(chunk.to_vec()))
                .send()
        })
        .collect::<Vec<_>>();

    try_join_all(futs).await?;

    Ok(())
}

async fn publish_alert_cdc_batch(entries: Vec<(AlertCDCPayload, Vec<String>)>) -> Result<()> {
    let alerting_sns_topic_arn = std::env::var("ALERTING_SNS_TOPIC_ARN")?;
    let internal_alerting_sns_topic_arn = std::env::var("INTERNAL_ALERTS_TOPIC_ARN")?;

    let internal_entries = entries
        .iter()
        .flat_map(|(p, destinations)| {
            destinations.iter().map(|d| InternalAlertCDCPayload {
                payload: p.clone(),
                destination: d.clone(),
            })
        })
        .map(|p| {
            anyhow::Ok((
                serde_json::to_string(&p)?,
                format!("{}#{}", &p.payload.updated_alert.id, &p.destination),
            ))
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(|(msg, group_id)| {
            PublishBatchRequestEntry::builder()
                .message(msg)
                .id(uuid::Uuid::new_v4().to_string())
                .message_group_id(group_id)
                .build()
        })
        .collect::<Vec<_>>();

    let main_entries = entries
        .into_iter()
        .map(|(p, _)| serde_json::to_string(&p))
        .collect::<Result<Vec<String>, _>>()?
        .into_iter()
        .map(|msg| {
            PublishBatchRequestEntry::builder()
                .message(msg)
                .id(uuid::Uuid::new_v4().to_string())
                .build()
        })
        .collect::<Vec<_>>();

    let main_fut = publish_sns_batch(alerting_sns_topic_arn, main_entries);
    let internal_fut = publish_sns_batch(internal_alerting_sns_topic_arn, internal_entries);

    try_join(main_fut, internal_fut).await?;

    Ok(())
}

async fn publish_rule_matches_sqs(
    queue_url: String,
    messages: Vec<serde_json::Value>,
) -> Result<()> {
    let sqs = SQS_CLIENT.get().await;

    let messages = encode_alerts_for_publish(messages.iter())?;
    let futs = messages.into_iter().map(|s| {
        sqs.send_message()
            .queue_url(&queue_url)
            .message_group_id(RULE_MATCHES_GROUP_ID)
            .message_body(s)
            .send()
    });
    try_join_all(futs).await?;

    Ok(())
}

async fn redrive_failed_alerts(failures: Vec<String>) -> Result<()> {
    info!("Redriving {} failed alerts: ", failures.len());

    let (dlq_failures, retry_failures): (Vec<_>, Vec<_>) = failures
        .iter()
        .map(|l| {
            let mut v: serde_json::Value = serde_json::from_str(l).context("err invalid json")?;
            let prev_retry_count = v["retry_count"].as_u64().unwrap_or(0);
            let new_retry_count = prev_retry_count + 1;
            v["retry_count"] = new_retry_count.into();
            anyhow::Ok((v, new_retry_count))
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .partition(|(_, retry_count)| *retry_count > 3);

    let dlq_failures = dlq_failures.into_iter().map(|(v, _)| v).collect::<Vec<_>>();
    let retry_failures = retry_failures
        .into_iter()
        .map(|(v, _)| v)
        .collect::<Vec<_>>();

    let dlq_count = dlq_failures.len();
    let retry_count = retry_failures.len();
    if dlq_count > 0 {
        info!("Sending {} alerts to DLQ", dlq_count);
    }
    if retry_count > 0 {
        info!("Sending {} alerts for retry", retry_count);
    }

    let futs = vec![
        (RULE_MATCHES_DLQ_URL.clone(), dlq_failures),
        (RULE_MATCHES_QUEUE_URL.clone(), retry_failures),
    ]
    .into_iter()
    .filter(|(_, v)| !v.is_empty())
    .map(|(queue_url, messages)| publish_rule_matches_sqs(queue_url, messages));

    try_join_all(futs).await?;

    Ok(())
}

async fn process_alert_line(l: String) -> Result<(AlertCDCPayload, Vec<String>)> {
    let alert_cdc_payload = get_cdc_payload(l).await?;
    let alert = alert_cdc_payload.updated_alert.clone();
    info!("Processing alert with ID: {}", &alert.id);
    let destinations = alert.destinations.clone();

    // update alert in dynamodb
    write_alert_ddb(&alert).await?;

    Ok((alert_cdc_payload, destinations))
}

fn get_alert_lines(
    bytes: Vec<u8>,
) -> Pin<Box<dyn Stream<Item = Result<String, anyhow::Error>> + std::marker::Send>> {
    let stream: tokio_stream::Iter<std::vec::IntoIter<Result<_, std::io::Error>>> =
        tokio_stream::iter(vec![anyhow::Ok(Bytes::from(bytes))
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))]);
    let reader = StreamReader::new(stream);

    let mut reader = ZstdDecoder::new(reader);
    reader.multiple_members(true);

    let lines = FramedRead::new(reader, LinesCodec::new()).map_err(|e| anyhow!(e));
    lines.boxed()
}

async fn handler(event: LambdaEvent<SqsEvent>) -> Result<()> {
    let bytes = event
        .payload
        .records
        .into_iter()
        .flat_map(|record| record.body.and_then(|b| Some(decode(b).ok()?)))
        .flatten()
        .collect::<Vec<_>>();

    let input_bytes = bytes.len();
    info!("Processing {} input bytes", input_bytes);

    let alert_lines = get_alert_lines(bytes);

    let failed_lines = vec![];
    let failed_lines = Arc::new(Mutex::new(failed_lines));

    let cdc_payloads = alert_lines
        // TODO: this shouldn't happen but maybe don't swallow?
        .filter_map(|r| async move { r.ok() })
        .map(|l| async move { (l.clone(), process_alert_line(l).await) })
        .buffer_unordered(50)
        .filter_map(|(l, r)| {
            let failed_lines = failed_lines.clone();
            async move {
                r.map_err(|e| {
                    error!("Encountered error processing alert: {:#}", e);
                    failed_lines.lock().unwrap().push(l)
                })
                .ok()
            }
        })
        .collect::<Vec<_>>()
        .await;

    let alert_payload_count = cdc_payloads.len();
    let processed_alert_ids = cdc_payloads
        .iter()
        .map(|(p, _)| p.updated_alert.id.clone())
        .collect::<Vec<_>>();

    // Only publish activated alerts
    let activated_entries = cdc_payloads
        .into_iter()
        .filter(|(p, _)| p.updated_alert.is_activated())
        .collect::<Vec<_>>();
    let publish_count = activated_entries.len();
    info!("Publishing {} alert cdc payloads.", alert_payload_count);
    publish_alert_cdc_batch(activated_entries).await?;

    let failed_lines = failed_lines.try_unwrap_arc_mutex()?;
    let had_error = !failed_lines.is_empty();

    if !failed_lines.is_empty() {
        redrive_failed_alerts(failed_lines).await?;
    }

    let log = json!({
        "type": "matano_service_log",
        "service": "alert_writer",
        "processed_alert_ids": processed_alert_ids,
        "input_bytes": input_bytes,
        "alert_processed_count": alert_payload_count,
        "alert_publish_count": publish_count,
        "error": had_error,
    });
    info!("{}", serde_json::to_string(&log).unwrap_or_default());

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

    let alert_tracker_table_name = std::env::var("ALERT_TRACKER_TABLE_NAME")?;
    let merged_alert_item = dynamodb
        .get_item()
        .table_name(&alert_tracker_table_name)
        .key("id", AttributeValue::S(alert_id.clone()))
        .send()
        .await?
        .item;
    let merged_alert = match merged_alert_item {
        Some(merged_alert_item) => {
            let merged_alert = from_item::<AlertItem>(merged_alert_item)
                .context("failed to deserialize existing alert")?;
            Some(merged_alert)
        }
        None => None,
    };
    let destination_info = merged_alert
        .as_ref()
        .map(|a| a.destination_to_alert_info.clone());
    let merged_alert = merged_alert.map(|a| a.alert);

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
    // Is null when alert not activated
    let alert_creation_time = vrl(
        r#"to_timestamp(int!(.created) * 1000, "nanoseconds") ?? null"#,
        &mut matano_alert_info,
    )?
    .0
    .as_timestamp()
    .map(|t| t.clone());
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
    let existing_context = merged_alert
        .as_ref()
        .map(|a| a.context.clone())
        .unwrap_or(Value::Object(BTreeMap::new()));
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
    let merged_alert = match merged_alert {
        None => new_alert.clone(),
        Some(mut a) => {
            a.match_count += new_alert.match_count;
            a.update_count += 1;
            a.context = merged_context_values;
            a
        }
    };

    Ok(AlertCDCPayload {
        updated_alert: merged_alert,
        incoming_rule_matches_context: new_alert.context,
        context_diff: context_diff_values,
        destination_to_alert_info: destination_info.unwrap_or_default(),
    })
}
