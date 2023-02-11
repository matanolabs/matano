use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;

use anyhow::{anyhow, Context as AnyhowContext, Error, Result};
use async_once::AsyncOnce;
use aws_config::SdkConfig;
use aws_lambda_events::event::sqs::SqsEvent;
use aws_sdk_s3::types::ByteStream;
use chrono::{DateTime, Duration, DurationRound};
use futures::future::join_all;
use futures::stream::FuturesOrdered;
use futures::{FutureExt, TryFutureExt};
use futures_util::stream::StreamExt;
use lambda_runtime::{run, service_fn, Error as LambdaError, LambdaEvent};
use lazy_static::lazy_static;
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use serde_json::json;
use shared::sqs_util::*;
use shared::{setup_logging, LOG_SOURCES_CONFIG};
use walkdir::WalkDir;

mod pullers;
use pullers::{LogSource, PullLogs, PullLogsContext};

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

lazy_static! {
    static ref REQ_CLIENT: reqwest::Client = reqwest::Client::new();
    static ref CONTEXTS: AsyncOnce<HashMap<String, PullLogsContext>> =
        AsyncOnce::new(async { build_contexts().await });
    static ref AWS_CONFIG: AsyncOnce<SdkConfig> =
        AsyncOnce::new(async { aws_config::load_from_env().await });
    static ref S3_CLIENT: AsyncOnce<aws_sdk_s3::Client> =
        AsyncOnce::new(async { aws_sdk_s3::Client::new(AWS_CONFIG.get().await) });
}

async fn build_contexts() -> HashMap<String, PullLogsContext> {
    let puller_log_source_types: Vec<String> =
        serde_json::from_str(&std::env::var("PULLER_LOG_SOURCE_TYPES").unwrap()).unwrap();
    let log_source_to_secret_arn_map: HashMap<String, String> =
        serde_json::from_str(&std::env::var("LOG_SOURCE_TO_SECRET_ARN_MAP").unwrap()).unwrap();

    let s3 = S3_CLIENT.get().await;

    let ret = WalkDir::new("/opt/config/log_sources")
        .min_depth(1)
        .max_depth(1)
        .into_iter()
        .flatten()
        .filter_map(|e| {
            let p = e.path().to_owned();
            p.is_dir().then_some(p)
        })
        .flat_map(|log_source_dir_path| {
            let ls_config_path = log_source_dir_path.join("log_source.yml");
            let ls_config_path = ls_config_path.as_path().to_str().unwrap();

            let file = std::fs::File::open(ls_config_path).unwrap();
            let config: serde_yaml::Value = serde_yaml::from_reader(file).unwrap();

            let ls_name = config
                .get("name")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let managed_type = config
                .get("managed")
                .and_then(|v| v.get("type"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string().to_lowercase());

            // Either regular managed log source or managed enrichment table.
            let managed_type = managed_type.or_else(|| {
                ls_name
                    .as_ref()
                    .and_then(|lsn| {
                        puller_log_source_types
                            .iter()
                            .find(|s| lsn.starts_with(s.as_str()))
                    })
                    .map(|s| s.trim_start_matches("enrich_").to_string())
            });

            let managed_properties = config
                .get("managed")
                .and_then(|v| v.get("properties"))
                .and_then(|v| v.as_mapping())
                .map(|v| v.to_owned())
                .unwrap_or(serde_yaml::Mapping::new());

            let log_source = managed_type
                .as_ref()
                .and_then(|lsn| LogSource::from_str(lsn));

            debug!(
                "Processed: log source name: {:?}, type: {:?}, is_log_source: {:?}",
                ls_name,
                managed_type,
                log_source.is_some()
            );

            if managed_type == Some("okta".to_string())
                && !managed_properties.contains_key("base_url")
            {
                return None;
            }

            Some((ls_name?, log_source?, managed_type?, managed_properties))
        })
        .map(|(ls_name, log_source, managed_type, managed_properties)| {
            let mut props = managed_properties
                .into_iter()
                .filter_map(|(k, v)| Some((k.as_str()?.to_string(), v.as_str()?.to_string())))
                .collect::<HashMap<_, _>>();
            props.insert("log_source_type".to_string(), managed_type);

            let tables_config = LOG_SOURCES_CONFIG.with(|c| {
                let log_sources_config = c.borrow();

                (*log_sources_config)
                    .get(&ls_name)
                    .unwrap()
                    .tables
                    .to_owned()
            });

            let secret_arn = log_source_to_secret_arn_map
                .get(&ls_name)
                .context("Need secret arn.")
                .unwrap();

            let ctx = PullLogsContext::new(
                ls_name.to_owned(),
                secret_arn.to_owned(),
                log_source,
                props,
                tables_config,
                s3.clone(),
            );

            (ls_name.to_string(), ctx)
        })
        .collect::<HashMap<_, _>>();
    ret
}

#[tokio::main]
async fn main() -> Result<(), LambdaError> {
    setup_logging();

    let func = service_fn(handler);
    run(func).await?;

    Ok(())
}

#[derive(Serialize, Deserialize, Debug)]
struct PullerRequest {
    log_source_name: String,
    time: String,
    rate_minutes: u32,
}

async fn handler(event: LambdaEvent<SqsEvent>) -> Result<Option<SQSBatchResponse>> {
    info!("Starting....");
    let client = REQ_CLIENT.clone();
    let contexts = CONTEXTS.get().await;

    let mut errors = vec![];

    let records = event
        .payload
        .records
        .into_iter()
        .flat_map(|msg| Some((msg.message_id?, msg.body?)))
        .filter_map(|(id, body)| {
            let maybe_req = serde_json::from_str::<PullerRequest>(&body)
                .map_err(|e| {
                    let sqs_err = SQSLambdaError::new(
                        anyhow!(e).context("Failed to deserialize").to_string(),
                        vec![id.clone()],
                    );
                    errors.push(sqs_err)
                })
                .ok();
            Some((id, maybe_req?))
        })
        .collect::<Vec<_>>();

    info!("Processing {} messages.", records.len());

    debug!("Using contexts: {:?}", contexts.keys().collect::<Vec<_>>());

    let futs = records
        .into_iter()
        .filter(|(_, record)| {
            let ctx = contexts.get(&record.log_source_name);
            if ctx.is_none() {
                debug!("Skipping invalid log source: {}", &record.log_source_name);
            }
            ctx.is_some()
        })
        .map(|(msg_id, record)| {
            process_record(msg_id.clone(), record, client.clone(), contexts)
                .map_err(|e| SQSLambdaError::new(format!("{:#}", e), vec![msg_id]))
        })
        .filter_map(|r| r.map_err(|e| errors.push(e)).ok())
        .collect::<Vec<_>>();

    join_all(futs)
        .await
        .into_iter()
        .filter_map(|r| r.map_err(|e| errors.push(e)).ok())
        .for_each(drop);

    sqs_errors_to_response(errors)
}

fn process_record(
    msg_id: String,
    record: PullerRequest,
    client: reqwest::Client,
    contexts: &'static HashMap<String, PullLogsContext>,
) -> Result<impl futures::Future<Output = Result<(), SQSLambdaError>>> {
    let event_dt = DateTime::parse_from_rfc3339(&record.time)?;

    let end_dt = event_dt.duration_trunc(Duration::minutes(1))?;
    let start_dt = end_dt - Duration::minutes(record.rate_minutes as i64);

    info!(
        "Processing log_source: {}, from {} to {}",
        &record.log_source_name, &start_dt, &end_dt
    );

    let ctx = contexts
        .get(&record.log_source_name)
        .context("Invalid log source.")?;

    let puller = ctx.log_source_type.clone();
    let log_source_name = record.log_source_name.clone();
    let client = client.clone();

    let fut = async move {
        ctx.load_checkpoint().await?;
        let data = puller.pull_logs(client, ctx, start_dt, end_dt).await?;
        let did_upload = upload_data(data, &record.log_source_name).await?;
        if did_upload {
            let checkpoint_json = ctx.checkpoint_json.lock().await.clone();
            let is_initial_run = checkpoint_json.is_none();
            if is_initial_run {
                ctx.upload_checkpoint(&json!({
                    "initial_run": "complete"
                }))
                .await?;
                info!(
                    "Marked initial run complete for log_source: {}",
                    ctx.log_source_name
                );
            } else {
                let checkpoint_json = checkpoint_json.unwrap();
                if checkpoint_json
                    != json!({
                        "initial_run": "complete"
                    })
                {
                    ctx.upload_checkpoint(&checkpoint_json).await?;
                    info!(
                        "Uploaded new checkpoint for log_source: {}, checkpoint state: {:?}",
                        ctx.log_source_name, checkpoint_json
                    );
                }
            }
        }
        anyhow::Ok(())
    }
    .map_err(move |e| {
        let e = e.context(format!("Error for log_source: {}", log_source_name));
        SQSLambdaError::new(format!("{:#}", e), vec![msg_id])
    });
    Ok(fut)
}

async fn upload_data(data: Vec<u8>, log_source: &str) -> Result<bool> {
    if data.is_empty() {
        info!("No new data for log_source: {}", log_source);
        return Ok(false);
    }
    info!("Uploading data for {}", log_source);
    let bucket = std::env::var("INGESTION_BUCKET_NAME")?;
    let key = format!(
        "{}/{}.json.zst",
        log_source,
        uuid::Uuid::new_v4().to_string()
    );
    let s3 = S3_CLIENT.get().await;
    info!("Writing to s3://{}/{}", bucket, key);

    let mut zencoder = zstd::Encoder::new(vec![], 0)?;
    zencoder.write_all(data.as_slice())?;
    let final_data = zencoder.finish()?;

    s3.put_object()
        .bucket(&bucket)
        .key(&key)
        .body(ByteStream::from(final_data))
        .content_encoding("application/zstd".to_string())
        .send()
        .await
        .map_err(|e| anyhow!(e).context(format!("Error putting {} to S3", key)))?;

    Ok(true)
}
