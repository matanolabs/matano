use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;

use anyhow::{anyhow, Context as AnyhowContext, Error, Result};
use async_once::AsyncOnce;
use aws_config::SdkConfig;
use aws_lambda_events::event::sqs::SqsEvent;
use aws_sdk_s3::types::ByteStream;
use chrono::{DateTime, Duration, DurationRound};
use futures::stream::FuturesOrdered;
use futures::{FutureExt, TryFutureExt};
use futures_util::stream::StreamExt;
use lambda_runtime::{run, service_fn, Error as LambdaError, LambdaEvent};
use lazy_static::lazy_static;
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
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

#[derive(Serialize, Deserialize, Debug)]
struct SQSBatchResponseItemFailure {
    itemIdentifier: String,
}
impl SQSBatchResponseItemFailure {
    fn new(id: String) -> SQSBatchResponseItemFailure {
        SQSBatchResponseItemFailure { itemIdentifier: id }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct SQSBatchResponse {
    batchItemFailures: Vec<SQSBatchResponseItemFailure>,
}

async fn handler(event: LambdaEvent<SqsEvent>) -> Result<Option<SQSBatchResponse>> {
    info!("Starting....");
    let client = REQ_CLIENT.clone();
    let contexts = CONTEXTS.get().await;

    let mut failures = vec![];

    let records = event
        .payload
        .records
        .into_iter()
        .flat_map(|msg| Some((msg.message_id?, msg.body?)))
        .filter_map(|(id, body)| {
            let maybe_req = serde_json::from_str::<PullerRequest>(&body)
                .map_err(|e| {
                    error!("Failed to deserialize for msg id: {}, err: {:#}", &id, e);
                    failures.push(SQSBatchResponseItemFailure::new(id.clone()));
                    e
                })
                .ok();
            Some((id, maybe_req?))
        })
        .collect::<Vec<_>>();

    let (msg_ids, records): (Vec<_>, Vec<_>) = records.into_iter().unzip();

    info!("Processing {} messages.", records.len());

    debug!("Using contexts: {:?}", contexts.keys().collect::<Vec<_>>());

    let futs = records
        .into_iter()
        .map(|record| {
            let event_dt = DateTime::parse_from_rfc3339(&record.time).expect("failed to parse");

            let end_dt = event_dt.duration_trunc(Duration::minutes(1)).unwrap();
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
                ctx.load_is_initial_run().await?;
                let data = puller
                    .pull_logs(client.clone(), ctx, start_dt, end_dt)
                    .await?;
                let did_upload = upload_data(data, &record.log_source_name).await?;
                if did_upload {
                    ctx.mark_initial_run_complete().await?;
                }
                anyhow::Ok(())
            }
            .map(move |r| r.with_context(|| format!("Error for log_source: {}", log_source_name)));
            anyhow::Ok(fut)
        })
        .zip(msg_ids.iter())
        .filter_map(|(r, msg_id)| {
            r.map_err(|e| {
                error!("{:?}", e);
                failures.push(SQSBatchResponseItemFailure::new(msg_id.to_string()));
                e
            })
            .ok()
            .and_then(|r| Some((msg_id, r)))
        });
    let (msg_ids, futs): (Vec<_>, Vec<_>) = futs.unzip();

    let results = futs
        .into_iter()
        .collect::<FuturesOrdered<_>>()
        .collect::<Vec<_>>()
        .await;

    for (result, msg_id) in results.into_iter().zip(msg_ids) {
        match result {
            Ok(_) => (),
            Err(e) => {
                error!("Failed: {:#}", e);
                failures.push(SQSBatchResponseItemFailure::new(msg_id.to_owned()));
            }
        };
    }

    if failures.is_empty() {
        Ok(None)
    } else {
        error!(
            "Encountered {} errors processing messages, returning to SQS",
            failures.len()
        );
        Ok(Some(SQSBatchResponse {
            batchItemFailures: failures,
        }))
    }
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
        .map_err(|e| {
            error!("Error putting {} to S3: {}", key, e);
            e
        })?;

    Ok(true)
}
