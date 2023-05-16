use std::collections::{HashMap, HashSet};
use std::time::Instant;

use anyhow::{anyhow, Result};
use async_once::AsyncOnce;
use aws_lambda_events::event::s3::S3Event;
use aws_lambda_events::event::sqs::SqsEvent;
use aws_sdk_sqs::types::SendMessageBatchRequestEntry;
use futures::future::{join_all, try_join_all};
use lambda_runtime::{run, service_fn, Error as LambdaError, LambdaEvent};
use lazy_static::lazy_static;
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use serde_json::json;
use shared::{
    duplicates_util::{check_ddb_duplicate, mark_ddb_duplicate_completed},
    setup_logging, DataBatcherOutputRecord,
};
use walkdir::WalkDir;

lazy_static! {
    static ref AWS_CONFIG: AsyncOnce<aws_config::SdkConfig> =
        AsyncOnce::new(async { aws_config::load_from_env().await });
    static ref SQS_CLIENT: AsyncOnce<aws_sdk_sqs::Client> =
        AsyncOnce::new(async { aws_sdk_sqs::Client::new(AWS_CONFIG.get().await) });
    static ref DDB_CLIENT: AsyncOnce<aws_sdk_dynamodb::Client> =
        AsyncOnce::new(async { aws_sdk_dynamodb::Client::new(AWS_CONFIG.get().await) });
    static ref LOG_SOURCE_KEY_PREFIX_MAP: HashMap<String, (Option<String>, Option<String>, Option<String>)> =
        build_log_source_key_prefix_map();
}

#[tokio::main]
async fn main() -> Result<(), LambdaError> {
    setup_logging();

    let func = service_fn(handler);
    run(func).await?;

    Ok(())
}

fn build_log_source_key_prefix_map(
) -> HashMap<String, (Option<String>, Option<String>, Option<String>)> {
    let mut ret = HashMap::new();
    for entry in WalkDir::new("/opt/config/log_sources")
        .min_depth(1)
        .max_depth(1)
    {
        let log_source_dir_path = match entry {
            Ok(v) => v,
            Err(e) => {
                error!(
                    "Invalid entry while walking children for log_sources/ directory: {}",
                    e
                );
                continue;
            }
        };
        let log_source_dir_path = log_source_dir_path.path();
        if !log_source_dir_path.is_dir() {
            continue;
        }

        let log_source_configuration_path = log_source_dir_path.join("log_source.yml");
        let log_source_configuration_path =
            log_source_configuration_path.as_path().to_str().unwrap();

        let file = std::fs::File::open(log_source_configuration_path).unwrap();
        let config: serde_yaml::Value = serde_yaml::from_reader(file).unwrap();

        let ls_name = config.get("name").and_then(|v| v.as_str());

        if let Some(log_source_name) = ls_name {
            let s3_source = config.get("ingest").and_then(|v| v.get("s3_source"));
            let bucket_name = s3_source
                .and_then(|v| v.get("bucket_name"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let key_prefix = s3_source
                .and_then(|v| v.get("key_prefix"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let key_pattern = s3_source
                .and_then(|v| v.get("key_pattern"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            ret.insert(
                log_source_name.to_string(),
                (bucket_name, key_prefix, key_pattern),
            );
        }
    }
    ret
}

fn get_log_source_from_object(bucket: &str, key: &str) -> Option<String> {
    let managed_bucket = std::env::var("MATANO_SOURCES_BUCKET").ok()?;
    LOG_SOURCE_KEY_PREFIX_MAP
        .iter()
        .find(|(_, (ls_bucket, ls_key_prefix, ls_key_pattern))| {
            // if all are None, skip key prefix/pattern based matching
            if ls_bucket.is_none() && ls_key_prefix.is_none() && ls_key_pattern.is_none() {
                return false;
            }
            let bucket_matches = ls_bucket
                .as_ref()
                .map_or(managed_bucket == bucket, |b| b == bucket);
            let ls_key_prefix = ls_key_prefix
                .as_ref()
                .map(|s| s.to_string())
                .unwrap_or("".to_string());
            let key_matches = key.starts_with(&ls_key_prefix)
                && match ls_key_pattern {
                    Some(pattern) => {
                        let re = regex::Regex::new(pattern).unwrap();
                        re.is_match(key)
                    }
                    None => true,
                };
            bucket_matches && key_matches
        })
        .map(|(ls, _)| ls.to_owned())
        .or_else(|| {
            // try managed
            if managed_bucket == bucket {
                key.split(std::path::MAIN_SEPARATOR).next().and_then(|ls| {
                    LOG_SOURCE_KEY_PREFIX_MAP
                        .contains_key(ls)
                        .then_some(ls.to_string())
                })
            } else {
                None
            }
        })
}

/// Checks if object is definitely compressed (false negatives)
fn is_compressed(key: &str) -> bool {
    key.ends_with("gz") || key.ends_with("gzip") || key.ends_with("zst") || key.ends_with("zstd")
}

trait S3EventExt {
    fn sequencer(&self) -> String;
}
impl S3EventExt for S3Event {
    fn sequencer(&self) -> String {
        self.records[0].s3.object.sequencer.clone().unwrap()
    }
}

async fn handler(event: LambdaEvent<SqsEvent>) -> Result<()> {
    let start = Instant::now();
    debug!("{:?}", event);
    let ddb = DDB_CLIENT.get().await;

    let duplicates_table_name = std::env::var("DUPLICATES_TABLE_NAME")?;
    let s3_events = event
        .payload
        .records
        .into_iter()
        .flat_map(|record| serde_json::from_str::<S3Event>(record.body.as_ref()?).ok())
        .map(|event| {
            let table_name = duplicates_table_name.clone();
            let msg_id = event.sequencer();
            async move {
                let is_dup = check_ddb_duplicate(ddb.clone(), &table_name, msg_id.clone()).await?;
                if is_dup {
                    info!("Duplicate event id: {}, skipping...", &msg_id);
                }
                anyhow::Ok((!is_dup).then_some(event))
            }
        });

    let s3_events = try_join_all(s3_events)
        .await?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

    let s3_event_ids = s3_events
        .iter()
        .map(|event| event.sequencer())
        .collect::<Vec<_>>();

    let s3_records = s3_events
        .into_iter()
        .flat_map(|event| event.records)
        .collect::<Vec<_>>();

    if s3_records.len() == 0 {
        info!("Empty event, returning...");
        return Ok(());
    }

    let mut total_input_bytes: i64 = 0;
    let mut relevant_input_records = 0;

    let mut new_records = s3_records
        .into_iter()
        .flat_map(|record| {
            let object = &record.s3.object;
            let bucket = record.s3.bucket.name.as_ref().unwrap().to_owned();
            let key = object.key.as_ref().unwrap().to_owned();
            let log_source = get_log_source_from_object(&bucket, &key);
            let size = object.size.unwrap();

            if let Some(ls) = log_source {
                total_input_bytes += size;
                relevant_input_records += 1;

                let r = DataBatcherOutputRecord {
                    bucket,
                    key,
                    size,
                    sequencer: object.sequencer.as_ref().unwrap().to_owned(),
                    log_source: ls,
                    retry_depth: Some(0),
                };
                Some(r)
            } else {
                info!("Skipping irrelevant key: {}", key);
                None
            }
        })
        .collect::<Vec<_>>();
    if new_records.len() == 0 {
        info!("No records to process, returning...");
        return Ok(());
    }

    new_records.sort_by_key(|r| (r.log_source.clone(), r.size));

    let target_size: i64 = 32 * 1000 * 1000; // 32 MB
    let mut record_chunks = vec![];
    let mut current_chunk_size = 0;
    let mut current_chunk = vec![];

    for record in &new_records {
        let mut bytes_contribution = record.size;
        if is_compressed(&record.key) {
            bytes_contribution *= 5; // compression factor
        }

        let start_new_chunk = current_chunk_size + bytes_contribution >= target_size;
        if start_new_chunk {
            record_chunks.push(current_chunk);
            current_chunk = vec![];
            current_chunk_size = 0;
        }

        current_chunk.push(record);
        current_chunk_size += bytes_contribution;
    }
    if current_chunk.len() > 0 {
        // leftover
        record_chunks.push(current_chunk);
    }

    let sqs_messages = record_chunks
        .into_iter()
        .filter(|chunk| !chunk.is_empty())
        .map(|chunk| {
            let json_body = serde_json::to_string(&chunk).unwrap();
            SendMessageBatchRequestEntry::builder()
                .id(uuid::Uuid::new_v4().to_string())
                .message_body(json_body)
                .build()
        })
        .collect::<Vec<_>>();

    let sqs = SQS_CLIENT.get().await;
    let final_sqs_chunks = sqs_messages.chunks(10).map(|x| x.to_vec());
    let futures = final_sqs_chunks
        .map(|chunk| {
            sqs.send_message_batch()
                .queue_url(std::env::var("OUTPUT_QUEUE_URL").unwrap())
                .set_entries(Some(chunk))
                .send()
        })
        .collect::<Vec<_>>();

    // TODO: properly handle errors including partial failures!
    let raw_resp = join_all(futures).await;
    let raw_resp: Result<Vec<_>, _> = raw_resp.into_iter().collect();
    let resp = raw_resp?;
    if resp
        .iter()
        .any(|output| output.failed().map_or(false, |e| e.len() > 0))
    {
        return Err(anyhow!("SQS upload failure!"));
    }

    let output_length: i64 = sqs_messages.len().try_into()?;
    let average_output_size: i64 = total_input_bytes / output_length;
    info!(
        "Coalesced {} bytes of data from {} input records to {} output records of average size {} bytes.",
        total_input_bytes,
        relevant_input_records,
        output_length,
        average_output_size,
    );

    let completed_futs = s3_event_ids
        .into_iter()
        .map(|id| mark_ddb_duplicate_completed(ddb.clone(), &duplicates_table_name, id));
    try_join_all(completed_futs).await?;

    let time_ms = i64::try_from(start.elapsed().as_millis()).ok();
    let log_sources = new_records
        .iter()
        .map(|r| r.log_source.clone())
        .collect::<HashSet<_>>();
    let log = json!({
        "matano_log": true,
        "type": "matano_service_log",
        "service": "data_batcher",
        "time": time_ms,
        "input_records_count": relevant_input_records,
        "output_records_count": output_length,
        "log_sources": log_sources,
        "bytes_processed": total_input_bytes,
    });
    info!("{}", serde_json::to_string(&log).unwrap_or_default());

    Ok(())
}
