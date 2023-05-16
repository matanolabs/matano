use arrow2::datatypes::Schema;
use async_once::AsyncOnce;
use aws_config::SdkConfig;
use futures::future::join_all;
use futures::AsyncReadExt;
use futures::TryFutureExt;
use futures::TryStreamExt;
use log::warn;
use rayon::prelude::IntoParallelIterator;
use rayon::prelude::ParallelIterator;
use serde::{Deserialize, Serialize};
use serde_json::json;
use shared::sqs_util::*;
use shared::*;

use std::collections::HashMap;
use std::sync::Arc;
use std::{time::Instant, vec};

use aws_lambda_events::event::sqs::SqsEvent;
use lambda_runtime::{run, service_fn, Error as LambdaError, LambdaEvent};

use lazy_static::lazy_static;
use log::{error, info};
use threadpool::ThreadPool;

use anyhow::{anyhow, Context, Result};
use futures::StreamExt;

use arrow2::io::avro::avro_schema::file::Block;
use arrow2::io::avro::avro_schema::read_async::{block_stream, decompress_block, read_metadata};
use arrow2::io::avro::read::deserialize;
use std::sync::Mutex;
use tokio_util::compat::TokioAsyncReadCompatExt;

mod common;
mod matano_alerts;
use common::{load_table_arrow_schema, write_arrow_to_s3_parquet};

use crate::common::struct_wrap_arrow2_for_ffi;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const ALERTS_TABLE_NAME: &str = "matano_alerts";

lazy_static! {
    static ref AWS_CONFIG: AsyncOnce<SdkConfig> =
        AsyncOnce::new(async { aws_config::load_from_env().await });
    static ref S3_CLIENT: AsyncOnce<aws_sdk_s3::Client> =
        AsyncOnce::new(async { aws_sdk_s3::Client::new(AWS_CONFIG.get().await) });
}

lazy_static! {
    static ref TABLE_SCHEMA_MAP: Arc<Mutex<HashMap<String, Schema>>> =
        Arc::new(Mutex::new(HashMap::new()));
}

#[tokio::main]
async fn main() -> Result<(), LambdaError> {
    setup_logging();

    let func = service_fn(my_handler);
    run(func).await?;

    Ok(())
}

pub(crate) async fn my_handler(event: LambdaEvent<SqsEvent>) -> Result<Option<SQSBatchResponse>> {
    let msg_logs = event
        .payload
        .records
        .iter()
        .map(|r| json!({ "message_id": r.message_id, "body": r.body }))
        .collect::<Vec<_>>();
    info!(
        "Received messages: {}",
        serde_json::to_string(&msg_logs).unwrap_or_default()
    );

    let records = event.payload.records;
    let downloads = records
        .iter()
        .flat_map(|r| Some((r.message_id.as_ref()?, r.body.as_ref()?)))
        .flat_map(|(message_id, body)| {
            let items = serde_json::from_str::<S3SQSMessage>(&body);
            anyhow::Ok((message_id.clone(), items?))
        })
        .collect::<Vec<_>>();
    let skipped_records = records.len() - downloads.len();
    if skipped_records > 0 {
        warn!("Skipped {} records not matching structure", skipped_records);
    }

    let input_download_len = downloads.len();
    if input_download_len == 0 {
        info!("Empty event, returning...");
        return Ok(None);
    }

    let resolved_table_name = downloads
        .first()
        .map(|(_, msg)| msg.resolved_table_name.clone())
        .context("No resolved table name")?;
    info!("Processing for table: {}", resolved_table_name);

    let mut table_schema_map = TABLE_SCHEMA_MAP
        .lock()
        .map_err(|e| anyhow!(e.to_string()))?;
    let table_schema = table_schema_map
        .entry(resolved_table_name.clone())
        .or_insert_with_key(|k| load_table_arrow_schema(k).unwrap());

    info!("Starting {} downloads from S3", downloads.len());

    let s3 = S3_CLIENT.get().await.clone();

    let pool = ThreadPool::new(4);
    let pool_ref = Arc::new(pool);

    let partition_hour_to_blocks_ref: Arc<
        Mutex<HashMap<String, Vec<Result<Block, SQSLambdaError>>>>,
    > = Arc::new(Mutex::new(HashMap::new()));
    let partition_hour_to_alert_blocks_ref: Arc<tokio::sync::Mutex<HashMap<String, Vec<Vec<u8>>>>> =
        Arc::new(tokio::sync::Mutex::new(HashMap::new()));

    let mut errors = vec![];
    let ts_hour_to_msg_ids: Arc<tokio::sync::Mutex<HashMap<String, Vec<String>>>> =
        Arc::new(tokio::sync::Mutex::new(HashMap::new()));

    let input_keys = downloads
        .iter()
        .map(|(_, msg)| msg.key.clone())
        .collect::<Vec<_>>();

    let tasks = downloads
        .into_iter()
        .map(|(msg_id, r)| {
            let s3 = &s3;
            let msg_id_copy = msg_id.clone();
            let ts_hour = r.ts_hour;
            let ts_hour_to_msg_ids = ts_hour_to_msg_ids.clone();

            let ret = async move {
                let obj = s3
                    .get_object()
                    .bucket(r.bucket)
                    .key(r.key.clone())
                    .send()
                    .await
                    .context(format!("Error downloading {} from S3", r.key))?;

                let stream = obj.body;
                let reader = TokioAsyncReadCompatExt::compat(stream.into_async_read());

                let mut ts_hour_to_msg_ids = ts_hour_to_msg_ids.lock().await;

                ts_hour_to_msg_ids
                    .entry(ts_hour.clone())
                    .or_insert_with(|| vec![])
                    .push(msg_id.clone());

                anyhow::Ok((msg_id, ts_hour, reader))
            }
            .map_err(|e| SQSLambdaError::new(format!("{:#}", e), vec![msg_id_copy]));
            ret
        })
        .collect::<Vec<_>>();

    let result = join_all(tasks)
        .await
        .into_iter()
        .filter_map(|r| r.map_err(|e| errors.push(e)).ok());

    let work_futures = result.map(|(msg_id, ts_hour, mut reader)| {
        let pool_ref = pool_ref.clone();
        let resolved_table_name = resolved_table_name.clone();
        let msg_id_copy = msg_id.clone();
        let partition_hour_to_blocks_ref = partition_hour_to_blocks_ref.clone();
        let partition_hour_to_alert_blocks_ref = partition_hour_to_alert_blocks_ref.clone();

        async move {
            let ret = if resolved_table_name == ALERTS_TABLE_NAME {
                let mut buf = vec![];
                reader.read_to_end(&mut buf).await?;

                let mut partition_hour_to_alert_blocks_ref =
                    partition_hour_to_alert_blocks_ref.lock().await;
                let alert_blocks_ref = partition_hour_to_alert_blocks_ref
                    .entry(ts_hour.clone())
                    .or_insert_with(|| vec![]);

                alert_blocks_ref.push(buf);

                anyhow::Ok(None)
            } else {
                let metadata = read_metadata(&mut reader).await.map_err(|e| anyhow!(e))?;

                let blocks = block_stream(&mut reader, metadata.marker).await;

                let fut = blocks.map_err(|e| anyhow!(e)).try_for_each_concurrent(
                    1000000,
                    move |mut block| {
                        let pool = pool_ref.clone();
                        let msg_id = msg_id.clone();
                        let ts_hour = ts_hour.clone();
                        let partition_hour_to_blocks_ref = partition_hour_to_blocks_ref.clone();

                        async move {
                            // the content here is CPU-bounded. It should run on a dedicated thread pool
                            pool.execute(move || {
                                let decompressed = Ok(Block::new(0, vec![]))
                                    .and_then(|mut decompressed_block| {
                                        decompress_block(
                                            &mut block,
                                            &mut decompressed_block,
                                            metadata.compression,
                                        )
                                        .map_err(|e| anyhow!(e))?;
                                        anyhow::Ok(decompressed_block)
                                    })
                                    .map_err(|e| {
                                        SQSLambdaError::new(
                                            format!("{:#}", e),
                                            vec![msg_id.clone()],
                                        )
                                    });

                                let mut partition_hour_to_blocks_ref =
                                    partition_hour_to_blocks_ref.lock().unwrap();
                                let blocks_ref = partition_hour_to_blocks_ref
                                    .entry(ts_hour.clone())
                                    .or_insert_with(|| vec![]);
                                blocks_ref.push(decompressed);
                                ()
                            });
                            anyhow::Ok(())
                        }
                    },
                );

                fut.await?;
                anyhow::Ok(Some(metadata))
            };
            ret
        }
        .map_err(move |e| SQSLambdaError::new(format!("{:#}", e), vec![msg_id_copy]))
    });

    let results = join_all(work_futures)
        .await
        .into_iter()
        .filter_map(|r| r.map_err(|e| errors.push(e)).ok())
        .collect::<Vec<_>>();

    let pool = pool_ref.clone();
    pool.join();

    let errors = Arc::new(Mutex::new(errors));

    if &resolved_table_name == ALERTS_TABLE_NAME {
        info!("Processing alerts...");
        let partition_hour_to_alert_blocks_ref =
            Arc::try_unwrap(partition_hour_to_alert_blocks_ref)
                .map_err(|_| anyhow!("fail get alert blocks"))?;
        let partition_hour_to_alert_blocks_ref = partition_hour_to_alert_blocks_ref.into_inner();
        for (partition_hour, alert_blocks) in partition_hour_to_alert_blocks_ref.into_iter() {
            info!("Processing alerts for partition {}", partition_hour);
            matano_alerts::process_alerts(s3.clone(), alert_blocks).await?;
        }
    } else {
        let partition_hour_to_blocks_ref = partition_hour_to_blocks_ref.try_unwrap_arc_mutex()?;
        let ts_hour_to_msg_ids = ts_hour_to_msg_ids.try_unwrap_arc_mutex()?;
        let futures = partition_hour_to_blocks_ref
            .into_par_iter()
            .map(|(partition_hour, blocks)| {
                let msg_ids = ts_hour_to_msg_ids
                    .get(&partition_hour)
                    .ok_or_else(|| anyhow!("no msg ids for partition {}", partition_hour))?;

                let blocks = blocks
                    .into_iter()
                    .filter_map(|r| r.map_err(|e| errors.lock().unwrap().push(e)).ok())
                    .collect::<Vec<_>>();

                let total_avro_rows = blocks.iter().map(|b| b.number_of_rows).sum::<usize>();
                let total_avro_bytes = blocks.iter().map(|b| b.data.len()).sum::<usize>();

                if !blocks.is_empty() {
                    // see TODO below
                    let metadata = results
                        .first()
                        .context("Need metadata!")?
                        .as_ref()
                        .context("Need metadata!")?;
                    let block = concat_blocks(blocks);
                    let projection = table_schema.fields.iter().map(|_| true).collect::<Vec<_>>();

                    // There's an edge case bug here when schema is updated. Using static schema
                    // on old data before schema will throw since field length unequal.
                    // TODO: Group block's by schema and then deserialize.
                    let chunk = deserialize(
                        &block,
                        &table_schema.fields,
                        &metadata.record.fields,
                        &projection,
                    )?;
                    let chunks = vec![chunk];

                    let (field, arrays) = struct_wrap_arrow2_for_ffi(&table_schema, chunks);

                    let result = write_arrow_to_s3_parquet(
                        s3.clone(),
                        resolved_table_name.clone(),
                        partition_hour,
                        field,
                        arrays,
                    )
                    .map_ok(move |(partition_hour, key, file_length)| {
                        (
                            total_avro_rows,
                            total_avro_bytes,
                            partition_hour,
                            key,
                            file_length,
                        )
                    })
                    .map_err(|e| SQSLambdaError::new(format!("{:#}", e), msg_ids.clone()));

                    Ok(Some(result))
                } else {
                    Ok(None)
                }
            })
            .collect::<Result<Vec<_>>>()?;

        let futures = futures.into_iter().filter_map(|r| r).collect::<Vec<_>>();

        let results = join_all(futures).await;

        // handle errors
        for result in results {
            match result {
                Err(e) => {
                    errors.lock().unwrap().push(e);
                }
                Ok((avro_rows, total_avro_bytes, partition_hour, key, file_length)) => {
                    let log = json!({
                        "matano_log": true,
                        "type": "matano_table_log",
                        "service": "lake_writer",
                        "table": &resolved_table_name,
                        "partition_hour": partition_hour,
                        "write_key": key,
                        "avro_bytes_uncompressed": total_avro_bytes,
                        "row_count": avro_rows,
                        "parquet_bytes": file_length,
                    });
                    info!("{}", serde_json::to_string(&log).unwrap_or_default());
                }
            };
        }
    }

    let errors = errors.try_unwrap_arc_mutex()?;

    let log = json!({
        "matano_log": true,
        "type": "matano_service_log",
        "service": "lake_writer",
        "input_keys": input_keys,
        "records_count": input_download_len,
        "table": &resolved_table_name,
        "error": !errors.is_empty()
    });
    info!("{}", serde_json::to_string(&log).unwrap_or_default());

    sqs_errors_to_response(errors)
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct S3SQSMessage {
    pub resolved_table_name: String,
    pub bucket: String,
    pub key: String,
    pub ts_hour: String,
}

fn concat_blocks(mut blocks: Vec<Block>) -> Block {
    let mut ret = Block::new(0, vec![]);
    for block in blocks.iter_mut() {
        ret.number_of_rows += block.number_of_rows;
        ret.data.extend(block.data.as_slice());
    }
    ret
}
