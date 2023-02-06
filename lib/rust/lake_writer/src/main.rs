use arrow2::datatypes::Schema;
use async_once::AsyncOnce;
use aws_config::SdkConfig;
use futures::future::join_all;
use futures::AsyncReadExt;
use futures::TryFutureExt;
use futures::TryStreamExt;
use log::warn;
use serde::{Deserialize, Serialize};
use serde_json::json;
use shared::sqs_util::*;
use shared::*;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
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

    if downloads.len() == 0 {
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
    let blocks = vec![];
    let blocks_ref = Arc::new(Mutex::new(blocks));

    let alert_blocks = vec![];
    let alert_blocks_ref = Arc::new(Mutex::new(alert_blocks));

    let mut errors = vec![];

    let tasks = downloads
        .into_iter()
        .map(|(msg_id, r)| {
            let s3 = &s3;
            let msg_id_copy = msg_id.clone();
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
                anyhow::Ok((msg_id, reader))
            }
            .map_err(|e| SQSLambdaError::new(format!("{:#}", e), msg_id_copy));
            ret
        })
        .collect::<Vec<_>>();

    let result = join_all(tasks)
        .await
        .into_iter()
        .filter_map(|r| r.map_err(|e| errors.push(e)).ok());

    let work_futures = result.map(|(msg_id, mut reader)| {
        let blocks_ref = blocks_ref.clone();
        let alert_blocks_ref = alert_blocks_ref.clone();
        let pool_ref = pool_ref.clone();
        let resolved_table_name = resolved_table_name.clone();
        let msg_id_copy = msg_id.clone();

        async move {
            let ret = if resolved_table_name == ALERTS_TABLE_NAME {
                let mut buf = vec![];
                reader.read_to_end(&mut buf).await?;
                let mut alert_blocks = alert_blocks_ref
                    .lock()
                    .map_err(|e| anyhow!(e.to_string()))?;
                alert_blocks.push(buf);

                anyhow::Ok(None)
            } else {
                let metadata = read_metadata(&mut reader).await.map_err(|e| anyhow!(e))?;

                let blocks = block_stream(&mut reader, metadata.marker).await;

                let fut = blocks.map_err(|e| anyhow!(e)).try_for_each_concurrent(
                    1000000,
                    move |mut block| {
                        let pool = pool_ref.clone();
                        let blocks_ref = blocks_ref.clone();
                        let msg_id = msg_id.clone();
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
                                        SQSLambdaError::new(format!("{:#}", e), msg_id.clone())
                                    });

                                let mut blocks = blocks_ref.lock().unwrap();
                                blocks.push(decompressed);
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
        .map_err(move |e| SQSLambdaError::new(format!("{:#}", e), msg_id_copy))
    });

    let results = join_all(work_futures)
        .await
        .into_iter()
        .filter_map(|r| r.map_err(|e| errors.push(e)).ok())
        .collect::<Vec<_>>();

    let pool = pool_ref.clone();
    pool.join();

    if resolved_table_name == ALERTS_TABLE_NAME {
        info!("Processing alerts...");
        let alert_blocks_ref =
            Arc::try_unwrap(alert_blocks_ref).map_err(|_| anyhow!("fail get rowgroups"))?;
        let alert_blocks = Mutex::into_inner(alert_blocks_ref)?;
        matano_alerts::process_alerts(s3, alert_blocks).await?;
    } else {
        let blocks_ref = Arc::try_unwrap(blocks_ref).map_err(|_| anyhow!("fail get rowgroups"))?;
        let blocks = Mutex::into_inner(blocks_ref)?;

        let blocks = blocks
            .into_iter()
            .filter_map(|r| r.map_err(|e| errors.push(e)).ok())
            .collect::<Vec<_>>();
        if !blocks.is_empty() {
            let metadata = results
                .first()
                .unwrap()
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
            // TODO: fix to use correct partition (@shaeq)
            let partition_hour = chrono::offset::Utc::now().format("%Y-%m-%d-%H").to_string();
            write_arrow_to_s3_parquet(s3, resolved_table_name, partition_hour, field, arrays)
                .await?;
        }
    }

    sqs_errors_to_response(errors)
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct S3SQSMessage {
    pub resolved_table_name: String,
    pub bucket: String,
    pub key: String,
}

fn concat_blocks(mut blocks: Vec<Block>) -> Block {
    let mut ret = Block::new(0, vec![]);
    for block in blocks.iter_mut() {
        ret.number_of_rows += block.number_of_rows;
        ret.data.extend(block.data.as_slice());
    }
    ret
}
