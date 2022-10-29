use arrow2::datatypes::Field;
use arrow2::datatypes::Schema;
use async_once::AsyncOnce;
use aws_config::SdkConfig;
use futures::future::join_all;
use futures::AsyncReadExt;
use serde::{Deserialize, Serialize};
use shared::*;
use uuid::Uuid;

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::{time::Instant, vec};

use aws_lambda_events::event::sqs::SqsEvent;
use lambda_runtime::{run, service_fn, Context, Error as LambdaError, LambdaEvent};

use lazy_static::lazy_static;
use log::{error, info};
use threadpool::ThreadPool;

use anyhow::{anyhow, Result};

// use tokio_stream::StreamExt;
use futures::StreamExt;

use arrow2::datatypes::DataType;
use arrow2::io::avro::avro_schema::file::Block;
use arrow2::io::avro::avro_schema::read_async::{block_stream, decompress_block, read_metadata};
use arrow2::io::avro::read::{deserialize, infer_schema};
use arrow2::io::parquet::write::{
    transverse, CompressionOptions, Encoding, FileWriter, RowGroupIterator, Version, WriteOptions,
};
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
    static ref TABLE_SCHEMA_MAP: Arc<Mutex<HashMap<String, Schema>>> =
        Arc::new(Mutex::new(HashMap::new()));
}

#[tokio::main]
async fn main() -> Result<(), LambdaError> {
    setup_logging();
    let start = Instant::now();

    // let ev11: SqsEvent = serde_json::from_reader(File::open(
    //     "/home/samrose/workplace/matano/lib/rust/garbage2.json",
    // )?)?;

    // my_handler(ev11).await.unwrap();

    let func = service_fn(my_handler);
    run(func).await?;

    info!("Call lambda took {:.2?}", start.elapsed());

    Ok(())
}

pub(crate) async fn my_handler(event: LambdaEvent<SqsEvent>) -> Result<()> {
    info!("Request: {:?}", event);

    let downloads = event
        .payload
        .records
        .iter()
        .flat_map(|record| {
            let body = record.body.as_ref().ok_or("SQS message body is required")?;
            let items = serde_json::from_str::<S3SQSMessage>(&body).map_err(|e| e.to_string());
            items
        })
        .collect::<Vec<_>>();

    if downloads.len() == 0 {
        info!("Empty event, returning...");
        return Ok(());
    }

    let resolved_table_name = downloads
        .first()
        .map(|m| m.resolved_table_name.clone())
        .unwrap();
    info!("Processing for table: {}", resolved_table_name);

    let mut table_schema_map = TABLE_SCHEMA_MAP.lock().unwrap();
    let table_schema = table_schema_map
        .entry(resolved_table_name.clone())
        .or_insert_with_key(|k| load_table_arrow_schema(k).unwrap());
    let table_schema_ref = Arc::new(table_schema.clone());

    info!("Starting {} downloads from S3", downloads.len());

    let s3 = S3_CLIENT.get().await.clone();

    let start = Instant::now();

    let pool = ThreadPool::new(4);
    let pool_ref = Arc::new(pool);
    let chunks = vec![];
    let chunks_ref = Arc::new(Mutex::new(chunks));

    let alert_blocks = vec![];
    let alert_blocks_ref = Arc::new(Mutex::new(alert_blocks));

    let tasks = downloads
        .into_iter()
        .map(|r| {
            let s3 = &s3;
            let ret = (async move {
                let obj_res = s3
                    .get_object()
                    .bucket(r.bucket)
                    .key(r.key.clone())
                    .send()
                    .await
                    .map_err(|e| {
                        error!("Error downloading {} from S3: {}", r.key, e);
                        e
                    });
                let obj = obj_res.unwrap();
                println!("Got object...");

                let stream = obj.body;
                let reader = TokioAsyncReadCompatExt::compat(stream.into_async_read());
                reader
            });
            ret
        })
        .collect::<Vec<_>>();

    let mut result = join_all(tasks).await;

    let work_futures = result.iter_mut().map(|reader| {
        let chunks_ref = chunks_ref.clone();
        let alert_blocks_ref = alert_blocks_ref.clone();
        let pool_ref = pool_ref.clone();
        let resolved_table_name = resolved_table_name.clone();
        let table_schema = table_schema_ref.clone();

        async move {
            if resolved_table_name == ALERTS_TABLE_NAME {
                let mut buf = vec![];
                reader.read_to_end(&mut buf).await.unwrap();

                let mut alert_blocks = alert_blocks_ref.lock().unwrap();
                alert_blocks.push(buf);

                return None;
            };

            let metadata = read_metadata(reader).await.unwrap();
            let schema = table_schema.clone();

            // // TODO move out
            let projection = Arc::new(schema.fields.iter().map(|_| true).collect::<Vec<_>>());

            let blocks = block_stream(reader, metadata.marker).await;

            let fut = blocks.for_each_concurrent(1000000, move |block| {
                println!("Getting block....");
                let mut block = block.unwrap();

                let schema = schema.clone();
                let metadata = metadata.clone();
                let projection = projection.clone();
                dbg!(block.number_of_rows);
                let chunks_ref = chunks_ref.clone();
                let pool = pool_ref.clone();

                // the content here is CPU-bounded. It should run on a dedicated thread pool
                pool.execute(move || {
                    let st1 = Instant::now();
                    let mut decompressed = Block::new(0, vec![]);

                    decompress_block(&mut block, &mut decompressed, metadata.compression).unwrap();

                    let chunk = deserialize(
                        &decompressed,
                        &schema.fields,
                        &metadata.record.fields,
                        &projection,
                    )
                    .unwrap();

                    let mut chunks = chunks_ref.lock().unwrap();
                    chunks.push(chunk);
                    info!("Thread Call took {:.2?}", st1.elapsed());
                    ()
                });
                async {}
            });

            fut.await;
            Some(())
        }
    });

    join_all(work_futures).await;

    let pool = pool_ref.clone();
    pool.join();

    if resolved_table_name == ALERTS_TABLE_NAME {
        info!("Processing alerts...");
        let alert_blocks_ref =
            Arc::try_unwrap(alert_blocks_ref).map_err(|e| anyhow!("fail get rowgroups"))?;
        let alert_blocks = Mutex::into_inner(alert_blocks_ref)?;
        matano_alerts::process_alerts(s3, alert_blocks).await?;
        return Ok(());
    }

    let chunks_ref = Arc::try_unwrap(chunks_ref).map_err(|e| anyhow!("fail get rowgroups"))?;
    let chunks = Mutex::into_inner(chunks_ref)?;

    if chunks.len() == 0 {
        return Ok(());
    }

    println!("Writing...");
    let (field, arrays) = struct_wrap_arrow2_for_ffi(&table_schema, chunks);
    // TODO: fix to use correct partition (@shaeq)
    let partition_hour = chrono::offset::Utc::now().format("%Y-%m-%d-%H").to_string();
    write_arrow_to_s3_parquet(s3, resolved_table_name, partition_hour, field, arrays).await?;

    println!("----------------  Call took {:.2?}", start.elapsed());

    Ok(())
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct S3SQSMessage {
    pub resolved_table_name: String,
    pub bucket: String,
    pub key: String,
}
