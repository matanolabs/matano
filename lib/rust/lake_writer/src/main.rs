use arrow2::datatypes::Field;
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use shared::*;
use uuid::Uuid;

use std::sync::Arc;
use std::sync::Mutex;
use std::{time::Instant, vec};

use aws_lambda_events::event::sqs::SqsEvent;
use lambda_runtime::{run, service_fn, Context, Error as LambdaError, LambdaEvent};

use log::{error, info};
use threadpool::ThreadPool;

use anyhow::{anyhow, Result};

// use tokio_stream::StreamExt;
use futures::StreamExt;

use arrow2::io::avro::avro_schema::file::Block;
use arrow2::io::avro::avro_schema::read_async::{block_stream, decompress_block, read_metadata};
use arrow2::io::avro::read::{deserialize, infer_schema};
use arrow2::io::parquet::write::{
    transverse, CompressionOptions, Encoding, FileWriter, RowGroupIterator, Version, WriteOptions,
};
use tokio_util::compat::TokioAsyncReadCompatExt;

mod common;
// mod matano_alerts;
use common::write_arrow_to_s3_parquet;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

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
        println!("Empty event, returning...");
        return Ok(());
    }

    let resolved_table_name = downloads
        .first()
        .map(|m| m.resolved_table_name.clone())
        .unwrap();
    info!("Processing for table: {}", resolved_table_name);

    info!("Starting {} downloads from S3", downloads.len());

    let config = aws_config::load_from_env().await;
    let s3 = aws_sdk_s3::Client::new(&config);

    println!("GOT HERE???");

    let start = Instant::now();

    let options = WriteOptions {
        write_statistics: true,
        compression: CompressionOptions::Zstd(None),
        version: Version::V2,
    };

    let pool = ThreadPool::new(4);
    let pool_ref = Arc::new(pool);
    let chunks = vec![];
    let chunks_ref = Arc::new(Mutex::new(chunks));

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
        let pool_ref = pool_ref.clone();

        async move {
            let metadata = read_metadata(reader).await.unwrap();
            let schema = infer_schema(&metadata.record).unwrap();
            let schema_copy = schema.clone();

            // // TODO move out
            let projection = Arc::new(schema.fields.iter().map(|_| true).collect::<Vec<_>>());

            let blocks = block_stream(reader, metadata.marker).await;

            let fut = blocks.for_each_concurrent(1000000, move |block| {
                println!("Getting block....");
                let mut block = block.unwrap();
                println!("GOT block");
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
                    println!(
                        "$$$$$$$$$$$$$$$$$$$$$$$$$$$$  THREAD Call took {:.2?}",
                        st1.elapsed()
                    );
                    ()
                });
                async {}
            });

            fut.await;
            schema_copy
        }
    });

    let res = join_all(work_futures).await;

    let pool = pool_ref.clone();
    pool.join();

    let chunks_ref = Arc::try_unwrap(chunks_ref).map_err(|e| anyhow!("fail get rowgroups"))?;
    let chunks = Mutex::into_inner(chunks_ref)?;
    dbg!(chunks.len());

    if chunks.len() == 0 {
        return Ok(());
    }

    println!("Writing...");
    let schema = res.first().unwrap();

    let field = Field::new(
        "root".to_owned(),
        arrow2::datatypes::DataType::Struct(schema.fields.clone()),
        false,
    );
    let arrays = chunks
        .iter()
        .map(|chunk| {
            let composite_array: Box<(dyn arrow2::array::Array)> =
                Box::new(arrow2::array::StructArray::from_data(
                    field.clone().data_type,
                    chunk.arrays().to_vec(),
                    None,
                ));
            composite_array
        })
        .collect::<Vec<_>>();

    // TODO: fix to use correct partition (@shaeq)
    let partition_day = chrono::offset::Utc::now().date_naive().to_string();
    write_arrow_to_s3_parquet(&s3, resolved_table_name, partition_day, field, arrays).await?;

    println!("----------------  Call took {:.2?}", start.elapsed());

    Ok(())
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct S3SQSMessage {
    pub resolved_table_name: String,
    pub bucket: String,
    pub key: String,
}
