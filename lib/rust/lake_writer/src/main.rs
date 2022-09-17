use arrow::record_batch::RecordBatchReader;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Field;
use arrow2::io::parquet::write::row_group_iter;
use arrow2::io::parquet::write::to_parquet_schema;
use aws_sdk_s3::types::ByteStream;
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use shared::*;
use tokio::fs;
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

use arrow2::io::ipc::write::FileWriter as IpcFileWriter;
use arrow2::io::print::write;

use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

// const ECS_PARQUET: &[u8] = include_bytes!("../../../../data/ecs_parquet_metadata.parquet");

// lazy_static! {
//     static ref ECS_SCHEMA: Schema = {
//         let rr1 = SerializedFileReader::new(Bytes::from(ECS_PARQUET)).unwrap();
//         let rr1_ref = Arc::new(rr1);
//         let mut rr2 = ParquetFileArrowReader::new(rr1_ref);
//         rr2.get_schema().unwrap()
//     };
// }

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

// fn getit<T>(v: Arc<Mutex<T>>) -> MutexGuard<'static, T> {
//     let ret = v.clone();
//     let ret = v.lock().unwrap();
//     ret
// }

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

    let log_source = downloads.first().map(|m| m.log_source.clone()).unwrap();
    info!("Processing for log_source: {}", log_source);

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
    let iter = Box::new(arrays.clone().into_iter().map(Ok)) as _;

    let mut arrow_array_stream = Box::new(arrow2::ffi::ArrowArrayStream::empty());

    *arrow_array_stream = arrow2::ffi::export_iterator(iter, field.clone());

    // import (arrow2)
    // let mut stream = unsafe { arrow2::ffi::ArrowArrayStreamReader::try_new(arrow_array_stream)? };

    // import (arrow)
    let stream_ptr =
        Box::into_raw(arrow_array_stream) as *mut arrow::ffi_stream::FFI_ArrowArrayStream;
    let stream_reader =
        unsafe { arrow::ffi_stream::ArrowArrayStreamReader::from_raw(stream_ptr).unwrap() };
    let imported_schema = stream_reader.schema();

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD)
        .build();

    let buf = std::io::Cursor::new(vec![]);
    let mut writer = ArrowWriter::try_new(buf, imported_schema, Some(props)).unwrap();

    for record_batch in stream_reader {
        let record_batch = record_batch?;
        writer.write(&record_batch)?;
    }
    writer.flush()?;

    // let schema_ffi = arrow2::ffi::export_field_to_c(&field);
    // let array_ffi = arrow2::ffi::export_array_to_c(composite_array);

    let bytes = writer.into_inner()?.into_inner();

    // return Ok(());

    // let filesize = writer.end(None)?;
    // println!("Parquet file size: {}", filesize);

    let bytestream = ByteStream::from(bytes);
    let bucket = std::env::var("OUT_BUCKET_NAME")?;
    let key_prefix = std::env::var("OUT_KEY_PREFIX")?;
    // lake/TABLE_NAME/data/partition_day=2022-07-05/<file>.parquet
    let table_name = log_source;
    let key = format!(
        "{}/{}/data/{}.parquet",
        key_prefix,
        table_name,
        Uuid::new_v4()
    );
    println!("Writing to: {}", key);

    println!("Starting upload...");
    let ws1 = Instant::now();
    let _upload_res = &s3
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(bytestream)
        .send()
        .await?;
    println!("Upload took: {:.2?}", ws1.elapsed());
    // (drop/release)
    unsafe {
        Box::from_raw(stream_ptr);
    }

    println!("----------------  Call took {:.2?}", start.elapsed());

    Ok(())
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct S3SQSMessage {
    pub log_source: String,
    pub bucket: String,
    pub key: String,
}
