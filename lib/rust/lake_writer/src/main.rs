use aws_sdk_s3::types::ByteStream;
use concurrent_queue::ConcurrentQueue;
use futures::future::join_all;
use parquet::arrow::{arrow_writer, ArrowReader, ParquetFileArrowReader};
use parquet::file::serialized_reader::SerializedFileReader;
use serde::{Deserialize, Serialize};
use shared::*;
use tokio::runtime::Handle;
use tokio_util::codec::{FramedRead, LinesCodec};
use tokio_util::io::StreamReader;
use uuid::Uuid;

use bytes::Bytes;
use std::fs::File;
use std::rc::Rc;
use std::sync::{Mutex, Condvar};
use std::sync::{Arc, MutexGuard};
use std::thread::{self, JoinHandle, ScopedJoinHandle};
use std::{time::Instant, vec};

use aws_lambda_events::event::sqs::SqsEvent;
use lambda_runtime::{handler_fn, Context as LambdaContext, Error as LambdaError};
use log::{debug, error, info};
use parquet::file::reader::FileReader;
use threadpool::ThreadPool;

use std::io::Write;

use arrow::json::ReaderBuilder;
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, Encoding},
    errors::ParquetError,
    file::properties::{EnabledStatistics, WriterProperties},
};

use std::io::{BufRead, BufReader, Seek};
use std::io::{Cursor, Read};
use tokio::io::AsyncBufReadExt;

use lazy_static::{__Deref, lazy_static};

use anyhow::{anyhow, Error, Result};
use arrow::datatypes::Schema;

use aws_sdk_s3::Region;
use futures::{Future, StreamExt, stream};

use tokio_stream::{wrappers::LinesStream, StreamMap};

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

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

    let func = handler_fn(my_handler);
    lambda_runtime::run(func).await?;

    info!("Call lambda took {:.2?}", start.elapsed());

    Ok(())
}

// fn getit<T>(v: Arc<Mutex<T>>) -> MutexGuard<'static, T> {
//     let ret = v.clone();
//     let ret = v.lock().unwrap();
//     ret
// }

// async fn process_message()
// ctx: LambdaContext
pub(crate) async fn my_handler(event: SqsEvent, _ctx: LambdaContext) -> Result<()> {
    info!("Request: {:?}", event);

    let downloads = event
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

    info!("Starting {} downloads from S3", downloads.len());

    let start = Instant::now();

    let config = aws_config::load_from_env().await;
    let s3 = aws_sdk_s3::Client::new(&config);


    let myq: ConcurrentQueue<Vec<String>> = ConcurrentQueue::unbounded();
    let myq_ref = Arc::new(std::sync::Mutex::new(myq));

    println!("GOT HERE???");

    let mut schema: Option<Arc<Schema>> = None;

    let tasks = downloads
        .into_iter()
        .map(|r| {
            let s3 = &s3;
            println!("Inside task map()");
            let ret = (async move {
                println!("inside move...");

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
                let mut obj = obj_res.unwrap();
                println!("Got object...");


                // let first = obj.body.next().await.unwrap();
                // let first_bytes = first.as_ref().unwrap().clone();
                // let newschema = Some(get_arrow_json_reader(first_bytes).unwrap().schema().clone());
                // schema_ref = newschema;
                // // (FramedRead::new(tokio::io::empty(), LinesCodec::new()).boxed());
            
                // let reader = tokio::io::BufReader::new(
                //     StreamReader::new(stream::iter(Some(first)).chain(obj.body)), // ).map_err(|e| ??)
                // );

                let reader = obj.body.into_async_read();
                let mut lines = tokio::io::BufReader::new(reader).lines();
                let lls = LinesStream::new(lines);
                lls
            });
            ret
        })
        .collect::<Vec<_>>();
    let result = join_all(tasks).await;
    let res = result.into_iter().map(|lines| lines).collect::<Vec<_>>();

    let mut map = StreamMap::new();


    let mut first_stream: Option<_> = None;
    for (i, x) in res.into_iter().enumerate() {
        dbg!(i);
        if i == 1 {
            first_stream = Some(Box::pin(x));
        } else {
            map.insert(format!("{}", i), Box::pin(x));
        }
    }

    let num_lines_per_chunk = 1024 * 1024 * 50;

    let mut schema_holder: Option<Arc<Schema>> = None;

    let pair = Arc::new((Mutex::new(schema_holder), Condvar::new()));
    let pair2 = pair.clone();

    let stream_map = map.filter_map(|x| async move { x.1.ok() });

    let s3_chunks = stream_map.chunks(num_lines_per_chunk);

    let s3_tasks = s3_chunks
        .for_each_concurrent(100, |lines| {
            let q_ref = myq_ref.clone();

            async move {
                let q = q_ref.lock().unwrap();
                q.push(lines).unwrap();
            }
        });

    
    first_stream.unwrap()
    .filter_map(|x| async move { x.ok() })
    .chunks(num_lines_per_chunk).for_each(|lines| {
        let q_ref = myq_ref.clone();

        let newschema = get_arrow_json_reader(lines.clone()).unwrap().schema();
        schema = Some(newschema);

        async move {
            let q = q_ref.lock().unwrap();
            q.push(lines).unwrap();
        }
    }).await;

    let schema_ref = schema.unwrap();

    let mut arrow_writer: ArrowWriter<File> = get_arrow_writer(schema_ref)?;
    let mut arrow_writer_ref = Arc::new(std::sync::Mutex::new(arrow_writer));

    let pool = ThreadPool::new(4);
    let pool_ref = Arc::new(Mutex::new(pool));

    println!("THRS:::");

    let please_stop = Arc::new(AtomicBool::new(false));

    thread::scope(|s| {
        let handle = s.spawn(|| {
            loop {
                let should_i_stop = please_stop.clone();
                let should_stop = should_i_stop.load(Ordering::SeqCst);
                if should_stop {
                    println!("I was told to stop, STOPPING..........");
                    break;
                }

                let pool = pool_ref.clone();
                let pool = pool.lock().unwrap();

                let q_ref = myq_ref.clone();
                let q = q_ref.lock().unwrap();

                let mut lines_batch = q.pop();
                match lines_batch {
                    Ok(lines) => {
                        let mut writer = arrow_writer_ref.clone();
                        println!("Firing to pool...");
                        pool.execute(move || {
                            let mut writer = writer.lock().unwrap();
                            write_rows(lines, writer).unwrap();
                        });

                    },
                    Err(e) => ()
                };
                
            }
        });

        println!("spawn1###");

        let st1 = Instant::now();

        // Await s3 task, all files downloaded
        let handle2 = s.spawn(|| {
            println!("IN THREAD: TOKIO RUNTIME BLOCK ON");
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            println!("TRY: block on s3tasks");
            runtime.block_on(s3_tasks);
            println!("FIN: block on s3tasks");
        });
        println!("GOT handle2");
        handle2.join().unwrap();

        info!("await took: {:.2?}", st1.elapsed());
        println!("JOINED TASKS");

        // Now shut the thread
        println!("GoNNA stop the thread....");
        please_stop.store(true, Ordering::SeqCst);
        println!("TOLD THREAAD to stop");

        println!("Trying to lock pool");
        let pool = pool_ref.clone();
        let pool = pool.lock().unwrap();
        println!("Got pool lock!");


        println!("TRY: Joining handle...");
        handle.join().unwrap();
        println!("FIN: Joining handle...");


        println!("TRY: Joining pool");
        pool.join();
        println!("FIN: pool JOIN");
    });

    println!("EXITED LOOP");

    // let st1 = Instant::now();
    // s3_tasks.await;
    // info!("await took: {:.2?}", st1.elapsed());
    // println!("JOINED TASKS");

    // let q_ref = myq_ref.clone();
    // let q = q_ref.lock().unwrap();

    // println!("QLEN: {}", q.len());
    // q.close();

    // let pool = pool_ref.clone();
    // let pool = pool.lock().unwrap();

    // pool.join();
    // println!("FIN POOL JOIN");

    // write to S3..
    println!("CLOSING WRITER");

    let arrow_writer =
        Arc::try_unwrap(arrow_writer_ref).map_err(|_| anyhow!("FAIL to unwrap writer."))?;
    let arrow_writer = Mutex::into_inner(arrow_writer)?;
    arrow_writer.close()?;

    Ok(())
}

fn get_arrow_writer(schema_ref: Arc<Schema>) -> Result<ArrowWriter<File>> {
    let writer_props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_created_by("matano".to_string())
        .build();

    let uuid = Uuid::new_v4().to_string();
    let outpath = format!("/tmp/{uuid}.parquet");
    println!("Writing to: {}", outpath);

    let output = File::create(outpath.clone())?;

    let mut writer = ArrowWriter::try_new(output, schema_ref, Some(writer_props))?;
    Ok(writer)
}

fn get_arrow_json_reader(lines: Vec<String>) -> Result<arrow::json::Reader<Cursor<String>>> {
    let mycursor = Cursor::new(lines.join("\n"));
    let builder = ReaderBuilder::new()
        // .with_schema(schema_ref.clone())
        .infer_schema(Some(3))
        .with_batch_size(JSON_READ_BATCH_SIZE);
    let json_reader = builder.build(mycursor)?;
    Ok(json_reader)
}

const JSON_READ_BATCH_SIZE: usize = 1024 * 100; 

fn write_rows(lines: Vec<String>, mut writer: MutexGuard<ArrowWriter<File>>) -> Result<()> {
    println!("I came to convert_parquet");
    // if 9 == 9 {
    //     // let mut f = File::create("/home/samrose/workplace/matano/lib/rust/simpy/oil.json")?;
    //     // f.write_all(lines.join("\n").as_bytes())?;
    //     return Ok(());
    // }
    println!("THE LENGTH IS: {:?}", lines.len());
    let rem = lines.get(0).unwrap();
    println!("&&&&&&&&&&&&&&&&&&&&&& JSON &&&&&&&&&&&&&&&&&&");
    println!("{:?}", rem);
    println!("&&&&&&&&&&&&&&&&&&&&&& JSON &&&&&&&&&&&&&&&&&&");
    // let schema_ref: Arc<Schema> = Arc::new(ECS_SCHEMA.to_owned());

    let mycursor = Cursor::new(lines.join("\n"));
    let builder = ReaderBuilder::new()
        // .with_schema(schema_ref.clone())
        .infer_schema(Some(3))
        .with_batch_size(JSON_READ_BATCH_SIZE);
    let json_reader = builder.build(mycursor)?;

    // let schema_ref = json_reader.schema();

    println!("----------------ABOUT TO READ..........");

    for batch in json_reader {
        println!("Writing batch...");
        let b = batch?;
        println!("{}", b.num_rows());
        writer.write(&b)?;
    }

    // println!("############## Now read.....");

    // let mut par2 = ParquetFileArrowReader::new(Arc::new(SerializedFileReader::new(File::open(
    //     outpath.clone(),
    // )?)?));
    // let sc11 = par2.get_schema()?;

    println!("Finished!");

    Ok(())
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct S3SQSMessage {
    pub bucket: String,
    pub key: String,
}
