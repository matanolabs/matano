// This example requires the following input to succeed:
// { "command": "do something" }
mod arrow;
mod avro;
mod models;
use ::value::value::timestamp_to_string;
use aws_sdk_sns::model::MessageAttributeValue;
use http::{HeaderMap, HeaderValue};
use std::borrow::Borrow;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::env::{set_var, var};
use std::mem::size_of_val;
use std::path::Path;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use tokio::fs;
use tokio::io::AsyncReadExt;
use tokio::task::spawn_blocking;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use apache_avro::{Codec, Schema, Writer};
use arrow2::datatypes::*;
use arrow2::io::avro::avro_schema::schema::{
    BytesLogical, Field as AvroField, Fixed, FixedLogical, IntLogical, LongLogical, Record,
    Schema as AvroSchema,
};
use arrow2::io::json::read;

use aws_sdk_s3::types::ByteStream;

use async_compression::tokio::bufread::{GzipDecoder, ZstdDecoder};
use async_stream::stream;
use futures::future::join_all;
use futures::{stream, Stream, StreamExt, TryStreamExt};
use serde_json::json;
use tokio_stream::StreamMap;
use tokio_util::codec::{FramedRead, LinesCodec};
use tokio_util::io::{ReaderStream, StreamReader};

use itertools::Itertools;
use lru::LruCache;
use std::iter::FromIterator;
use uuid::Uuid;
use vrl::TimeZone;

use models::*;
use rayon::prelude::*;
use shared::*;

use anyhow::{anyhow, Result};

use ::value::{Secrets, Value};
use aws_lambda_events::event::sqs::{SqsEvent, SqsMessage};
use lambda_runtime::{run, service_fn, Context, Error as LambdaError, LambdaEvent};
use log::{debug, error, info, warn};

use vrl::{diagnostic::Formatter, state, value, Program, Runtime, TargetValueRef};

use crate::arrow::{coerce_data_type, type_to_schema};
use crate::avro::TryIntoAvro;

thread_local! {
    pub static RUNTIME: RefCell<Runtime> = RefCell::new(Runtime::new(state::Runtime::default()));
    pub static LOG_SOURCES_CONFIG: RefCell<BTreeMap<String, LogSourceConfiguration>> = {
        let log_sources_configuration_path = Path::new(&var("LAMBDA_TASK_ROOT").unwrap().to_string()).join("log_sources_configuration.yml");
        let log_sources_configuration_string = std::fs::read_to_string(log_sources_configuration_path).expect("Unable to read file");

        let log_sources_configuration: Vec<LogSourceConfiguration> = serde_yaml::from_str(&log_sources_configuration_string).expect("Unable to parse log_sources_configuration.yml");
        let mut log_sources_configuration_map = BTreeMap::new();
        for log_source_configuration in log_sources_configuration.iter() {
            log_sources_configuration_map.insert(log_source_configuration.name.clone(), log_source_configuration.clone());
        }
        RefCell::new(log_sources_configuration_map)
    };
}

/// Compression schemes supported by Matano for log sources
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
// #[serde(rename_all = "lowercase")]
pub enum Compression {
    /// Infers the compression scheme from the object metadata
    Auto,
    /// Uncompressed.
    None,
    /// GZIP.
    Gzip,
    // ZSTD.
    Zstd,
}

#[tokio::main]
async fn main() -> Result<(), LambdaError> {
    setup_logging();
    let start = Instant::now();

    let func = service_fn(my_handler);
    run(func).await?;

    debug!("Call lambda took {:.2?}", start.elapsed());
    Ok(())
}

fn infer_compression(
    content_encoding: Option<&str>,
    content_type: Option<&str>,
    key: &str,
) -> Option<Compression> {
    let extension = std::path::Path::new(key)
        .extension()
        .and_then(std::ffi::OsStr::to_str);

    content_encoding
        .and_then(|encoding| match encoding {
            "gzip" => Some(Compression::Gzip),
            "zstd" => Some(Compression::Zstd),
            _ => None,
        })
        .or_else(|| {
            content_type.and_then(|content_type| match content_type {
                "application/gzip" | "application/x-gzip" => Some(Compression::Gzip),
                "application/zstd" => Some(Compression::Zstd),
                _ => None,
            })
        })
        .or_else(|| {
            extension.and_then(|extension| match extension {
                "gz" => Some(Compression::Gzip),
                "zst" => Some(Compression::Zstd),
                _ => None,
            })
        })
}

pub fn vrl<'a>(
    program: &'a str,
    value: &'a mut ::value::Value,
) -> Result<(::value::Value, &'a mut ::value::Value)> {
    thread_local!(
        static CACHE: RefCell<LruCache<String, Result<Program, String>>> =
            RefCell::new(LruCache::new(std::num::NonZeroUsize::new(400).unwrap()));
    );

    CACHE.with(|c| {
        let mut cache_ref = c.borrow_mut();
        let stored_result = (*cache_ref).get(program);

        let start = Instant::now();
        let compiled = match stored_result {
            Some(compiled) => match compiled {
                Ok(compiled) => Ok(compiled),
                Err(e) => {
                    return Err(anyhow!(e.clone()));
                }
            },
            None => match vrl::compile(&program, &vrl_stdlib::all()) {
                Ok(result) => {
                    println!(
                        "Compiled a vrl program ({}), took {:?}",
                        program
                            .lines()
                            .into_iter()
                            .skip(1)
                            .next()
                            .unwrap_or("expansion"),
                        start.elapsed()
                    );
                    (*cache_ref).put(program.to_string(), Ok(result.program));
                    if result.warnings.len() > 0 {
                        warn!("{:?}", result.warnings);
                    }
                    match (*cache_ref).get(program) {
                        Some(compiled) => match compiled {
                            Ok(compiled) => Ok(compiled),
                            Err(e) => {
                                return Err(anyhow!(e.clone()));
                            }
                        },
                        None => unreachable!(),
                    }
                }
                Err(diagnostics) => {
                    let msg = Formatter::new(&program, diagnostics).to_string();
                    (*cache_ref).put(program.to_string(), Err(msg.clone()));
                    Err(anyhow!(msg))
                }
            },
        }?;

        let mut metadata = ::value::Value::Object(BTreeMap::new());
        let mut secrets = ::value::Secrets::new();
        let mut target = TargetValueRef {
            value: value,
            metadata: &mut metadata,
            secrets: &mut secrets,
        };

        let time_zone_str = Some("tt".to_string()).unwrap_or_default();

        let time_zone = match TimeZone::parse(&time_zone_str) {
            Some(tz) => tz,
            None => TimeZone::Local,
        };

        let result = RUNTIME.with(|r| {
            let mut runtime = r.borrow_mut();

            match (*runtime).resolve(&mut target, &compiled, &time_zone) {
                Ok(result) => Ok(result),
                Err(err) => Err(anyhow!(err.to_string())),
            }
        });

        result.and_then(|output| Ok((output, value)))
    })
}

async fn read_lines_s3<'a>(
    s3: &aws_sdk_s3::Client,
    r: &DataBatcherRequestItem,
    compression: Option<Compression>,
) -> Result<Pin<Box<dyn Stream<Item = Result<Value>> + Send>>> {
    println!("Starting download");
    let res = s3
        .get_object()
        .bucket(r.bucket.clone())
        .key(r.key.clone())
        .send()
        .await
        .map_err(|e| {
            error!("Error downloading {} from S3: {}", r.key, e);
            e
        });
    let mut obj = res?;

    let mut reader = tokio::io::BufReader::new(obj.body.into_async_read());

    // let start = Instant::now();
    // let first = if let Some(first) = reader.next().await {
    //     println!("First byte: {:#?}", first);
    //     first
    // } else {
    //     return Ok(FramedRead::new(tokio::io::empty(), LinesCodec::new())
    //         .map_ok(|line|  Value::from(line.as_bytes()))
    //         .map_err(|e| anyhow!(e))
    //         .boxed());
    // };
    // println!("Finished reading first byte: took {:?}", start.elapsed());
    // let start = Instant::now();

    // let reader =
    //     StreamReader::new(stream::iter(Some(first)).chain(reader));

    let log_source = &r
        .key
        .split(std::path::MAIN_SEPARATOR)
        .next()
        .unwrap()
        .to_string();

    let compression = compression.unwrap_or(Compression::Auto);
    let compression = match compression {
        Compression::Auto => infer_compression(
            obj.content_encoding.as_deref(),
            obj.content_type.as_deref(),
            &r.key,
        )
        .unwrap_or(Compression::None),
        _ => compression,
    };

    let mut reader: Box<dyn tokio::io::AsyncRead + Send + Unpin> = match compression {
        Compression::Auto => unreachable!(),
        Compression::None => Box::new(reader),
        Compression::Gzip => Box::new({
            let mut decoder = GzipDecoder::new(reader);
            decoder.multiple_members(true);
            decoder
        }),
        Compression::Zstd => Box::new({
            let mut decoder = ZstdDecoder::new(reader);
            decoder.multiple_members(true);
            decoder
        }),
    };

    let expand_records_from_payload_expr = LOG_SOURCES_CONFIG.with(|c| {
        let log_sources_config = c.borrow();

        (*log_sources_config)
            .get(log_source)
            .unwrap()
            .ingest
            .as_ref()?
            .expand_records_from_payload
            .clone()
    });
    let lines = match expand_records_from_payload_expr {
        Some(prog) => {
            let start = Instant::now();
            let mut file_string = String::new();
            let cc = reader.read_to_string(&mut file_string).await?;
            println!("File read bytes: {}", cc);
            println!(
                "Finished download/decompressed into memory: took {:?}",
                start.elapsed()
            );
            let start = Instant::now();

            // println!("{}", file_string);
            let mut value = value!({ "__raw": file_string });
            let (expanded_records, _) = vrl(&prog, &mut value)?;
            println!("Expanded records from payload: took {:?}", start.elapsed(),);
            let expanded_records = match expanded_records {
                Value::Array(records) => records,
                _ => return Err(anyhow!("Expanded records must be an array")),
            };

            Box::pin(stream! {
                for record in expanded_records {
                    yield Ok(record);
                }
            })
        }
        None => FramedRead::new(reader, LinesCodec::new())
            .map(|v| match v {
                Ok(v) => Ok(Value::from(v.as_bytes())),
                Err(e) => Err(anyhow!(e)),
            })
            .boxed(),
    };

    Ok(lines)
}

pub(crate) async fn my_handler(event: LambdaEvent<SqsEvent>) -> Result<SuccessResponse> {
    info!("Request: {:?}", event);

    let s3_download_items = event
        .payload
        .records
        .iter()
        .flat_map(|record| {
            let body = record.body.as_ref().expect("SQS message body is required");
            let data_batcher_request_items: Vec<DataBatcherRequestItem> = serde_json::from_str(
                &body,
            )
            .expect("Could not deserialize SQS message body as list of DataBatcherRequestItems.");
            data_batcher_request_items
        })
        .filter(|d| d.size > 0)
        .filter(|d| {
            // TODO: needs to handle non matano managed sources (@shaeq)
            return d.is_matano_managed() && LOG_SOURCES_CONFIG.with(|c| {
                let log_sources_config = c.borrow();
                let log_source = &d
                            .key
                            .split(std::path::MAIN_SEPARATOR)
                            .next()
                            .unwrap()
                            .to_string();
                (*log_sources_config).contains_key(log_source)
            })
        })
        .collect::<Vec<_>>();

    info!(
        "Processing {} files from S3, of total size {} bytes",
        s3_download_items.len(),
        s3_download_items.iter().map(|d| d.size).sum::<i32>()
    );

    let config = aws_config::load_from_env().await;
    let s3 = aws_sdk_s3::Client::new(&config);
    let sns = aws_sdk_sns::Client::new(&config);

    println!("current_num_threads() = {}", rayon::current_num_threads());

    let transformed_lines_streams = s3_download_items
        .iter()
        .map(|item| {
            let s3 = &s3;

            async move {
                let raw_lines = read_lines_s3(s3, &item, None).await.unwrap();

                let reader = raw_lines;

                let transform_expr = LOG_SOURCES_CONFIG
                    .with(|c| {
                        let log_sources_config = c.borrow();

                        let log_source = &item
                            .key
                            .split(std::path::MAIN_SEPARATOR)
                            .next()
                            .unwrap()
                            .to_string();

                        (*log_sources_config)
                            .get(log_source)
                            .unwrap()
                            .transform
                            .clone()
                    })
                    .unwrap();

                let wrapped_transform_expr = format!(
                    "
                    .related.ip = []
                    .related.hash = []
                    .related.user = []
                    {}
                    del(.json)
                    ",
                    transform_expr
                );

                let transformed_lines_as_json = reader
                    .chunks(400)
                    .filter_map(move |chunk| {
                        let wrapped_transform_expr = wrapped_transform_expr.clone(); // hmm

                        async move {
                            let start = Instant::now();
                            let transformed_lines = async_rayon::spawn(move || {
                                let transformed_chunk = chunk
                                    .into_par_iter()
                                    .map(|r| match r {
                                        Ok(line) => {
                                            let mut v = value!({ "json": line });
                                            vrl(&wrapped_transform_expr, &mut v).map_err(|e| {
                                                anyhow!("Failed to transform: {}", e)
                                            })?;
                                            Ok(v)
                                        }
                                        Err(e) => Err(anyhow!("Malformed line: {}", e)),
                                    })
                                    .collect::<Vec<_>>();
                                transformed_chunk
                            })
                            .await;

                            println!("Transformed lines: took {:?}", start.elapsed());
                            Some(stream::iter(transformed_lines))
                        }
                    })
                    .flatten();

                let stream = transformed_lines_as_json.filter_map(|line| async move {
                    match line {
                        Ok(line) => Some(line),
                        Err(e) => {
                            error!("{}", e);
                            // Err(std::io::Error::new(std::io::ErrorKind::Other, e))
                            None
                        }
                    }
                });

                (item.key.clone(), stream)
            }
        })
        .collect::<Vec<_>>();

    let result = join_all(transformed_lines_streams).await;
    let result = result.into_iter().group_by(|(object_key, _)| {
        let log_source = object_key
            .split(std::path::MAIN_SEPARATOR)
            .next()
            .unwrap()
            .to_string();
        log_source
    });

    let mut merged: HashMap<String, StreamMap<String, Pin<Box<dyn Stream<Item = Value> + Send>>>> =
        HashMap::new();
    for (log_source, streams) in result.into_iter() {
        for (i, (_object_key, stream)) in streams.enumerate() {
            merged
                .entry(log_source.clone())
                .or_insert(StreamMap::new())
                .insert(
                    format!("{}", i), // object_key.clone(),
                    Box::pin(stream), // as Pin<Box<dyn Stream<Item = usize> + Send>>,
                );
        }
    }

    let futures = merged
        .into_iter()
        .map(|(log_source, stream)| {
            let s3 = &s3;
            let sns = &sns;
            let num_rows_to_infer_schema = 100;

            async move {
                let mut reader = stream.map(|(_, value)| value);

                let now = Instant::now();
                let head = reader
                    .by_ref()
                    .take(num_rows_to_infer_schema)
                    .collect::<Vec<_>>()
                    .await;
                println!("Read 100 lines from head, {:?}", now.elapsed());
                let now = Instant::now();

                let inferred_avro_schema = {
                    let head = head.clone();
                    spawn_blocking(move || {
                        // CPU bound (infer schemas)
                        let data_types = head
                            .iter()
                            .flat_map(|v| {
                                match read::infer(
                                    &read::json_deserializer::parse(
                                        &serde_json::to_vec(v).unwrap(),
                                    )
                                    .unwrap(),
                                ) {
                                    Ok(DataType::Null) => None,
                                    Ok(dt) => Some(dt),
                                    Err(e) => None,
                                }
                            })
                            .collect::<Vec<_>>();
                        let data_types = data_types.into_iter().collect::<HashSet<_>>();
                        let unioned_data_type =
                            &coerce_data_type(&data_types.iter().collect::<Vec<_>>());

                        type_to_schema(unioned_data_type, false, "root".to_string()).unwrap()
                    })
                }
                .await
                .unwrap();
                println!("Inferred schema, took {:?}", now.elapsed());

                let reader = stream::iter(head).chain(reader); // rewind

                let avro_schema_json_string =
                    serde_json::to_string_pretty(&inferred_avro_schema).unwrap();

                // let output_schemas_path = Path::new(&var("LAMBDA_TASK_ROOT").unwrap().to_string()).join("generated_schemas");
                // fs::create_dir_all(&output_schemas_path).await.unwrap();
                // let output_schema_path = output_schemas_path.join(format!("{}.avsc", log_source));
                // fs::write(output_schema_path,&avro_schema_json_string).await.unwrap();
                // let avro_schema_json_string = fs::read_to_string(output_schema_path).await.unwrap();

                let schemas_path = Path::new("/opt/schemas");
                let schema_path = schemas_path.join(&log_source).join("avro_schema.avsc");
                let avro_schema_json_string = fs::read_to_string(schema_path).await.unwrap();

                let mut inferred_avro_schema = Schema::parse_str(&avro_schema_json_string).unwrap();

                let inferred_avro_schema = Arc::new(inferred_avro_schema);

                let writer_schema = inferred_avro_schema.clone();
                let writer: Writer<Vec<u8>> = Writer::builder()
                    .schema(&writer_schema)
                    .writer(Vec::new())
                    .codec(Codec::Snappy)
                    .block_size(1 * 1024 * 1024 as usize)
                    .build();
                let writer = RefCell::new(writer);

                reader
                    .chunks(8000) // chunks(8000) (avro block size)
                    .for_each(|chunk| {
                        // .for_each_concurrent(100, |chunk| { // figure out how to make this work (lock writer so that we can write better blocks...)
                        let mut writer = writer.borrow_mut();
                        let schema = Arc::clone(&inferred_avro_schema);

                        async move {
                            let avro_values = spawn_blocking(move || {
                                // CPU bound (VRL transform)
                                let avro_values = chunk
                                    .into_par_iter()
                                    .map(|v| {
                                        let v_avro = TryIntoAvro::try_into(v).unwrap();
                                        v_avro.resolve(schema.as_ref()).unwrap()
                                    })
                                    .collect::<Vec<_>>();

                                avro_values
                            })
                            .await
                            .unwrap();

                            writer.extend_from_slice(&avro_values).unwrap(); // IO (well theoretically) TODO: since this actually does non-trivial comptuuation work too (schema validation), we need to figure out a way to prevent this from blocking our main async thread
                        }
                    })
                    .await;

                let writer = writer.into_inner();
                let bytes = writer.into_inner().unwrap();

                if bytes.len() == 0 {
                    return;
                }

                let uuid = Uuid::new_v4();
                let bucket = var("MATANO_REALTIME_BUCKET_NAME").unwrap().to_string();
                let key = format!("transformed/{}/{}.snappy.avro", &log_source, uuid);
                // let local_path = output_schemas_path.join(format!("{}.avro", log_source));
                // fs::write(local_path, bytes).await.unwrap();
                s3.put_object()
                    .bucket(&bucket)
                    .key(&key)
                    .body(ByteStream::from(bytes))
                    .content_type("application/avro-binary".to_string())
                    // .content_encoding("application/zstd".to_string())
                    .send()
                    .await
                    .map_err(|e| {
                        error!("Error putting {} to S3: {}", key, e);
                        e
                    })
                    .unwrap();

                sns.publish()
                    .topic_arn(var("MATANO_REALTIME_TOPIC_ARN").unwrap().to_string())
                    .message(
                        json!({
                            "bucket": &bucket,
                            "key": &key,
                            "log_source": log_source
                        })
                        .to_string(),
                    )
                    .message_attributes(
                        "log_source",
                        MessageAttributeValue::builder()
                            .data_type("String".to_string())
                            .string_value(log_source)
                            .build(),
                    )
                    .send()
                    .await
                    .map_err(|e| {
                        error!("Error publishing to SNS: {}", e);
                        e
                    })
                    .unwrap();
            }
        })
        .collect::<Vec<_>>();

    let mut start = Instant::now();
    println!("Starting processing");
    {
        let _ = join_all(futures).await;
        println!(
            "process + upload time (exclude download/decompress/expand): {:?}",
            start.elapsed()
        );
        start = Instant::now();
    }
    println!("time to drop {:?}", start.elapsed());

    let resp = SuccessResponse {
        req_id: event.context.request_id,
        msg: format!("Hello from Transformer! The command was executed."),
    };

    // return `Response` (it will be serialized to JSON automatically by the runtime)
    Ok(resp)
}
