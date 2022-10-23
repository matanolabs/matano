// This example requires the following input to succeed:
// { "command": "do something" }
mod arrow;
mod avro;
mod models;
use ::value::value::timestamp_to_string;
use aws_sdk_sns::model::MessageAttributeValue;
use chrono::Utc;
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
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncReadExt;
use tokio::task::spawn_blocking;
use walkdir::WalkDir;

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

use config::{Config, ConfigError, Environment, File};

thread_local! {
    pub static RUNTIME: RefCell<Runtime> = RefCell::new(Runtime::new(state::Runtime::default()));
    pub static LOG_SOURCES_CONFIG: RefCell<BTreeMap<String, LogSourceConfiguration>> = {
        let log_sources_configuration_path = Path::new(&var("LAMBDA_TASK_ROOT").unwrap().to_string()).join("log_sources");
        let mut log_sources_configuration_map: BTreeMap<String, LogSourceConfiguration> = BTreeMap::new();

        for entry in WalkDir::new(log_sources_configuration_path).min_depth(1).max_depth(1) {
            let log_source_dir_path = match entry {
                Ok(v) => v,
                Err(e) => {
                    error!("Invalid entry while walking children for log_sources/ directory: {}", e);
                    continue;
                }
            };
            let log_source_dir_path = log_source_dir_path.path();
            if !log_source_dir_path.is_dir() {
                continue;
            }

            let log_source_folder_name = match log_source_dir_path.file_name() {
                Some(v) => v,
                None => {
                    error!("Invalid entry name under log_sources/.");
                    continue;
                }
            };

            let log_source_folder_name = match log_source_folder_name.to_str() {
                Some(v) => v,
                None => {
                    error!("Invalid entry name under log_sources/");
                    continue;
                }
            };

            let log_source_configuration_path = log_source_dir_path.join("log_source.yml");
            let log_source_configuration_path = log_source_configuration_path.as_path().to_str().unwrap();
            let base_configuration_builder = Config::builder()
                .add_source(
                    File::with_name(log_source_configuration_path).required(true)
                );
            let base_configuration = base_configuration_builder.build().expect(format!("Failed to load base configuration for log_source: {}/", log_source_folder_name).as_str());

            let mut log_source_configuration = LogSourceConfiguration {
                base: base_configuration,
                tables: HashMap::new()
            };
            let log_source_name = log_source_configuration.base.get_string("name").unwrap();

            let tables_path = log_source_dir_path.join("tables");
            if tables_path.is_dir() {
                for entry in WalkDir::new(&tables_path).min_depth(1).max_depth(1) {
                    let tables_path = &tables_path.as_path().to_str().unwrap();

                    let table_configuration_path = entry.expect(format!("Invalid entry while walking children for {} directory.", &tables_path).as_str());
                    let table_configuration_path = table_configuration_path.path();

                    let err_str = format!("Invalid table entry: {}.", table_configuration_path.display());
                    let table_file_name = table_configuration_path.file_name().expect(&err_str).to_str().expect(&err_str);

                    let extension = table_configuration_path
                        .extension()
                        .and_then(std::ffi::OsStr::to_str);

                    if !table_configuration_path.is_file() || !(extension == Some("yml") || extension == Some("yaml")) {
                        continue;
                    }

                    let table_configuration_path = table_configuration_path.to_str().unwrap();

                    let table_configuration = Config::builder()
                    .add_source(
                        File::with_name(table_configuration_path).required(true)
                    )
                    .build();

                    let table_configuration = match table_configuration {
                        Ok(v) => v,
                        Err(e) => {
                            error!("Failed to load table configuration for log_source={}, table_path={}: {}", log_source_name, table_file_name, e);
                            continue;
                        }
                    };

                    let table_name = table_configuration.get_string("name").unwrap();

                    // .add_source(Environment::with_prefix("app"));
                    log_source_configuration.tables.insert(table_name, table_configuration);
                }
            }
            log_sources_configuration_map.insert(log_source_name, log_source_configuration);
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

impl Compression {
    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s {
            "infer" | "auto" => Ok(Self::Auto),
            "none" => Ok(Self::None),
            "gzip" | "gz" => Ok(Self::Gzip),
            "zstd" | "zst" => Ok(Self::Zstd),
            _ => Err(anyhow!("Unknown compression type {}", s)),
        }
    }
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

async fn infer_compression(
    reader: &mut tokio::io::BufReader<impl tokio::io::AsyncRead + Unpin>,
    content_encoding: Option<&str>,
    content_type: Option<&str>,
    key: &str,
) -> Option<Compression> {
    let extension = std::path::Path::new(key)
        .extension()
        .and_then(std::ffi::OsStr::to_str);

    let inferred_from_file_meta = content_encoding
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
        });

    match inferred_from_file_meta {
        Some(compression) => {
            debug!("Inferred compression from file metadata: {:?}", compression);
            Some(compression)
        }
        None => {
            let buf = reader.fill_buf().await.unwrap();
            let inferred_compression = infer::get(buf)
                .and_then(|kind| Compression::from_str(kind.extension()).ok())
                .or(Some(Compression::None));
            debug!(
                "Inferred compression from bytes: {:?}",
                inferred_compression
            );
            inferred_compression
        }
    }
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
                    debug!(
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

fn get_log_source_from_key(key: &str) -> Option<String> {
    LOG_SOURCES_CONFIG.with(|c| {
        let log_sources_config = c.borrow();

        // TODO: @shaeq: optimize?
        // Try get from BYOB or else assume managed and get from path
        let ret = (*log_sources_config)
            .iter()
            .find(|(_, v)| {
                v.base
                    .get_string("ingest.s3_source.key_prefix")
                    .map(|prefix| key.starts_with(prefix.as_str()))
                    .unwrap_or(false)
            })
            .map(|r| r.0.to_owned())
            .or_else(|| {
                let ls = key
                    .split(std::path::MAIN_SEPARATOR)
                    .next()
                    .unwrap()
                    .to_string();
                log_sources_config.contains_key(&ls).then_some(ls)
            });
        debug!("Got log source: {:?} from key: {}", ret, key);
        ret
    })
}

async fn read_events_s3<'a>(
    s3: &aws_sdk_s3::Client,
    r: &DataBatcherOutputRecord,
    log_source: &String,
) -> Result<(
    config::Config,
    Pin<Box<dyn Stream<Item = Result<Value>> + Send>>,
)> {
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

    let compression = Compression::Auto;
    let compression = match compression {
        Compression::Auto => infer_compression(
            &mut reader,
            obj.content_encoding.as_deref(),
            obj.content_type.as_deref(),
            &r.key,
        )
        .await
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

    let select_table_expr = LOG_SOURCES_CONFIG.with(|c| {
        let log_sources_config = c.borrow();

        (*log_sources_config)
            .get(log_source)
            .unwrap()
            .base
            .get_string("ingest.select_table_from_payload_metadata")
            .ok()
    });

    let table_name = match select_table_expr {
        Some(prog) => {
            let r_json = serde_json::to_value(r).unwrap();
            let mut value = value!({
                "__metadata": {
                    "s3": {
                        r_json
                    }
                }
            });

            let wrapped_prog = format!(
                // For type safety, TODO(shaeq): use SchemaKind's
                "
                .__metadata.s3.bucket = string!(.__metadata.s3.bucket)
                .__metadata.s3.key = string!(.__metadata.s3.key)
                .__metadata.s3.size = int!(.__metadata.s3.size)
                .__metadata.s3.sequencer = string!(.__metadata.s3.sequencer)

                {}
                ",
                prog
            );
            let (table_name, _) = vrl(&wrapped_prog, &mut value)?;

            match &table_name {
                Value::Null => Ok("default".to_owned()),
                Value::Bytes(b) => Ok(table_name.to_string_lossy()),
                _ => {
                    Err(anyhow!("Invalid table_name returned from select_table_from_payload_metadata expression: {}", table_name))
                }
            }
        }
        None => Ok("default".to_owned()),
    }?;

    let table_config = LOG_SOURCES_CONFIG.with(|c| {
        let log_sources_config = c.borrow();

        (*log_sources_config)
        .get(log_source)
        .unwrap()
        .tables.get(&table_name).cloned()
    }).ok_or(anyhow!("Configuration doesn't exist for table name returned from select_table_from_payload_metadata expression: {}", &table_name))?;

    let expand_records_from_payload_expr = table_config
        .get_string("ingest.expand_records_from_payload")
        .ok();

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
            let (expanded_records, _) = vrl(&prog, &mut value)
                .map_err(|e| anyhow!(e).context("Failed to expand records"))?;
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
                Ok(v) => Ok(Value::from(v)),
                Err(e) => Err(anyhow!(e)),
            })
            .boxed(),
    };

    let is_from_cloudwatch_log_subscription = table_config
        .get_bool("ingest.s3_source.is_from_cloudwatch_log_subscription")
        .ok()
        .unwrap_or(false);

    let events: Pin<Box<dyn Stream<Item = Result<Value>> + Send>> =
        if is_from_cloudwatch_log_subscription {
            Box::pin(lines.flat_map_unordered(50, |v| {
                // TODO: c'mon dont do this in VRL...
                let v = v.and_then(|mut v| {
                    vrl(r#". = parse_json!(string!(.))"#, &mut v)?;
                    Ok(v)
                });

                match v {
                    Ok(Value::Object(mut line)) => {
                        let log_events = line
                            .remove("logEvents")
                            .ok_or(anyhow!(
                                "logEvents not found in cloudwatch log subscription payload"
                            ))
                            .unwrap();

                        match log_events {
                            Value::Array(log_events) => {
                                let log_events = log_events.into_iter().map(|mut v| {
                                    let ts = v.as_object_mut_unwrap().remove("timestamp").unwrap();
                                    let message =
                                        v.as_object_mut_unwrap().remove("message").unwrap();
                                    value!({ "ts": ts, "message": message })
                                });
                                Box::pin(stream! {
                                    for log_event in log_events {
                                        yield Ok(log_event);
                                    }
                                })
                                    as Pin<Box<dyn Stream<Item = Result<Value>> + Send>>
                            }
                            _ => unreachable!(),
                        }
                    }
                    Ok(_) => unreachable!(),
                    Err(e) => Box::pin(stream! {
                            yield Err(e);
                    })
                        as Pin<Box<dyn Stream<Item = Result<Value>> + Send>>,
                }
            }))
        } else {
            lines
                .map_ok(|line| {
                    let now = Value::Timestamp(Utc::now());
                    match line {
                        Value::Object(_) => value!({ "ts": now, "json": line }),
                        Value::Bytes(_) => value!({ "ts": now, "message": line }),
                        _ => unimplemented!("Unsupported line type"),
                    }
                })
                .boxed()
        };

    Ok((table_config, events))
}

pub(crate) async fn my_handler(event: LambdaEvent<SqsEvent>) -> Result<()> {
    info!("Request: {:?}", event);

    let s3_download_items = event
        .payload
        .records
        .iter()
        .flat_map(|record| {
            let body = record.body.as_ref().expect("SQS message body is required");
            let data_batcher_request_items: Vec<DataBatcherOutputRecord> = serde_json::from_str(
                &body,
            )
            .expect("Could not deserialize SQS message body as list of DataBatcherRequestItems.");
            data_batcher_request_items
        })
        .filter(|d| d.size > 0)
        .filter_map(|d| {
            let log_source = get_log_source_from_key(&d.key);
            if log_source.is_none() {
                info!("Skipping irrelevant S3 object: {:?}", d);
            }
            log_source.map(|ls| (d, ls))
        })
        .collect::<Vec<_>>();

    info!(
        "Processing {} files from S3, of total size {} bytes",
        s3_download_items.len(),
        s3_download_items.iter().map(|(d, _)| d.size).sum::<i64>()
    );

    let config = aws_config::load_from_env().await;
    let s3 = aws_sdk_s3::Client::new(&config);
    let sns = aws_sdk_sns::Client::new(&config);

    println!("current_num_threads() = {}", rayon::current_num_threads());

    let transformed_lines_streams = s3_download_items
        .iter()
        .map(|(item, log_source)| {
            let s3 = &s3;

            async move {
                let (table_config, raw_lines): (
                    _,
                    Pin<Box<dyn Stream<Item = Result<Value, anyhow::Error>> + Send>>,
                ) = read_events_s3(s3, &item, log_source).await.unwrap();

                let reader = raw_lines;

                let transform_expr = table_config.get_string("transform").unwrap_or_default();

                // TODO: fix for non json, object check
                let wrapped_transform_expr = format!(
                    r#"
if .message != null {{
    .json, err = parse_json(string!(.message))
    if !is_object(.json) {{
        del(.json)
        err = "Failed to parse message as JSON object"
    }}
    if err == null {{
        del(.message)
    }}
}}

.related.ip = []
.related.hash = []
.related.user = []

{}
del(.json)

. = compact(.)
.ecs.version = "8.5.0"
                    "#,
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
                                            let mut v = line;

                                            // println!("v: {:?}", v);

                                            match vrl(&wrapped_transform_expr, &mut v).map_err(|e| {
                                                anyhow!("Failed to transform: {}", e)
                                            }) {
                                                Ok(_) => {},
                                                Err(e) => {
                                                    error!("Failed to process event, mutated event state: {}\n {}", v.to_string_lossy(), e);
                                                    return Err(e);
                                                }
                                            }

                                            Ok(v)
                                        }
                                        Err(e) => Err(anyhow!("Malformed line: {}", e)),
                                    })
                                    .collect::<Vec<_>>();
                                transformed_chunk
                            })
                            .await;

                            debug!("Transformed lines: took {:?}", start.elapsed());
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

                (table_config, stream)
            }
        })
        .collect::<Vec<_>>();

    let result = join_all(transformed_lines_streams).await;
    let result = result
        .into_iter()
        .group_by(|(table_config, _)| table_config.get_string("resolved_name").unwrap());

    let mut merged: HashMap<
        String,
        (
            config::Config,
            StreamMap<String, Pin<Box<dyn Stream<Item = Value> + Send>>>,
        ),
    > = HashMap::new();
    for (resolved_table_name, streams) in result.into_iter() {
        for (i, (table_config, stream)) in streams.enumerate() {
            merged
                .entry(resolved_table_name.clone())
                .or_insert((table_config, StreamMap::new()))
                .1
                .insert(
                    format!("{}", i), // object_key.clone(),
                    Box::pin(stream), // as Pin<Box<dyn Stream<Item = usize> + Send>>,
                );
        }
    }

    let futures = merged
        .into_iter()
        .map(|(resolved_table_name, (table_config, stream))| {
            let s3 = &s3;
            let sns = &sns;
            let num_rows_to_infer_schema = 100;

            async move {
                let mut reader = stream.map(|(_, value)| value);

                let schemas_path = Path::new("/opt/schemas");
                let schema_path = schemas_path
                    .join(&resolved_table_name)
                    .join("avro_schema.avsc");
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
                let mut rows = RefCell::new(0);

                reader
                    .chunks(8000) // chunks(8000) (avro block size)
                    .for_each(|chunk| {
                        // .for_each_concurrent(100, |chunk| { // figure out how to make this work (lock writer so that we can write better blocks...)
                        let mut writer = writer.borrow_mut();
                        let mut rows = rows.borrow_mut();

                        let schema = Arc::clone(&inferred_avro_schema);

                        async move {
                            let avro_values = spawn_blocking(move || {
                                // CPU bound (VRL transform)
                                let avro_values = chunk
                                    .into_par_iter()
                                    .map(|v| {
                                        let v_avro = TryIntoAvro::try_into(v).unwrap();
                                        let ret = v_avro.resolve(schema.as_ref());

                                        let ret = ret.map(|v| Some(v)).or_else(|e| {
                                            match e {
                                                apache_avro::Error::FindUnionVariant => {
                                                    // TODO: report errors
                                                    println!("USER_ERROR: Failed at FindUnionVariant, likely schema issue.");
                                                    // println!("{}", serde_json::to_string(&v).unwrap());
                                                    Ok(None)
                                                }
                                                e => Err(e)
                                            }
                                        }).unwrap();

                                        ret
                                    })
                                    .flatten()
                                    .collect::<Vec<_>>();

                                avro_values
                            })
                            .await
                            .unwrap();

                            *rows += avro_values.len();

                            writer.extend_from_slice(&avro_values).unwrap(); // IO (well theoretically) TODO: since this actually does non-trivial comptuuation work too (schema validation), we need to figure out a way to prevent this from blocking our main async thread
                        }
                    })
                    .await;

                let writer = writer.into_inner();
                let bytes = writer.into_inner().unwrap();

                let rows = rows.into_inner();
                if bytes.len() == 0 || rows == 0 {
                    return;
                }
                println!("Rows: {}", rows);

                let uuid = Uuid::new_v4();
                let bucket = var("MATANO_REALTIME_BUCKET_NAME").unwrap().to_string();
                let key = format!("transformed/{}/{}.snappy.avro", &resolved_table_name, uuid);
                info!("Writing Avro file to S3 path: {}/{}", bucket, key);
                // let local_path = output_schemas_path.join(format!("{}.avro", resolved_table_name));
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
                            "resolved_table_name": resolved_table_name
                        })
                        .to_string(),
                    )
                    .message_attributes(
                        "resolved_table_name",
                        MessageAttributeValue::builder()
                            .data_type("String".to_string())
                            .string_value(resolved_table_name)
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

    Ok(())
}
