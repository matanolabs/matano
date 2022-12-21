// This example requires the following input to succeed:
// { "command": "do something" }
mod arrow;
mod avro;
mod models;
use aws_sdk_sns::model::MessageAttributeValue;
use chrono::Utc;
use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::env::var;
use std::fs::read_to_string;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;
use tokio::fs;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncReadExt;
use tokio::task::spawn_blocking;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use apache_avro::{Codec, Schema, Writer};
use aws_sdk_s3::types::ByteStream;

use async_compression::tokio::bufread::{GzipDecoder, ZstdDecoder};
use async_stream::stream;
use futures::future::join_all;
use futures::{stream, Stream, StreamExt, TryStreamExt};
use serde_json::json;
use tokio_stream::StreamMap;
use tokio_util::codec::{FramedRead, LinesCodec};
use walkdir::WalkDir;

use itertools::Itertools;
use uuid::Uuid;

use rayon::prelude::*;
use shared::vrl_util::vrl;
use shared::*;
use vrl::{state, value};

use anyhow::{anyhow, Context as AnyhowContext, Result};
use async_once::AsyncOnce;
use lazy_static::lazy_static;

use ::value::{Secrets, Value};
use aws_config::SdkConfig;
use aws_lambda_events::event::sqs::{SqsEvent, SqsMessage};
use lambda_runtime::{run, service_fn, Context, Error as LambdaError, LambdaEvent};
use log::{debug, error, info, log_enabled, warn};

use crate::avro::TryIntoAvro;

lazy_static! {
    static ref AWS_CONFIG: AsyncOnce<SdkConfig> =
        AsyncOnce::new(async { aws_config::load_from_env().await });
    static ref S3_CLIENT: AsyncOnce<aws_sdk_s3::Client> =
        AsyncOnce::new(async { aws_sdk_s3::Client::new(AWS_CONFIG.get().await) });
    static ref SNS_CLIENT: AsyncOnce<aws_sdk_sns::Client> =
        AsyncOnce::new(async { aws_sdk_sns::Client::new(AWS_CONFIG.get().await) });
    static ref AVRO_SCHEMAS: HashMap<String, Schema> = {
        let mut m = HashMap::new();
        let schemas_path = Path::new("/opt/schemas");
        for entry in WalkDir::new(schemas_path).min_depth(1).max_depth(1) {
            let entry = entry.unwrap();
            let schema = read_to_string(entry.path().join("avro_schema.avsc")).unwrap();
            let schema = Schema::parse_str(&schema).unwrap();
            let resolved_table_name = entry.file_name().to_str().unwrap().to_string();
            m.insert(resolved_table_name, schema);
        }
        m
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

fn get_log_source_from_bucket_and_key(
    bucket: &str,
    key: &str,
    managed_bucket: &str,
) -> Option<String> {
    LOG_SOURCES_CONFIG.with(|c| {
        let log_sources_config = c.borrow();

        // Try get from BYOB or else assume managed and get from path
        let ret = (*log_sources_config)
            .iter()
            .find(|(_, v)| {
                v.base
                    .get_string("ingest.s3_source.key_prefix")
                    .map(|prefix| {
                        key.starts_with(prefix.as_str())
                            && v.base
                                .get_string("ingest.s3_source.bucket")
                                .ok()
                                .map_or(managed_bucket == bucket, |b| b.as_str() == bucket)
                    })
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
    let extension = Path::new(key).extension().and_then(std::ffi::OsStr::to_str);

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
                "gz" | "gzip" => Some(Compression::Gzip),
                "zst" | "zstd" => Some(Compression::Zstd),
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

const PRE_TRANSFORM_JSON_PARSE: &str = r#"
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

"#;

const TRANSFORM_RELATED: &str = r#"

.related.ip = []
.related.hash = []
.related.user = []

"#;
const TRANSFORM_PASSTHROUGH_JSON: &str = ". = object!(del(.json))\n";

const TRANSFORM_BASE_FOOTER: &str = r#"

del(.json)
. = compact(.)
.ecs.version = "8.5.0"

"#;

const TRANSFORM_DATA_FOOTER: &str = r#"

.partition_hour = format_timestamp!(.ts, "%Y-%m-%d-%H")
"#;

fn format_transform_expr(
    transform_expr: &str,
    select_table_from_payload_expr: &Option<String>,
    is_passthrough: bool,
) -> String {
    let mut ret = String::with_capacity(1000 + transform_expr.len());
    if !is_passthrough {
        ret.push_str(TRANSFORM_RELATED);
    }
    if is_passthrough && transform_expr == "" {
        ret.push_str(TRANSFORM_PASSTHROUGH_JSON)
    }
    ret.push_str(transform_expr);
    ret.push_str(TRANSFORM_BASE_FOOTER);
    if !is_passthrough {
        ret.push_str(TRANSFORM_DATA_FOOTER);
    }
    if let Some(select_table_from_payload_expr) = select_table_from_payload_expr {
        ret.push_str(&format!(
            r#"
        .__mtn_table = {{
            {}
        }}"#,
            select_table_from_payload_expr
        ));
    }
    return ret;
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
        None => {
            if r.key.clone().ends_with("csv") {
                let mut csv_reader = AsyncReader::from_reader(reader);
                let mut headers: StringRecord = StringRecord::new();
                { headers = csv_reader.headers().await?.to_owned(); } // do better here
                let records = csv_reader.into_records();

                records
                    .map(move |r| match r {  // move is used for headers scope.. do better here too
                        Ok(r) => {
                            let mut record = Map::new();
                            let _ = r.iter().enumerate().map(|(i, field)| {
                                record.insert(headers.get(i).unwrap().parse().unwrap(), field.parse().unwrap());
                            });
                            let json_string = serde_json::Value::from(record).to_string();
                            Ok(Value::from(json_string))
                        },
                        Err(e) => Err(anyhow!(e)),
                    })
                    .boxed()
            } else {
                FramedRead::new(reader, LinesCodec::new())
                    .map(|v| match v {
                        Ok(v) => {
                            println!("{:?}", v.clone());
                            Ok(Value::from(v))
                        },
                        Err(e) => Err(anyhow!(e)),
                    })
                    .boxed();
            }

        },
    };

    let is_from_cloudwatch_log_subscription = LOG_SOURCES_CONFIG
        .with(|c| {
            let log_sources_config = c.borrow();

            (*log_sources_config)
                .get(log_source)
                .unwrap()
                .base
                .get_bool("ingest.s3_source.is_from_cloudwatch_log_subscription")
                .ok()
        })
        .unwrap_or(false);

    let events: Pin<Box<dyn Stream<Item = Result<Value>> + Send>> =
        if is_from_cloudwatch_log_subscription {
            Box::pin(lines.flat_map_unordered(50, |v| {
                // TODO: c'mon dont do this in VRL...
                let v = v.and_then(|mut v| {
                    vrl(
                        r#". = if is_string(.) { parse_json!(string!(.)) } else { object(.) }"#,
                        &mut v,
                    )
                    .map_err(|e| {
                        anyhow!("Invaid format of cloudwatch log subscription line: {}", e)
                    })?;
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
            let managed_bucket = var("MATANO_SOURCES_BUCKET").unwrap();
            let log_source =
                get_log_source_from_bucket_and_key(&d.bucket, &d.key, managed_bucket.as_str());
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

    let s3 = S3_CLIENT.get().await;
    let sns = SNS_CLIENT.get().await;

    println!("current_num_threads() = {}", rayon::current_num_threads());

    let transformed_lines_streams = s3_download_items
        .into_iter()
        .map(|(item, log_source)| {
            let s3 = &s3;

            async move {
                let (ingest_table_config, raw_lines): (
                    _,
                    Pin<Box<dyn Stream<Item = Result<Value, anyhow::Error>> + Send>>,
                ) = read_events_s3(s3, &item, &log_source).await.unwrap();

                let reader = raw_lines;

                let select_table_from_payload_expr = LOG_SOURCES_CONFIG.with(|c| {
                    let log_sources_config = c.borrow();

                    (*log_sources_config)
                        .get(&log_source)
                        .unwrap()
                        .base
                        .get_string("ingest.select_table_from_payload")
                        .ok()
                });

                let transform_expr = ingest_table_config.get_string("transform").unwrap_or_default();
                let wrapped_transform_expr = format_transform_expr(&transform_expr, &select_table_from_payload_expr, log_source.starts_with("enrich_"));

                let transformed_lines_as_json = reader
                    .chunks(400)
                    .filter_map(move |chunk| {
                        let log_source = log_source.clone();
                        let wrapped_transform_expr = wrapped_transform_expr.clone(); // hmm
                        let select_table_from_payload_expr = select_table_from_payload_expr.clone();
                        let default_table_name = ingest_table_config.get_string("name").unwrap();

                        async move {
                            let start = Instant::now();
                            let transformed_lines = async_rayon::spawn(move || {
                                let transformed_chunk = chunk
                                    .into_par_iter()
                                    .map(|r| match r {
                                        Ok(line) => {
                                            let mut v = line;

                                            // println!("v: {:?}", v);

                                            match vrl(PRE_TRANSFORM_JSON_PARSE, &mut v).map_err(|e| {
                                                anyhow!("Failed to run pre-transform: {}", e)
                                            }) {
                                                Ok(_) => {},
                                                Err(e) => {
                                                    error!("Failed to process event, mutated event state: {}\n {}", v.to_string_lossy(), e);
                                                    return Err(e);
                                                }
                                            }

                                            let table_name = match select_table_from_payload_expr {
                                                Some(ref select_table_from_payload_expr) => match vrl(select_table_from_payload_expr, &mut v).map_err(|e| {
                                                    anyhow!("Failed to select table: {}", e)
                                                }) {
                                                    Ok((table_name, _)) => table_name.as_str().unwrap_or_else(|| Cow::from(default_table_name.to_owned())).to_string(),
                                                    Err(e) => {
                                                        error!("Failed to process event, mutated event state: {}\n {}", v.to_string_lossy(), e);
                                                        return Err(e);
                                                    }
                                                },
                                                None => default_table_name.to_owned(),
                                            };

                                            match vrl(&wrapped_transform_expr, &mut v).map_err(|e| {
                                                anyhow!("Failed to transform: {}", e)
                                            }) {
                                                Ok(_) => {},
                                                Err(e) => {
                                                    error!("Failed to process event, mutated event state: {}\n {}", v.to_string_lossy(), e);
                                                    return Err(e);
                                                }
                                            }

                                            let resolved_table_name = if table_name == "default" {
                                                log_source.to_owned()
                                            } else {
                                                format!("{}_{}", log_source.to_owned(), table_name)
                                            };

                                            let avro_schema = AVRO_SCHEMAS
                                                .get(&resolved_table_name)
                                                .expect("Failed to find avro schema");

                                            let v_clone = log_enabled!(log::Level::Debug).then(|| v.clone());

                                            let v_avro = TryIntoAvro::try_into(v)?;
                                            let v_avro = v_avro.resolve(avro_schema)
                                            .map_err(|e| {
                                                match e {
                                                    apache_avro::Error::FindUnionVariant => {
                                                        // TODO: report errors
                                                        if log_enabled!(log::Level::Debug) {
                                                            debug!("{}", serde_json::to_string(&v_clone.unwrap()).unwrap_or_default());
                                                        }
                                                        anyhow!("USER_ERROR: Failed at FindUnionVariant, likely schema issue.")
                                                    }
                                                    e => anyhow!(e)
                                                }
                                            })?;

                                            Ok((resolved_table_name, v_avro))
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
                            None
                        }
                    }
                });

                stream
            }
        })
        .collect::<Vec<_>>();

    let result = join_all(transformed_lines_streams).await;

    let mut writers_by_table: RefCell<HashMap<String, (Writer<Vec<u8>>, usize)>> =
        RefCell::new(HashMap::new());

    let mut merged: StreamMap<
        String,
        Pin<Box<dyn Stream<Item = (std::string::String, apache_avro::types::Value)> + Send>>,
    > = StreamMap::new();
    for (i, stream) in result.into_iter().enumerate() {
        merged.insert(
            format!("{}", i), // object_key.clone(),
            Box::pin(stream), // as Pin<Box<dyn Stream<Item = usize> + Send>>,
        );
    }

    merged
        .for_each(|(_, (resolved_table_name, v))| {
            let mut writers_by_table = writers_by_table.borrow_mut();

            async move {
                let (writer, rows) = writers_by_table
                    .entry(resolved_table_name.clone())
                    .or_insert_with(|| {
                        let avro_schema = AVRO_SCHEMAS
                            .get(&resolved_table_name)
                            .expect("Failed to find avro schema");

                        let writer: Writer<Vec<u8>> = Writer::builder()
                            .schema(avro_schema)
                            .writer(Vec::new())
                            .codec(Codec::Snappy)
                            .block_size(1 * 1024 * 1024 as usize)
                            .build();
                        (writer, 0)
                    });

                *rows += 1;
                writer.append_value_ref(&v).unwrap(); // IO (well theoretically) TODO: since this actually does non-trivial comptuuation work too (schema validation), we need to figure out a way to prevent this from blocking our main async thread
            }
        })
        .await;

    let writers_by_table = writers_by_table.into_inner();
    let futures = writers_by_table
        .into_iter()
        .map(|(resolved_table_name, (writer, rows))| {
            async move {
                let bytes = writer.into_inner().unwrap();
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
        });

    join_all(futures).await;
    Ok(())
}
