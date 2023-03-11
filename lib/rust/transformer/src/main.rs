mod arrow;
mod avro;
mod models;
use aws_sdk_sns::model::MessageAttributeValue;
use aws_sdk_sqs::model::SendMessageBatchRequestEntry;
use chrono::Utc;
use futures::join;
use futures::try_join;
use futures::TryFutureExt;
use shared::sqs_util::SQSLambdaError;
use shared::vrl_util::vrl_opt;
use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::HashSet;
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
use vrl::prelude::expression::Abort;
use vrl::Terminate;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use apache_avro::{Codec, Schema, Writer};
use aws_sdk_s3::types::ByteStream;

use async_compression::tokio::bufread::{GzipDecoder, ZstdDecoder};
use async_stream::stream;
use csv_async::AsyncReaderBuilder;
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
use urlencoding::decode;

use crate::avro::TryIntoAvro;

lazy_static! {
    static ref AWS_CONFIG: AsyncOnce<SdkConfig> =
        AsyncOnce::new(async { aws_config::load_from_env().await });
    static ref S3_CLIENT: AsyncOnce<aws_sdk_s3::Client> =
        AsyncOnce::new(async { aws_sdk_s3::Client::new(AWS_CONFIG.get().await) });
    static ref SNS_CLIENT: AsyncOnce<aws_sdk_sns::Client> =
        AsyncOnce::new(async { aws_sdk_sns::Client::new(AWS_CONFIG.get().await) });
    static ref SQS_CLIENT: AsyncOnce<aws_sdk_sqs::Client> =
        AsyncOnce::new(async { aws_sdk_sqs::Client::new(AWS_CONFIG.get().await) });
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

#[tokio::main]
async fn main() -> Result<(), LambdaError> {
    setup_logging();
    let start = Instant::now();

    let func = service_fn(handler);
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

fn format_transform_expr(transform_expr: &str, is_passthrough: bool) -> String {
    let mut ret = String::with_capacity(1000 + transform_expr.len());
    if !is_passthrough {
        ret.push_str(TRANSFORM_RELATED);
    }
    if is_passthrough && transform_expr == "" {
        ret.push_str(TRANSFORM_PASSTHROUGH_JSON)
    }
    ret.push_str(transform_expr);
    ret.push_str(TRANSFORM_BASE_FOOTER);

    return ret;
}

async fn read_events_s3<'a>(
    s3: &aws_sdk_s3::Client,
    r: &DataBatcherOutputRecord,
    log_source: &String,
) -> Result<
    Option<(
        config::Config,
        Pin<Box<dyn Stream<Item = Result<Value>> + Send>>,
    )>,
> {
    let dec_key = decode(&r.key.clone())?.into_owned();

    let res = s3
        .get_object()
        .bucket(r.bucket.clone())
        .key(dec_key.clone())
        .send()
        .await
        .map_err(|e| anyhow!(e).context(format!("Error downloading {} from S3", dec_key)));
    let obj = res?;

    let mut reader = tokio::io::BufReader::new(obj.body.into_async_read());

    let compression = Compression::Auto;
    let compression = match compression {
        Compression::Auto => infer_compression(
            &mut reader,
            obj.content_encoding.as_deref(),
            obj.content_type.as_deref(),
            dec_key.as_str(),
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
            .get(log_source)?
            .base
            .get_string("ingest.select_table_from_payload_metadata")
            .ok()
    });

    let table_name = match select_table_expr {
        Some(prog) => {
            let r_json = serde_json::to_value(r)?;
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

            match vrl_opt(&wrapped_prog, &mut value) {
                Ok(Some((table_name, _))) => match &table_name {
                    Value::Null => Ok("default".to_owned()),
                    Value::Bytes(b) => Ok(table_name.to_string_lossy()),
                    _ => {
                        Err(anyhow!("Invalid table_name returned from select_table_from_payload_metadata expression: {}", table_name))
                    }
                },
                Ok(None) => {
                    debug!("Skipping object: {} for log_source: {} due to select_table_from_payload_metadata aborting", dec_key, log_source);
                    return Ok(None)
                },
                Err(e) => Err(e),
            }
        }
        None => Ok("default".to_owned()),
    }?;

    let table_config = LOG_SOURCES_CONFIG.with(|c| {
        let log_sources_config = c.borrow();

        let table_conf = (*log_sources_config)
            .get(log_source)?
            .tables
            .get(&table_name)
            .cloned();

        table_conf.or(
            match (*log_sources_config)
                .get(log_source)?
                .base
                .get_string("ingest.select_table_from_payload")
                .ok()
            {
                Some(s) if !s.is_empty() => {
                    // TODO(shaeq): make this less hacky, we return a dummy config if select_table_from_payload is set
                    config::Config::builder()
                        .set_default("name", "dummy")
                        .ok()
                        .and_then(|c| c.build().ok())
                }
                Some(_) | None => None,
            },
        )
    });

    // TODO: might want to differentiate between a valid or invalid table.
    if table_config.is_none() {
        info!("Skipping key: {} as configuration doesn't exist for log source: {} table name: {} returned from select_table_from_payload_metadata expression", r.key, &log_source, &table_name);
        return Ok(None);
    }
    let table_config = table_config.unwrap();

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

            let mut value = value!({ "__raw": file_string });
            let (expanded_records, _) = vrl_opt(&prog, &mut value)
                .map_err(|e| anyhow!(e).context("Failed to expand records"))?
                .unwrap_or((Value::Array(Vec::with_capacity(0)), &mut Value::Null));

            info!("Expanded records from payload: took {:?}", start.elapsed(),);
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
            if dec_key.as_str().contains(".csv") {
                let headers = table_config
                    .get_array("ingest.csv_headers")
                    .ok()
                    .and_then(|arr| {
                        arr.into_iter()
                            .map(|x| x.into_string().ok())
                            .collect::<Option<Vec<_>>>()
                    });
                read_csv(headers, reader)
            } else {
                FramedRead::new(reader, LinesCodec::new())
                    .map(|v| match v {
                        Ok(v) => Ok(Value::from(v)),
                        Err(e) => Err(anyhow!(e)),
                    })
                    .boxed()
            }
        }
    };

    let is_from_cloudwatch_log_subscription = LOG_SOURCES_CONFIG
        .with(|c| {
            let log_sources_config = c.borrow();

            (*log_sources_config)
                .get(log_source)?
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
                            .context("logEvents not found in cloudwatch log subscription payload")
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

    Ok(Some((table_config, events)))
}

pub(crate) async fn handler(event: LambdaEvent<SqsEvent>) -> Result<()> {
    let start = Instant::now();
    event.payload.records.first().iter().for_each(|r| {
        info!("Request: {}", serde_json::to_string(&json!({ "message_id": r.message_id, "body": r.body, "message_attributes": r.message_attributes })).unwrap_or_default());
    });

    let mut errors = vec![];

    let data_batcher_records = event
        .payload
        .records
        .iter()
        .filter_map(|record| {
            let body = record
                .body
                .as_ref()
                .context("SQS message body is required")
                .ok()?;
            serde_json::from_str::<Vec<DataBatcherOutputRecord>>(&body).ok()
        })
        .flatten()
        .collect::<Vec<DataBatcherOutputRecord>>();

    let s3_download_items = data_batcher_records
        .iter()
        .filter(|d| d.size > 0)
        .map(|d| (d, d.log_source.clone()))
        .collect::<Vec<_>>();

    info!(
        "Processing {} files from S3, of total size {} bytes",
        s3_download_items.len(),
        s3_download_items.iter().map(|(d, _)| d.size).sum::<i64>()
    );

    let s3 = S3_CLIENT.get().await;
    let sns = SNS_CLIENT.get().await;

    let s3_download_items_copy = s3_download_items.clone();

    let transformed_lines_streams = s3_download_items
        .into_iter()
        .map(|(item, log_source)| {
            let s3 = &s3;

            let item_copy = item.clone();

            async move {
                let events = read_events_s3(s3, &item, &log_source).await?;
                if events.is_none() {
                    info!("Skipped S3 object: {}", item.key);
                    return Ok(None);
                }

                let (default_table_config, raw_lines): (
                    _,
                    Pin<Box<dyn Stream<Item = Result<Value, anyhow::Error>> + Send>>,
                ) = events.unwrap();

                let reader = raw_lines;

                let select_table_from_payload_expr = LOG_SOURCES_CONFIG.with(|c| {
                    let log_sources_config = c.borrow();

                    (*log_sources_config)
                        .get(&log_source)?
                        .base
                        .get_string("ingest.select_table_from_payload")
                        .ok()
                });

                let transformed_lines_as_json = reader
                    .chunks(400)
                    .filter_map(move |chunk| {
                        let log_source = log_source.clone();
                        let select_table_from_payload_expr = select_table_from_payload_expr.clone();
                        let default_table_name = default_table_config.get_string("name").unwrap();

                        async move {
                            let start = Instant::now();
                            let transformed_lines = async_rayon::spawn(move || {
                                let transformed_chunk = chunk
                                    .into_par_iter()
                                    .map(|r| match r {
                                        Ok(line) => {
                                            let mut v = line;

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
                                                Some(ref select_table_from_payload_expr) => match vrl_opt(select_table_from_payload_expr, &mut v).map_err(|e| {
                                                    anyhow!("Failed to select table: {}", e)
                                                }) {
                                                    Ok(Some((table_name, _))) => table_name.as_str().unwrap_or_else(|| Cow::from(default_table_name.to_owned())).to_string(),
                                                    Ok(None) => {
                                                        return Ok(None);
                                                    },
                                                    Err(e) => {
                                                        error!("Failed to process event, mutated event state: {}\n {}", v.to_string_lossy(), e);
                                                        return Err(e);
                                                    }
                                                },
                                                None => default_table_name.to_owned(),
                                            };

                                            // Treated differently than a regular table, e.g. no required ts. Currently just enrichment tables.
                                            let is_passthrough = log_source.starts_with("enrich_");

                                            // Use empty transform if no transform is defined
                                            let transform_expr = LOG_SOURCES_CONFIG.with(|c| {
                                                let log_sources_config = c.borrow();

                                                let transform = (*log_sources_config)
                                                .get(&log_source)?
                                                .tables.get(&table_name)?.get_string("transform");

                                                transform.ok()
                                            }).unwrap_or("".to_string());

                                            // TODO(shaeq): does this have performance implications?
                                            let wrapped_transform_expr = format_transform_expr(&transform_expr, log_source.starts_with("enrich_"));

                                            match vrl_opt(&wrapped_transform_expr, &mut v).map_err(|e| {
                                                anyhow!("Failed to transform: {}", e)
                                            }) {
                                                Ok(Some(_)) => {},
                                                Ok(None) => {
                                                    return Ok(None);
                                                }
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
                                                .context("Failed to find avro schema")?;

                                            let v_clone = log_enabled!(log::Level::Debug).then(|| v.clone());

                                            let ts_hour = if is_passthrough {
                                                chrono::Utc::now().format("%Y-%m-%d-%H").to_string()
                                            } else {
                                                v.get("ts").context("Failed to find matano timestamp")?.as_timestamp().context("Failed to parse matano timestamp")?.format("%Y-%m-%d-%H").to_string()
                                            };

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

                                            Ok(Some(((resolved_table_name, ts_hour), v_avro)))
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
                        Ok(line) => line,
                        Err(e) => {
                            error!("{}", e);
                            None
                        }
                    }
                });

                anyhow::Ok(Some(stream))
            }.map_err(move |e|
                SQSLambdaError::new(format!("{:#}", e), vec![item_copy.sequencer])
            )
        })
        .collect::<Vec<_>>();

    let result = join_all(transformed_lines_streams)
        .await
        .into_iter()
        .filter_map(|r| r.map_err(|e| errors.push(e)).ok())
        .flatten()
        .collect::<Vec<_>>();

    let mut writers_by_table_utc_hour_pair: RefCell<
        HashMap<(String, String), (Writer<Vec<u8>>, usize)>,
    > = RefCell::new(HashMap::new());

    let mut merged: StreamMap<
        String,
        Pin<Box<dyn Stream<Item = ((String, String), apache_avro::types::Value)> + Send>>,
    > = StreamMap::new();
    for (i, stream) in result.into_iter().enumerate() {
        merged.insert(
            format!("{}", i), // object_key.clone(),
            Box::pin(stream), // as Pin<Box<dyn Stream<Item = usize> + Send>>,
        );
    }

    merged
        .for_each(|(_, ((resolved_table_name, ts_hour), v))| {
            let mut writers_by_table_utc_hour_pair = writers_by_table_utc_hour_pair.borrow_mut();

            async move {
                let (writer, rows) = writers_by_table_utc_hour_pair
                    .entry((resolved_table_name.clone(), ts_hour.clone()))
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

    let writers_by_table = writers_by_table_utc_hour_pair.into_inner();
    let futures =
        writers_by_table
            .into_iter()
            .map(|((resolved_table_name, ts_hour), (writer, rows))| {
                let matching_record_seqs = s3_download_items_copy
                    .iter()
                    .filter_map(|(record, log_source)| {
                        resolved_table_name
                            .contains(log_source)
                            .then_some(record.sequencer.clone())
                    })
                    .collect::<Vec<_>>();

                async move {
                    let bytes = writer.into_inner()?;
                    if bytes.len() == 0 || rows == 0 {
                        return Ok(0);
                    }
                    info!("Writing number of Rows: {}", rows);

                    let uuid = Uuid::new_v4();
                    let bucket = var("MATANO_REALTIME_BUCKET_NAME")?;
                    let key = format!(
                        "transformed/{}/ts_hour={}/{}.snappy.avro",
                        &resolved_table_name, &ts_hour, uuid
                    );
                    info!("Writing Avro file to S3 path: {}/{}", bucket, key);

                    s3.put_object()
                        .bucket(&bucket)
                        .key(&key)
                        .body(ByteStream::from(bytes))
                        .content_type("application/avro-binary".to_string())
                        // .content_encoding("application/zstd".to_string())
                        .send()
                        .await
                        .map_err(|e| anyhow!(e).context(format!("Error putting {} to S3", key)))?;

                    sns.publish()
                        .topic_arn(var("MATANO_REALTIME_TOPIC_ARN")?)
                        .message(
                            json!({
                                "bucket": &bucket,
                                "key": &key,
                                "resolved_table_name": resolved_table_name,
                                "ts_hour": ts_hour,
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
                            anyhow!(e).context(format!(
                                "Error publishing SNS notification for S3 key: {}",
                                key
                            ))
                        })?;
                    anyhow::Ok(rows)
                }
                .map_err(move |e| SQSLambdaError::new(format!("{:#}", e), matching_record_seqs))
            });

    let total_rows_written = join_all(futures)
        .await
        .into_iter()
        .flat_map(|r| r.map_err(|e| errors.push(e)).ok())
        .sum::<usize>();

    // Error handling strategy:
    // If all records failed, we return an error and the lambda will retry
    // If some records failed, we retry the failed records up to 3 times by sending back to the queue with a retry_depth attribute
    // If some records failed and we've retried them 3 times, we send them to the DLQ
    let had_error = !errors.is_empty();
    let mut maybe_failing_log_sources = None;
    let is_total_failure = errors.len() == data_batcher_records.len();
    if had_error {
        errors.iter().for_each(|e| error!("{}", e));

        let error_ids = errors
            .into_iter()
            .flat_map(|e| e.ids)
            .collect::<HashSet<_>>();
        let failures = error_ids.iter().map(|error_id| {
            data_batcher_records
                .iter()
                .find(|r| &r.sequencer == error_id)
                .unwrap()
                .clone()
        });
        maybe_failing_log_sources = Some(
            failures
                .clone()
                .map(|r| r.log_source)
                .collect::<HashSet<_>>(),
        );

        if !is_total_failure {
            let (retryable, un_retryable): (Vec<_>, Vec<_>) =
                failures.partition(|r| r.retry_depth.unwrap_or(0) < 3);

            handle_all_partial_failures(retryable, un_retryable).await?;
        }
    }

    // emit log
    let time_ms = i64::try_from(start.elapsed().as_millis()).ok();
    let log_sources = s3_download_items_copy
        .iter()
        .map(|(_, ls)| ls)
        .collect::<HashSet<_>>();
    let bytes_processed: i64 = s3_download_items_copy.iter().map(|(r, _)| r.size).sum();
    let log = json!({
        "type": "matano_service_log",
        "service": "transformer",
        "time": time_ms,
        "log_sources": log_sources,
        "bytes_processed": bytes_processed,
        "rows_written": total_rows_written,
        "error": had_error,
        "failing_log_sources": maybe_failing_log_sources,
    });
    info!("{}", serde_json::to_string(&log).unwrap_or_default());

    if is_total_failure {
        Err(anyhow!("All records failed!"))
    } else {
        Ok(())
    }
}

async fn handle_all_partial_failures(
    mut retryable: Vec<DataBatcherOutputRecord>,
    un_retryable: Vec<DataBatcherOutputRecord>,
) -> Result<()> {
    let output_queue_url = std::env::var("MATANO_BATCHER_QUEUE_URL")?;
    let dlq_url = std::env::var("MATANO_BATCHER_DLQ_URL")?;

    retryable
        .iter_mut()
        .for_each(|item| item.increment_retry_depth());

    let retryable_fut = handle_partial_failure(retryable, output_queue_url);
    let un_retryable_fut = handle_partial_failure(un_retryable, dlq_url);

    try_join!(retryable_fut, un_retryable_fut)?;

    Ok(())
}

async fn handle_partial_failure(
    failures: Vec<DataBatcherOutputRecord>,
    queue_url: String,
) -> Result<()> {
    if failures.is_empty() {
        return Ok(());
    }
    let sqs = SQS_CLIENT.get().await.clone();
    let body = serde_json::to_string(&failures)?;

    error!(
        "Encountered partial failure, sending messages: {} to queue: {}.",
        &body, &queue_url
    );

    sqs.send_message()
        .message_body(body)
        .queue_url(queue_url)
        .send()
        .await?;

    Ok(())
}

fn read_csv(
    headers: Option<Vec<String>>,
    reader: Box<dyn tokio::io::AsyncRead + Send + Unpin>,
) -> Pin<Box<dyn Stream<Item = Result<Value, anyhow::Error>> + std::marker::Send>> {
    let headers = headers.map(|h| csv_async::StringRecord::from(h));

    let csv_rdr = AsyncReaderBuilder::new()
        .has_headers(headers.is_none())
        .flexible(true)
        .trim(csv_async::Trim::All)
        .create_reader(reader);

    csv_rdr
        .into_records()
        .map(move |r| {
            let r = r?;
            let out: HashMap<String, serde_json::Value> = r.deserialize(headers.as_ref())?;
            anyhow::Ok(out)
        })
        .map(|r| match r {
            Ok(r) => {
                let json_val: serde_json::Value =
                    serde_json::Value::Object(serde_json::Map::from_iter(r.into_iter()));
                Ok(Value::from(json_val))
            }
            Err(e) => Err(anyhow!(e)),
        })
        .boxed()
}
