// This example requires the following input to succeed:
// { "command": "do something" }
mod models;

use http::{HeaderMap, HeaderValue};
use std::borrow::Borrow;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::env::var;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

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

use aws_lambda_events::event::sqs::{SqsEvent, SqsMessage};
use lambda_runtime::{run, service_fn, Context, Error as LambdaError, LambdaEvent};
use log::{debug, error, info, warn};

use ::value::{Secrets, Value};
use vrl::{diagnostic::Formatter, state, value, Program, Runtime, TargetValueRef};

fn type_to_schema(data_type: &DataType, is_nullable: bool, name: String) -> Result<AvroSchema> {
    Ok(if is_nullable {
        AvroSchema::Union(vec![AvroSchema::Null, _type_to_schema(data_type, name)?])
    } else {
        _type_to_schema(data_type, name)?
    })
}

fn field_to_field(field: &Field) -> Result<AvroField> {
    let schema = type_to_schema(field.data_type(), true, field.name.clone())?;
    Ok(AvroField::new(&field.name, schema))
}

fn _type_to_schema(data_type: &DataType, name: String) -> Result<AvroSchema> {
    Ok(match data_type.to_logical_type() {
        DataType::Null => AvroSchema::Null,
        DataType::Boolean => AvroSchema::Boolean,
        DataType::Int32 => AvroSchema::Int(None),
        DataType::Int64 => AvroSchema::Long(None),
        DataType::Float32 => AvroSchema::Float,
        DataType::Float64 => AvroSchema::Double,
        DataType::Binary => AvroSchema::Bytes(None),
        DataType::LargeBinary => AvroSchema::Bytes(None),
        DataType::Utf8 => AvroSchema::String(None),
        DataType::LargeUtf8 => AvroSchema::String(None),
        DataType::LargeList(inner) | DataType::List(inner) => AvroSchema::Array(Box::new(
            type_to_schema(&inner.data_type, true, inner.name.clone())?,
        )),
        DataType::Struct(fields) => AvroSchema::Record(Record::new(
            name,
            fields
                .iter()
                .map(field_to_field)
                .collect::<Result<Vec<_>>>()?,
        )),
        DataType::Date32 => AvroSchema::Int(Some(IntLogical::Date)),
        DataType::Time32(TimeUnit::Millisecond) => AvroSchema::Int(Some(IntLogical::Time)),
        DataType::Time64(TimeUnit::Microsecond) => AvroSchema::Long(Some(LongLogical::Time)),
        DataType::Timestamp(TimeUnit::Millisecond, None) => {
            AvroSchema::Long(Some(LongLogical::LocalTimestampMillis))
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            AvroSchema::Long(Some(LongLogical::LocalTimestampMicros))
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let mut fixed = Fixed::new("", 12);
            fixed.logical = Some(FixedLogical::Duration);
            AvroSchema::Fixed(fixed)
        }
        DataType::FixedSizeBinary(size) => AvroSchema::Fixed(Fixed::new("", *size)),
        DataType::Decimal(p, s) => AvroSchema::Bytes(Some(BytesLogical::Decimal(*p, *s))),
        other => return Err(anyhow!("write {:?} to avro", other)),
    })
}

thread_local! {
    pub static RUNTIME: RefCell<Runtime> = RefCell::new(Runtime::new(state::Runtime::default()));
    pub static LOG_SOURCES_CONFIG: RefCell<BTreeMap<String, LogSourceConfiguration>> = {
        let log_sources_configuration_path = Path::new(&var("LAMBDA_TASK_ROOT").unwrap().to_string()).join("log_sources_configuration.json");
        let log_sources_configuration_string = std::fs::read_to_string(log_sources_configuration_path).expect("Unable to read file");

        let log_sources_configuration: Vec<LogSourceConfiguration> = serde_json::from_str(&log_sources_configuration_string).expect("Unable to parse log_sources_configuration.json");
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

    // let func = service_fn(my_handler);
    // run(func).await?;

    let mut headers = HeaderMap::new();
    headers.insert(
        "lambda-runtime-aws-request-id",
        HeaderValue::from_static("my-id"),
    );
    headers.insert(
        "lambda-runtime-deadline-ms",
        HeaderValue::from_static("123"),
    );
    let context = Context::try_from(headers).unwrap();
    let sqs_event = json!(
        {
            "Records": [
              {
                "messageId": "059f36b4-87a3-44ab-83d2-661975830a7d",
                "receiptHandle": "AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a...",
                "body": "[{\"bucket\":\"matanodpcommonstack-raweventsbucket024cde12-1oynz3pfccqum\",\"key\": \"cloudtrail/flaws_cloudtrail_logs/flaws_cloudtrail04.json.gz\",\"size\":10000000,\"sequencer\":\"dkdjkfjk\"}]",
                "attributes": {
                  "ApproximateReceiveCount": "1",
                  "SentTimestamp": "1545082649183",
                  "SenderId": "SSS",
                  "ApproximateFirstReceiveTimestamp": "1545082649185"
                },
                "messageAttributes": {},
                "md5OfBody": "e4e68fb7bd0e697a0ae8f1bb342846b3",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-east-2:123456789012:my-queue",
                "awsRegion": "us-east-1"
              }
            ]
          }
    );
    let sqs_event = SqsEvent { records: vec![
        SqsMessage {
            attributes:HashMap::new(),
            message_attributes:HashMap::new(),
            md5_of_message_attributes:None,
            md5_of_body:Some("e4e68fb7bd0e697a0ae8f1bb342846b3".to_string()),
            event_source:Some("aws:sqs".to_string()),
            event_source_arn:Some("arn:aws:sqs:us-east-2:123456789012:my-queue".to_string()),
            aws_region:Some("us-east-1".to_string()),
            body:Some("[{\"bucket\":\"matanodpcommonstack-raweventsbucket024cde12-1oynz3pfccqum\",\"key\": \"cloudtrail/flaws_cloudtrail_logs/flaws_cloudtrail04.json.gz\",\"size\":10000000,\"sequencer\":\"dkdjkfjk\"}]".to_string()),
            message_id: None, receipt_handle: None
        }
    ]
     };
    let event = LambdaEvent::new(sqs_event, context);
    my_handler(event).await?;

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

pub fn vrl<'a>(program: &'a str, value: &'a mut Value) -> Result<(Value, &'a mut Value)> {
    thread_local!(
        static CACHE: RefCell<LruCache<String, Result<Program, String>>> =
            RefCell::new(LruCache::new(400));
    );

    CACHE.with(|c| {
        let mut cache_ref = c.borrow_mut();
        let stored_result = (*cache_ref).get(program);

        let start = Instant::now();
        let compiled = match stored_result {
            Some(compiled) => match compiled {
                Ok(compiled) => Ok(compiled.to_owned()),
                Err(e) => {
                    return Err(anyhow!(e.clone()));
                }
            },
            None => match vrl::compile(&program, &vrl_stdlib::all()) {
                Ok(result) => {
                    println!(
                        "Compiled a vrl program ({}), took {:?}", program.lines().into_iter().skip(1).next().unwrap_or("expansion"), start.elapsed()
                    );
                    (*cache_ref).put(program.to_string(), Ok(result.program));
                    if result.warnings.len() > 0 {
                        warn!("{:?}", result.warnings);
                    }
                    match (*cache_ref).get(program) {
                        Some(compiled) => match compiled {
                            Ok(compiled) => Ok(compiled.to_owned()),
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

        let mut metadata = Value::Object(BTreeMap::new());
        let mut secrets = Secrets::new();
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

async fn read_lines_s3(
    s3: &aws_sdk_s3::Client,
    r: &DataBatcherRequestItem,
    compression: Option<Compression>,
) -> Result<Pin<Box<dyn Stream<Item = Result<serde_json::Value>> + Send>>> {
    println!("Starting download");
    let start = Instant::now();

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

    let first = if let Some(first) = obj.body.next().await {
        first
    } else {
        return Ok(FramedRead::new(tokio::io::empty(), LinesCodec::new())
            .map_ok(|line| serde_json::from_str(&line).unwrap())
            .map_err(|e| anyhow!(e))
            .boxed());
    };
    println!("Finished reading first byte: took {:?}", start.elapsed());
    let start = Instant::now();


    let reader = tokio::io::BufReader::new(
        StreamReader::new(stream::iter(Some(first)).chain(obj.body)), // ).map_err(|e| ??)
    );

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

    let reader: Box<dyn tokio::io::AsyncRead + Send + Unpin> = match compression {
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
            let file_bytes = ReaderStream::new(reader)
                .filter_map(|r| async move { r.ok() })
                .collect::<Vec<_>>()
                .await;
            println!("Finished download/decompressed into memory: took {:?}", start.elapsed());
            let start = Instant::now();

            let file_string = String::from_utf8(file_bytes.concat()).unwrap();
            // println!("{}", file_string);
            let expanded_records = vrl(&prog, &mut value!({ "__raw": file_string }))?
                .0
                .as_array()
                .ok_or(anyhow!("Expanded records must be an array"))?
                .to_owned();
            println!("Expanded records from payload: took {:?}, {} records", start.elapsed(), expanded_records.len());

            Box::pin(stream! {
                for record in expanded_records {
                    // let line = record.to_string();
                    //.ok_or(anyhow!("Expanded record must be a string"))?.;
                    // println!("{}", line);
                    let record_json: serde_json::Value  = record.try_into().unwrap();
                    yield Ok(record_json)
                }
            })
        }
        None => FramedRead::new(reader, LinesCodec::new())
            .map_ok(|line| serde_json::from_str(&line).unwrap())
            .map_err(|e| anyhow!(e))
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
        .collect::<Vec<_>>();

    info!(
        "Processing {} files from S3, of total size {} bytes",
        s3_download_items.len(),
        s3_download_items.iter().map(|d| d.size).sum::<i32>()
    );


    let config = aws_config::load_from_env().await;
    let s3 = aws_sdk_s3::Client::new(&config);

    let mut total_bytes_processed = 0;

    // let item = s3_download_items.first().unwrap();
    println!("current_num_threads() = {}", rayon::current_num_threads());

    let transformed_lines_streams = s3_download_items
        .iter()
        .map(|item| {
            let s3 = &s3;

            async move {
                // let ids: Vec<_> = (0..2)
                // .into_par_iter()
                // .map(|_|rayon::current_thread_index().unwrap_or(500))
                // .collect();
                // println!("{:?}", ids);
                let raw_lines = read_lines_s3(&s3, &item, None).await.unwrap();

                let transformed_lines_as_json = raw_lines
                    .chunks(10000)
                    .filter_map(move |batch| async move {
                        let transformed_batch = async_rayon::spawn(move || {
                            // CPU bound (VRL transforms)
                            batch
                                // .par_chunks(2000)
                                .chunks(2000)
                                .flat_map(|chunk| {
                                    debug!(
                                        "vrl_thread={}",
                                        rayon::current_thread_index().unwrap_or(500)
                                    );
                                    let transform_expr = LOG_SOURCES_CONFIG
                                        .with(|c| {
                                            let log_sources_config = c.borrow();

                                            let log_source = &"cloudtrail".to_string();
                                            // println!("{:?}, {}", log_sources_config, log_source);
                                            (*log_sources_config)
                                                .get(log_source)
                                                .unwrap()
                                                .transform
                                                .clone()
                                        })
                                        .unwrap();

                                    let wrapped_transform_expr = format!(
                                        "
                                         .related.user = []
                                         .related.hash = []
                                         .related.ip = []
                                         {}
                                         del(.json)
                                         ",
                                        1
                                    );

                                    let my = Instant::now();
                                    let transformed_chunk = chunk
                                        .into_iter()
                                        .map(|r| match r {
                                            Ok(line) => {
                                                let mut v = value!({"json": line});

                                                let _ = vrl(&wrapped_transform_expr, &mut v)
                                                    .map_err(|e| {
                                                        anyhow!("Failed to transform: {}", e)
                                                    })?
                                                    .1;

                                                let v_json: serde_json::Value =
                                                    v.try_into().unwrap();

                                                // println!("{:?}", v_json);
                                                Ok(v_json)
                                            }
                                            Err(e) => Err(anyhow!("Malformed line: {}", e)),
                                        })
                                        .collect::<Vec<_>>();
                                    println!("Transformed chunk (from thread_{}): took  {:?}",rayon::current_thread_index().unwrap_or(500), my.elapsed());
                                    // let transformed_chunk = chunk
                                    //     .into_iter()
                                    //     .map(|r| match r {
                                    //         Ok(line) => {
                                    //             // let mut v = value!({"message": (line.clone())});
                                    //             // let _ = vrl(&wrapped_transform_expr, &mut v)
                                    //             //     .map_err(|e| {
                                    //             //         anyhow!("Failed to transform: {}", e)
                                    //             //     })?
                                    //             //     .1;

                                    //             let v_json: serde_json::Value = line.clone();

                                    //             Ok(v_json)
                                    //         }
                                    //         Err(e) => Err(anyhow!("Malformed line: {}", e)),
                                    //     })
                                    //     .collect::<Vec<_>>();
                                    transformed_chunk
                                })
                                .collect::<Vec<_>>()
                        })
                        .await;

                        Some(stream::iter(transformed_batch))
                    })
                    .flatten();

                let reader = transformed_lines_as_json
                    .filter_map(|line| async move {
                        match line {
                            Ok(line) => Some(line),
                            Err(e) => {
                                error!("{}", e);
                                // Err(std::io::Error::new(std::io::ErrorKind::Other, e))
                                None
                            }
                        }
                    })
                    .boxed();

                (item.key.clone(), reader)
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

    let mut merged: HashMap<
        String,
        StreamMap<String, Pin<Box<Pin<Box<dyn Stream<Item = serde_json::Value> + Send>>>>>,
    > = HashMap::new();
    for (log_source, streams) in result.into_iter() {
        for (i, (_object_key, stream)) in streams.enumerate() {
            merged
                .entry(log_source.clone())
                .or_insert(StreamMap::new())
                .insert(
                    format!("{}", i), // object_key.clone(),
                    Box::pin(stream), // as Pin<Box<dyn Stream<Item = usize> + Send>>
                );
        }
    }

    let dd = merged
        .into_iter()
        .map(|(log_source, stream)| {
            // let total_bytes_processed = &mut total_bytes_processed;
            let s3 = &s3;

            async move {
                let number_of_rows = 50;

                let mut reader = stream.map(|(_, value)| value);

                let now = Instant::now();
                let head = reader
                    .by_ref()
                    .take(number_of_rows)
                    .collect::<Vec<_>>()
                    .await;


                debug!("io_thread={}", rayon::current_thread_index().unwrap_or(500));

                println!("Read 50 lines from head,  {:?}", now.elapsed());

                let now = Instant::now();

                let inferred_avro_schema = {
                    let head = head.clone();
                    async_rayon::spawn(move || {
                        // CPU bound (infer schemas)
                        println!(
                            "infer_thread={}",
                            rayon::current_thread_index().unwrap_or(500)
                        );
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
                        let data_types: HashSet<DataType> =
                            HashSet::from_iter(data_types.into_iter());
                        let unioned_data_type = data_types
                            .into_iter()
                            .collect::<Vec<_>>()
                            .first()
                            .unwrap()
                            .to_owned(); // todo: arrow2::io::json::read::coerce_data_type(&data_types);
                                         // let reader = stream::iter(head).chain(reader.borrow_mut());
                                         // let ss = reader.next().await;

                        // println!("{:#?}", unioned_data_type);
                        type_to_schema(&unioned_data_type, false, "root".to_string()).unwrap()
                    })
                }
                .await;
                println!("Inferred schema, took {:?}", now.elapsed());


                let reader = stream::iter(head).chain(reader); // rewind

                let avro_schema_json_string = serde_json::to_string(&inferred_avro_schema).unwrap();
                let inferred_avro_schema = Schema::parse_str(&avro_schema_json_string).unwrap();
                let writer = RefCell::new(Writer::with_codec(
                    &inferred_avro_schema,
                    Vec::new(),
                    Codec::Zstandard,
                ));

                reader
                    .chunks(10000)
                    .for_each(|chunk| {
                        let schema = inferred_avro_schema.clone();
                        let mut writer = writer.borrow_mut();

                        async move {
                            let avro_values = async_rayon::spawn(move || {
                                // CPU bound (VRL transform)
                                chunk
                                    // .par_chunks(2000)
                                    .chunks(2000)
                                    .flat_map(|values| {
                                        debug!(
                                            "avro_thread={}",
                                            rayon::current_thread_index().unwrap_or(500)
                                        );
                                        values.iter().map(|v| {
                                            let v_avro: apache_avro::types::Value =
                                                v.clone().clone().try_into().unwrap();
                                            v_avro.resolve(&schema).unwrap()
                                        })
                                    })
                                    .collect::<Vec<_>>()
                            })
                            .await;

                            // avro_values
                            writer.extend_from_slice(&avro_values).unwrap(); // IO (well theoretically)
                        }
                    })
                    .await;

                let writer = writer.into_inner();
                let bytes = writer.into_inner().unwrap();

                println!("total bytes processed: {:#?}, this file's bytelength: {}", total_bytes_processed, bytes.len());

                let uuid = Uuid::new_v4();
                let key = format!("transformed/{}/{}.avro", log_source, uuid);
                s3.put_object()
                    .bucket(var("MATANO_REALTIME_BUCKET_NAME").unwrap().to_string())
                    .key(&key)
                    .body(ByteStream::from(bytes))
                    .content_type("application/avro-binary".to_string())
                    .content_encoding("application/zstd".to_string())
                    .send()
                    .await
                    .map_err(|e| {
                        error!("Error putting {} to S3: {}", key, e);
                        e
                    })
                    .unwrap();
            }
        })
        .collect::<Vec<_>>();

    let start = Instant::now();
    let _ = join_all(dd).await;
    println!("process time (exclud download/decompress): {:?}", start.elapsed());

    println!(
        "total_bytes_processed={:?}",
        total_bytes_processed,
    );

    let resp = SuccessResponse {
        req_id: event.context.request_id,
        msg: format!("Hello from Lambda 1! The command was executed."),
    };

    // return `Response` (it will be serialized to JSON automatically by the runtime)
    Ok(resp)
}
