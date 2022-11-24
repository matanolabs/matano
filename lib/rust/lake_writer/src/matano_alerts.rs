use crate::{
    common::{load_table_arrow_schema, struct_wrap_arrow2_for_ffi, write_arrow_to_s3_parquet},
    ALERTS_TABLE_NAME, AWS_CONFIG,
};
use anyhow::{anyhow, Result};
use arrow2::chunk::Chunk;
use arrow2::datatypes::*;
use arrow2::io::avro::avro_schema;
use arrow2::io::avro::avro_schema::file::{Block, CompressedBlock, Compression};
use arrow2::io::avro::avro_schema::schema::Record;
use arrow2::io::avro::avro_schema::write::compress;
use arrow2::io::avro::write;
use async_once::AsyncOnce;
use aws_sdk_lambda::types::Blob;
use aws_sdk_sns::model::MessageAttributeValue;
use bytes::Bytes;
use futures::future::{join_all, try_join_all};
use lazy_static::lazy_static;
use log::{debug, error, info};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    env::var,
    io::Write,
    path::Path,
    sync::Arc,
    time::Instant,
    vec,
};
use tokio::fs;
use tokio::task::spawn_blocking;

use base64::encode;
use std::mem;
struct ByteArrayChunker<I: Iterator> {
    iter: I,
    chunk: Vec<I::Item>,
    max_total_size: usize,
    total_size: usize,
}

impl<I> Iterator for ByteArrayChunker<I>
where
    I: Iterator,
    I::Item: AsRef<Vec<u8>>,
{
    type Item = Vec<I::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.iter.next() {
                Some(item) => {
                    let item_size = item.as_ref().len();
                    let mut ret: Option<Self::Item> = None;
                    if self.total_size + item_size > self.max_total_size {
                        self.total_size = 0;
                        ret = Some(mem::take(&mut self.chunk));
                    }
                    self.chunk.push(item);
                    self.total_size += item_size;

                    if let Some(_) = ret {
                        return ret;
                    }
                }
                None => {
                    return if self.chunk.is_empty() {
                        None
                    } else {
                        Some(mem::take(&mut self.chunk))
                    }
                }
            }
        }
    }
}

trait ChunkExt: Iterator + Sized {
    fn chunks_total_size(self, max_total_size: usize) -> ByteArrayChunker<Self> {
        ByteArrayChunker {
            iter: self,
            chunk: Vec::new(),
            max_total_size: max_total_size,
            total_size: 0,
        }
    }
}

impl<I: Iterator + Sized> ChunkExt for I {}

lazy_static! {
    static ref ALERTS_AVRO_SCHEMA: apache_avro::Schema = get_alerts_avro_schema().unwrap();
    static ref ALERTS_ARROW_SCHEMA: arrow2::datatypes::Schema =
        load_table_arrow_schema("matano_alerts").unwrap();
    static ref LAMBDA_CLIENT: AsyncOnce<aws_sdk_lambda::Client> =
        AsyncOnce::new(async { aws_sdk_lambda::Client::new(AWS_CONFIG.get().await) });
    static ref SNS_CLIENT: AsyncOnce<aws_sdk_sns::Client> =
        AsyncOnce::new(async { aws_sdk_sns::Client::new(AWS_CONFIG.get().await) });
}

/// Here's what we do:
/// 1. Pass in new data, a list of avro files representing new rule matches.
/// 2. Read the data into avro values using apache_avro.
/// 3. Load the last 1 days' matano_alerts data as avro values by:
///     1. Invoking the helper Iceberg function to get the list of files using JVM Iceberg API.
///     2. Downloading those parquet files and read them as chunks using arrow2.
///     3. Converting them to avro using arrow2 and serializing them.
///     4. Deserializing the serialized avro values into avro values using apache_avro crate.
/// 4. Aggregate the new data by (rule_name, dedupe).
/// 4. Aggregate the existing data by (rule_name, dedupe) and calculate active alerts and their counts + window start times.
/// 5. Determine the alert ids for new values to add (create if necessary).
/// 6. Mutate the new avro values and add in fields like matano.alert.id,breached,created, etc.
/// 7. Track if an existing alert was just breached, and if so, mutate the corresponding values to set matano.alert,breached = true.
/// 8. Encode modified/created partition values as parquet and write to S3.
/// 9. Call iceberg helper lambda to do Iceberg commit with overwrites/appends as necessary.
/// FIN!
pub async fn process_alerts(s3: aws_sdk_s3::Client, data: Vec<Vec<u8>>) -> Result<()> {
    if data.is_empty() {
        return Ok(());
    }

    // read new data into avro values using apache_avro
    let new_values = data
        .into_iter()
        .flat_map(|buf| {
            let data = std::io::Cursor::new(buf);
            let r = apache_avro::Reader::with_schema(&ALERTS_AVRO_SCHEMA, data).unwrap();
            r.map(|v| v.unwrap()).collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    info!("Loaded new values.");

    type MaybeString = Option<String>;

    let st11 = std::time::Instant::now();
    let existing_values_vecs = get_existing_values(&s3).await?;
    println!(
        "**************** TIME: EXISTING_VALUES {:?}",
        st11.elapsed()
    );
    info!("Loaded existing values.");

    let now = chrono::Utc::now();
    let now_ts_hour = now.format("%Y-%m-%d-%H").to_string();
    let current_micros = now.timestamp_micros();

    let mut new_value_agg = HashMap::<(MaybeString, MaybeString), NewAlertData>::new();

    // Aggregate new values per (rule_name, dedupe)
    info!("Aggregating new values...");
    for new_value in new_values.iter() {
        let matano_alert_rule_name = get_av_path(new_value, "matano.alert.rule.name");
        let matano_alert_dedupe = get_av_path(new_value, "matano.alert.dedupe");

        let matano_alert_rule_name_s = cast_av_str(matano_alert_rule_name);
        let matano_alert_dedupe_s = cast_av_str(matano_alert_dedupe);

        let matano_rule_match_id = get_av_path(new_value, "matano.alert.rule.match.id");
        let matano_rule_match_id_s = cast_av_str(matano_rule_match_id).unwrap();

        let deduplication_window =
            get_av_path(&new_value, "matano.alert.rule.deduplication_window");
        let deduplication_window_seconds: i64 =
            cast_av_int(deduplication_window).unwrap_or(3600).into();
        let deduplication_window_micros = deduplication_window_seconds * 1000000;

        let alert_threshold = get_av_path(&new_value, "matano.alert.rule.threshold");
        let alert_threshold = cast_av_int(alert_threshold).unwrap_or(1);

        let key = (matano_alert_rule_name_s, matano_alert_dedupe_s);

        let default = NewAlertData {
            rule_match_ids: vec![],
            alert_id: None,
            breached: None,
            created_micros: None,
            first_matched_at_micros: None,
            deduplication_window_micros,
            threshold: alert_threshold,
        };

        let entry = new_value_agg.entry(key).or_insert(default);
        entry.rule_match_ids.push(matano_rule_match_id_s);
    }

    let st11 = std::time::Instant::now();
    // aggregate existing values to get active alerts, and their counts and window starts
    info!("Aggregating existing values...");
    let mut existing_aggregation_map =
        HashMap::<(MaybeString, MaybeString), ExistingAlertsData>::new();
    for (_, existing_values) in existing_values_vecs.iter() {
        for value in existing_values.iter() {
            let matano_rule_match_id = get_av_path(&value, "matano.alert.rule.match.id");
            let matano_alert_rule_name = get_av_path(&value, "matano.alert.rule.name");
            let matano_alert_dedupe = get_av_path(&value, "matano.alert.dedupe");
            let matano_alert_id = get_av_path(&value, "matano.alert.id");
            let matano_alert_first_matched_at =
                get_av_path(&value, "matano.alert.first_matched_at");
            let matano_alert_created = get_av_path(&value, "matano.alert.created");

            let matano_alert_breached = get_av_path(&value, "matano.alert.breached");

            let matano_alert_rule_name = cast_av_str(matano_alert_rule_name);
            let matano_alert_dedupe = cast_av_str(matano_alert_dedupe);

            let matano_alert_id_s = cast_av_str(matano_alert_id);
            let matano_alert_first_matched_at_ts = cast_av_ts(matano_alert_first_matched_at);
            let matano_alert_created_ts = cast_av_ts(matano_alert_created);

            let matano_alert_breached_bool = cast_av_bool(matano_alert_breached).unwrap_or(false);

            let new_alert_entry =
                new_value_agg.get(&(matano_alert_rule_name.clone(), matano_alert_dedupe.clone()));

            if matano_alert_id_s.is_some() && new_alert_entry.is_some() {
                let deduplication_window_micros =
                    new_alert_entry.unwrap().deduplication_window_micros;

                if matano_alert_first_matched_at_ts.map_or(false, |ts| {
                    ts + deduplication_window_micros > current_micros
                }) {
                    let alert_id = matano_alert_id_s.unwrap();
                    let first_matched_at = matano_alert_first_matched_at_ts.unwrap();

                    let rule_match_id = cast_av_str(matano_rule_match_id).unwrap();
                    let data = ExistingAlertsData {
                        alert_id,
                        rule_match_ids: vec![],
                        first_matched_at,
                        created: matano_alert_created_ts,
                        breached: matano_alert_breached_bool,
                        breached_changed: false,
                    };
                    let key = (matano_alert_rule_name, matano_alert_dedupe);
                    let entry = existing_aggregation_map.entry(key).or_insert(data);
                    entry.rule_match_ids.push(rule_match_id);
                }
            }
        }
    }

    // Compute alert ids and breached status for new values.
    // Also, if an alert was just breached, mark it so we can update the existing values
    info!("Computing alert ids...");
    for ((rule_name, dedupe), new_alert_data) in new_value_agg.iter_mut() {
        let key = (rule_name.clone(), dedupe.clone());
        let existing_entry = existing_aggregation_map.get_mut(&key);

        let threshold: usize = new_alert_data.threshold.try_into().unwrap();
        let new_match_count = new_alert_data.rule_match_ids.len();

        if let Some(entry) = existing_entry {
            let alert_id = entry.alert_id.clone();
            let existing_count = entry.rule_match_ids.len();
            let first_matched_at = entry.first_matched_at;
            let did_breach = (existing_count + new_match_count) >= threshold;
            let breached_changed = !entry.breached && did_breach;

            let created_ts = match (entry.created, breached_changed) {
                (Some(ts), _) => Some(ts),
                (None, true) => Some(current_micros),
                (None, false) => None,
            };

            entry.breached_changed = breached_changed;
            new_alert_data.alert_id = Some(alert_id);
            new_alert_data.breached = Some(did_breach);
            new_alert_data.created_micros = created_ts;
            new_alert_data.first_matched_at_micros = Some(first_matched_at);
        } else {
            let new_alert_id = uuid::Uuid::new_v4().to_string();
            let did_breach = new_match_count >= threshold;
            new_alert_data.alert_id = Some(new_alert_id);
            new_alert_data.breached = Some(did_breach);
            if did_breach {
                new_alert_data.created_micros = Some(current_micros);
            }
            new_alert_data.first_matched_at_micros = Some(current_micros);
        }
    }

    let mut alerts_to_deliver = json!({});

    // update the new values based on the alert it was added to
    info!("Updating new values...");
    for new_value in new_values.iter() {
        let matano_alert_rule_name = get_av_path(new_value, "matano.alert.rule.name");
        let matano_alert_dedupe = get_av_path(new_value, "matano.alert.dedupe");

        let matano_alert_rule_name_s = cast_av_str(matano_alert_rule_name);
        let matano_alert_dedupe_s = cast_av_str(matano_alert_dedupe);

        let key = (matano_alert_rule_name_s, matano_alert_dedupe_s);
        let new_alert_data = new_value_agg.get(&key).unwrap();

        let is_breached = existing_aggregation_map
            .get(&key)
            .map_or(new_alert_data.breached.unwrap_or(false), |e| e.breached);
        let new_value_mut = unsafe { make_mut(new_value) };

        insert_av_path(
            new_value_mut,
            "matano.alert.id".to_string(),
            avro_null_union(apache_avro::types::Value::String(
                new_alert_data.alert_id.as_ref().unwrap().clone(),
            )),
        );
        insert_av_path(
            new_value_mut,
            "matano.alert.breached".to_string(),
            avro_null_union(apache_avro::types::Value::Boolean(
                *new_alert_data.breached.as_ref().unwrap(),
            )),
        );
        insert_av_path(
            new_value_mut,
            "matano.alert.first_matched_at".to_string(),
            avro_null_union(apache_avro::types::Value::TimestampMicros(
                new_alert_data.first_matched_at_micros.unwrap(),
            )),
        );
        let created_av = match new_alert_data.created_micros {
            Some(created_micros) => {
                avro_null_union(apache_avro::types::Value::TimestampMicros(created_micros))
            }
            None => apache_avro::types::Value::Null,
        };
        insert_av_path(
            new_value_mut,
            "matano.alert.created".to_string(),
            created_av,
        );

        if is_breached {
            let rule_match = new_value_mut.clone();
            let rule_match_json =
                apache_avro::from_value::<serde_json::Value>(&rule_match).unwrap();
            let alert_id = new_alert_data.alert_id.as_ref().unwrap().clone();
            alerts_to_deliver
                .as_object_mut()
                .unwrap()
                .entry(alert_id.clone())
                .or_insert(json!({
                    "alert_id": alert_id,
                    "rule_matches": {},
                }))["rule_matches"]
                .as_object_mut()
                .unwrap()
                .entry("new")
                .or_insert(json!([]))
                .as_array_mut()
                .unwrap()
                .push(rule_match_json);
        }
    }

    // Update the existing values if an alert was breached just now
    info!("Updating existing values...");
    let mut modified_partitions = HashSet::with_capacity(existing_values_vecs.len());
    for ((_, partition), existing_values) in existing_values_vecs.iter() {
        for value in existing_values.iter() {
            let matano_alert_rule_name = get_av_path(&value, "matano.alert.rule.name");
            let matano_alert_dedupe = get_av_path(&value, "matano.alert.dedupe");
            let matano_alert_id = get_av_path(&value, "matano.alert.id");
            let matano_alert_id_s = cast_av_str(matano_alert_id);

            let key = (
                cast_av_str(matano_alert_rule_name),
                cast_av_str(matano_alert_dedupe),
            );
            if let Some(existing_data) = existing_aggregation_map.get(&key) {
                if existing_data.breached_changed
                    && matano_alert_id_s.map_or(false, |id| id == existing_data.alert_id)
                {
                    let value_mut = unsafe { make_mut(value) };
                    insert_av_path(
                        value_mut,
                        "matano.alert.breached".to_string(),
                        avro_null_union(apache_avro::types::Value::Boolean(true)),
                    );
                    modified_partitions.insert(partition.clone());

                    // add the existing value to the alerts to deliver
                    // let rule_match = value_mut.clone();
                    // let rule_match_json =
                    //     apache_avro::from_value::<serde_json::Value>(&rule_match).unwrap();
                    let alert_id = existing_data.alert_id.clone();
                    let rule_matches_obj = alerts_to_deliver
                        .as_object_mut()
                        .unwrap()
                        .entry(alert_id.clone())
                        .or_insert(json!({
                            "alert_id": alert_id,
                            "rule_matches": {},
                        }))["rule_matches"]
                        .as_object_mut()
                        .unwrap();

                    rule_matches_obj
                        .entry("existing")
                        .or_insert(json!({}))
                        .as_object_mut()
                        .unwrap()
                        .entry("count")
                        .and_modify(|v| *v = json!(v.as_i64().unwrap() + 1))
                        .or_insert(json!(1));
                }
            }
        }
    }
    println!(
        "**************** TIME: AVRO_MODIFICATION {:?}",
        st11.elapsed()
    );

    // if new data fits in existing partition, add there, else create a new partition
    let mut partition_data = existing_values_vecs;
    let existing_partition = partition_data
        .iter_mut()
        .find(|((_, partition), _)| partition == &now_ts_hour);
    if let Some((_, e_vec)) = existing_partition {
        e_vec.extend(new_values);
    } else {
        partition_data.push(((None, now_ts_hour.clone()), new_values));
    }

    let mut upload_join_handles = Vec::with_capacity(partition_data.len());

    // filter out unchanged partitions
    let partition_data = partition_data.into_iter().filter(|((_, partition), _)| {
        partition == &now_ts_hour || modified_partitions.contains(partition)
    });

    info!("Writing final values to parquet...");
    let st11 = std::time::Instant::now();
    let final_data = partition_data
        .map(|((old_key, partition), partition_values)| {
            // encode values to avro using apache_avro
            let mut writer = apache_avro::Writer::new(&ALERTS_AVRO_SCHEMA, Vec::new());
            writer.extend(partition_values).unwrap();
            let data = writer.into_inner().unwrap();

            // then read back using arrow2 into arrow
            let mut data_r = std::io::Cursor::new(data);
            let metadata = arrow2::io::avro::avro_schema::read::read_metadata(&mut data_r).unwrap();
            let schema = &ALERTS_ARROW_SCHEMA;
            let chunks =
                arrow2::io::avro::read::Reader::new(data_r, metadata, schema.fields.clone(), None)
                    .map(|c| c.unwrap())
                    .collect::<Vec<_>>();

            // and write to parquet
            let (field, arrays) = struct_wrap_arrow2_for_ffi(&schema, chunks);
            let fut = write_arrow_to_s3_parquet(
                s3.clone(),
                ALERTS_TABLE_NAME.into(),
                partition,
                field,
                arrays,
            );

            let join_handle = tokio::spawn(fut);
            upload_join_handles.push(join_handle);

            old_key
        })
        .collect::<Vec<_>>();
    println!("**************** TIME: WRITE_PARQUET {:?}", st11.elapsed());

    let written_keys = join_all(upload_join_handles)
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    let commit_req_items = final_data
        .into_iter()
        .zip(written_keys.into_iter())
        .map(
            |(old_key, (partition, new_key, file_size_bytes))| IcebergCommitRequestItem {
                old_key,
                new_key,
                ts_hour: partition,
                file_size_bytes,
            },
        )
        .collect::<Vec<_>>();

    info!("Doing Iceberg commit...");
    let st11 = std::time::Instant::now();
    do_iceberg_commit(commit_req_items).await?;
    println!("**************** TIME: DO_COMMIT {:?}", st11.elapsed());

    // TODO: reduce intermediate byte vec allocations
    let alerts_to_deliver = alerts_to_deliver
        .as_object()
        .unwrap()
        .values()
        .par_bridge()
        .into_par_iter()
        .map(|v| {
            let mut bytes_v = v.to_string().into_bytes();
            bytes_v.push(b'\n');
            bytes_v
        })
        .collect::<Vec<_>>();
    println!(
        "Delivering {} alerts that contained new rule matches...",
        alerts_to_deliver.len()
    );
    debug!("Delivering alerts: {:?}", alerts_to_deliver);
    // TODO(shaeq): handle case if >256kb compressed new rule matches for a single alert..
    let alerts_to_deliver_chunks = alerts_to_deliver
        .into_iter()
        .chunks_total_size(256 * 1024)
        .collect::<Vec<_>>();
    let alerts_to_deliver_compressed_chunks = alerts_to_deliver_chunks
        .into_par_iter()
        .map(|c| {
            let mut encoder = zstd::Encoder::new(Vec::new(), 1).unwrap();
            for l in c {
                encoder.write_all(&l).unwrap();
            }
            encoder.finish().unwrap()
        })
        .collect::<Vec<_>>()
        .into_iter()
        .chunks_total_size(192 * 1024)
        .map(|c| encode(c.into_iter().flatten().collect::<Vec<_>>()))
        .collect::<Vec<_>>();

    let sns = SNS_CLIENT.get().await;

    let alert_futures = alerts_to_deliver_compressed_chunks.into_iter().map(|s| {
        sns.publish()
            .topic_arn(var("ALERTING_SNS_TOPIC_ARN").unwrap().to_string())
            .message(s)
            .message_attributes(
                "destination",
                MessageAttributeValue::builder()
                    .data_type("String".to_string())
                    .string_value("slack")
                    .build(),
            )
            .send()
    });

    let res = try_join_all(alert_futures).await?;

    Ok(())
}

fn serialize_to_block<R: AsRef<dyn arrow2::array::Array>>(
    columns: &Chunk<R>,
    record: Record,
    compression: Option<Compression>,
) -> Result<CompressedBlock> {
    let mut serializers = columns
        .arrays()
        .iter()
        .map(|x| x.as_ref())
        .zip(record.fields.iter())
        .map(|(array, field)| write::new_serializer(array, &field.schema))
        .collect::<Vec<_>>();
    let mut block = Block::new(columns.len(), vec![]);

    write::serialize(&mut serializers, &mut block);

    let mut compressed_block = CompressedBlock::default();
    compress(&mut block, &mut compressed_block, compression).unwrap();

    Ok(compressed_block)
}

// arrow2 currently writes incorrect avro record names when converting arrow schema -> avro record
// this is a hack to 'fix' this by adding random record names
// see https://github.com/jorgecarleitao/arrow2/issues/1269 (TODO: remove this when merged)
fn rand_name() -> String {
    "record".to_owned() + &uuid::Uuid::new_v4().simple().to_string()
}
fn hack_fix_arrow2_avro_schema(schema: avro_schema::schema::Schema) -> avro_schema::schema::Schema {
    match schema {
        avro_schema::schema::Schema::Record(rec) => {
            avro_schema::schema::Schema::Record(avro_schema::schema::Record {
                name: rand_name(),
                namespace: rec.namespace,
                doc: rec.doc,
                aliases: rec.aliases,
                fields: rec
                    .fields
                    .into_iter()
                    .map(|f| {
                        arrow2::io::avro::avro_schema::schema::Field::new(
                            f.name,
                            hack_fix_arrow2_avro_schema(f.schema),
                        )
                    })
                    .collect(),
            })
        }
        avro_schema::schema::Schema::Array(schema) => {
            avro_schema::schema::Schema::Array(Box::new(hack_fix_arrow2_avro_schema(*schema)))
        }
        avro_schema::schema::Schema::Union(schemas) => avro_schema::schema::Schema::Union(
            schemas
                .into_iter()
                .map(hack_fix_arrow2_avro_schema)
                .collect(),
        ),
        x => x,
    }
}
fn hack_fix_arrow2_avro_record(record: avro_schema::schema::Record) -> avro_schema::schema::Record {
    let schema = avro_schema::schema::Schema::Record(record);
    let new_schema = hack_fix_arrow2_avro_schema(schema);
    match new_schema {
        avro_schema::schema::Schema::Record(rec) => rec,
        _ => panic!("invalid record"),
    }
}

// We have a bug somewhere where our Timestamps have timezone information when read from Parquet (adjust to UTC = true)
// This is hack to remove the timezone since we don't actually have tz-aware timestamps
// TODO: fix the root cause
fn hack_fix_arrow2_schema_ts(schema: Schema) -> Schema {
    Schema {
        fields: schema
            .fields
            .iter()
            .map(|field| {
                Field::new(
                    field.name.clone(),
                    hack_fix_arrow2_dtyp_ts(field.data_type.clone()),
                    field.is_nullable,
                )
            })
            .collect(),
        metadata: Default::default(),
    }
}
fn hack_fix_arrow2_dtyp_ts(dtype: DataType) -> DataType {
    match dtype {
        DataType::Timestamp(tu, _) => DataType::Timestamp(tu, None),
        DataType::List(field) => DataType::List(Box::new(Field::new(
            field.name,
            hack_fix_arrow2_dtyp_ts(field.data_type),
            field.is_nullable,
        ))),
        DataType::LargeList(field) => DataType::LargeList(Box::new(Field::new(
            field.name,
            hack_fix_arrow2_dtyp_ts(field.data_type),
            field.is_nullable,
        ))),
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| {
                    Field::new(
                        field.name.clone(),
                        hack_fix_arrow2_dtyp_ts(field.data_type.clone()),
                        field.is_nullable,
                    )
                })
                .collect(),
        ),
        x => x,
    }
}

// need this rn b/c arrow2 cannot read Athena optimized parquet files (delta length byte array)
// so we need to deserialize using arrow crate and serialize back for arrow2
// TODO: Make FFI work (weird bugs)
fn arrow_arrow2_parquet(reader: Bytes) -> Vec<u8> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(reader).unwrap();
    let parq_reader = builder.build().unwrap();

    let buf = vec![];
    let props = parquet::file::properties::WriterProperties::builder().build();

    let batches = parq_reader
        .collect::<Vec<_>>()
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let schema = batches.first().unwrap().schema();
    let mut writer = parquet::arrow::ArrowWriter::try_new(buf, schema, Some(props)).unwrap();

    for batch in batches {
        writer.write(&batch).unwrap();
    }
    writer.flush().unwrap();
    writer.into_inner().unwrap()
}

async fn get_existing_values(
    s3: &aws_sdk_s3::Client,
) -> Result<Vec<((Option<String>, String), Vec<apache_avro::types::Value>)>> {
    let st11 = std::time::Instant::now();
    let iceberg_paths = get_iceberg_read_files().await?;
    println!("**************** TIME: READ_FILES {:?}", st11.elapsed());
    println!("{:?}", iceberg_paths);
    if iceberg_paths.is_empty() {
        return Ok(vec![]);
    }
    let st11 = std::time::Instant::now();

    let iceberg_partitions = iceberg_paths
        .iter()
        .map(|(_, key)| {
            let partition = key
                .split("/")
                .find(|p| p.starts_with("ts_hour="))
                .unwrap()
                .trim_matches('/')
                .replace("ts_hour=", "");
            (Some(key.clone()), partition)
        })
        .collect::<Vec<_>>();

    let dl_tasks = iceberg_paths.into_iter().map(|(bucket, key)| {
        let ret = async move {
            let obj_res = s3
                .get_object()
                .bucket(bucket)
                .key(key)
                .send()
                .await
                .map_err(|e| {
                    error!("Error downloading {} from S3: {}", "", e);
                    e
                });
            let obj = obj_res.unwrap();
            println!("Got object...");
            println!("_________len: {}", obj.content_length());

            let stream = obj.body;
            let body = stream.collect().await.unwrap();
            body.into_bytes()
        };
        ret
    });

    let readers = join_all(dl_tasks).await.into_iter();

    let s1 = Instant::now();
    let readers = readers.map(arrow_arrow2_parquet);
    println!(
        "********TIME: arrow parquet re serialization took: {:?}",
        s1.elapsed()
    );

    use arrow2::io::parquet::read;

    let mut avro_record = Box::new(None);

    let all_chunks = readers
        .into_iter()
        .map(|reader| {
            let mut reader = std::io::Cursor::new(reader);
            let metadata = read::read_metadata(&mut reader).unwrap();
            let schema = read::infer_schema(&metadata).unwrap();
            let schema = hack_fix_arrow2_schema_ts(schema);
            let row_groups = metadata.row_groups;

            let start = std::time::Instant::now();

            let chunks =
                read::FileReader::new(reader, row_groups, schema.clone(), None, None, None)
                    .into_iter()
                    .map(|r| r.unwrap())
                    .collect::<Vec<_>>();
            println!("**************** parq read time {:?}", start.elapsed());

            if !avro_record.is_some() {
                let record =
                    hack_fix_arrow2_avro_record(write::to_record(&schema.clone()).unwrap());
                avro_record = Box::new(Some(record));
            }
            chunks
        })
        .collect::<Vec<_>>();

    let final_values = all_chunks
        .into_iter()
        .map(|chunks| {
            let mut buf = vec![];
            avro_schema::write::write_metadata(&mut buf, avro_record.clone().unwrap(), None)
                .unwrap();
            for chunk in chunks {
                let rec = avro_record.clone().unwrap();
                let block = serialize_to_block(&chunk, rec, None).unwrap();
                avro_schema::write::write_block(&mut buf, &block).unwrap();
            }
            let data = std::io::Cursor::new(buf);
            let r = apache_avro::Reader::with_schema(&ALERTS_AVRO_SCHEMA, data).unwrap();
            let values = r.map(|v| v.unwrap()).collect::<Vec<_>>();
            values
        })
        .collect::<Vec<_>>();

    let ret = iceberg_partitions
        .into_iter()
        .zip(final_values.into_iter())
        .collect::<Vec<_>>();

    println!(
        "**************** TIME: LOAD_EXISTING_VALUES {:?}",
        st11.elapsed()
    );
    Ok(ret)
}

async fn call_helper_lambda(payload: String) -> Result<Option<Vec<u8>>> {
    let lambda = LAMBDA_CLIENT.get().await;
    let helper_function_name = std::env::var("ALERT_HELPER_FUNCTION_NAME")?;

    let func_res = lambda
        .invoke()
        .function_name(helper_function_name)
        .payload(Blob::new(payload))
        .send()
        .await?;

    let resp_payload = func_res.payload().map(|blob| blob.to_owned().into_inner());
    if func_res.function_error().is_some() {
        let err_obj_bytes = resp_payload.unwrap_or_default();
        let err_obj = String::from_utf8_lossy(&err_obj_bytes);
        Err(anyhow!("read_files Lambda failed: {}", err_obj))
    } else {
        Ok(resp_payload)
    }
}

async fn do_iceberg_commit(req_items: Vec<IcebergCommitRequestItem>) -> Result<()> {
    let payload = json!({
        "operation": "do_commit",
        "data": req_items,
    });
    let payload = serde_json::to_string(&payload)?;

    call_helper_lambda(payload).await?;

    Ok(())
}

async fn get_iceberg_read_files() -> Result<Vec<(String, String)>> {
    let payload = json!({
        "operation": "read_files",
    });
    let payload = serde_json::to_string(&payload)?;

    let func_resp = call_helper_lambda(payload).await?;

    let func_resp_payload = func_resp.expect("Read files Function didn't return payload!");
    let read_files: Vec<String> = serde_json::from_slice(func_resp_payload.as_slice())?;
    let read_files = read_files.into_iter().map(parse_s3_uri).collect::<Vec<_>>();

    Ok(read_files)
}

fn parse_s3_uri(p: String) -> (String, String) {
    // s3://bucket/key/a/b/c
    let p = &p
        .as_str()
        .trim_start_matches("s3://")
        .splitn(2, '/')
        .collect::<Vec<_>>();
    let bucket = p.get(0).unwrap().trim_matches('/');
    let key = p.get(1).unwrap().trim_matches('/');
    (bucket.to_string(), key.to_string())
}

fn get_alerts_avro_schema() -> Result<apache_avro::Schema> {
    let schemas_path = Path::new("/opt/schemas");
    let schema_path = schemas_path
        .join(ALERTS_TABLE_NAME)
        .join("avro_schema.avsc");
    let avro_schema_json_string = std::fs::read_to_string(schema_path).unwrap();

    let schema = apache_avro::Schema::parse_str(avro_schema_json_string.as_str())?;
    Ok(schema)
}

fn get_single_record(
    value: &apache_avro::types::Value,
    path: String,
) -> &apache_avro::types::Value {
    match value {
        apache_avro::types::Value::Record(ref vals) => {
            let v = vals.iter().find(|(k, _)| k == &path);
            match v {
                Some((_, v)) => v,
                None => panic!("No value for path {}", path),
            }
        }
        apache_avro::types::Value::Union(pos, v) => {
            if *pos == 0 {
                panic!("gsr empty!!")
            } else {
                get_single_record(v, path)
            }
        }
        _ => panic!("gsr!"),
    }
}

fn get_av_path<'a>(
    value: &'a apache_avro::types::Value,
    path: &'a str,
) -> &'a apache_avro::types::Value {
    let parts = path.split(".");
    let mut curval = value;
    for part in parts {
        let oldval = curval;
        curval = get_single_record(oldval, part.to_string());
    }
    curval
}

fn insert_av(
    record_value: &mut apache_avro::types::Value,
    insert_field_name: String,
    insert_value: apache_avro::types::Value,
) {
    match record_value {
        apache_avro::types::Value::Record(ref mut vals) => {
            let index = &vals.iter().position(|(k, _)| k == &insert_field_name);
            if let Some(idx) = index {
                let val_ref = vals.get_mut(*idx).unwrap();
                *val_ref = (insert_field_name, insert_value);
            }
        }
        apache_avro::types::Value::Union(pos, v) => {
            if *pos == 0 {
            } else {
                insert_av(v, insert_field_name, insert_value)
            }
        }
        _ => panic!("gsr!"),
    }
}

fn insert_av_path(
    value: &mut apache_avro::types::Value,
    path: String,
    insert_value: apache_avro::types::Value,
) {
    let parts = path.split(".").collect::<Vec<_>>();
    let (tail, parts) = parts.split_last().unwrap();
    let mut curval = value;
    for part in parts {
        let oldval = curval;
        curval = unsafe { make_mut(get_single_record(oldval, part.to_string())) };
    }
    insert_av(curval, tail.to_string(), insert_value);
}

fn cast_av_ts(v: &apache_avro::types::Value) -> Option<i64> {
    match v {
        apache_avro::types::Value::TimestampMicros(micros) => Some(*micros),
        apache_avro::types::Value::Union(pos, v) => {
            if *pos == 0 {
                None
            } else {
                cast_av_ts(v)
            }
        }
        _ => panic!("gsr!"),
    }
}

fn cast_av_str(v: &apache_avro::types::Value) -> Option<String> {
    match v {
        apache_avro::types::Value::String(s) => Some(s.clone()),
        apache_avro::types::Value::Union(pos, v) => {
            if *pos == 0 {
                None
            } else {
                cast_av_str(v)
            }
        }
        _ => panic!("gsr!"),
    }
}

fn cast_av_int(v: &apache_avro::types::Value) -> Option<i32> {
    match v {
        apache_avro::types::Value::Int(x) => Some(*x),
        apache_avro::types::Value::Union(pos, v) => {
            if *pos == 0 {
                None
            } else {
                cast_av_int(v)
            }
        }
        _ => panic!("gsr!"),
    }
}

fn cast_av_bool(v: &apache_avro::types::Value) -> Option<bool> {
    match v {
        apache_avro::types::Value::Boolean(s) => Some(*s),
        apache_avro::types::Value::Union(pos, v) => {
            if *pos == 0 {
                None
            } else {
                cast_av_bool(v)
            }
        }
        _ => panic!("gsr!"),
    }
}

fn avro_null_union(v: apache_avro::types::Value) -> apache_avro::types::Value {
    apache_avro::types::Value::Union(1, Box::new(v))
}

// TODO: remove this, get avro change merged.
unsafe fn make_mut<T>(reference: &T) -> &mut T {
    &mut *((reference as *const T) as *mut T)
}

#[derive(Debug, Clone)]
struct ExistingAlertsData {
    alert_id: String,
    rule_match_ids: Vec<String>,
    first_matched_at: i64,
    created: Option<i64>,
    breached: bool,
    breached_changed: bool,
}

#[derive(Debug, Clone)]
struct NewAlertData {
    rule_match_ids: Vec<String>,
    alert_id: Option<String>,
    breached: Option<bool>,
    first_matched_at_micros: Option<i64>,
    created_micros: Option<i64>,
    deduplication_window_micros: i64,
    threshold: i32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct IcebergCommitRequestItem {
    old_key: Option<String>,
    new_key: String,
    ts_hour: String,
    file_size_bytes: usize,
}
