use crate::{
    common::{load_table_arrow_schema, struct_wrap_arrow2_for_ffi, write_arrow_to_s3_parquet},
    ALERTS_TABLE_NAME, AWS_CONFIG,
};
use anyhow::{anyhow, Context, Result};
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
    sync::{Arc, Mutex},
    time::Instant,
    vec,
};

use shared::alert_util::RULE_MATCHES_GROUP_ID;
use shared::alert_util::{encode_alerts_for_publish, ChunkExt};
use shared::avro::AvroValueExt;

use base64::encode;

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
/// 6. Mutate the new avro values and add in fields like matano.alert.id,activated,created, etc.
/// 7. Track if an existing alert was just activated, and if so, mutate the corresponding values to set matano.alert,activated = true.
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

    let existing_values_vecs = get_existing_values(&s3).await?;
    info!("Loaded existing values.");

    let now = chrono::Utc::now();
    let now_ts_hour = now.format("%Y-%m-%d-%H").to_string();
    let current_micros = now.timestamp_micros();

    let mut new_value_agg = HashMap::<(MaybeString, MaybeString), NewAlertData>::new();

    // Aggregate new values per (rule_name, dedupe)
    info!("Aggregating new values...");
    for new_value in new_values.iter() {
        let matano_alert_rule_name = new_value
            .get_nested("matano.alert.rule.name")
            .and_then(|v| Some(v.as_str()?.to_string()));
        let matano_alert_dedupe = new_value
            .get_nested("matano.alert.dedupe")
            .and_then(|v| Some(v.as_str()?.to_string()));

        let matano_rule_match_id = new_value
            .get_nested("matano.alert.rule.match.id")
            .and_then(|v| v.as_str())
            .context("Need Alert Id")?
            .to_string();

        let deduplication_window_seconds: i64 = new_value
            .get_nested("matano.alert.rule.deduplication_window")
            .and_then(|v| v.as_int())
            .unwrap_or(3600)
            .into();
        let deduplication_window_micros = deduplication_window_seconds * 1000000;

        let alert_threshold = new_value
            .get_nested("matano.alert.rule.threshold")
            .and_then(|v| v.as_int())
            .unwrap_or(1);

        let key = (matano_alert_rule_name, matano_alert_dedupe);

        let default = NewAlertData {
            rule_match_ids: vec![],
            alert_id: None,
            activated: None,
            created_micros: None,
            first_matched_at_micros: None,
            deduplication_window_micros,
            threshold: alert_threshold,
        };

        let entry = new_value_agg.entry(key).or_insert(default);
        entry.rule_match_ids.push(matano_rule_match_id);
    }

    let st11 = std::time::Instant::now();
    // aggregate existing values to get active alerts, and their counts and window starts
    info!("Aggregating existing values...");
    let mut existing_aggregation_map =
        HashMap::<(MaybeString, MaybeString), ExistingAlertsData>::new();
    for (_, existing_values) in existing_values_vecs.iter() {
        for value in existing_values.iter() {
            let matano_rule_match_id = value
                .get_nested("matano.alert.rule.match.id")
                .and_then(|v| v.as_str());

            let matano_alert_rule_name = value
                .get_nested("matano.alert.rule.name")
                .and_then(|v| Some(v.as_str()?.to_string()));
            let matano_alert_dedupe = value
                .get_nested("matano.alert.dedupe")
                .and_then(|v| Some(v.as_str()?.to_string()));

            let matano_alert_id = value.get_nested("matano.alert.id").and_then(|v| v.as_str());
            let matano_alert_first_matched_at = value
                .get_nested("matano.alert.first_matched_at")
                .and_then(|v| v.as_ts());
            let matano_alert_created = value
                .get_nested("matano.alert.created")
                .and_then(|v| v.as_ts());

            let matano_alert_activated = value
                .get_nested("matano.alert.activated")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

            let new_alert_entry =
                new_value_agg.get(&(matano_alert_rule_name.clone(), matano_alert_dedupe.clone()));

            if matano_alert_id.is_some() && new_alert_entry.is_some() {
                let deduplication_window_micros =
                    new_alert_entry.unwrap().deduplication_window_micros;

                if matano_alert_first_matched_at.map_or(false, |ts| {
                    ts + deduplication_window_micros > current_micros
                }) {
                    let alert_id = matano_alert_id.unwrap().to_string();
                    let first_matched_at = matano_alert_first_matched_at.unwrap();

                    let rule_match_id = matano_rule_match_id.unwrap().to_string();
                    let data = ExistingAlertsData {
                        alert_id,
                        rule_match_ids: vec![],
                        first_matched_at,
                        created: matano_alert_created,
                        activated: matano_alert_activated,
                        activated_changed: false,
                    };
                    let key = (matano_alert_rule_name, matano_alert_dedupe);
                    let entry = existing_aggregation_map.entry(key).or_insert(data);
                    entry.rule_match_ids.push(rule_match_id);
                }
            }
        }
    }

    // Compute alert ids and activated status for new values.
    // Also, if an alert was just activated, mark it so we can update the existing values
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
            let did_activate = (existing_count + new_match_count) >= threshold;
            let activated_changed = !entry.activated && did_activate;

            let created_ts = match (entry.created, activated_changed) {
                (Some(ts), _) => Some(ts),
                (None, true) => Some(current_micros),
                (None, false) => None,
            };

            entry.activated_changed = activated_changed;
            new_alert_data.alert_id = Some(alert_id);
            new_alert_data.activated = Some(did_activate);
            new_alert_data.created_micros = created_ts;
            new_alert_data.first_matched_at_micros = Some(first_matched_at);
        } else {
            let new_alert_id = uuid::Uuid::new_v4().to_string();
            let did_activate = new_match_count >= threshold;
            new_alert_data.alert_id = Some(new_alert_id);
            new_alert_data.activated = Some(did_activate);
            if did_activate {
                new_alert_data.created_micros = Some(current_micros);
            }
            new_alert_data.first_matched_at_micros = Some(current_micros);
        }
    }

    let mut alerts_to_deliver = json!({});

    // update the new values based on the alert it was added to
    info!("Updating new values...");
    for new_value in new_values.iter() {
        let matano_alert_rule_name = new_value
            .get_nested("matano.alert.rule.name")
            .and_then(|v| Some(v.as_str()?.to_string()));
        let matano_alert_dedupe = new_value
            .get_nested("matano.alert.dedupe")
            .and_then(|v| Some(v.as_str()?.to_string()));

        let key = (matano_alert_rule_name, matano_alert_dedupe);
        let new_alert_data = new_value_agg.get(&key).unwrap();

        let is_activated = existing_aggregation_map
            .get(&key)
            .map_or(new_alert_data.activated.unwrap_or(false), |e| e.activated);
        let new_value_mut = new_value.as_mut();

        new_value_mut.insert_record_nested(
            "matano.alert.id",
            apache_avro::types::Value::String(new_alert_data.alert_id.as_ref().unwrap().clone())
                .into_union(),
        )?;

        new_value_mut.insert_record_nested(
            "matano.alert.activated",
            apache_avro::types::Value::Boolean(*new_alert_data.activated.as_ref().unwrap())
                .into_union(),
        )?;

        new_value_mut.insert_record_nested(
            "matano.alert.first_matched_at",
            apache_avro::types::Value::TimestampMicros(
                new_alert_data.first_matched_at_micros.unwrap(),
            )
            .into_union(),
        )?;

        let created_av = match new_alert_data.created_micros {
            Some(created_micros) => apache_avro::types::Value::TimestampMicros(created_micros),
            None => apache_avro::types::Value::Null,
        }
        .into_union();
        new_value_mut.insert_record_nested("matano.alert.created", created_av)?;

        if is_activated {
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

    // Update the existing values if an alert was activated just now
    info!("Updating existing values...");
    let mut modified_partitions = HashSet::with_capacity(existing_values_vecs.len());
    for ((_, partition), existing_values) in existing_values_vecs.iter() {
        for value in existing_values.iter() {
            let matano_alert_rule_name = value
                .get_nested("matano.alert.rule.name")
                .and_then(|v| Some(v.as_str()?.to_string()));
            let matano_alert_dedupe = value
                .get_nested("matano.alert.dedupe")
                .and_then(|v| Some(v.as_str()?.to_string()));
            let matano_alert_id = value.get_nested("matano.alert.id").and_then(|v| v.as_str());

            let key = (matano_alert_rule_name, matano_alert_dedupe);
            if let Some(existing_data) = existing_aggregation_map.get(&key) {
                if existing_data.activated_changed
                    && matano_alert_id.map_or(false, |id| id == existing_data.alert_id)
                {
                    value.as_mut().insert_record_nested(
                        "matano.alert.activated",
                        apache_avro::types::Value::Boolean(true).into_union(),
                    )?;
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
    debug!("avro modification time: {:?}", st11.elapsed());

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
    info!("Time to write Parquet: {:?}", st11.elapsed());

    drop(existing_aggregation_map);
    drop(new_value_agg);

    let st = std::time::Instant::now();
    info!("Waiting for Parquet uploads to finish...");
    let written_keys = join_all(upload_join_handles)
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    info!("Parquet uploads finished in: {:?}", st.elapsed());

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
    info!("Completed Iceberg commit in: {:?}", st11.elapsed());

    debug!("Delivering alerts: {:?}", alerts_to_deliver);

    // TODO: reduce intermediate byte vec allocations
    let alerts_to_deliver = alerts_to_deliver
        .as_object()
        .context("alerts_to_deliver is not an object")?
        .values();

    let alerts_len = alerts_to_deliver.len();
    // TODO: could probably avoid this earlier.
    let alerts_to_deliver = alerts_to_deliver.filter(|v| {
        v.as_object()
            .and_then(|o| o.get("rule_matches")?.get("new"))
            .is_some()
    });

    info!(
        "Delivering {} alerts that contained new rule matches...",
        alerts_len
    );
    let alerts_to_deliver_compressed_chunks = encode_alerts_for_publish(alerts_to_deliver)?;

    let sns = SNS_CLIENT.get().await;

    let rule_matches_sns_topic_arn =
        var("RULE_MATCHES_SNS_TOPIC_ARN").context("missing rule matches topic")?;
    let alert_futures = alerts_to_deliver_compressed_chunks.into_iter().map(|s| {
        sns.publish()
            .topic_arn(rule_matches_sns_topic_arn.clone())
            .message_group_id(RULE_MATCHES_GROUP_ID)
            .message(s)
            .send()
    });

    try_join_all(alert_futures).await?;

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
    info!("Time to read files: {:?}", st11.elapsed());
    debug!("{:?}", iceberg_paths);
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

            let stream = obj.body;
            let body = stream.collect().await.unwrap();
            body.into_bytes()
        };
        ret
    });

    let readers = join_all(dl_tasks).await.into_iter();

    let s1 = Instant::now();
    let readers = readers.map(arrow_arrow2_parquet);
    info!("Arrow parquet re serialization took: {:?}", s1.elapsed());

    use arrow2::io::parquet::read;

    let mut avro_record = Box::new(None);

    let all_chunks = readers
        .into_iter()
        .map(|reader| {
            let mut reader = std::io::Cursor::new(reader);
            let metadata = read::read_metadata(&mut reader).unwrap();
            let schema = read::infer_schema(&metadata).unwrap();
            let row_groups = metadata.row_groups;

            let start = std::time::Instant::now();

            let chunks =
                read::FileReader::new(reader, row_groups, schema.clone(), None, None, None)
                    .into_iter()
                    .map(|r| r.unwrap())
                    .collect::<Vec<_>>();
            debug!("Parquet deserialization time: {:?}", start.elapsed());

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

    info!("Time to Load existing values: {:?}", st11.elapsed());
    Ok(ret)
}

async fn call_helper_lambda(payload: String) -> Result<Option<Vec<u8>>> {
    let lambda = LAMBDA_CLIENT.get().await;
    let helper_function_name = std::env::var("ALERT_HELPER_FUNCTION_NAME")?;

    let func_res = lambda
        .invoke()
        .function_name(&helper_function_name)
        .payload(Blob::new(payload))
        .send()
        .await?;

    let resp_payload = func_res.payload().map(|blob| blob.to_owned().into_inner());
    if func_res.function_error().is_some() {
        let err_obj_bytes = resp_payload.unwrap_or_default();
        let err_obj = String::from_utf8_lossy(&err_obj_bytes);
        Err(anyhow!(
            "{} Lambda failed: {}",
            &helper_function_name,
            err_obj
        ))
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

    let func_resp_payload = func_resp.context("Read files Function didn't return payload!")?;
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

#[derive(Debug, Clone)]
struct ExistingAlertsData {
    alert_id: String,
    rule_match_ids: Vec<String>,
    first_matched_at: i64,
    created: Option<i64>,
    activated: bool,
    activated_changed: bool,
}

#[derive(Debug, Clone)]
struct NewAlertData {
    rule_match_ids: Vec<String>,
    alert_id: Option<String>,
    activated: Option<bool>,
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
