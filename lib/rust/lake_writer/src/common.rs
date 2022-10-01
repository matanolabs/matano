use arrow::record_batch::RecordBatchReader;
use aws_sdk_s3::types::ByteStream;
use uuid::Uuid;
use std::{time::Instant, vec};
use log::{error, info};
use anyhow::{anyhow, Result};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

pub async fn write_arrow_to_s3_parquet(
    s3: &aws_sdk_s3::Client,
    table_name: String,
    partition_day: String,
    field: arrow2::datatypes::Field,
    arrays: Vec<Box<dyn arrow2::array::Array>>,
) -> Result<String> {
    let iter = Box::new(arrays.clone().into_iter().map(Ok)) as _;

    let mut arrow_array_stream = Box::new(arrow2::ffi::ArrowArrayStream::empty());

    *arrow_array_stream = arrow2::ffi::export_iterator(iter, field.clone());

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

    let bytes = writer.into_inner()?.into_inner();

    let bytestream = ByteStream::from(bytes);
    let bucket = std::env::var("OUT_BUCKET_NAME")?;
    let key_prefix = std::env::var("OUT_KEY_PREFIX")?;

    // lake/TABLE_NAME/data/ts_day=2022-07-05/<file>.parquet
    let key = format!(
        "{}/{}/data/ts_day={}/{}.parquet",
        key_prefix,
        table_name,
        partition_day,
        Uuid::new_v4()
    );
    println!("Writing to: {}", key);

    println!("Starting upload...");
    let ws1 = Instant::now();
    let _upload_res = &s3
        .put_object()
        .bucket(bucket)
        .key(key.clone())
        .body(bytestream)
        .send()
        .await?;
    println!("Upload took: {:.2?}", ws1.elapsed());
    // (drop/release)
    unsafe {
        Box::from_raw(stream_ptr);
    };

    Ok(key)
}
