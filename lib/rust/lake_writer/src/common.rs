use anyhow::{anyhow, Result};
use arrow::record_batch::RecordBatchReader;
use arrow2::chunk::Chunk;
use aws_sdk_s3::types::ByteStream;
use log::{error, info};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::{path::Path, time::Instant, vec};
use uuid::Uuid;

pub fn struct_wrap_arrow2_for_ffi(
    schema: &arrow2::datatypes::Schema,
    chunks: Vec<Chunk<Box<dyn arrow2::array::Array>>>,
) -> (arrow2::datatypes::Field, Vec<Box<dyn arrow2::array::Array>>) {
    let field = arrow2::datatypes::Field::new(
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
    (field, arrays)
}

pub fn serialize_arrow_parquet(
    field: arrow2::datatypes::Field,
    arrays: Vec<Box<dyn arrow2::array::Array>>,
) -> Result<Vec<u8>> {
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
    // (drop/release)
    unsafe {
        Box::from_raw(stream_ptr);
    };

    Ok(bytes)
}

pub async fn write_arrow_to_s3_parquet(
    s3: aws_sdk_s3::Client,
    table_name: String,
    partition_hour: String,
    field: arrow2::datatypes::Field,
    arrays: Vec<Box<dyn arrow2::array::Array>>,
) -> Result<(String, String, usize)> {
    let bytes = serialize_arrow_parquet(field, arrays)?;
    let file_length = bytes.len();

    let bytestream = ByteStream::from(bytes);
    let bucket = std::env::var("OUT_BUCKET_NAME")?;
    let key_prefix = std::env::var("OUT_KEY_PREFIX")?;

    // lake/TABLE_NAME/data/ts_hour=2022-07-05-00/partition_hour=2022-07-05-00/<file>.parquet
    let key = format!(
        "{}/{}/data/ts_hour={}/partition_hour={}/{}_mtn_append.zstd.parquet",
        key_prefix,
        table_name,
        partition_hour,
        partition_hour,
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

    Ok((partition_hour, key, file_length))
}

pub fn load_table_arrow_schema(table_name: &str) -> Result<arrow2::datatypes::Schema> {
    let schema_path = Path::new("/opt/schemas")
        .join(table_name)
        .join("metadata.parquet");
    let mut schema_file = std::fs::File::open(schema_path)?;
    let meta = arrow2::io::parquet::read::read_metadata(&mut schema_file)?;
    let schema = arrow2::io::parquet::read::infer_schema(&meta)?;
    Ok(schema)
}
