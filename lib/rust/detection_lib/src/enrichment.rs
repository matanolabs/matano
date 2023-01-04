use anyhow::{anyhow, Context, Result};
use apache_avro::from_value;
use async_once::AsyncOnce;
use aws_config::SdkConfig;
use futures::join;
use lazy_static::lazy_static;
use once_cell::sync::OnceCell;
use pyo3::exceptions::PyRuntimeError;
use pythonize::pythonize;

use async_compression::tokio::bufread::ZstdDecoder;
use pyo3::prelude::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::runtime::Runtime;

use crate::avro_index::AvroIndex;

lazy_static! {
    static ref RT: Runtime = Runtime::new().unwrap();
}

lazy_static! {
    static ref AWS_CONFIG: AsyncOnce<SdkConfig> =
        AsyncOnce::new(async { aws_config::load_from_env().await });
    static ref S3_CLIENT: AsyncOnce<aws_sdk_s3::Client> =
        AsyncOnce::new(async { aws_sdk_s3::Client::new(AWS_CONFIG.get().await) });
}

#[pyclass]
pub struct EnrichmentTable {
    #[pyo3(get)]
    name: String,
    primary_key: String,
    avro_index: OnceCell<AvroIndex>,
}

impl EnrichmentTable {
    fn new(name: String, primary_key: String) -> Self {
        EnrichmentTable {
            name,
            primary_key,
            avro_index: OnceCell::new(),
        }
    }

    fn avro_index(&self) -> Result<&AvroIndex> {
        self.avro_index
            .get_or_try_init(|| RT.block_on(load_avro_index(&self.name, &self.primary_key)))
    }

    fn get_by_key_internal(&self, key: &str) -> Result<Option<serde_json::Value>> {
        match self.avro_index()?.get_by_key(key) {
            Ok(Some(v)) => Ok(Some(from_value::<serde_json::Value>(&v)?)),
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

#[pymethods]
impl EnrichmentTable {
    pub fn get(&self, key: &str) -> PyResult<Py<PyAny>> {
        let val = self.get_by_key_internal(key)?;
        Python::with_gil(|py| pythonize(py, &val))
            .map_err(|e| PyRuntimeError::new_err(format!("Error: {}", e)))
    }
}

#[pyfunction]
pub fn create_enrichment_tables(
    configs: Vec<(String, Option<String>)>,
) -> PyResult<Vec<EnrichmentTable>> {
    let tables = configs
        .into_iter()
        .filter_map(|(n, pk)| Some((n, pk?)))
        .map(|(table_name, primary_key)| EnrichmentTable::new(table_name, primary_key))
        .collect::<Vec<_>>();
    Ok(tables)
}

async fn load_avro_index(table_name: &str, primary_key: &str) -> Result<AvroIndex> {
    let s3 = S3_CLIENT.get().await;
    let bucket = std::env::var("ENRICHMENT_TABLES_BUCKET")?;
    let avro_key = format!("tables/{}.avro", table_name);
    let index_key = format!("tables/{}_index.json.zst", table_name);
    let local_dir = std::env::temp_dir().join("tables");
    let local_avro_path = local_dir.join(format!("{}.zstd.avro", table_name));

    let load_avro = || async {
        let avro_reader = s3
            .get_object()
            .bucket(&bucket)
            .key(&avro_key)
            .send()
            .await?
            .body
            .into_async_read();
        let mut avro_reader = tokio::io::BufReader::new(avro_reader);

        tokio::fs::create_dir_all(local_dir).await?;

        let file = tokio::fs::File::create(&local_avro_path).await?;
        let mut writer = tokio::io::BufWriter::new(file);

        tokio::io::copy(&mut avro_reader, &mut writer).await?;
        anyhow::Ok(Vec::<u8>::with_capacity(0)) // just to align types
    };

    let load_index = || async {
        let index_reader = s3
            .get_object()
            .bucket(&bucket)
            .key(&index_key)
            .send()
            .await?
            .body
            .into_async_read();
        let mut index_reader = ZstdDecoder::new(tokio::io::BufReader::new(index_reader));

        let mut index_bytes = vec![];
        index_reader.read_to_end(&mut index_bytes).await?;
        anyhow::Ok(index_bytes)
    };

    let (index_bytes, _) = join!(load_index(), load_avro());

    let local_avro_path = local_avro_path.to_str().context("Invalid path")?;
    let avro_index = AvroIndex::new(index_bytes?.as_slice(), primary_key, local_avro_path)?;

    Ok(avro_index)
}
