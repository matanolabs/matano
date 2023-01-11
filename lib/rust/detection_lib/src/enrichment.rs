use std::collections::HashMap;

use anyhow::{anyhow, Context, Result};
use apache_avro::from_value;
use async_once::AsyncOnce;
use aws_config::SdkConfig;
use futures::future::join_all;
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
use shared::utils::load_enrichment_config;

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
    lookup_keys: Vec<String>,
    avro_index: OnceCell<AvroIndex>,
}

impl EnrichmentTable {
    fn new(name: String, lookup_keys: Vec<String>) -> Self {
        EnrichmentTable {
            name,
            lookup_keys,
            avro_index: OnceCell::new(),
        }
    }

    fn avro_index(&self) -> Result<&AvroIndex> {
        self.avro_index
            .get_or_try_init(|| RT.block_on(load_avro_index(&self.name, &self.lookup_keys)))
    }

    fn get_by_key_internal(
        &self,
        key: &str,
        index_key: Option<&str>,
    ) -> Result<Option<serde_json::Value>> {
        match self.avro_index()?.get_by_key(key, index_key) {
            Ok(Some(v)) => Ok(Some(from_value::<serde_json::Value>(&v)?)),
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

#[pymethods]
impl EnrichmentTable {
    #[args(index_key = "None")]
    pub fn get(&self, key: &str, index_key: Option<&str>) -> PyResult<Py<PyAny>> {
        let val = self.get_by_key_internal(key, index_key)?;
        Python::with_gil(|py| pythonize(py, &val))
            .map_err(|e| PyRuntimeError::new_err(format!("Error: {}", e)))
    }
}

#[pyfunction]
pub fn create_enrichment_tables(table_names: Vec<String>) -> PyResult<Vec<EnrichmentTable>> {
    let tables = table_names
        .into_iter()
        .filter_map(|table_name| {
            // Skip tables with no lookup keys
            let lookup_keys = get_enrichment_table_lookup_keys(&table_name);
            lookup_keys.map(|lks| EnrichmentTable::new(table_name, lks))
        })
        .collect::<Vec<_>>();
    Ok(tables)
}

fn get_enrichment_table_lookup_keys(table_name: &str) -> Option<Vec<String>> {
    lazy_static! {
        static ref ENRICHMENT_CONFIG: HashMap<String, serde_yaml::Value> =
            load_enrichment_config().unwrap();
    }
    ENRICHMENT_CONFIG
        .get(table_name)
        .and_then(|v| v.get("lookup_keys"))
        .and_then(|v| v.as_sequence())
        .map(|v| {
            v.into_iter()
                .map(|v| v.as_str().unwrap().to_string())
                .collect::<Vec<_>>()
        })
}

async fn load_avro_index(table_name: &str, lookup_keys: &Vec<String>) -> Result<AvroIndex> {
    let s3 = S3_CLIENT.get().await;
    let bucket = std::env::var("ENRICHMENT_TABLES_BUCKET")?;
    let avro_key = format!("tables/{}.avro", table_name);
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

        tokio::fs::create_dir_all(&local_dir).await?;

        let file = tokio::fs::File::create(&local_avro_path).await?;
        let mut writer = tokio::io::BufWriter::new(file);

        tokio::io::copy(&mut avro_reader, &mut writer).await?;
        anyhow::Ok(HashMap::<String, String>::with_capacity(0)) // just to align types
    };

    let load_indices = || async {
        let load_futs = lookup_keys
            .iter()
            .map(|lookup_key| {
                let index_s3_key = format!("tables/{}_index_{}.json.zst", table_name, &lookup_key);
                let local_index_path = local_dir
                    .join(format!("{}_index_{}.json", table_name, &lookup_key))
                    .to_str()
                    .unwrap()
                    .to_string();
                async {
                    let index_reader = s3
                        .get_object()
                        .bucket(&bucket)
                        .key(index_s3_key)
                        .send()
                        .await?
                        .body
                        .into_async_read();
                    let mut index_reader =
                        ZstdDecoder::new(tokio::io::BufReader::new(index_reader));

                    let file = tokio::fs::File::create(&local_index_path).await?;
                    let mut writer = tokio::io::BufWriter::new(file);

                    tokio::io::copy(&mut index_reader, &mut writer).await?;
                    anyhow::Ok((lookup_key.to_string(), local_index_path))
                }
            })
            .collect::<Vec<_>>();
        let results = join_all(load_futs)
            .await
            .into_iter()
            .collect::<Result<HashMap<String, String>>>()?;

        anyhow::Ok(results)
    };

    let (indices_paths, _) = join!(load_indices(), load_avro());

    let local_avro_path = local_avro_path.to_str().context("Invalid path")?;

    AvroIndex::new(indices_paths?, local_avro_path)
}
