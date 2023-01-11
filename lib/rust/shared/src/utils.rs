//! Shared utilities
//!
use anyhow::{anyhow, Context, Result};
use config::{Config, File};
use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap},
    env::var,
    path::Path,
};
use tracing::log::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;
use walkdir::WalkDir;

pub fn setup_logging() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        // Setup from the environment (RUST_LOG)
        .with_env_filter(EnvFilter::from_default_env())
        // this needs to be set to false, otherwise ANSI color codes will
        // show up in a confusing manner in CloudWatch logs.
        .with_ansi(false)
        // disabling time is handy because CloudWatch will add the ingestion time.
        .without_time()
        .init();
}

thread_local! {
    pub static LOG_SOURCES_CONFIG: RefCell<BTreeMap<String, crate::LogSourceConfiguration>> = {
        let log_sources_configuration_map = load_log_sources_configuration_map();
        RefCell::new(log_sources_configuration_map)
    };
}

pub fn load_log_sources_configuration_map(
) -> BTreeMap<String, crate::models::LogSourceConfiguration> {
    let log_sources_configuration_path =
        Path::new(&var("LAMBDA_TASK_ROOT").unwrap().to_string()).join("log_sources");
    let mut log_sources_configuration_map: BTreeMap<String, crate::models::LogSourceConfiguration> =
        BTreeMap::new();

    for entry in WalkDir::new(log_sources_configuration_path)
        .min_depth(1)
        .max_depth(1)
    {
        let log_source_dir_path = match entry {
            Ok(v) => v,
            Err(e) => {
                error!(
                    "Invalid entry while walking children for log_sources/ directory: {}",
                    e
                );
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
        let log_source_configuration_path =
            log_source_configuration_path.as_path().to_str().unwrap();
        let base_configuration_builder = Config::builder()
            .add_source(File::with_name(log_source_configuration_path).required(true));
        let base_configuration = base_configuration_builder.build().expect(
            format!(
                "Failed to load base configuration for log_source: {}/",
                log_source_folder_name
            )
            .as_str(),
        );

        let mut log_source_configuration = crate::models::LogSourceConfiguration {
            base: base_configuration,
            tables: HashMap::new(),
        };
        let log_source_name = log_source_configuration.base.get_string("name").unwrap();

        let tables_path = log_source_dir_path.join("tables");
        if tables_path.is_dir() {
            for entry in WalkDir::new(&tables_path).min_depth(1).max_depth(1) {
                let tables_path = &tables_path.as_path().to_str().unwrap();

                let table_configuration_path = entry.expect(
                    format!(
                        "Invalid entry while walking children for {} directory.",
                        &tables_path
                    )
                    .as_str(),
                );
                let table_configuration_path = table_configuration_path.path();

                let err_str = format!(
                    "Invalid table entry: {}.",
                    table_configuration_path.display()
                );
                let table_file_name = table_configuration_path
                    .file_name()
                    .expect(&err_str)
                    .to_str()
                    .expect(&err_str);

                let extension = table_configuration_path
                    .extension()
                    .and_then(std::ffi::OsStr::to_str);

                if !table_configuration_path.is_file()
                    || !(extension == Some("yml") || extension == Some("yaml"))
                {
                    continue;
                }

                let table_configuration_path = table_configuration_path.to_str().unwrap();

                let table_configuration = Config::builder()
                    .add_source(File::with_name(table_configuration_path).required(true))
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
                log_source_configuration
                    .tables
                    .insert(table_name, table_configuration);
            }
        }
        log_sources_configuration_map.insert(log_source_name, log_source_configuration);
    }
    log_sources_configuration_map
}

pub fn load_enrichment_config() -> Result<HashMap<String, serde_yaml::Value>> {
    let config_path = "/opt/config/enrichment";
    let ret = WalkDir::new(config_path)
        .min_depth(2)
        .max_depth(2)
        .into_iter()
        .flatten()
        .filter_map(|p| {
            (p.file_name() == "enrichment.yml").then_some(p.path().to_str()?.to_string())
        })
        .map(|p| {
            let file = std::io::BufReader::new(std::fs::File::open(&p)?);
            let config: serde_yaml::Value = serde_yaml::from_reader(file)?;
            let name = config
                .get("name")
                .context("Need name in enrichment!")?
                .as_str()
                .context("Need name in enrichment!")?
                .to_string();
            anyhow::Ok((name, config))
        })
        .collect::<Result<HashMap<String, serde_yaml::Value>>>();
    ret
}

pub trait JsonValueExt {
    fn into_array(self) -> Option<Vec<serde_json::Value>>;
    fn into_object(self) -> Option<serde_json::Map<String, serde_json::Value>>;
    fn into_str(self) -> Option<String>;
}

impl JsonValueExt for serde_json::Value {
    fn into_array(self) -> Option<Vec<serde_json::Value>> {
        match self {
            serde_json::Value::Array(arr) => Some(arr),
            _ => None,
        }
    }

    fn into_object(self) -> Option<serde_json::Map<String, serde_json::Value>> {
        match self {
            serde_json::Value::Object(map) => Some(map),
            _ => None,
        }
    }

    fn into_str(self) -> Option<String> {
        match self {
            serde_json::Value::String(s) => Some(s),
            _ => None,
        }
    }
}

pub fn convert_json_array_str_to_ndjson(s: &str) -> Result<String> {
    let deserialized: Vec<Box<serde_json::value::RawValue>> = serde_json::from_str(s)?;

    let length = deserialized.len();
    let res = deserialized
        .into_iter()
        .enumerate()
        .fold(String::new(), |mut acc, (i, v)| {
            acc.push_str(v.get());
            if i != length - 1 {
                acc.push_str("\n");
            }
            acc
        });

    Ok(res)
}
