use aws_lambda_events::sqs::SqsEvent;
use aws_sdk_dynamodb::types::AttributeValue;
use serde_dynamo::aws_sdk_dynamodb_0_25::from_item;
use serde_json::json;
use std::collections::HashMap;

use anyhow::{anyhow, Context, Result};
use async_once::AsyncOnce;
use lambda_runtime::{
    run, service_fn, Context as LambdaContext, Error as LambdaError, LambdaEvent,
};
use lazy_static::lazy_static;
use log::{debug, error, info};
use shared::alert_util::*;
use shared::secrets::load_secret;
use shared::setup_logging;

mod forwarders;
use forwarders::AlertForwarder;

use crate::forwarders::SendAlert;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

lazy_static! {
    static ref AWS_CONFIG: AsyncOnce<aws_config::SdkConfig> =
        AsyncOnce::new(async { aws_config::load_from_env().await });
    static ref SQS_CLIENT: AsyncOnce<aws_sdk_sqs::Client> =
        AsyncOnce::new(async { aws_sdk_sqs::Client::new(AWS_CONFIG.get().await) });
    static ref SES_CLIENT: AsyncOnce<aws_sdk_ses::Client> =
        AsyncOnce::new(async { aws_sdk_ses::Client::new(AWS_CONFIG.get().await) });
    static ref DYNAMODB_CLIENT: AsyncOnce<aws_sdk_dynamodb::Client> =
        AsyncOnce::new(async { aws_sdk_dynamodb::Client::new(AWS_CONFIG.get().await) });
    static ref DESTINATION_TO_CONFIGURATION_MAP: HashMap<String, serde_json::Value> = {
        let dest_config_map_str = std::env::var("DESTINATION_TO_CONFIGURATION_MAP").unwrap();
        let dest_config_map: HashMap<String, serde_json::Value> =
            serde_json::from_str(&dest_config_map_str).unwrap();
        dest_config_map
    };
    static ref DESTINATION_TO_SECRET_ARN_MAP: HashMap<String, String> = {
        let dest_secret_arn_map_str = std::env::var("DESTINATION_TO_SECRET_ARN_MAP").unwrap();
        let dest_secret_arn_map: HashMap<String, String> =
            serde_json::from_str(&dest_secret_arn_map_str).unwrap();
        dest_secret_arn_map
    };
}

#[tokio::main]
async fn main() -> Result<(), LambdaError> {
    setup_logging();

    let func = service_fn(handler);
    run(func).await?;

    core::result::Result::Ok(())
}

#[derive(Debug, Clone)]
struct DestinationConfig {
    pub name: String,
    pub destination_type: String,
    pub config: serde_json::Value,
}

async fn get_secret_for_destination(destination_name: &str) -> Result<HashMap<String, String>> {
    let secret_arn = DESTINATION_TO_SECRET_ARN_MAP
        .get(destination_name)
        .context("Destination name not found")?
        .to_owned();
    let secret = load_secret(secret_arn).await;
    secret
}

async fn write_destination_info(
    alert_id: &str,
    destination: &str,
    destination_info: &str,
) -> Result<()> {
    let dynamodb = DYNAMODB_CLIENT.get().await;
    let alert_tracker_table_name = std::env::var("ALERT_TRACKER_TABLE_NAME")?;

    dynamodb
        .update_item()
        .table_name(&alert_tracker_table_name)
        .key("id", AttributeValue::S(alert_id.to_string()))
        .update_expression("SET #dest_alert_info.#dest_name = :dest_info")
        .expression_attribute_names("#dest_alert_info", "destination_alert_info")
        .expression_attribute_names("#dest_name", destination)
        .expression_attribute_values(
            ":dest_info",
            AttributeValue::S(destination_info.to_string()),
        )
        .send()
        .await?;
    Ok(())
}

async fn get_existing_destination_alert_info(alert_id: &str) -> Result<HashMap<String, String>> {
    let dynamodb = DYNAMODB_CLIENT.get().await;
    let alert_tracker_table_name = std::env::var("ALERT_TRACKER_TABLE_NAME")?;
    let alert_item = dynamodb
        .get_item()
        .consistent_read(true)
        .table_name(&alert_tracker_table_name)
        .key("id", AttributeValue::S(alert_id.to_string()))
        .send()
        .await?
        .item;

    let info = alert_item
        .and_then(|item| match item.get("destination_alert_info") {
            Some(AttributeValue::M(m)) => Some(m.clone()),
            _ => None,
        })
        .map(|m| from_item::<HashMap<String, String>>(m))
        .transpose()?
        .unwrap_or_default();

    Ok(info)
}

async fn process_payload(alert_cdc_payload: InternalAlertCDCPayload) -> Result<()> {
    let updated_alert = alert_cdc_payload.payload.updated_alert.clone();

    let destination = alert_cdc_payload.destination;
    let destination_alert_info = get_existing_destination_alert_info(&updated_alert.id).await?;
    let destination_info = destination_alert_info
        .get(&destination)
        .map(|s| serde_json::from_str::<serde_json::Value>(s))
        .transpose()?;

    let dest_config = DESTINATION_TO_CONFIGURATION_MAP
        .get(&destination)
        .and_then(|config| {
            Some(DestinationConfig {
                destination_type: config["type"].as_str()?.to_string(),
                name: config["name"].as_str()?.to_string(),
                config: config.clone(),
            })
        })
        .context("Destination not found")?;

    let forwarder =
        AlertForwarder::from_str(&dest_config.destination_type).context("Missing forwarder")?;

    let res = forwarder
        .send_alert(
            alert_cdc_payload.payload.clone(),
            dest_config.name.clone(),
            dest_config.config.clone(),
            destination_info.clone(),
        )
        .await?;

    let new_dest_info = serialize_json_str(&res)?;
    write_destination_info(&updated_alert.id, &destination, &new_dest_info).await?;

    Ok(())
}

async fn handler(event: LambdaEvent<SqsEvent>) -> Result<()> {
    let record = event.payload.records.get(0).context("Need body")?;
    let body = record.body.clone().context("Need body")?;

    let payload: InternalAlertCDCPayload =
        serde_json::from_str(&body).context("Message has an invalid format")?;
    let alert_id = payload.payload.updated_alert.id.clone();
    let destination = payload.destination.clone();

    info!(
        "Processing alert id: {} for destination: {}",
        payload.payload.updated_alert.id, payload.destination
    );

    let result = process_payload(payload).await.map_err(|e| {
        error!("Error: {:#}", e);
        e
    });

    let log = json!({
        "type": "matano_service_log",
        "service": "alert_forwarder",
        "alert_id": alert_id,
        "destination": destination,
        "error": result.is_err(),
    });
    info!("{}", serde_json::to_string(&log).unwrap_or_default());

    result
}

fn serialize_json_str(v: &serde_json::Value) -> Result<String, serde_json::Error> {
    v.as_str()
        .map(|s| Ok(s.to_string()))
        .unwrap_or_else(|| serde_json::to_string(v))
}
