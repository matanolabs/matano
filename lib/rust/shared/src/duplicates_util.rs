use std::time::SystemTime;

use anyhow::Result;
use aws_sdk_dynamodb::{operation::update_item::UpdateItemError, types::AttributeValue, Client};

const DDB_DUPLICATES_ITEM_EXPIRE_SECONDS: u64 = 1 * 24 * 60 * 60;

pub async fn check_ddb_duplicate(
    ddb: Client,
    duplicates_table: &str,
    id: String,
) -> Result<bool, UpdateItemError> {
    let expire_time = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + DDB_DUPLICATES_ITEM_EXPIRE_SECONDS;

    let res = ddb
        .update_item()
        .table_name(duplicates_table)
        .key("pk", AttributeValue::S(id.clone()))
        .update_expression("SET #ttl = :ttl, completed = :completed")
        .expression_attribute_names("#ttl", "ttl")
        .expression_attribute_values(":ttl", AttributeValue::N(expire_time.to_string()))
        .expression_attribute_values(":completed", AttributeValue::Bool(false))
        .condition_expression("attribute_not_exists(pk) OR completed = :false")
        .expression_attribute_values(":false", AttributeValue::Bool(false))
        .send()
        .await;
    match res {
        Ok(_) => Ok(false),
        Err(e) => {
            let se = e.into_service_error();
            match se {
                UpdateItemError::ConditionalCheckFailedException(_) => Ok(true),
                _ => Err(se),
            }
        }
    }
}

pub async fn mark_ddb_duplicate_completed(
    ddb: Client,
    duplicates_table: &str,
    id: String,
) -> Result<()> {
    ddb.update_item()
        .table_name(duplicates_table)
        .key("pk", AttributeValue::S(id))
        .update_expression("SET completed = :true")
        .condition_expression("attribute_exists(pk)")
        .expression_attribute_values(":true", AttributeValue::Bool(true))
        .send()
        .await?;
    Ok(())
}
