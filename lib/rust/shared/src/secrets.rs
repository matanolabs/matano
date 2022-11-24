use std::collections::HashMap;

use anyhow::{anyhow, Result};
use async_once::AsyncOnce;
use cached::proc_macro::cached;
use lazy_static::lazy_static;

use aws_config::SdkConfig;

lazy_static! {
    static ref AWS_CONFIG: AsyncOnce<SdkConfig> =
        AsyncOnce::new(async { aws_config::load_from_env().await });
    static ref SECRETS_CLIENT: AsyncOnce<aws_sdk_secretsmanager::Client> =
        AsyncOnce::new(async { aws_sdk_secretsmanager::Client::new(AWS_CONFIG.get().await) });
}

#[cached(time = 60, result = true)]
pub async fn load_secret(secret_id: String) -> Result<HashMap<String, String>> {
    let client = SECRETS_CLIENT.get().await;
    let response = client
        .get_secret_value()
        .secret_id(secret_id)
        .send()
        .await?;
    let secret_string = response.secret_string().unwrap().to_owned();
    let secret: HashMap<String, String> = serde_json::from_str(&secret_string)?;

    Ok(secret)
}
