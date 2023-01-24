use aws_sdk_inspector2::{
    input::ListFindingsInput,
    model::{DateFilter, FilterCriteria},
};
use std::io::Write;
use std::sync::Arc;
use tokio::sync::Mutex;

use anyhow::{anyhow, Context as AnyhowContext, Error, Result};
use async_trait::async_trait;
use aws_smithy_client::{
    self,
    erase::{DynConnector, DynMiddleware},
};
use aws_smithy_types::DateTime as SmithyDateTime;
use aws_smithy_types_convert::date_time::DateTimeExt;
use chrono::{DateTime, FixedOffset};
use lazy_static::lazy_static;
use log::{debug, error, info};

use super::{PullLogs, PullLogsContext};
use async_once::AsyncOnce;
use shared::JsonValueExt;

#[derive(Clone)]
pub struct AmazonInspectorPuller;

lazy_static! {
    static ref AWS_CONFIG: AsyncOnce<aws_config::SdkConfig> =
        AsyncOnce::new(async { aws_config::load_from_env().await });
    static ref INSPECTOR_RAW_CLIENT: aws_smithy_client::Client = make_inspector_raw_client();
}

fn make_inspector_raw_client() -> aws_smithy_client::Client {
    let middleware = DynMiddleware::new(aws_sdk_inspector2::middleware::DefaultMiddleware::new());
    aws_smithy_client::Client::builder()
        .dyn_https_connector(
            aws_smithy_client::http_connector::ConnectorSettings::builder().build(),
        )
        .middleware::<DynMiddleware<DynConnector>>(middleware)
        .build()
}

#[async_trait]
impl PullLogs for AmazonInspectorPuller {
    async fn pull_logs(
        self,
        client: reqwest::Client,
        ctx: &PullLogsContext,
        start_dt: DateTime<FixedOffset>,
        end_dt: DateTime<FixedOffset>,
    ) -> Result<Vec<u8>> {
        info!("Pulling Amazon Inspector logs....");

        let raw_client = &INSPECTOR_RAW_CLIENT;
        let sdk_conf = AWS_CONFIG.get().await;
        let client_conf = aws_sdk_inspector2::config::Config::new(sdk_conf);

        let start_dt = if ctx.is_initial_run() {
            info!("Initial run for Amazon Inspector.");
            end_dt - chrono::Duration::days(1)
        } else {
            start_dt
        };

        let date_filter = DateFilter::builder()
            .start_inclusive(SmithyDateTime::from_chrono_fixed(start_dt))
            .end_inclusive(SmithyDateTime::from_chrono_fixed(end_dt))
            .build();
        let filter = FilterCriteria::builder()
            .last_observed_at(date_filter)
            .build();

        let mut next_token: Option<String> = None;
        let mut is_first = true;

        let mut all_findings = vec![];

        while is_first || next_token.is_some() {
            let input = ListFindingsInput::builder()
                .filter_criteria(filter.clone())
                .set_next_token(next_token.clone())
                .build()?;
            let op = input.make_operation(&client_conf).await?;
            let resp = raw_client.call_raw(op).await?;
            let (raw_resp, _) = resp.raw.into_parts();
            let raw_body = raw_resp.body();

            let body_val: serde_json::Value = serde_json::from_slice(raw_body.bytes().unwrap())?;
            let mut body_val = body_val.into_object().context("Must be object")?;

            let findings = body_val
                .remove("findings")
                .and_then(|v| v.into_array())
                .unwrap_or_default();

            all_findings.extend(findings);

            next_token = body_val.remove("nextToken").and_then(|v| v.into_str());
            is_first = false;

            debug!("Loaded page for Amazon Inspector");
        }

        info!(
            "Loaded {} findings for Amazon Inspector",
            all_findings.len()
        );
        // Could be concurrent with IO...
        let mut json_bytes = vec![];
        for finding in all_findings {
            json_bytes.write(serde_json::to_vec(&finding)?.as_slice())?;
            json_bytes.write(b"\n")?;
        }

        Ok(json_bytes)
    }
}
