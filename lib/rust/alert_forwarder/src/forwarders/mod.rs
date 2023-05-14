use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use enum_dispatch::enum_dispatch;

mod ses;
mod slack;
use slack::SlackForwarder;

#[async_trait]
#[enum_dispatch]
pub trait SendAlert {
    async fn send_alert(
        self,
        alert_payload: crate::AlertCDCPayload,
        destination_name: String,
        config: serde_json::Value,
        destination_info: Option<serde_json::Value>,
    ) -> Result<serde_json::Value>;
}

#[derive(Clone)]
#[enum_dispatch(SendAlert)]
pub enum AlertForwarder {
    Slack(SlackForwarder),
    SesEmail(ses::SesEmailForwarder),
}

impl AlertForwarder {
    pub fn from_str(s: &str) -> Option<AlertForwarder> {
        match s.to_lowercase().as_str() {
            "slack" => Some(AlertForwarder::Slack(SlackForwarder {})),
            "ses" => Some(AlertForwarder::SesEmail(ses::SesEmailForwarder {})),
            _ => None,
        }
    }
}
