use crate::{AlertCDCPayload, Alert};
use aws_sdk_ses::{Client, model::{Destination, Content, Message, Body}};
use build_html::HtmlContainer;
use ::value::Value;
use anyhow::{Error, Ok, Result};
use log::{debug, error, info};
use serde_json::json;
use shared::vrl_util::vrl;
use build_html::*;

const CONTEXT_TO_STR_FMT: &str = r#"
key_to_label = {
    "matano.table": "Log Source",
    "related.ip": "IP",
    "related.user": "User",
    "related.hosts": "Host",
    "related.hash": "Hash"
}

context = flatten(.)
ret = {}

for_each(context) -> |k, v| {
    label = get(key_to_label, [k]) ?? null
    values = array!(v)
    value_str_prefix = if label != null { label } else { k }

    value_str_prefix, err = "<b>" + to_string(value_str_prefix) + ":</b> "
    vals, err = map_values(values) -> |v| {
        to_string(v)
    }
    more_count_short = length(vals) - 5
    values_short = slice!(vals, 0, 5)
    value_short_str = value_str_prefix + join!(values_short, "  ")
    if more_count_short > 0 {
        value_short_str = value_short_str + " +" + to_string(more_count_short) + " more..."
    }

    more_count_long = length(vals) - 25
    values_long = slice!(vals, 0, 25)
    value_long_str = value_str_prefix + join!(values_long, "  ")
    if more_count_long > 0 {
        value_long_str = value_long_str + " +" + to_string(more_count_long) + " more..."
    }

    k_parts = split(k, ".")
    k_parts = map_values(k_parts) -> |value| { 
        if is_float(value) {
            to_string(value)
        } else {
            value
        } 
    }

    ret.long_fmt = set!(object!(ret.long_fmt || {}), k_parts, value_long_str)
    ret.short_fmt = set!(object!(ret.short_fmt || {}), k_parts, value_short_str)
}

ret
"#;

async fn send_message(
    client: &Client,
    from: &str,
    to: Option<Vec<String>>,
    subject: &String,
    html_message: String,
) -> Result<serde_json::Value, Error> {
    let dest = Destination::builder().set_to_addresses(to).build();
    let subject_content = Content::builder().data(subject).charset("UTF-8").build();
    let body_content = Content::builder().data(html_message).charset("UTF-8").build();
    let body: Body = Body::builder().set_html(Some(body_content)).build();

    let msg = Message::builder()
        .subject(subject_content)
        .body(body)
        .build();

    client
        .send_email()
        .set_source(Some(from.to_string()))
        .set_destination(Some(dest))
        .set_message(Some(msg))
        .send()
        .await?;

    info!("Email sent.");

    Ok(json!({"code": 204}))
}

// Main SES alert publishing function
pub async fn publish_alert_to_ses(
    alert_payload: &mut AlertCDCPayload,
    client: &Client,
    from: &str,
    to: &Vec<serde_json::Value>,
) -> Result<serde_json::Value> {

    let mut res = json!(null);

    let alert = &alert_payload.updated_alert;

    // Only send an alert if one has not already been sent to minimize email noise
    if alert.update_count == 0 {

        let alert_title = &alert.title;
        let alert_runbook = &alert.runbook;
        let alert_creation_time_str = format!(
            "{}",
            alert.creation_time.to_rfc3339()
        );

        let recipients: Vec<String> = to.iter()
        .filter_map(|c| c.as_str())
        .map(|s| s.to_owned())
        .collect();

        let mut context_fmt = vrl(
            CONTEXT_TO_STR_FMT,
            &mut alert_payload.incoming_rule_matches_context,
        )?
        .0;

        let related_strs = vrl(".short_fmt.related", &mut context_fmt)?.0;
        let related_strs = match related_strs {
            Value::Object(related_strs) => related_strs
                .into_values()
                .map(|v| v.as_str().unwrap().to_string())
                .collect::<Vec<String>>(),
            _ => vec![],
        };

        // Build HTML email template
        let html_message = build_html::HtmlPage::new()
        .with_style(r#"td{padding:5px;}th{padding:5px;background:#86B4CE;}h1{color:#1C3149;}h2{color:#1C3149;}"#)
        .with_header(1, alert_title)
        .with_table(Table::from(&[
            [alert_creation_time_str, alert.tables.join(",")]
        ])
        .with_header_row(&["Created", "Log Source"]))
        .with_paragraph(alert_runbook)
        .with_header(2, "<br />Context")
        .with_paragraph(related_strs.join(" <br />"))
        .to_html_string();

        res = send_message(
            client, 
            from, 
            Some(recipients), 
            &alert.title, 
            html_message
        ).await?;

    }

    Ok(res)
}
