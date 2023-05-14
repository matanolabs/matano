use anyhow::anyhow;
use base64::encode;
use chrono::{DateTime, Utc};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, io::Write, mem};
use value::Value;

pub struct ByteArrayChunker<I: Iterator> {
    iter: I,
    chunk: Vec<I::Item>,
    max_total_size: usize,
    total_size: usize,
}

impl<I> Iterator for ByteArrayChunker<I>
where
    I: Iterator,
    I::Item: AsRef<Vec<u8>>,
{
    type Item = Vec<I::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.iter.next() {
                Some(item) => {
                    let item_size = item.as_ref().len();
                    let mut ret: Option<Self::Item> = None;
                    if self.total_size + item_size > self.max_total_size {
                        self.total_size = 0;
                        ret = Some(mem::take(&mut self.chunk));
                    }
                    self.chunk.push(item);
                    self.total_size += item_size;

                    if let Some(_) = ret {
                        return ret;
                    }
                }
                None => {
                    return if self.chunk.is_empty() {
                        None
                    } else {
                        Some(mem::take(&mut self.chunk))
                    }
                }
            }
        }
    }
}

pub trait ChunkExt: Iterator + Sized {
    fn chunks_total_size(self, max_total_size: usize) -> ByteArrayChunker<Self> {
        ByteArrayChunker {
            iter: self,
            chunk: Vec::new(),
            max_total_size: max_total_size,
            total_size: 0,
        }
    }
}

impl<I: Iterator + Sized> ChunkExt for I {}

// TODO(shaeq): handle case if >256kb compressed new rule matches for a single alert..
pub fn encode_alerts_for_publish<'a, I>(alerts: I) -> anyhow::Result<Vec<String>>
where
    I: Iterator<Item = &'a serde_json::Value> + Send,
{
    let alerts_to_deliver = alerts
        .par_bridge()
        .into_par_iter()
        .map(|v| {
            let mut bytes_v = v.to_string().into_bytes();
            bytes_v.push(b'\n');
            bytes_v
        })
        .collect::<Vec<_>>();

    let alerts_to_deliver_chunks = alerts_to_deliver
        .into_iter()
        .chunks_total_size(256 * 1024)
        .collect::<Vec<_>>();

    let alerts_to_deliver_compressed_chunks = alerts_to_deliver_chunks
        .into_par_iter()
        .map(|c| {
            let mut encoder = zstd::Encoder::new(Vec::new(), 1).unwrap();
            for l in c {
                encoder.write_all(&l)?;
            }
            encoder.finish().map_err(|e| anyhow!(e))
        })
        .collect::<anyhow::Result<Vec<_>>>()?
        .into_iter()
        .chunks_total_size(192 * 1024)
        .map(|c| encode(c.into_iter().flatten().collect::<Vec<_>>()))
        .collect::<Vec<_>>();

    Ok(alerts_to_deliver_compressed_chunks)
}

// model

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Alert {
    pub id: String,

    /// Is None for non-activated alerts.
    pub creation_time: Option<DateTime<Utc>>,
    pub title: String,
    pub severity: String,
    pub severity_icon_url: String,
    pub runbook: String,
    pub false_positives: Vec<String>,
    pub destinations: Vec<String>,
    pub context: Value,
    pub tables: Vec<String>,

    pub match_count: i64,
    /// The number of times this alert has been updated (0 on create).
    pub update_count: i64,
}

impl Alert {
    pub fn activated_creation_time(&self) -> DateTime<Utc> {
        self.creation_time.unwrap()
    }
    pub fn is_activated(&self) -> bool {
        self.creation_time.is_some()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AlertCDCPayload {
    pub updated_alert: Alert,
    pub destination_to_alert_info: HashMap<String, String>,
    pub incoming_rule_matches_context: Value,
    pub context_diff: Value,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct InternalAlertCDCPayload {
    pub payload: AlertCDCPayload,
    pub destination: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AlertItem {
    pub alert: Alert,
    #[serde(default)]
    pub destination_to_alert_info: HashMap<String, String>,
}

pub const RULE_MATCHES_GROUP_ID: &str = "mtn-rule-matches";
