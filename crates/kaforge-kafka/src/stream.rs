// Copyright 2026 xhofe.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use rdkafka::Message;
use rdkafka::consumer::{BaseConsumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::Headers;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::Duration;

pub const STREAM_MAX_MESSAGES: usize = 10_000;
pub const STREAM_MAX_BYTES: usize = 200 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct ConsumedRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp_ms: Option<i64>,
    pub key: String,
    pub value: String,
    pub headers: String,
    pub bytes: usize,
}

pub enum StreamEvent {
    Message(ConsumedRecord),
    Error(String),
    Ended,
}

pub struct StreamSession {
    stop: Arc<AtomicBool>,
    pub rx: std::sync::mpsc::Receiver<StreamEvent>,
}

impl Drop for StreamSession {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
    }
}

pub fn start_stream(
    consumer: BaseConsumer<IdleContext>,
    stop: Arc<AtomicBool>,
    decode: impl Fn(&[u8]) -> String + Send + 'static,
) -> StreamSession {
    let (tx, rx) = std::sync::mpsc::channel();
    let stop_thread = stop.clone();
    thread::Builder::new()
        .name("kaforge-stream".into())
        .spawn(move || {
            let mut n = 0usize;
            let mut bytes = 0usize;
            while !stop_thread.load(Ordering::SeqCst) {
                match consumer.poll(Duration::from_millis(200)) {
                    None => continue,
                    Some(Err(e)) => {
                        let _ = tx.send(StreamEvent::Error(e.to_string()));
                        break;
                    }
                    Some(Ok(msg)) => {
                        let rec = record_from_message(&msg, &decode);
                        n += 1;
                        bytes += rec.bytes;
                        if n > STREAM_MAX_MESSAGES || bytes > STREAM_MAX_BYTES {
                            let _ = tx.send(StreamEvent::Error(
                                "stream cap reached (10000 messages or 200MB)".into(),
                            ));
                            break;
                        }
                        if tx.send(StreamEvent::Message(rec)).is_err() {
                            break;
                        }
                    }
                }
            }
            let _ = tx.send(StreamEvent::Ended);
        })
        .ok();
    StreamSession { stop, rx }
}

pub struct IdleContext;

impl rdkafka::client::ClientContext for IdleContext {}
impl ConsumerContext for IdleContext {
    fn pre_rebalance(&self, _: &BaseConsumer<Self>, _: &Rebalance<'_>) {}
    fn post_rebalance(&self, _: &BaseConsumer<Self>, _: &Rebalance<'_>) {}
    fn commit_callback(&self, _: KafkaResult<()>, _: &rdkafka::TopicPartitionList) {}
}

pub fn record_from_message<M: Message>(msg: &M, decode: impl Fn(&[u8]) -> String) -> ConsumedRecord {
    let key = msg
        .key()
        .map(|k| String::from_utf8_lossy(k).into_owned())
        .unwrap_or_default();
    let raw = msg.payload().unwrap_or(&[]);
    let value = decode(raw);
    let headers = msg
        .headers()
        .map(|h| {
            (0..h.count())
                .map(|i| {
                    let header = h.get(i);
                    format!(
                        "{}={}",
                        header.key,
                        String::from_utf8_lossy(header.value.unwrap_or(&[]))
                    )
                })
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_default();
    ConsumedRecord {
        topic: msg.topic().to_string(),
        partition: msg.partition(),
        offset: msg.offset(),
        timestamp_ms: match msg.timestamp() {
            rdkafka::Timestamp::NotAvailable => None,
            rdkafka::Timestamp::CreateTime(ms) | rdkafka::Timestamp::LogAppendTime(ms) => Some(ms),
        },
        key,
        value,
        headers,
        bytes: raw.len(),
    }
}
