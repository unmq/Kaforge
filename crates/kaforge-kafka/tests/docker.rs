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

//! PLAINTEXT Kafka round-trip against a compose-managed broker.
//! Run with `make test-int` (ignored under `make test` so CI stays docker-free).

use kaforge_kafka::{ConnectionConfig, ConnectionHandle, ConsumeRequest, ProduceRecord};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

const PROJECT: &str = "kaforge-it";
const BOOTSTRAP: &str = "127.0.0.1:19092";

struct KafkaDocker {
    compose_file: PathBuf,
}

impl KafkaDocker {
    fn start() -> Self {
        assert!(
            docker_compose_ok(),
            "docker compose is required for this test (install Docker Desktop / engine)"
        );
        let compose_file = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("docker-compose.yml");
        let cluster = Self { compose_file };
        cluster.compose(&["down", "-v", "--remove-orphans"]);
        let status = cluster.compose(&["up", "-d"]);
        assert!(status.success(), "docker compose up failed");
        cluster
    }

    fn compose(&self, args: &[&str]) -> std::process::ExitStatus {
        let mut cmd = Command::new("docker");
        cmd.args(["compose", "-p", PROJECT, "-f"])
            .arg(&self.compose_file)
            .args(args);
        cmd.status().expect("failed to spawn docker compose")
    }
}

impl Drop for KafkaDocker {
    fn drop(&mut self) {
        let _ = self.compose(&["down", "-v", "--remove-orphans"]);
    }
}

fn docker_compose_ok() -> bool {
    Command::new("docker")
        .args(["compose", "version"])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn wait_port(addr: &str, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if TcpStream::connect(addr).is_ok() {
            return true;
        }
        thread::sleep(Duration::from_millis(400));
    }
    false
}

fn wait_kafka() {
    assert!(
        wait_port(BOOTSTRAP, Duration::from_secs(60)),
        "kafka port {BOOTSTRAP} did not open"
    );
    let mut last = None;
    for _ in 0..30 {
        let cfg = ConnectionConfig {
            bootstrap_servers: BOOTSTRAP.into(),
            ..ConnectionConfig::new_blank()
        };
        match ConnectionHandle::test(cfg) {
            Ok(()) => return,
            Err(e) => last = Some(e),
        }
        thread::sleep(Duration::from_secs(2));
    }
    panic!(
        "kafka at {BOOTSTRAP} never accepted a client: {}",
        last.map(|e| e.to_string()).unwrap_or_else(|| "no attempts".into())
    );
}

#[test]
#[ignore = "needs docker: make test-int"]
fn produce_and_consume_roundtrip() {
    let _cluster = KafkaDocker::start();
    wait_kafka();

    let cfg = ConnectionConfig {
        bootstrap_servers: BOOTSTRAP.into(),
        ..ConnectionConfig::new_blank()
    };
    let handle = ConnectionHandle::connect(cfg).expect("connect");
    let topic = format!("kaforge-it-{}", std::process::id());
    handle
        .create_topics(std::slice::from_ref(&topic), 1, 1)
        .expect("create topic");
    handle
        .produce(ProduceRecord {
            topic: topic.clone(),
            key: "k".into(),
            value: "hello-kaforge".into(),
            partition: None,
            headers: vec![],
            count: 1,
            compression: String::new(),
        })
        .expect("produce");
    let recs = handle
        .consume_once(ConsumeRequest {
            topic,
            group: format!("kaforge-it-{}", std::process::id()),
            from_beginning: true,
            max_messages: 1,
            commit: false,
        })
        .expect("consume");
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].value, "hello-kaforge");
}
