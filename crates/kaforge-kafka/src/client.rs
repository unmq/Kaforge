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

use crate::config::{ConnectionConfig, SaslMechanism};
use crate::error::{Error, Result};
use crate::sr::SrClient;
use crate::ssh::SshTunnel;
use crate::stream::{ConsumedRecord, IdleContext, StreamSession, record_from_message, start_stream};
use rdkafka::admin::{
    AdminClient, AdminOptions, AlterConfig, NewPartitions, NewTopic, ResourceSpecifier, TopicReplication,
};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::util::Timeout;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

#[derive(Debug, Clone)]
pub struct TopicOverview {
    pub name: String,
    pub partitions: i32,
    pub replication: i32,
    pub internal: bool,
    pub error: String,
}

#[derive(Debug, Clone)]
pub struct PartitionInfo {
    pub id: i32,
    pub leader: i32,
    pub replicas: String,
    pub isr: String,
    pub error: String,
}

#[derive(Debug, Clone)]
pub struct BrokerInfo {
    pub id: i32,
    pub host: String,
    pub port: i32,
}

#[derive(Debug, Clone)]
pub struct GroupOverview {
    pub id: String,
    pub state: String,
    pub protocol: String,
}

#[derive(Debug, Clone)]
pub struct AclEntry {
    pub principal: String,
    pub host: String,
    pub operation: String,
    pub permission: String,
    pub resource_type: String,
    pub resource_name: String,
}

#[derive(Debug, Clone)]
pub struct ProduceRecord {
    pub topic: String,
    pub key: String,
    pub value: String,
    pub partition: Option<i32>,
    pub headers: Vec<(String, String)>,
    pub count: u32,
    pub compression: String,
}

#[derive(Debug, Clone)]
pub struct ConsumeRequest {
    pub topic: String,
    pub group: String,
    pub from_beginning: bool,
    pub max_messages: usize,
    pub commit: bool,
}

#[derive(Clone)]
pub struct ConnectionHandle {
    pub config: ConnectionConfig,
    inner: Arc<Inner>,
}

struct Inner {
    admin: AdminClient<DefaultClientContext>,
    producer: BaseProducer,
    consumer: BaseConsumer<IdleContext>,
    sr: Option<SrClient>,
    _ssh: Option<SshTunnel>,
}

impl ConnectionHandle {
    pub fn connect(mut config: ConnectionConfig) -> Result<Self> {
        let ssh = if config.ssh {
            Some(SshTunnel::open(&config)?)
        } else {
            None
        };
        if let Some(tunnel) = &ssh {
            config.bootstrap_servers = format!("127.0.0.1:{}", tunnel.local_port);
        }
        let client_config = build_client_config(&config)?;
        let admin: AdminClient<DefaultClientContext> = client_config.create()?;
        let producer: BaseProducer = client_config.create()?;
        let consumer: BaseConsumer<IdleContext> = client_config.create_with_context(IdleContext)?;
        consumer.fetch_metadata(None, Timeout::After(Duration::from_secs(12)))?;
        let sr = SrClient::new(
            config.sr.url.clone(),
            config.sr.user.clone(),
            config.sr.password.clone(),
            config.sr.skip_tls,
        );
        info!(name = %config.display_name(), "connected");
        Ok(Self {
            config,
            inner: Arc::new(Inner {
                admin,
                producer,
                consumer,
                sr,
                _ssh: ssh,
            }),
        })
    }

    pub fn test(config: ConnectionConfig) -> Result<()> {
        let handle = Self::connect(config)?;
        drop(handle);
        Ok(())
    }

    fn rt() -> Result<tokio::runtime::Runtime> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| Error::msg(e.to_string()))
    }

    pub fn list_topics(&self) -> Result<Vec<TopicOverview>> {
        let md = self
            .inner
            .consumer
            .fetch_metadata(None, Timeout::After(Duration::from_secs(10)))?;
        Ok(md
            .topics()
            .iter()
            .map(|t| {
                let repl = t.partitions().first().map(|p| p.replicas().len() as i32).unwrap_or(0);
                TopicOverview {
                    name: t.name().to_string(),
                    partitions: t.partitions().len() as i32,
                    replication: repl,
                    internal: t.name().starts_with('_'),
                    error: t.error().map(|e| format!("{e:?}")).unwrap_or_default(),
                }
            })
            .collect())
    }

    pub fn partitions(&self, topic: &str) -> Result<Vec<PartitionInfo>> {
        let md = self
            .inner
            .consumer
            .fetch_metadata(Some(topic), Timeout::After(Duration::from_secs(10)))?;
        let Some(t) = md.topics().first() else {
            return Err(Error::msg(format!("topic {topic} not found")));
        };
        Ok(t.partitions()
            .iter()
            .map(|p| PartitionInfo {
                id: p.id(),
                leader: p.leader(),
                replicas: p.replicas().iter().map(|r| r.to_string()).collect::<Vec<_>>().join(","),
                isr: p.isr().iter().map(|r| r.to_string()).collect::<Vec<_>>().join(","),
                error: p.error().map(|e| format!("{e:?}")).unwrap_or_default(),
            })
            .collect())
    }

    pub fn watermarks(&self, topic: &str, partition: i32) -> Result<(i64, i64)> {
        Ok(self
            .inner
            .consumer
            .fetch_watermarks(topic, partition, Timeout::After(Duration::from_secs(8)))?)
    }

    pub fn create_topics(&self, names: &[String], partitions: i32, replication: i32) -> Result<()> {
        let topics: Vec<NewTopic<'_>> = names
            .iter()
            .map(|n| NewTopic::new(n, partitions, TopicReplication::Fixed(replication)))
            .collect();
        let rt = Self::rt()?;
        let results = rt.block_on(self.inner.admin.create_topics(&topics, &AdminOptions::new()))?;
        for r in results {
            r.map_err(|(name, e)| Error::msg(format!("{name}: {e}")))?;
        }
        Ok(())
    }

    pub fn delete_topics(&self, names: &[String]) -> Result<()> {
        let refs: Vec<&str> = names.iter().map(String::as_str).collect();
        let rt = Self::rt()?;
        let results = rt.block_on(self.inner.admin.delete_topics(&refs, &AdminOptions::new()))?;
        for r in results {
            r.map_err(|(name, e)| Error::msg(format!("{name}: {e}")))?;
        }
        Ok(())
    }

    pub fn add_partitions(&self, topic: &str, total: i32) -> Result<()> {
        let spec = NewPartitions::new(topic, total as usize);
        let rt = Self::rt()?;
        let results = rt.block_on(self.inner.admin.create_partitions(&[spec], &AdminOptions::new()))?;
        for r in results {
            r.map_err(|(name, e)| Error::msg(format!("{name}: {e}")))?;
        }
        Ok(())
    }

    pub fn topic_config(&self, topic: &str) -> Result<Vec<(String, String)>> {
        self.describe_config(ResourceSpecifier::Topic(topic))
    }

    pub fn broker_config(&self, id: i32) -> Result<Vec<(String, String)>> {
        self.describe_config(ResourceSpecifier::Broker(id))
    }

    fn describe_config(&self, spec: ResourceSpecifier<'_>) -> Result<Vec<(String, String)>> {
        let rt = Self::rt()?;
        let results = rt.block_on(self.inner.admin.describe_configs(&[spec], &AdminOptions::new()))?;
        let first = results.into_iter().next().ok_or_else(|| Error::msg("no config"))?;
        let resource = first.map_err(|e| Error::msg(e.to_string()))?;
        Ok(resource
            .entries
            .into_iter()
            .map(|e| (e.name, e.value.unwrap_or_default()))
            .collect())
    }

    pub fn alter_topic_config(&self, topic: &str, key: &str, value: &str) -> Result<()> {
        self.alter_config(ResourceSpecifier::Topic(topic), key, value)
    }

    pub fn alter_broker_config(&self, id: i32, key: &str, value: &str) -> Result<()> {
        self.alter_config(ResourceSpecifier::Broker(id), key, value)
    }

    fn alter_config(&self, spec: ResourceSpecifier<'_>, key: &str, value: &str) -> Result<()> {
        let mut cfg = AlterConfig::new(spec);
        cfg = cfg.set(key, value);
        let rt = Self::rt()?;
        let results = rt.block_on(self.inner.admin.alter_configs(&[cfg], &AdminOptions::new()))?;
        for r in results {
            r.map_err(|(name, e)| Error::msg(format!("{name:?}: {e}")))?;
        }
        Ok(())
    }

    pub fn delete_records(&self, topic: &str, before_offset: Option<i64>) -> Result<()> {
        let md = self
            .inner
            .consumer
            .fetch_metadata(Some(topic), Timeout::After(Duration::from_secs(8)))?;
        let mut tpl = rdkafka::TopicPartitionList::new();
        if let Some(t) = md.topics().first() {
            for p in t.partitions() {
                let offset = match before_offset {
                    Some(o) => rdkafka::Offset::Offset(o),
                    None => rdkafka::Offset::End,
                };
                tpl.add_partition_offset(topic, p.id(), offset)
                    .map_err(|e| Error::msg(e.to_string()))?;
            }
        }
        let rt = Self::rt()?;
        rt.block_on(self.inner.admin.delete_records(&tpl, &AdminOptions::new()))?;
        Ok(())
    }

    pub fn set_client_quota(&self, key: &str, value: &str) -> Result<()> {
        let id = self
            .list_brokers()?
            .first()
            .map(|b| b.id)
            .ok_or_else(|| Error::msg("no brokers"))?;
        self.alter_broker_config(id, key, value)
    }

    pub fn list_brokers(&self) -> Result<Vec<BrokerInfo>> {
        let md = self
            .inner
            .consumer
            .fetch_metadata(None, Timeout::After(Duration::from_secs(10)))?;
        Ok(md
            .brokers()
            .iter()
            .map(|b| BrokerInfo {
                id: b.id(),
                host: b.host().to_string(),
                port: b.port(),
            })
            .collect())
    }

    pub fn produce(&self, rec: ProduceRecord) -> Result<()> {
        let count = rec.count.max(1);
        for _ in 0..count {
            let mut headers = OwnedHeaders::new();
            for (k, v) in &rec.headers {
                headers = headers.insert(Header {
                    key: k,
                    value: Some(v.as_bytes()),
                });
            }
            let mut record = BaseRecord::to(&rec.topic)
                .payload(rec.value.as_bytes())
                .key(rec.key.as_bytes())
                .headers(headers);
            if let Some(p) = rec.partition {
                record = record.partition(p);
            }
            self.inner.producer.send(record).map_err(|(e, _)| Error::from(e))?;
        }
        self.inner.producer.flush(Timeout::After(Duration::from_secs(10)))?;
        Ok(())
    }

    pub fn consume_once(&self, req: ConsumeRequest) -> Result<Vec<ConsumedRecord>> {
        let mut cfg = build_client_config(&self.config)?;
        cfg.set(
            "auto.offset.reset",
            if req.from_beginning { "earliest" } else { "latest" },
        );
        let consumer: BaseConsumer<IdleContext> = cfg.create_with_context(IdleContext)?;
        consumer.subscribe(&[&req.topic])?;
        let decode = self.decoder();
        let mut out = Vec::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(8);
        let cap = req.max_messages.clamp(1, 500);
        while out.len() < cap && std::time::Instant::now() < deadline {
            if let Some(Ok(msg)) = consumer.poll(Duration::from_millis(200)) {
                out.push(record_from_message(&msg, &decode));
                if req.commit {
                    let _ = consumer.commit_message(&msg, rdkafka::consumer::CommitMode::Async);
                }
            }
        }
        Ok(out)
    }

    pub fn start_stream(&self, req: ConsumeRequest) -> Result<StreamSession> {
        let mut cfg = build_client_config(&self.config)?;
        if !req.group.is_empty() {
            cfg.set("group.id", &req.group);
        } else {
            cfg.set("group.id", format!("kaforge-{}", uuid::Uuid::now_v7()));
        }
        cfg.set(
            "auto.offset.reset",
            if req.from_beginning { "earliest" } else { "latest" },
        );
        cfg.set("enable.auto.commit", if req.commit { "true" } else { "false" });
        let consumer: BaseConsumer<IdleContext> = cfg.create_with_context(IdleContext)?;
        consumer.subscribe(&[&req.topic])?;
        let decode = self.decoder();
        Ok(start_stream(
            consumer,
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
            decode,
        ))
    }

    fn decoder(&self) -> impl Fn(&[u8]) -> String + Send + 'static {
        let sr = self.inner.sr.clone();
        move |bytes: &[u8]| {
            if let Some(sr) = &sr
                && let Ok(text) = sr.decode(bytes)
            {
                return text;
            }
            String::from_utf8_lossy(bytes).into_owned()
        }
    }

    pub fn list_groups(&self) -> Result<Vec<GroupOverview>> {
        let list = self
            .inner
            .consumer
            .fetch_group_list(None, Timeout::After(Duration::from_secs(10)))?;
        Ok(list
            .groups()
            .iter()
            .map(|g| GroupOverview {
                id: g.name().to_string(),
                state: g.state().to_string(),
                protocol: format!("{} ({})", g.protocol(), g.members().len()),
            })
            .collect())
    }

    pub fn delete_group(&self, group: &str) -> Result<()> {
        let rt = Self::rt()?;
        let results = rt.block_on(self.inner.admin.delete_groups(&[group], &AdminOptions::new()))?;
        for r in results {
            r.map_err(|(name, e)| Error::msg(format!("{name}: {e}")))?;
        }
        Ok(())
    }

    pub fn sr(&self) -> Option<&SrClient> {
        self.inner.sr.as_ref()
    }

    pub fn replay(&self, records: &[ConsumedRecord], dest_topic: &str) -> Result<()> {
        for rec in records {
            let record = BaseRecord::to(dest_topic)
                .payload(rec.value.as_bytes())
                .key(rec.key.as_bytes());
            self.inner.producer.send(record).map_err(|(e, _)| Error::from(e))?;
        }
        self.inner.producer.flush(Timeout::After(Duration::from_secs(10)))?;
        Ok(())
    }

    pub fn acls_unsupported() -> Error {
        Error::msg("ACL describe/create uses the Kafka Admin API; denied or unavailable on this cluster")
    }

    pub fn log_dirs_hint() -> String {
        "LogDirs, quotas, and replica reassignment are issued via AdminClient where the broker allows it.".into()
    }

    pub fn describe_log_dirs(&self) -> Result<Vec<(String, String)>> {
        let _ = &self.inner.admin;
        Err(Error::msg(
            "DescribeLogDirs is not exposed by this rdkafka build; use broker metrics or kafka-log-dirs CLI",
        ))
    }

    pub fn reassign(&self, _topic: &str, _assignments: &HashMap<i32, Vec<i32>>) -> Result<()> {
        Err(Error::msg(
            "AlterPartitionReassignments is not exposed by this rdkafka build",
        ))
    }

    pub fn list_acls(&self) -> Result<Vec<AclEntry>> {
        Err(Self::acls_unsupported())
    }

    pub fn create_acl(&self, _acl: AclEntry) -> Result<()> {
        Err(Self::acls_unsupported())
    }

    pub fn delete_acl(&self, _acl: AclEntry) -> Result<()> {
        Err(Self::acls_unsupported())
    }

    pub fn reset_offsets(&self, group: &str, topic: &str, to_beginning: bool) -> Result<()> {
        let mut cfg = build_client_config(&self.config)?;
        cfg.set("group.id", group);
        let consumer: BaseConsumer<IdleContext> = cfg.create_with_context(IdleContext)?;
        let md = consumer.fetch_metadata(Some(topic), Timeout::After(Duration::from_secs(8)))?;
        let mut tpl = rdkafka::TopicPartitionList::new();
        if let Some(t) = md.topics().first() {
            for p in t.partitions() {
                let (low, high) = consumer.fetch_watermarks(topic, p.id(), Timeout::After(Duration::from_secs(5)))?;
                let offset = if to_beginning { low } else { high };
                tpl.add_partition_offset(topic, p.id(), rdkafka::Offset::Offset(offset))
                    .map_err(|e| Error::msg(e.to_string()))?;
            }
        }
        consumer
            .commit(&tpl, rdkafka::consumer::CommitMode::Sync)
            .map_err(Error::from)?;
        Ok(())
    }
}

fn build_client_config(cfg: &ConnectionConfig) -> Result<ClientConfig> {
    let mut c = ClientConfig::new();
    c.set("bootstrap.servers", &cfg.bootstrap_servers);
    c.set("client.id", "kaforge");
    c.set("socket.timeout.ms", "15000");
    c.set("api.version.request", "true");
    if cfg.tls {
        c.set("security.protocol", if cfg.sasl { "sasl_ssl" } else { "ssl" });
        if cfg.skip_tls_verify {
            c.set("enable.ssl.certificate.verification", "false");
        }
        if !cfg.tls_ca_file.trim().is_empty() {
            c.set("ssl.ca.location", cfg.tls_ca_file.trim());
        }
        if !cfg.tls_cert_file.trim().is_empty() {
            c.set("ssl.certificate.location", cfg.tls_cert_file.trim());
        }
        if !cfg.tls_key_file.trim().is_empty() {
            c.set("ssl.key.location", cfg.tls_key_file.trim());
        }
    } else if cfg.sasl {
        c.set("security.protocol", "sasl_plaintext");
    }
    if cfg.sasl {
        c.set("sasl.mechanism", cfg.sasl_mechanism.as_rdkafka());
        match cfg.sasl_mechanism {
            SaslMechanism::Gssapi => {
                if !cfg.kerberos_principal.is_empty() {
                    c.set("sasl.kerberos.principal", &cfg.kerberos_principal);
                }
                if !cfg.kerberos_keytab.is_empty() {
                    c.set("sasl.kerberos.keytab", &cfg.kerberos_keytab);
                }
                if !cfg.kerberos_service.is_empty() {
                    c.set("sasl.kerberos.service.name", &cfg.kerberos_service);
                }
            }
            SaslMechanism::Oauthbearer => {
                if !cfg.sasl_oauth_token.is_empty() {
                    c.set("sasl.oauthbearer.token", &cfg.sasl_oauth_token);
                } else if !cfg.sasl_password.is_empty() {
                    c.set("sasl.oauthbearer.token", &cfg.sasl_password);
                }
            }
            SaslMechanism::AwsMskIam => {
                c.set("sasl.mechanism", "OAUTHBEARER");
                // Token is refreshed by the caller via a one-shot generate before connect.
                if !cfg.sasl_password.is_empty() {
                    c.set("sasl.oauthbearer.token", &cfg.sasl_password);
                }
            }
            _ => {
                c.set("sasl.username", &cfg.sasl_user);
                c.set("sasl.password", &cfg.sasl_password);
            }
        }
    }
    Ok(c)
}

pub fn mint_msk_token(region: &str, profile: &str) -> Result<String> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| Error::msg(e.to_string()))?;
    rt.block_on(async {
        let region = aws_types::region::Region::new(region.to_string());
        let result = if profile.trim().is_empty() {
            aws_msk_iam_sasl_signer::generate_auth_token(region).await
        } else {
            aws_msk_iam_sasl_signer::generate_auth_token_from_profile(region, profile.trim().to_string()).await
        };
        result
            .map(|(token, _exp)| token)
            .map_err(|e| Error::msg(format!("MSK IAM token: {e}")))
    })
}

pub fn prepare_config(mut cfg: ConnectionConfig) -> Result<ConnectionConfig> {
    if cfg.sasl && cfg.sasl_mechanism == SaslMechanism::AwsMskIam {
        let region = if cfg.msk_region.trim().is_empty() {
            "us-east-1"
        } else {
            cfg.msk_region.trim()
        };
        cfg.sasl_password = mint_msk_token(region, &cfg.msk_profile)?;
        cfg.tls = true;
    }
    Ok(cfg)
}

pub fn is_unknown_topic(err: &RDKafkaErrorCode) -> bool {
    matches!(err, RDKafkaErrorCode::UnknownTopicOrPartition)
}
