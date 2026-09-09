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

use serde::{Deserialize, Serialize};

fn default_true() -> bool {
    true
}

/// Saved cluster. Passwords stay plaintext in TOML on purpose (easy copy).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ConnectionConfig {
    pub id: String,
    pub name: String,
    pub bootstrap_servers: String,
    pub tls: bool,
    pub skip_tls_verify: bool,
    pub tls_cert_file: String,
    pub tls_key_file: String,
    pub tls_ca_file: String,
    pub sasl: bool,
    pub sasl_mechanism: SaslMechanism,
    pub sasl_user: String,
    pub sasl_password: String,
    pub sasl_oauth_token: String,
    pub msk_region: String,
    pub msk_profile: String,
    pub kerberos_keytab: String,
    pub kerberos_krb5: String,
    pub kerberos_principal: String,
    pub kerberos_realm: String,
    pub kerberos_service: String,
    pub ssh: bool,
    pub ssh_host: String,
    pub ssh_port: u16,
    pub ssh_user: String,
    pub ssh_password: String,
    pub ssh_key_file: String,
    pub sr: SchemaRegistryConfig,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum SaslMechanism {
    #[default]
    Plain,
    ScramSha256,
    ScramSha512,
    Gssapi,
    Oauthbearer,
    AwsMskIam,
}

impl SaslMechanism {
    pub fn as_rdkafka(self) -> &'static str {
        match self {
            Self::Plain => "PLAIN",
            Self::ScramSha256 => "SCRAM-SHA-256",
            Self::ScramSha512 => "SCRAM-SHA-512",
            Self::Gssapi => "GSSAPI",
            Self::Oauthbearer | Self::AwsMskIam => "OAUTHBEARER",
        }
    }

    pub fn from_king(name: &str) -> Self {
        match name.to_ascii_uppercase().as_str() {
            "SCRAM-SHA-256" => Self::ScramSha256,
            "SCRAM-SHA-512" => Self::ScramSha512,
            "GSSAPI" => Self::Gssapi,
            "OAUTHBEARER" => Self::Oauthbearer,
            "AWS_MSK_IAM" | "AWS-MSK-IAM" => Self::AwsMskIam,
            _ => Self::Plain,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SchemaRegistryConfig {
    pub url: String,
    pub user: String,
    pub password: String,
    #[serde(default = "default_true")]
    pub skip_tls: bool,
}

impl ConnectionConfig {
    pub fn new_blank() -> Self {
        Self {
            id: uuid::Uuid::now_v7().to_string(),
            ssh_port: 22,
            kerberos_service: "kafka".into(),
            ..Default::default()
        }
    }

    pub fn display_name(&self) -> &str {
        if self.name.trim().is_empty() {
            self.bootstrap_servers.as_str()
        } else {
            self.name.as_str()
        }
    }
}
