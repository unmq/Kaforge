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

//! Saved Kafka connections in `<config_dir>/connections.toml`.

use crate::error::Error;
use crate::helpers::{get_or_create_config_dir, write_file_atomic_with_backup};
use kaforge_kafka::{ConnectionConfig, SaslMechanism, SchemaRegistryConfig};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ConnectionFile {
    pub connections: Vec<ConnectionConfig>,
}

fn path() -> Result<PathBuf> {
    Ok(get_or_create_config_dir()?.join("connections.toml"))
}

pub fn load() -> Result<Vec<ConnectionConfig>> {
    let path = path()?;
    if !path.exists() {
        return Ok(Vec::new());
    }
    let text = std::fs::read_to_string(&path)?;
    if text.trim().is_empty() {
        return Ok(Vec::new());
    }
    let file: ConnectionFile = toml::from_str(&text)?;
    Ok(file.connections)
}

pub fn save(connections: &[ConnectionConfig]) -> Result<()> {
    let file = ConnectionFile {
        connections: connections.to_vec(),
    };
    let text = toml::to_string_pretty(&file)?;
    write_file_atomic_with_backup(&path()?, text.as_bytes())?;
    Ok(())
}

pub fn upsert(conn: ConnectionConfig) -> Result<Vec<ConnectionConfig>> {
    let mut all = load()?;
    if let Some(existing) = all.iter_mut().find(|c| c.id == conn.id) {
        *existing = conn;
    } else {
        all.push(conn);
    }
    save(&all)?;
    Ok(all)
}

pub fn remove(id: &str) -> Result<Vec<ConnectionConfig>> {
    let mut all = load()?;
    all.retain(|c| c.id != id);
    save(&all)?;
    Ok(all)
}

pub fn export_toml(connections: &[ConnectionConfig]) -> Result<String> {
    Ok(toml::to_string_pretty(&ConnectionFile {
        connections: connections.to_vec(),
    })?)
}

/// Kafka-King `config.yaml` `connects` list (and optional global schema_registry).
pub fn import_king_yaml(text: &str) -> Result<Vec<ConnectionConfig>> {
    #[derive(Deserialize)]
    struct KingRoot {
        #[serde(default)]
        connects: Vec<KingConnect>,
        #[serde(default)]
        schema_registry: Option<KingSr>,
    }
    #[derive(Deserialize, Default)]
    struct KingSr {
        #[serde(default)]
        url: String,
        #[serde(default)]
        user: String,
        #[serde(default)]
        pass: String,
        #[serde(default)]
        skip_tls: String,
    }
    #[derive(Deserialize, Default)]
    struct KingConnect {
        #[serde(default)]
        id: i64,
        #[serde(default)]
        name: String,
        #[serde(default)]
        bootstrap_servers: String,
        #[serde(default)]
        tls: String,
        #[serde(rename = "skipTLSVerify", default)]
        skip_tls_verify: String,
        #[serde(default)]
        tls_cert_file: String,
        #[serde(default)]
        tls_key_file: String,
        #[serde(default)]
        tls_ca_file: String,
        #[serde(default)]
        sasl: String,
        #[serde(default)]
        sasl_mechanism: String,
        #[serde(default)]
        sasl_user: String,
        #[serde(default)]
        sasl_pwd: String,
        #[serde(default)]
        kerberos_user_keytab: String,
        #[serde(default)]
        kerberos_krb5_conf: String,
        #[serde(rename = "Kerberos_user", default)]
        kerberos_user: String,
        #[serde(rename = "Kerberos_realm", default)]
        kerberos_realm: String,
        #[serde(default)]
        kerberos_service_name: String,
        #[serde(default)]
        use_ssh: String,
        #[serde(default)]
        ssh_host: String,
        #[serde(default)]
        ssh_port: i64,
        #[serde(default)]
        ssh_user: String,
        #[serde(default)]
        ssh_password: String,
        #[serde(default)]
        ssh_key_file: String,
    }
    let root: KingRoot = serde_yaml::from_str(text).map_err(|e| Error::Invalid { message: e.to_string() })?;
    let global_sr = root.schema_registry.unwrap_or_default();
    let mut out = Vec::new();
    for k in root.connects {
        let enabled = |s: &str| matches!(s.to_ascii_lowercase().as_str(), "enable" | "true" | "1" | "yes");
        out.push(ConnectionConfig {
            id: if k.id == 0 {
                uuid::Uuid::now_v7().to_string()
            } else {
                format!("king-{id}", id = k.id)
            },
            name: k.name,
            bootstrap_servers: k.bootstrap_servers,
            tls: enabled(&k.tls),
            skip_tls_verify: enabled(&k.skip_tls_verify),
            tls_cert_file: k.tls_cert_file,
            tls_key_file: k.tls_key_file,
            tls_ca_file: k.tls_ca_file,
            sasl: enabled(&k.sasl),
            sasl_mechanism: SaslMechanism::from_king(&k.sasl_mechanism),
            sasl_user: k.sasl_user,
            sasl_password: k.sasl_pwd,
            sasl_oauth_token: String::new(),
            msk_region: String::new(),
            msk_profile: String::new(),
            kerberos_keytab: k.kerberos_user_keytab,
            kerberos_krb5: k.kerberos_krb5_conf,
            kerberos_principal: k.kerberos_user,
            kerberos_realm: k.kerberos_realm,
            kerberos_service: if k.kerberos_service_name.is_empty() {
                "kafka".into()
            } else {
                k.kerberos_service_name
            },
            ssh: enabled(&k.use_ssh),
            ssh_host: k.ssh_host,
            ssh_port: if k.ssh_port <= 0 { 22 } else { k.ssh_port as u16 },
            ssh_user: k.ssh_user,
            ssh_password: k.ssh_password,
            ssh_key_file: k.ssh_key_file,
            sr: SchemaRegistryConfig {
                url: global_sr.url.clone(),
                user: global_sr.user.clone(),
                password: global_sr.pass.clone(),
                skip_tls: enabled(&global_sr.skip_tls) || global_sr.skip_tls.is_empty(),
            },
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn king_yaml_maps_sasl_and_ssh() {
        let yaml = r#"
connects:
  - id: 3
    name: prod
    bootstrap_servers: kafka:9092
    tls: enable
    sasl: enable
    sasl_mechanism: SCRAM-SHA-512
    sasl_user: alice
    sasl_pwd: secret
    use_ssh: enable
    ssh_host: bastion
    ssh_port: 22
    ssh_user: ops
"#;
        let got = import_king_yaml(yaml).expect("parse");
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].id, "king-3");
        assert!(got[0].tls);
        assert!(got[0].sasl);
        assert_eq!(got[0].sasl_mechanism, SaslMechanism::ScramSha512);
        assert!(got[0].ssh);
        assert_eq!(got[0].ssh_host, "bastion");
    }
}
