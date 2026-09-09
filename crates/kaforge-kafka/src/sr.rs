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

use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use ureq::tls::{TlsConfig, TlsProvider};

#[derive(Clone)]
pub struct SrClient {
    url: String,
    user: String,
    password: String,
    skip_tls: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SrSubject {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SrVersion {
    pub id: i32,
    pub version: i32,
    pub schema: String,
    #[serde(default)]
    pub schema_type: String,
}

impl SrClient {
    pub fn new(url: String, user: String, password: String, skip_tls: bool) -> Option<Self> {
        let url = url.trim().trim_end_matches('/').to_string();
        if url.is_empty() {
            None
        } else {
            Some(Self {
                url,
                user,
                password,
                skip_tls,
            })
        }
    }

    fn agent(&self) -> Result<ureq::Agent> {
        let tls = TlsConfig::builder()
            .provider(TlsProvider::Rustls)
            .disable_verification(self.skip_tls)
            .build();
        Ok(ureq::Agent::config_builder().tls_config(tls).build().into())
    }

    fn auth(
        &self,
        req: ureq::RequestBuilder<ureq::typestate::WithoutBody>,
    ) -> ureq::RequestBuilder<ureq::typestate::WithoutBody> {
        if self.user.is_empty() {
            req
        } else {
            req.header(
                "Authorization",
                format!(
                    "Basic {}",
                    base64::Engine::encode(
                        &base64::engine::general_purpose::STANDARD,
                        format!("{}:{}", self.user, self.password)
                    )
                ),
            )
        }
    }

    pub fn subjects(&self) -> Result<Vec<String>> {
        let agent = self.agent()?;
        let url = format!("{}/subjects", self.url);
        let req = self.auth(agent.get(&url));
        let names: Vec<String> = req
            .call()
            .map_err(|e| Error::msg(format!("SR subjects: {e}")))?
            .body_mut()
            .read_json()
            .map_err(|e| Error::msg(format!("SR subjects json: {e}")))?;
        Ok(names)
    }

    pub fn versions(&self, subject: &str) -> Result<Vec<i32>> {
        let agent = self.agent()?;
        let url = format!("{}/subjects/{}/versions", self.url, urlencoding(subject));
        let req = self.auth(agent.get(&url));
        req.call()
            .map_err(|e| Error::msg(format!("SR versions: {e}")))?
            .body_mut()
            .read_json()
            .map_err(|e| Error::msg(format!("SR versions json: {e}")))
    }

    pub fn schema(&self, subject: &str, version: i32) -> Result<SrVersion> {
        let agent = self.agent()?;
        let url = format!("{}/subjects/{}/versions/{version}", self.url, urlencoding(subject));
        let req = self.auth(agent.get(&url));
        req.call()
            .map_err(|e| Error::msg(format!("SR schema: {e}")))?
            .body_mut()
            .read_json()
            .map_err(|e| Error::msg(format!("SR schema json: {e}")))
    }

    pub fn delete_subject(&self, subject: &str) -> Result<()> {
        let agent = self.agent()?;
        let url = format!("{}/subjects/{}", self.url, urlencoding(subject));
        self.auth(agent.delete(&url))
            .call()
            .map_err(|e| Error::msg(format!("SR delete: {e}")))?;
        Ok(())
    }

    pub fn set_compatibility(&self, subject: &str, level: &str) -> Result<()> {
        let agent = self.agent()?;
        let url = format!("{}/config/{}", self.url, urlencoding(subject));
        agent
            .put(&url)
            .header("Content-Type", "application/vnd.schemaregistry.v1+json")
            .send(serde_json::json!({ "compatibility": level }).to_string())
            .map_err(|e| Error::msg(format!("SR compatibility: {e}")))?;
        Ok(())
    }

    pub fn decode(&self, payload: &[u8]) -> Result<String> {
        if payload.len() < 6 || payload[0] != 0 {
            return Ok(String::from_utf8_lossy(payload).into_owned());
        }
        let id = i32::from_be_bytes([payload[1], payload[2], payload[3], payload[4]]);
        let body = &payload[5..];
        let agent = self.agent()?;
        let url = format!("{}/schemas/ids/{id}", self.url);
        let raw: serde_json::Value = self
            .auth(agent.get(&url))
            .call()
            .map_err(|e| Error::msg(format!("SR id {id}: {e}")))?
            .body_mut()
            .read_json()
            .map_err(|e| Error::msg(format!("SR id json: {e}")))?;
        let schema_str = raw.get("schema").and_then(|v| v.as_str()).unwrap_or("").to_string();
        if schema_str.is_empty() {
            return Ok(String::from_utf8_lossy(body).into_owned());
        }
        let schema =
            apache_avro::Schema::parse_str(&schema_str).map_err(|e| Error::msg(format!("Avro schema: {e}")))?;
        let value = apache_avro::from_avro_datum(&schema, &mut std::io::Cursor::new(body), None)
            .map_err(|e| Error::msg(format!("Avro decode: {e}")))?;
        Ok(format!("{value:?}"))
    }
}

fn urlencoding(s: &str) -> String {
    s.replace('/', "%2F")
}
