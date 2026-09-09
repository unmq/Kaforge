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

use crate::config::ConnectionConfig;
use crate::error::{Error, Result};
use std::net::{TcpListener, TcpStream};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::Duration;
use tracing::{error, info};

/// Local port that forwards every Kafka broker TCP connection through SSH.
pub struct SshTunnel {
    pub local_port: u16,
    stop: Arc<AtomicBool>,
}

impl Drop for SshTunnel {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
    }
}

impl SshTunnel {
    pub fn open(cfg: &ConnectionConfig) -> Result<Self> {
        if cfg.ssh_host.trim().is_empty() || cfg.ssh_user.trim().is_empty() {
            return Err(Error::msg("SSH host and user are required"));
        }
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let local_port = listener.local_addr()?.port();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_thread = stop.clone();
        let ssh_host = cfg.ssh_host.clone();
        let ssh_port = if cfg.ssh_port == 0 { 22 } else { cfg.ssh_port };
        let ssh_user = cfg.ssh_user.clone();
        let ssh_password = cfg.ssh_password.clone();
        let ssh_key_file = cfg.ssh_key_file.clone();
        let brokers = cfg.bootstrap_servers.clone();
        let known_hosts = cfg.ssh_known_hosts_path.clone();
        thread::Builder::new()
            .name("kaforge-ssh".into())
            .spawn(move || {
                if let Err(e) = run_forward(SshForward {
                    listener,
                    stop: stop_thread,
                    ssh_host,
                    ssh_port,
                    ssh_user,
                    ssh_password,
                    ssh_key_file,
                    brokers,
                    known_hosts,
                }) {
                    error!(error = %e, "SSH tunnel exited");
                }
            })
            .map_err(|e| Error::msg(format!("SSH thread: {e}")))?;
        thread::sleep(Duration::from_millis(200));
        info!(local_port, "SSH local forward listening");
        Ok(Self { local_port, stop })
    }
}

struct SshForward {
    listener: TcpListener,
    stop: Arc<AtomicBool>,
    ssh_host: String,
    ssh_port: u16,
    ssh_user: String,
    ssh_password: String,
    ssh_key_file: String,
    brokers: String,
    known_hosts: String,
}

fn run_forward(fwd: SshForward) -> Result<()> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| Error::msg(e.to_string()))?;
    rt.block_on(async move {
        let session = open_session(
            &fwd.ssh_host,
            fwd.ssh_port,
            &fwd.ssh_user,
            &fwd.ssh_password,
            &fwd.ssh_key_file,
            &fwd.known_hosts,
        )
        .await?;
        let first = fwd
            .brokers
            .split(',')
            .next()
            .unwrap_or("127.0.0.1:9092")
            .trim()
            .to_string();
        let (remote_host, remote_port) = split_host_port(&first);
        while !fwd.stop.load(Ordering::SeqCst) {
            match fwd.listener.accept() {
                Ok((incoming, _)) => {
                    let channel = session
                        .channel_open_direct_tcpip(&remote_host, u32::from(remote_port), "127.0.0.1", 0)
                        .await
                        .map_err(|e| Error::msg(format!("SSH forward: {e}")))?;
                    tokio::spawn(copy_tunnel(incoming, channel));
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    tokio::time::sleep(Duration::from_millis(40)).await;
                }
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    })
}

async fn open_session(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    key_file: &str,
    known_hosts: &str,
) -> Result<russh::client::Handle<TofuHost>> {
    let config = Arc::new(russh::client::Config::default());
    let handler = TofuHost {
        host: format!("{host}:{port}"),
        path: known_hosts.to_string(),
    };
    let mut session = russh::client::connect(config, (host, port), handler)
        .await
        .map_err(|e| Error::msg(format!("SSH connect {host}:{port}: {e}")))?;
    let authed = if !key_file.trim().is_empty() {
        let key =
            russh::keys::load_secret_key(key_file.trim(), None).map_err(|e| Error::msg(format!("SSH key: {e}")))?;
        session
            .authenticate_publickey(user, russh::keys::PrivateKeyWithHashAlg::new(Arc::new(key), None))
            .await
            .map_err(|e| Error::msg(format!("SSH key auth: {e}")))?
            .success()
    } else {
        session
            .authenticate_password(user, password)
            .await
            .map_err(|e| Error::msg(format!("SSH password auth: {e}")))?
            .success()
    };
    if !authed {
        return Err(Error::msg("SSH authentication failed"));
    }
    Ok(session)
}

async fn copy_tunnel(mut local: TcpStream, mut channel: russh::Channel<russh::client::Msg>) {
    let _ = local.set_nonblocking(true);
    let mut buf = [0u8; 8192];
    loop {
        match std::io::Read::read(&mut local, &mut buf) {
            Ok(0) => break,
            Ok(n) => {
                if channel.data(&buf[..n]).await.is_err() {
                    break;
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(_) => break,
        }
        match channel.wait().await {
            Some(russh::ChannelMsg::Data { ref data }) => {
                if std::io::Write::write_all(&mut local, data).is_err() {
                    break;
                }
            }
            Some(russh::ChannelMsg::Eof) | None => break,
            _ => {}
        }
    }
}

fn split_host_port(input: &str) -> (String, u16) {
    if let Some((h, p)) = input.rsplit_once(':')
        && let Ok(port) = p.parse()
    {
        return (h.to_string(), port);
    }
    (input.to_string(), 9092)
}

/// Trust-on-first-use host key check. Stored as `host:port fingerprint` lines.
struct TofuHost {
    host: String,
    path: String,
}

impl russh::client::Handler for TofuHost {
    type Error = russh::Error;

    async fn check_server_key(&mut self, server_public_key: &russh::keys::PublicKey) -> Result<bool, Self::Error> {
        let fp = server_public_key.fingerprint(russh::keys::HashAlg::Sha256).to_string();
        if self.path.trim().is_empty() {
            return Ok(true);
        }
        let needle = format!("{} {fp}", self.host);
        let existing = std::fs::read_to_string(&self.path).unwrap_or_default();
        for line in existing.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            if let Some((host, stored)) = line.split_once(' ')
                && host == self.host
            {
                return Ok(stored == fp);
            }
        }
        let mut out = existing;
        if !out.is_empty() && !out.ends_with('\n') {
            out.push('\n');
        }
        out.push_str(&needle);
        out.push('\n');
        if let Some(parent) = std::path::Path::new(&self.path).parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        std::fs::write(&self.path, out).map_err(|_| russh::Error::Disconnect)?;
        Ok(true)
    }
}
