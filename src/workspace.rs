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

use crate::connections;
use crate::states::{
    GlobalStore, NotificationAction, PersistedSession, PersistedTab, notify, update_app_state_and_save_quiet,
};
use crate::views::docs::{DocKind, DocPane};
use gpui::{App, Entity, Window, prelude::*};
use kaforge_kafka::{ConnectionConfig, ConnectionHandle, prepare_config};
use tracing::error;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SessionStatus {
    Idle,
    Connecting,
    Live,
}

pub struct Session {
    pub connection_id: String,
    pub name: String,
    pub status: SessionStatus,
    pub error: Option<String>,
    pub handle: Option<ConnectionHandle>,
    pub tabs: Vec<DocTab>,
    pub active_tab: usize,
}

pub struct DocTab {
    pub kind: DocKind,
    pub payload: Option<String>,
    pub pane: Entity<DocPane>,
}

pub struct Workspace {
    pub saved: Vec<ConnectionConfig>,
    pub sessions: Vec<Session>,
    pub active_id: Option<String>,
}

impl Workspace {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let saved = connections::load().unwrap_or_else(|e| {
            error!(error = %e, "load connections.toml");
            Vec::new()
        });
        let (persisted, active_id) = {
            let store = cx.global::<GlobalStore>().read(cx);
            (
                store.open_sessions().to_vec(),
                store.active_connection_id().map(str::to_string),
            )
        };
        let mut sessions = Vec::new();
        for spec in persisted {
            let Some(cfg) = saved.iter().find(|c| c.id == spec.connection_id) else {
                continue;
            };
            let mut tabs = Vec::new();
            for tab in spec.tabs {
                let kind = DocKind::from_name(&tab.kind).unwrap_or(DocKind::Topics);
                let pane = cx.new(|cx| DocPane::new(kind, tab.payload.clone(), None, window, cx));
                tabs.push(DocTab {
                    kind,
                    payload: tab.payload,
                    pane,
                });
            }
            if tabs.is_empty() {
                tabs.push(Self::topics_tab(window, cx, None));
            }
            sessions.push(Session {
                connection_id: cfg.id.clone(),
                name: cfg.display_name().to_string(),
                status: SessionStatus::Idle,
                error: None,
                handle: None,
                active_tab: spec.active_tab.min(tabs.len().saturating_sub(1)),
                tabs,
            });
        }
        let active_id = active_id.filter(|id| sessions.iter().any(|s| &s.connection_id == id));
        Self {
            saved,
            sessions,
            active_id,
        }
    }

    fn topics_tab(window: &mut Window, cx: &mut Context<Self>, handle: Option<ConnectionHandle>) -> DocTab {
        let pane = cx.new(|cx| DocPane::new(DocKind::Topics, None, handle, window, cx));
        DocTab {
            kind: DocKind::Topics,
            payload: None,
            pane,
        }
    }

    pub fn reload_saved(&mut self, cx: &mut Context<Self>) {
        self.saved = connections::load().unwrap_or_else(|e| {
            error!(error = %e, "reload connections.toml");
            Vec::new()
        });
        cx.notify();
    }

    pub fn active_session(&self) -> Option<&Session> {
        let id = self.active_id.as_ref()?;
        self.sessions.iter().find(|s| &s.connection_id == id)
    }

    pub fn active_session_mut(&mut self) -> Option<&mut Session> {
        let id = self.active_id.clone()?;
        self.sessions.iter_mut().find(|s| s.connection_id == id)
    }

    pub fn is_open(&self, id: &str) -> bool {
        self.sessions.iter().any(|s| s.connection_id == id)
    }

    pub fn persist(&self, cx: &mut App) {
        let sessions: Vec<PersistedSession> = self
            .sessions
            .iter()
            .map(|s| PersistedSession {
                connection_id: s.connection_id.clone(),
                tabs: s
                    .tabs
                    .iter()
                    .map(|t| PersistedTab {
                        kind: t.kind.as_str().to_string(),
                        payload: t.payload.clone(),
                    })
                    .collect(),
                active_tab: s.active_tab,
            })
            .collect();
        let active = self.active_id.clone();
        update_app_state_and_save_quiet(cx, "save_sessions", move |state, _| {
            state.set_open_sessions(sessions.clone(), active.clone());
        });
    }

    pub fn open_saved(&mut self, id: &str, window: &mut Window, cx: &mut Context<Self>) {
        if self.is_open(id) {
            self.active_id = Some(id.to_string());
            self.ensure_connected(cx);
            self.persist(cx);
            cx.notify();
            return;
        }
        let Some(cfg) = self.saved.iter().find(|c| c.id == id).cloned() else {
            return;
        };
        let tab = Self::topics_tab(window, cx, None);
        self.sessions.push(Session {
            connection_id: cfg.id.clone(),
            name: cfg.display_name().to_string(),
            status: SessionStatus::Idle,
            error: None,
            handle: None,
            tabs: vec![tab],
            active_tab: 0,
        });
        self.active_id = Some(cfg.id);
        self.ensure_connected(cx);
        self.persist(cx);
        cx.notify();
    }

    pub fn close_session(&mut self, id: &str, cx: &mut Context<Self>) {
        self.sessions.retain(|s| s.connection_id != id);
        if self.active_id.as_deref() == Some(id) {
            self.active_id = self.sessions.first().map(|s| s.connection_id.clone());
        }
        self.persist(cx);
        cx.notify();
    }

    pub fn activate(&mut self, id: &str, cx: &mut Context<Self>) {
        if self.is_open(id) {
            self.active_id = Some(id.to_string());
            self.ensure_connected(cx);
            self.persist(cx);
            cx.notify();
        }
    }

    pub fn open_doc(&mut self, kind: DocKind, payload: Option<String>, window: &mut Window, cx: &mut Context<Self>) {
        let handle = self.active_session().and_then(|s| s.handle.clone());
        let Some(session) = self.active_session_mut() else {
            return;
        };
        if !matches!(kind, DocKind::Producer | DocKind::Consumer)
            && let Some(ix) = session.tabs.iter().position(|t| t.kind == kind)
        {
            session.active_tab = ix;
            self.persist(cx);
            cx.notify();
            return;
        }
        let pane = cx.new(|cx| DocPane::new(kind, payload.clone(), handle, window, cx));
        session.tabs.push(DocTab { kind, payload, pane });
        session.active_tab = session.tabs.len() - 1;
        self.persist(cx);
        cx.notify();
    }

    pub fn close_tab(&mut self, ix: usize, cx: &mut Context<Self>) {
        let id = self.active_id.clone();
        let Some(session) = self.active_session_mut() else {
            return;
        };
        if ix >= session.tabs.len() {
            return;
        }
        session.tabs.remove(ix);
        if session.tabs.is_empty() {
            if let Some(id) = id {
                self.close_session(&id, cx);
            }
            return;
        }
        if session.active_tab >= session.tabs.len() {
            session.active_tab = session.tabs.len() - 1;
        } else if session.active_tab > ix {
            session.active_tab -= 1;
        }
        self.persist(cx);
        cx.notify();
    }

    pub fn activate_tab(&mut self, ix: usize, cx: &mut Context<Self>) {
        let Some(session) = self.active_session_mut() else {
            return;
        };
        if ix < session.tabs.len() {
            session.active_tab = ix;
            self.persist(cx);
            cx.notify();
        }
    }

    pub fn close_other_tabs(&mut self, ix: usize, cx: &mut Context<Self>) {
        let Some(session) = self.active_session_mut() else {
            return;
        };
        if ix >= session.tabs.len() || session.tabs.len() <= 1 {
            return;
        }
        let keep = session.tabs.remove(ix);
        session.tabs.clear();
        session.tabs.push(keep);
        session.active_tab = 0;
        self.persist(cx);
        cx.notify();
    }

    pub fn close_tabs_to_right(&mut self, ix: usize, cx: &mut Context<Self>) {
        let Some(session) = self.active_session_mut() else {
            return;
        };
        if ix + 1 >= session.tabs.len() {
            return;
        }
        session.tabs.truncate(ix + 1);
        if session.active_tab > ix {
            session.active_tab = ix;
        }
        self.persist(cx);
        cx.notify();
    }

    pub fn move_tab(&mut self, from: usize, to: usize, cx: &mut Context<Self>) {
        let Some(session) = self.active_session_mut() else {
            return;
        };
        if from == to || from >= session.tabs.len() || to >= session.tabs.len() {
            return;
        }
        let tab = session.tabs.remove(from);
        session.tabs.insert(to, tab);
        session.active_tab = crate::root::moved_active_index(session.active_tab, from, to);
        self.persist(cx);
        cx.notify();
    }

    fn ensure_connected(&mut self, cx: &mut Context<Self>) {
        let Some(id) = self.active_id.clone() else {
            return;
        };
        let Some(ix) = self.sessions.iter().position(|s| s.connection_id == id) else {
            return;
        };
        if self.sessions[ix].handle.is_some() || self.sessions[ix].status == SessionStatus::Connecting {
            return;
        }
        let Some(mut cfg) = self.saved.iter().find(|c| c.id == id).cloned() else {
            return;
        };
        if cfg.ssh
            && cfg.ssh_known_hosts_path.trim().is_empty()
            && let Ok(dir) = crate::helpers::get_or_create_config_dir()
        {
            cfg.ssh_known_hosts_path = dir.join("known_hosts").display().to_string();
        }
        self.sessions[ix].status = SessionStatus::Connecting;
        self.sessions[ix].error = None;
        cx.notify();
        cx.spawn(async move |this, cx| {
            let result = smol::unblock(move || prepare_config(cfg).and_then(ConnectionHandle::connect)).await;
            this.update(cx, |this, cx| {
                let Some(session) = this.sessions.iter_mut().find(|s| s.connection_id == id) else {
                    return;
                };
                match result {
                    Ok(handle) => {
                        session.status = SessionStatus::Live;
                        session.error = None;
                        for tab in &session.tabs {
                            tab.pane.update(cx, |pane, cx| pane.set_handle(handle.clone(), cx));
                        }
                        session.handle = Some(handle);
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        session.status = SessionStatus::Idle;
                        session.error = Some(msg.clone());
                        for tab in &session.tabs {
                            tab.pane.update(cx, |pane, cx| pane.set_error(msg.clone(), cx));
                        }
                        notify(cx, NotificationAction::new_error(msg.into()));
                    }
                }
                cx.notify();
            })
            .ok();
        })
        .detach();
    }
}
