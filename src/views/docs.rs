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

use crate::helpers::format_unix_secs;
use crate::states::{i18n_common, i18n_kafka};
use gpui::{App, Entity, SharedString, Window, prelude::*, px};
use gpui_kit::component::{
    ActiveTheme,
    button::{Button, ButtonVariants},
    h_flex,
    input::{Input, InputState, Textarea, TextareaState},
    label::Label,
    table::{DataTable, TableState},
    v_flex,
};
use kaforge_kafka::{
    AclEntry, ConnectionHandle, ConsumeRequest, ConsumedRecord, ProduceRecord, StreamEvent, StreamSession,
};
use kaforge_ui::{TextColumn, TextTable};

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum DocKind {
    Topics,
    Nodes,
    Groups,
    Acl,
    Sr,
    Monitor,
    Producer,
    Consumer,
}

impl DocKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Topics => "topics",
            Self::Nodes => "nodes",
            Self::Groups => "groups",
            Self::Acl => "acl",
            Self::Sr => "sr",
            Self::Monitor => "monitor",
            Self::Producer => "producer",
            Self::Consumer => "consumer",
        }
    }

    pub fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "topics" => Self::Topics,
            "nodes" => Self::Nodes,
            "groups" => Self::Groups,
            "acl" => Self::Acl,
            "sr" => Self::Sr,
            "monitor" => Self::Monitor,
            "producer" => Self::Producer,
            "consumer" => Self::Consumer,
            _ => return None,
        })
    }

    pub fn all_nav() -> [Self; 6] {
        [
            Self::Topics,
            Self::Nodes,
            Self::Groups,
            Self::Acl,
            Self::Sr,
            Self::Monitor,
        ]
    }
}

pub struct DocPane {
    kind: DocKind,
    payload: Option<String>,
    handle: Option<ConnectionHandle>,
    status: SharedString,
    table: Entity<TableState<TextTable>>,
    topic: Entity<InputState>,
    extra: Entity<InputState>,
    body: Entity<TextareaState>,
    stream: Option<StreamSession>,
    consume_rows: Vec<ConsumedRecord>,
}

impl DocPane {
    pub fn new(
        kind: DocKind,
        payload: Option<String>,
        handle: Option<ConnectionHandle>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let cols = columns_for(kind, cx);
        let table = cx.new(|cx| TableState::new(TextTable::new(cols, i18n_common(cx, "copied")), window, cx));
        let topic = cx.new(|cx| InputState::new(window, cx).placeholder(i18n_kafka(cx, "topic_placeholder")));
        let extra = cx.new(|cx| InputState::new(window, cx).placeholder(i18n_kafka(cx, "extra_placeholder")));
        let body = cx.new(|cx| TextareaState::new(window, cx));
        if let Some(topic_name) = payload.as_deref() {
            topic.update(cx, |input, cx| input.set_value(topic_name, window, cx));
        }
        let mut pane = Self {
            kind,
            payload,
            handle,
            status: SharedString::default(),
            table,
            topic,
            extra,
            body,
            stream: None,
            consume_rows: Vec::new(),
        };
        pane.refresh(window, cx);
        pane
    }

    pub fn set_handle(&mut self, handle: ConnectionHandle, cx: &mut Context<Self>) {
        self.handle = Some(handle);
        cx.notify();
        self.refresh_from_cx(cx);
    }

    fn refresh_from_cx(&mut self, cx: &mut Context<Self>) {
        let handle = self.handle.clone();
        let kind = self.kind;
        let payload = self.payload.clone();
        self.status = i18n_kafka(cx, "loading");
        cx.notify();
        cx.spawn(async move |this, cx| {
            let result = smol::unblock(move || load_rows(kind, payload.as_deref(), handle.as_ref())).await;
            this.update(cx, |this, cx| {
                match result {
                    Ok(rows) => {
                        this.status = format!("{} {}", rows.len(), i18n_kafka(cx, "rows")).into();
                        this.table.update(cx, |state, cx| {
                            state.delegate_mut().set_rows(rows);
                            cx.notify();
                        });
                    }
                    Err(e) => this.status = e.into(),
                }
                cx.notify();
            })
            .ok();
        })
        .detach();
    }

    fn refresh(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.refresh_from_cx(cx);
    }

    fn produce(&mut self, cx: &mut Context<Self>) {
        let Some(handle) = self.handle.clone() else {
            self.status = i18n_kafka(cx, "not_connected");
            cx.notify();
            return;
        };
        let topic = self.topic.read(cx).value().to_string();
        let key = self.extra.read(cx).value().to_string();
        let value = self.body.read(cx).value().to_string();
        self.status = i18n_kafka(cx, "sending");
        cx.notify();
        cx.spawn(async move |this, cx| {
            let rec = ProduceRecord {
                topic,
                key,
                value,
                partition: None,
                headers: Vec::new(),
                count: 1,
                compression: String::new(),
            };
            let result = smol::unblock(move || handle.produce(rec)).await;
            this.update(cx, |this, cx| {
                this.status = match result {
                    Ok(()) => i18n_kafka(cx, "produced"),
                    Err(e) => e.to_string().into(),
                };
                cx.notify();
            })
            .ok();
        })
        .detach();
    }

    fn consume_once(&mut self, cx: &mut Context<Self>) {
        let Some(handle) = self.handle.clone() else {
            self.status = i18n_kafka(cx, "not_connected");
            cx.notify();
            return;
        };
        let topic = self.topic.read(cx).value().to_string();
        let group = self.extra.read(cx).value().to_string();
        cx.spawn(async move |this, cx| {
            let req = ConsumeRequest {
                topic,
                group,
                from_beginning: false,
                max_messages: 50,
                commit: false,
            };
            let result = smol::unblock(move || handle.consume_once(req)).await;
            this.update(cx, |this, cx| {
                match result {
                    Ok(rows) => {
                        this.consume_rows = rows.clone();
                        this.status = format!("{} {}", rows.len(), i18n_kafka(cx, "rows")).into();
                        this.table.update(cx, |state, cx| {
                            state.delegate_mut().set_rows(records_to_rows(&rows));
                            cx.notify();
                        });
                    }
                    Err(e) => this.status = e.to_string().into(),
                }
                cx.notify();
            })
            .ok();
        })
        .detach();
    }

    fn start_stream(&mut self, cx: &mut Context<Self>) {
        let Some(handle) = self.handle.clone() else {
            return;
        };
        let topic = self.topic.read(cx).value().to_string();
        let group = self.extra.read(cx).value().to_string();
        match handle.start_stream(ConsumeRequest {
            topic,
            group,
            from_beginning: false,
            max_messages: 10_000,
            commit: false,
        }) {
            Ok(session) => {
                self.stream = Some(session);
                self.status = i18n_kafka(cx, "streaming");
                self.poll_stream(cx);
            }
            Err(e) => self.status = e.to_string().into(),
        }
        cx.notify();
    }

    fn poll_stream(&mut self, cx: &mut Context<Self>) {
        cx.spawn(async move |this, cx| {
            loop {
                smol::Timer::after(std::time::Duration::from_millis(200)).await;
                let cont = this
                    .update(cx, |this, cx| {
                        let Some(session) = this.stream.as_mut() else {
                            return false;
                        };
                        while let Ok(ev) = session.rx.try_recv() {
                            match ev {
                                StreamEvent::Message(rec) => {
                                    this.consume_rows.push(rec.clone());
                                    this.table.update(cx, |state, cx| {
                                        state.delegate_mut().push_front(record_row(&rec));
                                        cx.notify();
                                    });
                                }
                                StreamEvent::Error(e) => this.status = e.into(),
                                StreamEvent::Ended => {
                                    this.stream = None;
                                    return false;
                                }
                            }
                        }
                        true
                    })
                    .unwrap_or(false);
                if !cont {
                    break;
                }
            }
        })
        .detach();
    }

    fn create_topic(&mut self, cx: &mut Context<Self>) {
        let Some(handle) = self.handle.clone() else {
            return;
        };
        let name = self.topic.read(cx).value().to_string();
        if name.trim().is_empty() {
            return;
        }
        cx.spawn(async move |this, cx| {
            let names = vec![name];
            let result = smol::unblock(move || handle.create_topics(&names, 1, 1)).await;
            this.update(cx, |this, cx| {
                this.status = match result {
                    Ok(()) => i18n_kafka(cx, "created"),
                    Err(e) => e.to_string().into(),
                };
                this.refresh_from_cx(cx);
            })
            .ok();
        })
        .detach();
    }

    fn delete_selected_topic(&mut self, cx: &mut Context<Self>) {
        let Some(handle) = self.handle.clone() else {
            return;
        };
        let name = self.topic.read(cx).value().to_string();
        if name.trim().is_empty() {
            return;
        }
        cx.spawn(async move |this, cx| {
            let names = vec![name];
            let result = smol::unblock(move || handle.delete_topics(&names)).await;
            this.update(cx, |this, cx| {
                this.status = match result {
                    Ok(()) => i18n_kafka(cx, "deleted"),
                    Err(e) => e.to_string().into(),
                };
                this.refresh_from_cx(cx);
            })
            .ok();
        })
        .detach();
    }
}

impl Render for DocPane {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let status = self.status.clone();
        let muted = cx.theme().muted_foreground;
        v_flex()
            .size_full()
            .min_h_0()
            .gap_2()
            .p_3()
            .child(toolbar(self, cx))
            .child(Label::new(status).text_xs().text_color(muted))
            .child(v_flex().flex_1().min_h_0().child(DataTable::new(&self.table)))
    }
}

fn toolbar(pane: &DocPane, cx: &mut Context<DocPane>) -> impl IntoElement {
    match pane.kind {
        DocKind::Topics => h_flex()
            .gap_2()
            .child(Input::new(&pane.topic).h(px(32.)).w(px(220.)))
            .child(
                Button::new("refresh")
                    .label(i18n_kafka(cx, "refresh"))
                    .on_click(cx.listener(|this, _, window, cx| this.refresh(window, cx))),
            )
            .child(
                Button::new("create")
                    .primary()
                    .label(i18n_kafka(cx, "create_topic"))
                    .on_click(cx.listener(|this, _, _, cx| this.create_topic(cx))),
            )
            .child(
                Button::new("delete")
                    .danger()
                    .label(i18n_kafka(cx, "delete_topic"))
                    .on_click(cx.listener(|this, _, _, cx| this.delete_selected_topic(cx))),
            )
            .into_any_element(),
        DocKind::Producer => h_flex()
            .gap_2()
            .child(Input::new(&pane.topic).h(px(32.)).w(px(180.)))
            .child(Input::new(&pane.extra).h(px(32.)).w(px(140.)))
            .child(Textarea::new(&pane.body).h(px(32.)).w(px(280.)))
            .child(
                Button::new("produce")
                    .primary()
                    .label(i18n_kafka(cx, "produce"))
                    .on_click(cx.listener(|this, _, _, cx| this.produce(cx))),
            )
            .into_any_element(),
        DocKind::Consumer => h_flex()
            .gap_2()
            .child(Input::new(&pane.topic).h(px(32.)).w(px(180.)))
            .child(Input::new(&pane.extra).h(px(32.)).w(px(140.)))
            .child(
                Button::new("poll")
                    .label(i18n_kafka(cx, "poll"))
                    .on_click(cx.listener(|this, _, _, cx| this.consume_once(cx))),
            )
            .child(
                Button::new("stream")
                    .primary()
                    .label(i18n_kafka(cx, "stream"))
                    .on_click(cx.listener(|this, _, _, cx| this.start_stream(cx))),
            )
            .into_any_element(),
        _ => h_flex()
            .gap_2()
            .child(
                Button::new("refresh")
                    .label(i18n_kafka(cx, "refresh"))
                    .on_click(cx.listener(|this, _, window, cx| this.refresh(window, cx))),
            )
            .into_any_element(),
    }
}

fn columns_for(kind: DocKind, cx: &App) -> Vec<TextColumn> {
    match kind {
        DocKind::Topics => vec![
            TextColumn::new("name", i18n_kafka(cx, "col_name"), 220.).sortable(),
            TextColumn::new("partitions", i18n_kafka(cx, "col_partitions"), 90.)
                .sortable()
                .numeric(),
            TextColumn::new("replication", i18n_kafka(cx, "col_replication"), 90.)
                .sortable()
                .numeric(),
            TextColumn::new("error", i18n_kafka(cx, "col_error"), 200.),
        ],
        DocKind::Nodes => vec![
            TextColumn::new("id", i18n_kafka(cx, "col_id"), 80.)
                .sortable()
                .numeric(),
            TextColumn::new("host", i18n_kafka(cx, "col_host"), 240.).sortable(),
            TextColumn::new("port", i18n_kafka(cx, "col_port"), 80.)
                .sortable()
                .numeric(),
        ],
        DocKind::Groups => vec![
            TextColumn::new("id", i18n_kafka(cx, "col_id"), 240.).sortable(),
            TextColumn::new("state", i18n_kafka(cx, "col_state"), 120.),
            TextColumn::new("protocol", i18n_kafka(cx, "col_protocol"), 120.),
        ],
        DocKind::Acl => vec![
            TextColumn::new("principal", i18n_kafka(cx, "col_principal"), 180.),
            TextColumn::new("op", i18n_kafka(cx, "col_operation"), 120.),
            TextColumn::new("perm", i18n_kafka(cx, "col_permission"), 100.),
            TextColumn::new("resource", i18n_kafka(cx, "col_resource"), 200.),
        ],
        DocKind::Sr => vec![TextColumn::new("subject", i18n_kafka(cx, "col_subject"), 280.).sortable()],
        DocKind::Monitor | DocKind::Consumer => vec![
            TextColumn::new("topic", i18n_kafka(cx, "col_topic"), 160.),
            TextColumn::new("partition", i18n_kafka(cx, "col_partition"), 80.).numeric(),
            TextColumn::new("offset", i18n_kafka(cx, "col_offset"), 100.).numeric(),
            TextColumn::new("time", i18n_kafka(cx, "col_time"), 180.),
            TextColumn::new("key", i18n_kafka(cx, "col_key"), 140.),
            TextColumn::new("value", i18n_kafka(cx, "col_value"), 320.),
        ],
        DocKind::Producer => vec![TextColumn::new("info", i18n_kafka(cx, "col_info"), 400.)],
    }
}

fn load_rows(
    kind: DocKind,
    _payload: Option<&str>,
    handle: Option<&ConnectionHandle>,
) -> std::result::Result<Vec<Vec<SharedString>>, String> {
    let Some(handle) = handle else {
        return Ok(Vec::new());
    };
    match kind {
        DocKind::Topics => Ok(handle
            .list_topics()
            .map_err(|e| e.to_string())?
            .into_iter()
            .map(|t| {
                vec![
                    t.name.into(),
                    t.partitions.to_string().into(),
                    t.replication.to_string().into(),
                    t.error.into(),
                ]
            })
            .collect()),
        DocKind::Nodes => Ok(handle
            .list_brokers()
            .map_err(|e| e.to_string())?
            .into_iter()
            .map(|b| vec![b.id.to_string().into(), b.host.into(), b.port.to_string().into()])
            .collect()),
        DocKind::Groups => Ok(handle
            .list_groups()
            .map_err(|e| e.to_string())?
            .into_iter()
            .map(|g| vec![g.id.into(), g.state.into(), g.protocol.into()])
            .collect()),
        DocKind::Acl => match handle.list_acls() {
            Ok(rows) => Ok(rows.into_iter().map(acl_row).collect()),
            Err(e) => Ok(vec![vec![e.to_string().into(), "".into(), "".into(), "".into()]]),
        },
        DocKind::Sr => match handle.sr() {
            Some(sr) => Ok(sr
                .subjects()
                .map_err(|e| e.to_string())?
                .into_iter()
                .map(|s| vec![s.into()])
                .collect()),
            None => Ok(vec![vec![SharedString::from("Schema Registry URL is not set")]]),
        },
        DocKind::Monitor => {
            let topics = handle.list_topics().map_err(|e| e.to_string())?;
            let mut rows = Vec::new();
            for t in topics.into_iter().take(20) {
                if let Ok(parts) = handle.partitions(&t.name) {
                    for p in parts.into_iter().take(8) {
                        if let Ok((low, high)) = handle.watermarks(&t.name, p.id) {
                            rows.push(vec![
                                t.name.clone().into(),
                                p.id.to_string().into(),
                                format!("{low}..{high}").into(),
                                "".into(),
                                "".into(),
                                format!("lag-window {}", high.saturating_sub(low)).into(),
                            ]);
                        }
                    }
                }
            }
            Ok(rows)
        }
        DocKind::Producer | DocKind::Consumer => Ok(Vec::new()),
    }
}

fn acl_row(a: AclEntry) -> Vec<SharedString> {
    vec![
        a.principal.into(),
        a.operation.into(),
        a.permission.into(),
        format!("{}:{}", a.resource_type, a.resource_name).into(),
    ]
}

fn record_row(rec: &ConsumedRecord) -> Vec<SharedString> {
    vec![
        rec.topic.clone().into(),
        rec.partition.to_string().into(),
        rec.offset.to_string().into(),
        rec.timestamp_ms
            .map(|ms| ms / 1000)
            .and_then(format_unix_secs)
            .unwrap_or_default()
            .into(),
        rec.key.clone().into(),
        rec.value.clone().into(),
    ]
}

fn records_to_rows(rows: &[ConsumedRecord]) -> Vec<Vec<SharedString>> {
    rows.iter().map(record_row).collect()
}
