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
use gpui::{App, Entity, SharedString, Window, div, prelude::*, px};
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
use kaforge_ui::{Combobox, TextColumn, TextTable};

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

    pub fn all_nav() -> [Self; 8] {
        [
            Self::Topics,
            Self::Nodes,
            Self::Groups,
            Self::Acl,
            Self::Sr,
            Self::Monitor,
            Self::Producer,
            Self::Consumer,
        ]
    }

    pub fn icon(self) -> gpui_kit::component::IconName {
        use gpui_kit::component::IconName;
        match self {
            Self::Topics => IconName::FileText,
            Self::Nodes => IconName::HardDrive,
            Self::Groups => IconName::User,
            Self::Acl => IconName::Asterisk,
            Self::Sr => IconName::BookOpen,
            Self::Monitor => IconName::ChartPie,
            Self::Producer => IconName::ArrowUp,
            Self::Consumer => IconName::Inbox,
        }
    }
}

#[derive(Clone)]
struct LagPoint {
    label: String,
    lag: f64,
}

pub struct DocPane {
    kind: DocKind,
    payload: Option<String>,
    handle: Option<ConnectionHandle>,
    status: SharedString,
    table: Entity<TableState<TextTable>>,
    topic: Entity<InputState>,
    topic_combo: Entity<Combobox>,
    extra: Entity<InputState>,
    body: Entity<TextareaState>,
    stream: Option<StreamSession>,
    consume_rows: Vec<ConsumedRecord>,
    from_beginning: bool,
    commit: bool,
    lag_points: Vec<LagPoint>,
    load_gen: u64,
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
        let topic_combo = cx.new(|cx| Combobox::new(Vec::new(), None, window, cx));
        let extra = cx.new(|cx| {
            let placeholder = match kind {
                DocKind::Groups => i18n_kafka(cx, "group_placeholder"),
                _ => i18n_kafka(cx, "extra_placeholder"),
            };
            InputState::new(window, cx).placeholder(placeholder)
        });
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
            topic_combo,
            extra,
            body,
            stream: None,
            consume_rows: Vec::new(),
            from_beginning: false,
            commit: false,
            lag_points: Vec::new(),
            load_gen: 0,
        };
        if pane.handle.is_some() {
            pane.refresh(window, cx);
        } else {
            pane.status = i18n_kafka(cx, "not_connected");
        }
        pane
    }

    pub fn set_handle(&mut self, handle: ConnectionHandle, cx: &mut Context<Self>) {
        self.handle = Some(handle);
        cx.notify();
        self.refresh_from_cx(cx);
    }

    pub fn set_error(&mut self, err: impl Into<SharedString>, cx: &mut Context<Self>) {
        self.status = err.into();
        cx.notify();
    }

    fn refresh_from_cx(&mut self, cx: &mut Context<Self>) {
        let Some(handle) = self.handle.clone() else {
            self.status = i18n_kafka(cx, "not_connected");
            cx.notify();
            return;
        };
        let kind = self.kind;
        let payload = self.payload.clone();
        self.load_gen = self.load_gen.saturating_add(1);
        let ticket = self.load_gen;
        self.status = i18n_kafka(cx, "loading");
        cx.notify();
        let handle_topics = handle.clone();
        let prefer = payload.clone();
        cx.spawn(async move |this, cx| {
            let names = smol::unblock(move || {
                handle_topics
                    .list_topics()
                    .map(|topics| topics.into_iter().map(|t| t.name).collect::<Vec<_>>())
            })
            .await;
            if let Ok(names) = names {
                this.update_in(cx, |this, window, cx| {
                    this.topic_combo.update(cx, |combo, cx| {
                        combo.set_items(names, prefer.as_deref(), window, cx);
                    });
                })
                .ok();
            }
        })
        .detach();
        cx.spawn(async move |this, cx| {
            let result = smol::unblock(move || load_rows(kind, payload.as_deref(), &handle)).await;
            this.update(cx, |this, cx| {
                if this.load_gen != ticket {
                    return;
                }
                match result {
                    Ok(rows) => {
                        if kind == DocKind::Monitor {
                            this.lag_points = rows
                                .iter()
                                .enumerate()
                                .map(|(i, row)| {
                                    let lag = row
                                        .get(3)
                                        .and_then(|s| s.split_whitespace().last())
                                        .and_then(|s| s.parse().ok())
                                        .unwrap_or(0.0);
                                    LagPoint {
                                        label: i.to_string(),
                                        lag,
                                    }
                                })
                                .collect();
                            let max_lag = this.lag_points.iter().map(|p| p.lag).fold(0.0, f64::max);
                            if let Some(handle) = &this.handle {
                                let url = handle.config.monitor_webhook.clone();
                                let threshold = handle.config.monitor_lag_threshold as f64;
                                if !url.is_empty() && threshold > 0.0 && max_lag > threshold {
                                    let body = format!("{{\"lag\":{max_lag}}}");
                                    cx.spawn(async move |_, _| {
                                        smol::unblock(move || {
                                            let _ =
                                                ureq::post(&url).header("Content-Type", "application/json").send(&body);
                                        })
                                        .await;
                                    })
                                    .detach();
                                }
                            }
                        }
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

    fn topic_name(&self, cx: &App) -> String {
        if matches!(self.kind, DocKind::Consumer | DocKind::Producer) {
            self.topic_combo.read(cx).selected_value(cx).unwrap_or_default()
        } else {
            self.topic.read(cx).value().to_string()
        }
    }

    fn produce(&mut self, cx: &mut Context<Self>) {
        let Some(handle) = self.handle.clone() else {
            self.status = i18n_kafka(cx, "not_connected");
            cx.notify();
            return;
        };
        let topic = self.topic_name(cx);
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
        let topic = self.topic_name(cx);
        let group = self.extra.read(cx).value().to_string();
        let from_beginning = self.from_beginning;
        let commit = self.commit;
        cx.spawn(async move |this, cx| {
            let req = ConsumeRequest {
                topic,
                group,
                from_beginning,
                max_messages: 50,
                commit,
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
        let topic = self.topic_name(cx);
        let group = self.extra.read(cx).value().to_string();
        match handle.start_stream(ConsumeRequest {
            topic,
            group,
            from_beginning: self.from_beginning,
            max_messages: 10_000,
            commit: self.commit,
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

    fn add_partitions(&mut self, cx: &mut Context<Self>) {
        let Some(handle) = self.handle.clone() else {
            return;
        };
        let name = self.topic.read(cx).value().to_string();
        let total = self.extra.read(cx).value().parse::<i32>().unwrap_or(0);
        if name.trim().is_empty() || total <= 0 {
            self.status = "topic and partition count required".into();
            cx.notify();
            return;
        }
        cx.spawn(async move |this, cx| {
            let result = smol::unblock(move || handle.add_partitions(&name, total)).await;
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

    fn delete_records(&mut self, cx: &mut Context<Self>) {
        let Some(handle) = self.handle.clone() else {
            return;
        };
        let name = self.topic.read(cx).value().to_string();
        if name.trim().is_empty() {
            return;
        }
        cx.spawn(async move |this, cx| {
            let result = smol::unblock(move || handle.delete_records(&name, None)).await;
            this.update(cx, |this, cx| {
                this.status = match result {
                    Ok(()) => i18n_kafka(cx, "deleted"),
                    Err(e) => e.to_string().into(),
                };
                cx.notify();
            })
            .ok();
        })
        .detach();
    }

    fn alter_named_config(&mut self, cx: &mut Context<Self>) {
        let Some(handle) = self.handle.clone() else {
            return;
        };
        let name = self.topic.read(cx).value().to_string();
        let spec = self.extra.read(cx).value().to_string();
        let Some((key, value)) = spec.split_once('=') else {
            self.status = "use extra field as key=value".into();
            cx.notify();
            return;
        };
        let key = key.to_string();
        let value = value.to_string();
        let kind = self.kind;
        cx.spawn(async move |this, cx| {
            let result = smol::unblock(move || match kind {
                DocKind::Nodes => {
                    let id = name.parse::<i32>().unwrap_or(0);
                    handle.alter_broker_config(id, &key, &value)
                }
                _ => handle.alter_topic_config(&name, &key, &value),
            })
            .await;
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

    fn reset_group(&mut self, cx: &mut Context<Self>) {
        let Some(handle) = self.handle.clone() else {
            return;
        };
        let group = self.extra.read(cx).value().to_string();
        let topic = self.topic_name(cx);
        let to_beginning = self.from_beginning;
        cx.spawn(async move |this, cx| {
            let result = smol::unblock(move || handle.reset_offsets(&group, &topic, to_beginning)).await;
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

    fn delete_group(&mut self, cx: &mut Context<Self>) {
        let Some(handle) = self.handle.clone() else {
            return;
        };
        let group = self.extra.read(cx).value().to_string();
        if group.trim().is_empty() {
            return;
        }
        cx.spawn(async move |this, cx| {
            let result = smol::unblock(move || handle.delete_group(&group)).await;
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

    fn export_csv(&mut self, cx: &mut Context<Self>) {
        let mut csv = String::from("topic,partition,offset,key,value\n");
        for rec in &self.consume_rows {
            csv.push_str(&format!(
                "{},{},{},{},{}\n",
                rec.topic,
                rec.partition,
                rec.offset,
                rec.key.replace(',', " "),
                rec.value.replace(['\n', ','], " ")
            ));
        }
        cx.write_to_clipboard(gpui::ClipboardItem::new_string(csv));
        self.status = i18n_common(cx, "copied");
        cx.notify();
    }

    fn export_json(&mut self, cx: &mut Context<Self>) {
        let rows: Vec<serde_json::Value> = self
            .consume_rows
            .iter()
            .map(|rec| {
                serde_json::json!({
                    "topic": rec.topic,
                    "partition": rec.partition,
                    "offset": rec.offset,
                    "key": rec.key,
                    "value": rec.value,
                })
            })
            .collect();
        let text = serde_json::to_string_pretty(&rows).unwrap_or_else(|_| "[]".into());
        cx.write_to_clipboard(gpui::ClipboardItem::new_string(text));
        self.status = i18n_common(cx, "copied");
        cx.notify();
    }

    fn replay(&mut self, cx: &mut Context<Self>) {
        let Some(handle) = self.handle.clone() else {
            return;
        };
        let dest = self.extra.read(cx).value().to_string();
        let rows = self.consume_rows.clone();
        if dest.trim().is_empty() || rows.is_empty() {
            return;
        }
        cx.spawn(async move |this, cx| {
            let result = smol::unblock(move || handle.replay(&rows, &dest)).await;
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

    fn filter_consumed(&mut self, cx: &mut Context<Self>) {
        let q = self.extra.read(cx).value().to_string().to_ascii_lowercase();
        let rows: Vec<_> = self
            .consume_rows
            .iter()
            .filter(|r| {
                q.is_empty() || r.key.to_ascii_lowercase().contains(&q) || r.value.to_ascii_lowercase().contains(&q)
            })
            .cloned()
            .collect();
        self.status = format!("{} {}", rows.len(), i18n_kafka(cx, "rows")).into();
        self.table.update(cx, |state, cx| {
            state.delegate_mut().set_rows(records_to_rows(&rows));
            cx.notify();
        });
        cx.notify();
    }

    fn delete_sr_subject(&mut self, cx: &mut Context<Self>) {
        let Some(handle) = self.handle.clone() else {
            return;
        };
        let name = self.topic.read(cx).value().to_string();
        cx.spawn(async move |this, cx| {
            let result = smol::unblock(move || {
                handle
                    .sr()
                    .ok_or_else(|| "no schema registry".to_string())
                    .and_then(|sr| sr.delete_subject(&name).map_err(|e| e.to_string()))
            })
            .await;
            this.update(cx, |this, cx| {
                this.status = match result {
                    Ok(()) => i18n_kafka(cx, "deleted"),
                    Err(e) => e.into(),
                };
                this.refresh_from_cx(cx);
            })
            .ok();
        })
        .detach();
    }

    fn set_sr_compat(&mut self, cx: &mut Context<Self>) {
        let Some(handle) = self.handle.clone() else {
            return;
        };
        let subject = self.topic.read(cx).value().to_string();
        let level = self.extra.read(cx).value().to_string();
        cx.spawn(async move |this, cx| {
            let result = smol::unblock(move || {
                handle
                    .sr()
                    .ok_or_else(|| "no schema registry".to_string())
                    .and_then(|sr| sr.set_compatibility(&subject, &level).map_err(|e| e.to_string()))
            })
            .await;
            this.update(cx, |this, cx| {
                this.status = match result {
                    Ok(()) => i18n_kafka(cx, "created"),
                    Err(e) => e.into(),
                };
                cx.notify();
            })
            .ok();
        })
        .detach();
    }

    fn touch_acl(&mut self, create: bool, cx: &mut Context<Self>) {
        let Some(handle) = self.handle.clone() else {
            return;
        };
        let principal = self.extra.read(cx).value().to_string();
        let resource = self.topic.read(cx).value().to_string();
        let acl = AclEntry {
            principal,
            host: "*".into(),
            operation: "All".into(),
            permission: "Allow".into(),
            resource_type: "Topic".into(),
            resource_name: resource,
        };
        cx.spawn(async move |this, cx| {
            let result = smol::unblock(move || {
                if create {
                    handle.create_acl(acl)
                } else {
                    handle.delete_acl(acl)
                }
            })
            .await;
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

    fn confirm_delete_topic(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let name = self.topic.read(cx).value().to_string();
        if name.trim().is_empty() {
            return;
        }
        let title = i18n_kafka(cx, "delete_topic");
        let body = i18n_kafka(cx, "confirm_delete").to_string().replace("{name}", &name);
        let entity = cx.entity();
        kaforge_ui::Dialog::new_alert(title, body)
            .button_props(crate::states::dialog_button_props(cx))
            .ok_text(i18n_common(cx, "delete"))
            .on_ok(move |_, _, cx| {
                entity.update(cx, |this, cx| this.delete_selected_topic(cx));
                true
            })
            .open(window, cx);
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
            .when(self.kind == DocKind::Monitor && !self.lag_points.is_empty(), |this| {
                let points = self.lag_points.clone();
                this.child(
                    div().h(px(160.)).w_full().child(
                        gpui_kit::component::chart::LineChart::new(points)
                            .x(|p: &LagPoint| p.label.clone())
                            .y(|p: &LagPoint| p.lag),
                    ),
                )
            })
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
                    .on_click(cx.listener(|this, _, window, cx| this.confirm_delete_topic(window, cx))),
            )
            .child(Input::new(&pane.extra).h(px(32.)).w(px(120.)))
            .child(
                Button::new("add-parts")
                    .label(i18n_kafka(cx, "add_partitions"))
                    .on_click(cx.listener(|this, _, _, cx| this.add_partitions(cx))),
            )
            .child(
                Button::new("del-recs")
                    .label(i18n_kafka(cx, "delete_records"))
                    .on_click(cx.listener(|this, _, _, cx| this.delete_records(cx))),
            )
            .child(
                Button::new("alter-topic")
                    .label(i18n_kafka(cx, "save_plan"))
                    .on_click(cx.listener(|this, _, _, cx| this.alter_named_config(cx))),
            )
            .into_any_element(),
        DocKind::Producer => h_flex()
            .gap_2()
            .child(div().h(px(32.)).w(px(220.)).child(pane.topic_combo.clone()))
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
            .child(div().h(px(32.)).w(px(220.)).child(pane.topic_combo.clone()))
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
            .when(pane.stream.is_some(), |this| {
                this.child(
                    Button::new("stop-stream")
                        .danger()
                        .label(i18n_kafka(cx, "stop_stream"))
                        .on_click(cx.listener(|this, _, _, cx| {
                            this.stream = None;
                            cx.notify();
                        })),
                )
            })
            .child(
                Button::new("from-beg")
                    .label(i18n_kafka(cx, "from_beginning"))
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.from_beginning = !this.from_beginning;
                        cx.notify();
                    })),
            )
            .child(
                Button::new("commit")
                    .label(i18n_kafka(cx, "commit"))
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.commit = !this.commit;
                        cx.notify();
                    })),
            )
            .child(
                Button::new("search")
                    .label(i18n_kafka(cx, "search"))
                    .on_click(cx.listener(|this, _, _, cx| this.filter_consumed(cx))),
            )
            .child(
                Button::new("export")
                    .label(i18n_kafka(cx, "export_csv"))
                    .on_click(cx.listener(|this, _, _, cx| this.export_csv(cx))),
            )
            .child(
                Button::new("export-json")
                    .label(i18n_kafka(cx, "export_json"))
                    .on_click(cx.listener(|this, _, _, cx| this.export_json(cx))),
            )
            .child(
                Button::new("replay")
                    .label(i18n_kafka(cx, "replay"))
                    .on_click(cx.listener(|this, _, _, cx| this.replay(cx))),
            )
            .into_any_element(),
        DocKind::Groups => h_flex()
            .gap_2()
            .child(Input::new(&pane.extra).h(px(32.)).w(px(180.)))
            .child(Input::new(&pane.topic).h(px(32.)).w(px(180.)))
            .child(
                Button::new("refresh")
                    .label(i18n_kafka(cx, "refresh"))
                    .on_click(cx.listener(|this, _, window, cx| this.refresh(window, cx))),
            )
            .child(
                Button::new("reset")
                    .label(i18n_kafka(cx, "reset_offsets"))
                    .on_click(cx.listener(|this, _, _, cx| this.reset_group(cx))),
            )
            .child(
                Button::new("del-group")
                    .danger()
                    .label(i18n_kafka(cx, "delete_group"))
                    .on_click(cx.listener(|this, _, _, cx| this.delete_group(cx))),
            )
            .into_any_element(),
        DocKind::Sr => h_flex()
            .gap_2()
            .child(Input::new(&pane.topic).h(px(32.)).w(px(220.)))
            .child(Input::new(&pane.extra).h(px(32.)).w(px(140.)))
            .child(
                Button::new("refresh")
                    .label(i18n_kafka(cx, "refresh"))
                    .on_click(cx.listener(|this, _, window, cx| this.refresh(window, cx))),
            )
            .child(
                Button::new("del-sr")
                    .danger()
                    .label(i18n_common(cx, "delete"))
                    .on_click(cx.listener(|this, _, _, cx| this.delete_sr_subject(cx))),
            )
            .child(
                Button::new("compat")
                    .label(i18n_kafka(cx, "save_plan"))
                    .on_click(cx.listener(|this, _, _, cx| this.set_sr_compat(cx))),
            )
            .into_any_element(),
        DocKind::Nodes => h_flex()
            .gap_2()
            .child(
                Button::new("refresh")
                    .label(i18n_kafka(cx, "refresh"))
                    .on_click(cx.listener(|this, _, window, cx| this.refresh(window, cx))),
            )
            .into_any_element(),
        DocKind::Monitor => h_flex()
            .gap_2()
            .child(
                Button::new("refresh")
                    .label(i18n_kafka(cx, "refresh"))
                    .on_click(cx.listener(|this, _, window, cx| this.refresh(window, cx))),
            )
            .into_any_element(),
        DocKind::Acl => h_flex()
            .gap_2()
            .child(Input::new(&pane.extra).h(px(32.)).w(px(160.)))
            .child(Input::new(&pane.topic).h(px(32.)).w(px(160.)))
            .child(
                Button::new("refresh")
                    .label(i18n_kafka(cx, "refresh"))
                    .on_click(cx.listener(|this, _, window, cx| this.refresh(window, cx))),
            )
            .child(
                Button::new("acl-add")
                    .label(i18n_kafka(cx, "create_acl"))
                    .on_click(cx.listener(|this, _, _, cx| this.touch_acl(true, cx))),
            )
            .child(
                Button::new("acl-del")
                    .danger()
                    .label(i18n_kafka(cx, "delete_acl"))
                    .on_click(cx.listener(|this, _, _, cx| this.touch_acl(false, cx))),
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
        DocKind::Monitor => vec![
            TextColumn::new("topic", i18n_kafka(cx, "col_topic"), 160.),
            TextColumn::new("partition", i18n_kafka(cx, "col_partition"), 80.).numeric(),
            TextColumn::new("offset", i18n_kafka(cx, "col_offset"), 100.).numeric(),
            TextColumn::new("lag", i18n_kafka(cx, "col_lag"), 100.).numeric(),
        ],
        DocKind::Consumer => vec![
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
    handle: &ConnectionHandle,
) -> std::result::Result<Vec<Vec<SharedString>>, String> {
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
        DocKind::Nodes => {
            let mut rows: Vec<Vec<SharedString>> = handle
                .list_brokers()
                .map_err(|e| e.to_string())?
                .into_iter()
                .map(|b| vec![b.id.to_string().into(), b.host.into(), b.port.to_string().into()])
                .collect();
            match handle.describe_log_dirs() {
                Ok(dirs) => {
                    for (k, v) in dirs {
                        rows.push(vec!["".into(), k.into(), v.into()]);
                    }
                }
                Err(e) => rows.push(vec!["".into(), "logdirs".into(), e.to_string().into()]),
            }
            Ok(rows)
        }
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
                                high.saturating_sub(low).to_string().into(),
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
