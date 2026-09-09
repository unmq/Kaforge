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
use crate::states::{NotificationAction, dialog_button_props, i18n_common, i18n_kafka, notify};
use crate::workspace::Workspace;
use gpui::{App, ClipboardItem, Entity, WeakEntity, Window, div, prelude::*, px};
use gpui_kit::component::{
    ActiveTheme, StyledExt,
    button::{Button, ButtonVariants},
    h_flex,
    input::{Input, InputState, Textarea, TextareaState},
    label::Label,
    v_flex,
};
use kaforge_kafka::{ConnectionConfig, ConnectionHandle, SaslMechanism, prepare_config};
use kaforge_ui::Dialog;

pub struct ConnectionPicker {
    workspace: WeakEntity<Workspace>,
    name: Entity<InputState>,
    bootstrap: Entity<InputState>,
    sasl_user: Entity<InputState>,
    sasl_pwd: Entity<InputState>,
    sr_url: Entity<InputState>,
    mechanism: Entity<InputState>,
    ssh_host: Entity<InputState>,
    ssh_user: Entity<InputState>,
    kerberos: Entity<InputState>,
    msk_region: Entity<InputState>,
    webhook: Entity<InputState>,
    yaml: Entity<TextareaState>,
    tls: bool,
    ssh: bool,
    selected: Option<String>,
}

impl ConnectionPicker {
    pub fn new(workspace: Entity<Workspace>, window: &mut Window, cx: &mut Context<Self>) -> Self {
        Self {
            workspace: workspace.downgrade(),
            name: cx.new(|cx| InputState::new(window, cx).placeholder("prod")),
            bootstrap: cx.new(|cx| InputState::new(window, cx).placeholder("localhost:9092")),
            sasl_user: cx.new(|cx| InputState::new(window, cx).placeholder("user")),
            sasl_pwd: cx.new(|cx| InputState::new(window, cx).masked(true).placeholder("password")),
            sr_url: cx.new(|cx| InputState::new(window, cx).placeholder("http://localhost:8081")),
            mechanism: cx.new(|cx| {
                InputState::new(window, cx).placeholder("PLAIN / SCRAM-SHA-512 / GSSAPI / OAUTHBEARER / AWS-MSK-IAM")
            }),
            ssh_host: cx.new(|cx| InputState::new(window, cx).placeholder("bastion.example")),
            ssh_user: cx.new(|cx| InputState::new(window, cx).placeholder("ec2-user")),
            kerberos: cx.new(|cx| InputState::new(window, cx).placeholder("kafka/principal@REALM")),
            msk_region: cx.new(|cx| InputState::new(window, cx).placeholder("us-east-1")),
            webhook: cx.new(|cx| InputState::new(window, cx).placeholder("https://hooks.example/lag")),
            yaml: cx.new(|cx| TextareaState::new(window, cx)),
            tls: false,
            ssh: false,
            selected: None,
        }
    }

    fn saved(&self, cx: &App) -> Vec<ConnectionConfig> {
        self.workspace
            .upgrade()
            .map(|w| w.read(cx).saved.clone())
            .unwrap_or_default()
    }

    fn save_new(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let mut cfg = if let Some(id) = &self.selected {
            self.saved(cx)
                .into_iter()
                .find(|c| &c.id == id)
                .unwrap_or_else(ConnectionConfig::new_blank)
        } else {
            ConnectionConfig::new_blank()
        };
        cfg.name = self.name.read(cx).value().to_string();
        cfg.bootstrap_servers = self.bootstrap.read(cx).value().to_string();
        cfg.sasl_user = self.sasl_user.read(cx).value().to_string();
        cfg.sasl_password = self.sasl_pwd.read(cx).value().to_string();
        cfg.sasl = !cfg.sasl_user.is_empty();
        cfg.sr.url = self.sr_url.read(cx).value().to_string();
        cfg.tls = self.tls;
        cfg.ssh = self.ssh;
        cfg.ssh_host = self.ssh_host.read(cx).value().to_string();
        cfg.ssh_user = self.ssh_user.read(cx).value().to_string();
        cfg.kerberos_principal = self.kerberos.read(cx).value().to_string();
        cfg.msk_region = self.msk_region.read(cx).value().to_string();
        cfg.monitor_webhook = self.webhook.read(cx).value().to_string();
        if cfg.monitor_lag_threshold == 0 && !cfg.monitor_webhook.is_empty() {
            cfg.monitor_lag_threshold = 10_000;
        }
        cfg.sasl_mechanism = SaslMechanism::from_king(&self.mechanism.read(cx).value());
        if cfg.sasl_mechanism == SaslMechanism::Gssapi
            || cfg.sasl_mechanism == SaslMechanism::Oauthbearer
            || cfg.sasl_mechanism == SaslMechanism::AwsMskIam
        {
            cfg.sasl = true;
        }
        if cfg.bootstrap_servers.trim().is_empty() {
            notify(cx, NotificationAction::new_error("bootstrap servers required".into()));
            return;
        }
        match connections::upsert(cfg.clone()) {
            Ok(_) => {
                if let Some(ws) = self.workspace.upgrade() {
                    ws.update(cx, |ws, cx| {
                        ws.reload_saved(cx);
                        ws.open_saved(&cfg.id, window, cx);
                    });
                }
            }
            Err(e) => notify(cx, NotificationAction::new_error(e.to_string().into())),
        }
    }

    fn open_selected(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(id) = self.selected.clone() else {
            return;
        };
        if let Some(ws) = self.workspace.upgrade() {
            ws.update(cx, |ws, cx| ws.open_saved(&id, window, cx));
        }
    }

    fn delete_selected(&mut self, cx: &mut Context<Self>) {
        let Some(id) = self.selected.clone() else {
            return;
        };
        if let Err(e) = connections::remove(&id) {
            notify(cx, NotificationAction::new_error(e.to_string().into()));
            return;
        }
        self.selected = None;
        if let Some(ws) = self.workspace.upgrade() {
            ws.update(cx, |ws, cx| ws.reload_saved(cx));
        }
        cx.notify();
    }

    fn test_selected(&mut self, cx: &mut Context<Self>) {
        let Some(id) = &self.selected else {
            return;
        };
        let Some(cfg) = self.saved(cx).into_iter().find(|c| &c.id == id) else {
            return;
        };
        cx.spawn(async move |this, cx| {
            let result = smol::unblock(move || prepare_config(cfg).and_then(ConnectionHandle::test)).await;
            this.update(cx, |_this, cx| match result {
                Ok(()) => {
                    let msg = i18n_kafka(cx, "test_ok");
                    notify(cx, NotificationAction::new_success(msg));
                }
                Err(e) => notify(cx, NotificationAction::new_error(e.to_string().into())),
            })
            .ok();
        })
        .detach();
    }

    fn import_yaml(&mut self, text: String, cx: &mut Context<Self>) {
        match connections::import_king_yaml(&text) {
            Ok(list) => {
                let mut n = 0;
                for cfg in list {
                    if connections::upsert(cfg).is_ok() {
                        n += 1;
                    }
                }
                if let Some(ws) = self.workspace.upgrade() {
                    ws.update(cx, |ws, cx| ws.reload_saved(cx));
                }
                notify(
                    cx,
                    NotificationAction::new_success(format!("imported {n} connections").into()),
                );
                cx.notify();
            }
            Err(e) => notify(cx, NotificationAction::new_error(e.to_string().into())),
        }
    }
}

impl Render for ConnectionPicker {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let saved = self.saved(cx);
        let selected = self.selected.clone();
        v_flex()
            .gap_3()
            .w(px(520.))
            .child(Label::new(i18n_kafka(cx, "saved_title")).font_bold())
            .child(v_flex().gap_1().max_h(px(180.)).children(saved.into_iter().map(|c| {
                let id = c.id.clone();
                let active = selected.as_deref() == Some(&id);
                let label = format!("{}  {}", c.display_name(), c.bootstrap_servers);
                Button::new(format!("saved-conn-{id}"))
                    .when(active, |b| b.primary())
                    .when(!active, |b| b.ghost())
                    .label(label)
                    .on_click(cx.listener(move |this, _, window, cx| {
                        this.selected = Some(id.clone());
                        if let Some(cfg) = this.saved(cx).into_iter().find(|c| c.id == id) {
                            this.name.update(cx, |input, cx| input.set_value(&cfg.name, window, cx));
                            this.bootstrap
                                .update(cx, |input, cx| input.set_value(&cfg.bootstrap_servers, window, cx));
                            this.sasl_user
                                .update(cx, |input, cx| input.set_value(&cfg.sasl_user, window, cx));
                            this.sasl_pwd
                                .update(cx, |input, cx| input.set_value(&cfg.sasl_password, window, cx));
                            this.sr_url
                                .update(cx, |input, cx| input.set_value(&cfg.sr.url, window, cx));
                            this.mechanism.update(cx, |input, cx| {
                                input.set_value(cfg.sasl_mechanism.as_rdkafka(), window, cx)
                            });
                            this.ssh_host
                                .update(cx, |input, cx| input.set_value(&cfg.ssh_host, window, cx));
                            this.ssh_user
                                .update(cx, |input, cx| input.set_value(&cfg.ssh_user, window, cx));
                            this.kerberos
                                .update(cx, |input, cx| input.set_value(&cfg.kerberos_principal, window, cx));
                            this.msk_region
                                .update(cx, |input, cx| input.set_value(&cfg.msk_region, window, cx));
                            this.webhook
                                .update(cx, |input, cx| input.set_value(&cfg.monitor_webhook, window, cx));
                            this.tls = cfg.tls;
                            this.ssh = cfg.ssh;
                        }
                        cx.notify();
                    }))
            })))
            .child(
                h_flex()
                    .gap_2()
                    .child(
                        Button::new("open")
                            .primary()
                            .label(i18n_kafka(cx, "open"))
                            .on_click(cx.listener(|this, _, window, cx| this.open_selected(window, cx))),
                    )
                    .child(
                        Button::new("test")
                            .label(i18n_kafka(cx, "test"))
                            .on_click(cx.listener(|this, _, _, cx| this.test_selected(cx))),
                    )
                    .child(
                        Button::new("delete")
                            .danger()
                            .label(i18n_common(cx, "delete"))
                            .on_click(cx.listener(|this, _, _, cx| this.delete_selected(cx))),
                    ),
            )
            .child(div().h(px(1.)).bg(cx.theme().border))
            .child(Label::new(i18n_kafka(cx, "new_title")).font_bold())
            .child(Input::new(&self.name).h(px(32.)))
            .child(Input::new(&self.bootstrap).h(px(32.)))
            .child(
                h_flex()
                    .gap_2()
                    .child(Input::new(&self.sasl_user).h(px(32.)).flex_1())
                    .child(Input::new(&self.sasl_pwd).h(px(32.)).flex_1()),
            )
            .child(Input::new(&self.sr_url).h(px(32.)))
            .child(Label::new(i18n_kafka(cx, "sasl")).text_xs())
            .child(Input::new(&self.mechanism).h(px(32.)))
            .child(Label::new(i18n_kafka(cx, "kerberos")).text_xs())
            .child(Input::new(&self.kerberos).h(px(32.)))
            .child(Label::new(i18n_kafka(cx, "msk")).text_xs())
            .child(Input::new(&self.msk_region).h(px(32.)))
            .child(Label::new(i18n_kafka(cx, "webhook")).text_xs())
            .child(Input::new(&self.webhook).h(px(32.)))
            .child(
                h_flex()
                    .gap_2()
                    .child(
                        Button::new("tls")
                            .when(self.tls, |b| b.primary())
                            .when(!self.tls, |b| b.ghost())
                            .label(i18n_kafka(cx, "tls"))
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.tls = !this.tls;
                                cx.notify();
                            })),
                    )
                    .child(
                        Button::new("ssh")
                            .when(self.ssh, |b| b.primary())
                            .when(!self.ssh, |b| b.ghost())
                            .label(i18n_kafka(cx, "ssh"))
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.ssh = !this.ssh;
                                cx.notify();
                            })),
                    ),
            )
            .child(
                h_flex()
                    .gap_2()
                    .child(Input::new(&self.ssh_host).h(px(32.)).flex_1())
                    .child(Input::new(&self.ssh_user).h(px(32.)).flex_1()),
            )
            .child(Label::new(i18n_kafka(cx, "ssh_hint")).text_xs())
            .child(
                Button::new("save-open")
                    .primary()
                    .label(i18n_kafka(cx, "save_open"))
                    .on_click(cx.listener(|this, _, window, cx| this.save_new(window, cx))),
            )
            .child(div().h(px(1.)).bg(cx.theme().border))
            .child(Label::new(i18n_kafka(cx, "import_yaml")).font_bold())
            .child(Label::new(i18n_kafka(cx, "yaml_placeholder")).text_xs())
            .child(Textarea::new(&self.yaml).h(px(88.)))
            .child(
                h_flex()
                    .gap_2()
                    .child(
                        Button::new("import")
                            .label(i18n_kafka(cx, "import_yaml"))
                            .on_click(cx.listener(|this, _, _, cx| {
                                let text = this.yaml.read(cx).value().to_string();
                                this.import_yaml(text, cx);
                            })),
                    )
                    .child(
                        Button::new("export")
                            .label(i18n_kafka(cx, "export"))
                            .on_click(cx.listener(|this, _, _window, cx| {
                                match connections::export_toml(&this.saved(cx)) {
                                    Ok(text) => {
                                        cx.write_to_clipboard(ClipboardItem::new_string(text));
                                        let msg = i18n_common(cx, "copied");
                                        notify(cx, NotificationAction::new_success(msg));
                                    }
                                    Err(e) => notify(cx, NotificationAction::new_error(e.to_string().into())),
                                }
                            })),
                    ),
            )
    }
}

pub fn open_connection_picker(workspace: Entity<Workspace>, window: &mut Window, cx: &mut App) {
    let view = cx.new(|cx| ConnectionPicker::new(workspace, window, cx));
    Dialog::new(i18n_kafka(cx, "picker_title"))
        .button_props(dialog_button_props(cx))
        .ok_text(i18n_common(cx, "cancel"))
        .child(move || view.clone())
        .overlay_closable(true)
        .open(window, cx);
}
