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
use gpui::{App, ClipboardItem, Div, Entity, MouseButton, SharedString, WeakEntity, Window, div, prelude::*, px};
use gpui_kit::component::{
    ActiveTheme, IconName, Sizable, WindowExt,
    button::{Button, ButtonVariants},
    checkbox::Checkbox,
    h_flex,
    input::{Input, InputState, Textarea, TextareaState},
    label::Label,
    list::ListItem,
    menu::{DropdownMenu, PopupMenuItem},
    switch::Switch,
    v_flex,
};
use kaforge_kafka::{ConnectionConfig, ConnectionHandle, SaslMechanism, prepare_config};
use kaforge_ui::{Dialog, Select};

struct SavedPicker {
    workspace: WeakEntity<Workspace>,
    yaml: Entity<TextareaState>,
    yaml_open: bool,
}

impl SavedPicker {
    fn new(workspace: Entity<Workspace>, window: &mut Window, cx: &mut Context<Self>) -> Self {
        Self {
            workspace: workspace.downgrade(),
            yaml: cx.new(|cx| TextareaState::new(window, cx)),
            yaml_open: false,
        }
    }

    fn saved(&self, cx: &App) -> Vec<ConnectionConfig> {
        self.workspace
            .upgrade()
            .map(|w| w.read(cx).saved.clone())
            .unwrap_or_default()
    }

    fn open_id(&mut self, id: &str, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(ws) = self.workspace.upgrade() {
            ws.update(cx, |ws, cx| ws.open_saved(id, window, cx));
        }
        window.close_dialog(cx);
    }

    fn edit_id(&mut self, id: &str, window: &mut Window, cx: &mut Context<Self>) {
        let Some(cfg) = self.saved(cx).into_iter().find(|c| c.id == id) else {
            return;
        };
        let Some(ws) = self.workspace.upgrade() else {
            return;
        };
        window.close_dialog(cx);
        open_connection_form(ws, Some(cfg), window, cx);
    }

    fn delete_id(&mut self, id: &str, cx: &mut Context<Self>) {
        if let Err(e) = connections::remove(id) {
            notify(cx, NotificationAction::new_error(e.to_string().into()));
            return;
        }
        if let Some(ws) = self.workspace.upgrade() {
            ws.update(cx, |ws, cx| ws.reload_saved(cx));
        }
        cx.notify();
    }

    fn test_id(&mut self, id: &str, cx: &mut Context<Self>) {
        let Some(cfg) = self.saved(cx).into_iter().find(|c| c.id == id) else {
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

    fn export_toml(&self, cx: &mut Context<Self>) {
        match connections::export_toml(&self.saved(cx)) {
            Ok(text) => {
                cx.write_to_clipboard(ClipboardItem::new_string(text));
                let msg = i18n_common(cx, "copied");
                notify(cx, NotificationAction::new_success(msg));
            }
            Err(e) => notify(cx, NotificationAction::new_error(e.to_string().into())),
        }
    }

    fn open_new_form(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(ws) = self.workspace.upgrade() else {
            return;
        };
        window.close_dialog(cx);
        open_connection_form(ws, None, window, cx);
    }
}

fn saved_row(
    picker: Entity<SavedPicker>,
    cfg: ConnectionConfig,
    test: SharedString,
    edit: SharedString,
    delete: SharedString,
    more: SharedString,
    cx: &App,
) -> impl IntoElement {
    let id = cfg.id.clone();
    let open_id = id.clone();
    let menu_id = id.clone();
    let name = SharedString::from(cfg.display_name().to_string());
    let bootstrap = SharedString::from(cfg.bootstrap_servers);
    ListItem::new(format!("saved-{id}"))
        .rounded(cx.theme().radius)
        .on_click({
            let picker = picker.clone();
            move |_, window, cx| {
                picker.update(cx, |this, cx| this.open_id(&open_id, window, cx));
            }
        })
        .suffix(move |_, _cx| {
            div()
                .on_mouse_down(MouseButton::Left, |_, _, cx| cx.stop_propagation())
                .child(
                    Button::new(format!("saved-more-{id}"))
                        .ghost()
                        .icon(IconName::Ellipsis)
                        .xsmall()
                        .tooltip(more.clone())
                        .dropdown_menu({
                            let picker = picker.clone();
                            let menu_id = menu_id.clone();
                            let test = test.clone();
                            let edit = edit.clone();
                            let delete = delete.clone();
                            move |menu, _, _| {
                                menu.item(PopupMenuItem::new(test.clone()).on_click({
                                    let picker = picker.clone();
                                    let id = menu_id.clone();
                                    move |_, _, cx| {
                                        picker.update(cx, |this, cx| this.test_id(&id, cx));
                                    }
                                }))
                                .item(PopupMenuItem::new(edit.clone()).on_click({
                                    let picker = picker.clone();
                                    let id = menu_id.clone();
                                    move |_, window, cx| {
                                        picker.update(cx, |this, cx| this.edit_id(&id, window, cx));
                                    }
                                }))
                                .item(PopupMenuItem::new(delete.clone()).on_click({
                                    let picker = picker.clone();
                                    let id = menu_id.clone();
                                    move |_, _, cx| {
                                        picker.update(cx, |this, cx| this.delete_id(&id, cx));
                                    }
                                }))
                            }
                        }),
                )
        })
        .child(
            v_flex()
                .min_w_0()
                .w_full()
                .gap_0()
                .child(Label::new(name))
                .child(Label::new(bootstrap).text_xs().text_color(cx.theme().muted_foreground)),
        )
}

impl Render for SavedPicker {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let saved = self.saved(cx);
        let picker = cx.entity();
        let test = i18n_kafka(cx, "test");
        let edit = i18n_kafka(cx, "edit");
        let delete = i18n_common(cx, "delete");
        let more = i18n_common(cx, "more");
        let muted = cx.theme().muted_foreground;
        v_flex()
            .gap_3()
            .w_full()
            .child(
                h_flex()
                    .w_full()
                    .items_center()
                    .justify_between()
                    .gap_2()
                    .child(
                        Button::new("new-conn")
                            .outline()
                            .label(i18n_kafka(cx, "new_title"))
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.open_new_form(window, cx);
                            })),
                    )
                    .child(
                        h_flex()
                            .gap_1()
                            .child(
                                Button::new("import-toggle")
                                    .ghost()
                                    .label(i18n_kafka(cx, "import_yaml"))
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.yaml_open = !this.yaml_open;
                                        cx.notify();
                                    })),
                            )
                            .child(
                                Button::new("export")
                                    .ghost()
                                    .label(i18n_kafka(cx, "export"))
                                    .on_click(cx.listener(|this, _, _, cx| this.export_toml(cx))),
                            ),
                    ),
            )
            .when(saved.is_empty(), |this| {
                this.child(Label::new(i18n_kafka(cx, "empty_saved")).text_sm().text_color(muted))
            })
            .children(saved.into_iter().map(|c| {
                saved_row(
                    picker.clone(),
                    c,
                    test.clone(),
                    edit.clone(),
                    delete.clone(),
                    more.clone(),
                    cx,
                )
            }))
            .when(self.yaml_open, |this| {
                this.child(div().h(px(1.)).bg(cx.theme().border))
                    .child(
                        Label::new(i18n_kafka(cx, "yaml_placeholder"))
                            .text_xs()
                            .text_color(muted),
                    )
                    .child(Textarea::new(&self.yaml).h(px(88.)))
                    .child(
                        Button::new("import")
                            .label(i18n_kafka(cx, "import_yaml"))
                            .on_click(cx.listener(|this, _, _, cx| {
                                let text = this.yaml.read(cx).value().to_string();
                                this.import_yaml(text, cx);
                            })),
                    )
            })
    }
}

struct ConnectionForm {
    workspace: WeakEntity<Workspace>,
    name: Entity<InputState>,
    bootstrap: Entity<InputState>,
    sasl_user: Entity<InputState>,
    sasl_pwd: Entity<InputState>,
    sr_url: Entity<InputState>,
    mechanism: Entity<Select>,
    ssh_host: Entity<InputState>,
    ssh_user: Entity<InputState>,
    kerberos: Entity<InputState>,
    msk_region: Entity<InputState>,
    webhook: Entity<InputState>,
    tls: bool,
    skip_tls: bool,
    sasl: bool,
    ssh: bool,
    selected: Option<String>,
}

impl ConnectionForm {
    fn new(
        workspace: Entity<Workspace>,
        existing: Option<ConnectionConfig>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let labels = SaslMechanism::ALL.iter().map(|m| m.king_label().to_string()).collect();
        let mut this = Self {
            workspace: workspace.downgrade(),
            name: cx.new(|cx| InputState::new(window, cx).placeholder("prod")),
            bootstrap: cx.new(|cx| InputState::new(window, cx).placeholder("localhost:9092")),
            sasl_user: cx.new(|cx| InputState::new(window, cx).placeholder("user")),
            sasl_pwd: cx.new(|cx| InputState::new(window, cx).masked(true).placeholder("password")),
            sr_url: cx.new(|cx| InputState::new(window, cx).placeholder("http://localhost:8081")),
            mechanism: cx.new(|cx| Select::new(labels, Some(0), window, cx)),
            ssh_host: cx.new(|cx| InputState::new(window, cx).placeholder("bastion.example")),
            ssh_user: cx.new(|cx| InputState::new(window, cx).placeholder("ec2-user")),
            kerberos: cx.new(|cx| InputState::new(window, cx).placeholder("kafka/principal@REALM")),
            msk_region: cx.new(|cx| InputState::new(window, cx).placeholder("us-east-1")),
            webhook: cx.new(|cx| InputState::new(window, cx).placeholder("https://hooks.example/lag")),
            tls: false,
            skip_tls: false,
            sasl: false,
            ssh: false,
            selected: None,
        };
        if let Some(cfg) = existing {
            this.load_cfg(&cfg, window, cx);
        }
        this
    }

    fn saved(&self, cx: &App) -> Vec<ConnectionConfig> {
        self.workspace
            .upgrade()
            .map(|w| w.read(cx).saved.clone())
            .unwrap_or_default()
    }

    fn mechanism(&self, cx: &App) -> SaslMechanism {
        self.mechanism
            .read(cx)
            .selected_index(cx)
            .and_then(|i| SaslMechanism::ALL.get(i).copied())
            .unwrap_or_default()
    }

    fn load_cfg(&mut self, cfg: &ConnectionConfig, window: &mut Window, cx: &mut Context<Self>) {
        self.selected = Some(cfg.id.clone());
        self.name.update(cx, |input, cx| input.set_value(&cfg.name, window, cx));
        self.bootstrap
            .update(cx, |input, cx| input.set_value(&cfg.bootstrap_servers, window, cx));
        self.sasl_user
            .update(cx, |input, cx| input.set_value(&cfg.sasl_user, window, cx));
        self.sasl_pwd
            .update(cx, |input, cx| input.set_value(&cfg.sasl_password, window, cx));
        self.sr_url
            .update(cx, |input, cx| input.set_value(&cfg.sr.url, window, cx));
        let idx = cfg.sasl_mechanism.index();
        self.mechanism
            .update(cx, |sel, cx| sel.set_selected_index(idx, window, cx));
        self.ssh_host
            .update(cx, |input, cx| input.set_value(&cfg.ssh_host, window, cx));
        self.ssh_user
            .update(cx, |input, cx| input.set_value(&cfg.ssh_user, window, cx));
        self.kerberos
            .update(cx, |input, cx| input.set_value(&cfg.kerberos_principal, window, cx));
        self.msk_region
            .update(cx, |input, cx| input.set_value(&cfg.msk_region, window, cx));
        self.webhook
            .update(cx, |input, cx| input.set_value(&cfg.monitor_webhook, window, cx));
        self.tls = cfg.tls;
        self.skip_tls = cfg.skip_tls_verify;
        self.sasl = cfg.sasl;
        self.ssh = cfg.ssh;
    }

    fn draft(&self, cx: &App) -> ConnectionConfig {
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
        cfg.sasl = self.sasl;
        cfg.sasl_mechanism = self.mechanism(cx);
        cfg.sr.url = self.sr_url.read(cx).value().to_string();
        cfg.tls = self.tls;
        cfg.skip_tls_verify = self.skip_tls;
        cfg.ssh = self.ssh;
        cfg.ssh_host = self.ssh_host.read(cx).value().to_string();
        cfg.ssh_user = self.ssh_user.read(cx).value().to_string();
        cfg.kerberos_principal = self.kerberos.read(cx).value().to_string();
        cfg.msk_region = self.msk_region.read(cx).value().to_string();
        cfg.monitor_webhook = self.webhook.read(cx).value().to_string();
        if cfg.monitor_lag_threshold == 0 && !cfg.monitor_webhook.is_empty() {
            cfg.monitor_lag_threshold = 10_000;
        }
        cfg
    }

    fn save_open(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let cfg = self.draft(cx);
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
                window.close_dialog(cx);
            }
            Err(e) => notify(cx, NotificationAction::new_error(e.to_string().into())),
        }
    }

    fn test_draft(&mut self, cx: &mut Context<Self>) {
        let cfg = self.draft(cx);
        if cfg.bootstrap_servers.trim().is_empty() {
            notify(cx, NotificationAction::new_error("bootstrap servers required".into()));
            return;
        }
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
}

fn labeled(label: SharedString, child: impl IntoElement) -> Div {
    v_flex().gap_1().child(Label::new(label).text_xs()).child(child)
}

impl Render for ConnectionForm {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let mech = self.mechanism(cx);
        v_flex()
            .gap_3()
            .w_full()
            .child(labeled(i18n_kafka(cx, "conn_name"), Input::new(&self.name).h(px(32.))))
            .child(labeled(
                i18n_kafka(cx, "bootstrap"),
                Input::new(&self.bootstrap).h(px(32.)),
            ))
            .child(
                Switch::new("tls")
                    .label(i18n_kafka(cx, "tls"))
                    .checked(self.tls)
                    .on_click(cx.listener(|this, checked, _, cx| {
                        this.tls = *checked;
                        cx.notify();
                    })),
            )
            .when(self.tls, |this| {
                this.child(
                    Checkbox::new("skip-tls")
                        .label(i18n_kafka(cx, "skip_tls"))
                        .checked(self.skip_tls)
                        .on_click(cx.listener(|this, checked, _, cx| {
                            this.skip_tls = *checked;
                            cx.notify();
                        })),
                )
            })
            .child(
                Switch::new("sasl")
                    .label(i18n_kafka(cx, "sasl"))
                    .checked(self.sasl)
                    .on_click(cx.listener(|this, checked, _, cx| {
                        this.sasl = *checked;
                        cx.notify();
                    })),
            )
            .when(self.sasl, |this| {
                this.child(labeled(
                    i18n_kafka(cx, "sasl_mechanism"),
                    div().h(px(32.)).child(self.mechanism.clone()),
                ))
                .child(
                    h_flex()
                        .gap_2()
                        .child(
                            labeled(
                                i18n_kafka(cx, "sasl_user"),
                                Input::new(&self.sasl_user).h(px(32.)).flex_1(),
                            )
                            .flex_1(),
                        )
                        .child(
                            labeled(
                                i18n_kafka(cx, "sasl_pwd"),
                                Input::new(&self.sasl_pwd).h(px(32.)).flex_1(),
                            )
                            .flex_1(),
                        ),
                )
                .when(mech == SaslMechanism::Gssapi, |this| {
                    this.child(labeled(
                        i18n_kafka(cx, "kerberos"),
                        Input::new(&self.kerberos).h(px(32.)),
                    ))
                })
                .when(mech == SaslMechanism::AwsMskIam, |this| {
                    this.child(labeled(i18n_kafka(cx, "msk"), Input::new(&self.msk_region).h(px(32.))))
                })
            })
            .child(
                Switch::new("ssh")
                    .label(i18n_kafka(cx, "ssh"))
                    .checked(self.ssh)
                    .on_click(cx.listener(|this, checked, _, cx| {
                        this.ssh = *checked;
                        cx.notify();
                    })),
            )
            .when(self.ssh, |this| {
                this.child(
                    h_flex()
                        .gap_2()
                        .child(
                            labeled(
                                i18n_kafka(cx, "ssh_host"),
                                Input::new(&self.ssh_host).h(px(32.)).flex_1(),
                            )
                            .flex_1(),
                        )
                        .child(
                            labeled(
                                i18n_kafka(cx, "ssh_user"),
                                Input::new(&self.ssh_user).h(px(32.)).flex_1(),
                            )
                            .flex_1(),
                        ),
                )
                .child(Label::new(i18n_kafka(cx, "ssh_hint")).text_xs())
            })
            .child(labeled(i18n_kafka(cx, "sr_url"), Input::new(&self.sr_url).h(px(32.))))
            .child(labeled(i18n_kafka(cx, "webhook"), Input::new(&self.webhook).h(px(32.))))
            .child(
                h_flex()
                    .gap_2()
                    .child(
                        Button::new("test-draft")
                            .label(i18n_kafka(cx, "test"))
                            .on_click(cx.listener(|this, _, _, cx| this.test_draft(cx))),
                    )
                    .child(
                        Button::new("save-open")
                            .primary()
                            .label(i18n_kafka(cx, "save_open"))
                            .on_click(cx.listener(|this, _, window, cx| this.save_open(window, cx))),
                    ),
            )
    }
}

pub fn open_connection_picker(workspace: Entity<Workspace>, window: &mut Window, cx: &mut App) {
    let view = cx.new(|cx| SavedPicker::new(workspace, window, cx));
    Dialog::new(i18n_kafka(cx, "picker_title"))
        .w(px(480.))
        .max_h(px(520.))
        .child(move || view.clone())
        .overlay_closable(true)
        .open(window, cx);
}

pub fn open_connection_form(
    workspace: Entity<Workspace>,
    existing: Option<ConnectionConfig>,
    window: &mut Window,
    cx: &mut App,
) {
    let title = if existing.is_some() {
        i18n_kafka(cx, "edit")
    } else {
        i18n_kafka(cx, "new_title")
    };
    let view = cx.new(|cx| ConnectionForm::new(workspace, existing, window, cx));
    Dialog::new(title)
        .w(px(560.))
        .max_h(px(640.))
        .button_props(dialog_button_props(cx))
        .ok_text(i18n_common(cx, "cancel"))
        .child(move || view.clone())
        .overlay_closable(true)
        .open(window, cx);
}
