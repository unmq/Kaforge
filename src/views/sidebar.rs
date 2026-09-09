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

use crate::states::{i18n_kafka, i18n_sidebar, update_app_state_and_save};
use crate::views::docs::DocKind;
use crate::views::{open_about_window, open_connection_picker};
use crate::workspace::{SessionStatus, Workspace};
use gpui::{App, Entity, Window, prelude::*};
use gpui_kit::component::{
    ActiveTheme, IconName, Sizable,
    button::{Button, ButtonVariants},
    sidebar::{Sidebar as KitSidebar, SidebarMenu, SidebarMenuItem, SidebarToggleButton},
    v_flex,
};

pub struct Sidebar {
    workspace: Entity<Workspace>,
}

impl Sidebar {
    pub fn new(workspace: Entity<Workspace>, _cx: &mut App) -> Self {
        Self { workspace }
    }
}

impl Render for Sidebar {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let store = cx.global::<crate::states::GlobalStore>().read(cx);
        let collapsed = store.sidebar_collapsed();
        let ws = self.workspace.read(cx);
        let active = ws.active_id.clone();
        let active_kind = ws
            .active_session()
            .and_then(|s| s.tabs.get(s.active_tab).map(|t| t.kind));
        let sessions: Vec<(String, String, SessionStatus, bool)> = ws
            .sessions
            .iter()
            .map(|s| (s.connection_id.clone(), s.name.clone(), s.status, s.error.is_some()))
            .collect();
        let kinds = DocKind::all_nav();
        let workspace = self.workspace.clone();

        KitSidebar::new("session-nav")
            .collapsed(collapsed)
            .border_color(cx.theme().border)
            .header(
                Button::new("nav-open-connection")
                    .ghost()
                    .icon(IconName::Plus)
                    .when(!collapsed, |b| b.label(i18n_sidebar(cx, "open_connection")))
                    .on_click(cx.listener(|this, _, window, cx| {
                        open_connection_picker(this.workspace.clone(), window, cx);
                    })),
            )
            .child(
                SidebarMenu::new().children(sessions.into_iter().map(|(id, name, status, has_err)| {
                    let selected = active.as_deref() == Some(&id);
                    let suffix = match (status, has_err) {
                        (SessionStatus::Connecting, _) => " …",
                        (_, true) => " !",
                        _ => "",
                    };
                    let switch_id = id.clone();
                    let close_id = id.clone();
                    let ws_click = workspace.clone();
                    let ws_close = workspace.clone();
                    SidebarMenuItem::new(format!("{name}{suffix}"))
                        .icon(IconName::Network)
                        .active(selected)
                        .default_open(selected)
                        .click_to_open(true)
                        .on_click(move |_, _, cx| {
                            ws_click.update(cx, |ws, cx| ws.activate(&switch_id, cx));
                        })
                        .suffix(move |_, _cx| {
                            Button::new(format!("close-session-{close_id}"))
                                .ghost()
                                .xsmall()
                                .icon(IconName::Close)
                                .on_click({
                                    let ws_close = ws_close.clone();
                                    let close_id = close_id.clone();
                                    move |_, _, cx| {
                                        ws_close.update(cx, |ws, cx| ws.close_session(&close_id, cx));
                                    }
                                })
                        })
                        .children(kinds.into_iter().map(|kind| {
                            let key = kind.as_str();
                            let ws_doc = workspace.clone();
                            SidebarMenuItem::new(i18n_kafka(cx, key))
                                .icon(kind.icon())
                                .active(selected && active_kind == Some(kind))
                                .on_click(move |_, window, cx| {
                                    ws_doc.update(cx, |ws, cx| ws.open_doc(kind, None, window, cx));
                                })
                        }))
                })),
            )
            .footer(
                v_flex()
                    .w_full()
                    .items_start()
                    .gap_1()
                    .child(
                        Button::new("nav-settings")
                            .ghost()
                            .icon(IconName::Settings)
                            .when(!collapsed, |b| b.label(i18n_sidebar(cx, "preferences")))
                            .on_click(|_, _, cx| crate::views::open_settings_window(cx)),
                    )
                    .child(
                        Button::new("nav-about")
                            .ghost()
                            .icon(IconName::Info)
                            .when(!collapsed, |b| b.label(i18n_sidebar(cx, "about")))
                            .on_click(|_, _, cx| open_about_window(cx)),
                    )
                    .child(
                        SidebarToggleButton::new()
                            .collapsed(collapsed)
                            .on_click(move |_, _, cx| {
                                update_app_state_and_save(cx, "toggle_sidebar", move |state, _| {
                                    state.set_sidebar_collapsed(!collapsed);
                                });
                            }),
                    ),
            )
    }
}
