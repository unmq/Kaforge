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

use crate::helpers::OpenConnectionAction;
use crate::states::{i18n_kafka, i18n_sidebar, update_app_state_and_save};
use crate::views::docs::DocKind;
use crate::views::open_about_window;
use crate::workspace::{SessionStatus, Workspace};
use gpui::{App, Entity, Window, div, prelude::*};
use gpui_kit::component::{
    ActiveTheme, IconName, Sizable,
    button::{Button, ButtonVariants},
    h_flex, v_flex,
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
        let width = store.sidebar_px();
        let ws = self.workspace.read(cx);
        let active = ws.active_id.clone();
        let sessions: Vec<(String, String, SessionStatus, bool)> = ws
            .sessions
            .iter()
            .map(|s| (s.connection_id.clone(), s.name.clone(), s.status, s.error.is_some()))
            .collect();
        let kinds = DocKind::all_nav();

        v_flex()
            .w(width)
            .h_full()
            .p_2()
            .gap_1()
            .border_r_1()
            .border_color(cx.theme().border)
            .bg(cx.theme().sidebar)
            .child(
                Button::new("nav-open-connection")
                    .ghost()
                    .icon(IconName::Plus)
                    .when(!collapsed, |b| b.label(i18n_sidebar(cx, "open_connection")))
                    .on_click(|_, _, cx| cx.dispatch_action(&OpenConnectionAction::Open)),
            )
            .children(sessions.into_iter().map(|(id, name, status, has_err)| {
                let selected = active.as_deref() == Some(&id);
                let close_id = id.clone();
                let switch_id = id.clone();
                let suffix = match (status, has_err) {
                    (SessionStatus::Connecting, _) => " …",
                    (_, true) => " !",
                    _ => "",
                };
                h_flex()
                    .w_full()
                    .gap_1()
                    .child(
                        Button::new(format!("open-session-{id}"))
                            .when(selected, |b| b.primary())
                            .when(!selected, |b| b.ghost())
                            .when(!collapsed, |b| b.label(format!("{name}{suffix}")))
                            .on_click(cx.listener(move |this, _, _, cx| {
                                this.workspace.update(cx, |ws, cx| ws.activate(&switch_id, cx));
                            })),
                    )
                    .when(!collapsed, |row| {
                        row.child(
                            Button::new(format!("close-session-{id}"))
                                .ghost()
                                .xsmall()
                                .icon(IconName::Close)
                                .on_click(cx.listener(move |this, _, _, cx| {
                                    this.workspace.update(cx, |ws, cx| ws.close_session(&close_id, cx));
                                })),
                        )
                    })
            }))
            .when(active.is_some() && !collapsed, |this| {
                this.child(div().h_px().bg(cx.theme().border))
                    .children(kinds.into_iter().map(|kind| {
                        let key = kind.as_str();
                        Button::new(format!("doc-kind-{key}"))
                            .ghost()
                            .label(i18n_kafka(cx, key))
                            .on_click(cx.listener(move |this, _, window, cx| {
                                this.workspace.update(cx, |ws, cx| ws.open_doc(kind, None, window, cx));
                            }))
                    }))
                    .child(
                        h_flex()
                            .gap_1()
                            .child(
                                Button::new("nav-producer")
                                    .ghost()
                                    .label(i18n_kafka(cx, "producer"))
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.workspace.update(cx, |ws, cx| {
                                            ws.open_doc(DocKind::Producer, None, window, cx);
                                        });
                                    })),
                            )
                            .child(
                                Button::new("nav-consumer")
                                    .ghost()
                                    .label(i18n_kafka(cx, "consumer"))
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.workspace.update(cx, |ws, cx| {
                                            ws.open_doc(DocKind::Consumer, None, window, cx);
                                        });
                                    })),
                            ),
                    )
            })
            .child(div().flex_1())
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
                h_flex().child(
                    Button::new("sidebar-collapse")
                        .ghost()
                        .icon(if collapsed {
                            IconName::PanelLeftOpen
                        } else {
                            IconName::PanelLeftClose
                        })
                        .on_click(move |_, _, cx| {
                            update_app_state_and_save(cx, "toggle_sidebar", move |state, _| {
                                state.set_sidebar_collapsed(!collapsed);
                            });
                        }),
                ),
            )
    }
}
