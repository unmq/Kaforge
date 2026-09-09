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

use crate::states::{GlobalStore, i18n_sidebar, update_app_state_and_save};
use crate::views::open_about_window;
use gpui::{App, Window, div, prelude::*};
use gpui_kit::component::{
    ActiveTheme, IconName,
    button::{Button, ButtonVariants},
    v_flex,
};

pub struct Sidebar;

impl Sidebar {
    pub fn new(_cx: &mut App) -> Self {
        Self
    }
}

impl Render for Sidebar {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let store = cx.global::<GlobalStore>().read(cx);
        let collapsed = store.sidebar_collapsed();
        let width = store.sidebar_px();

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
                    .when(!collapsed, |b| b.label(i18n_sidebar(cx, "open_connection"))),
            )
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
                Button::new("nav-collapse")
                    .ghost()
                    .icon(if collapsed {
                        IconName::PanelLeftOpen
                    } else {
                        IconName::PanelLeftClose
                    })
                    .on_click(|_, _, cx| {
                        update_app_state_and_save(cx, "toggle_sidebar", |state, _| {
                            state.set_sidebar_collapsed(!state.sidebar_collapsed());
                        });
                    }),
            )
    }
}
