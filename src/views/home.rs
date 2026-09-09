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

use crate::helpers::{card_background, format_unix_secs, now_datetime};
use crate::states::i18n_home;
use chrono::Utc;
use gpui::{App, Window, prelude::*};
use gpui_kit::component::{ActiveTheme, label::Label, v_flex};

pub struct Home;

impl Render for Home {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        v_flex()
            .size_full()
            .p_8()
            .gap_2()
            .items_center()
            .justify_center()
            .bg(card_background(cx))
            .child(
                Label::new(i18n_home(cx, "title"))
                    .text_lg()
                    .font_weight(gpui::FontWeight::BOLD)
                    .text_color(cx.theme().foreground),
            )
            .child(
                Label::new(i18n_home(cx, "body"))
                    .text_sm()
                    .text_color(cx.theme().muted_foreground),
            )
            .child(
                Label::new(format_unix_secs(Utc::now().timestamp()).unwrap_or_else(now_datetime))
                    .text_xs()
                    .text_color(cx.theme().muted_foreground),
            )
    }
}

impl Home {
    pub fn new(_cx: &mut App) -> Self {
        Self
    }
}
