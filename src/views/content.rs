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

use super::home::Home;
use gpui::{Entity, Window, prelude::*};
use gpui_kit::component::v_flex;

pub struct Content {
    home: Entity<Home>,
}

impl Content {
    pub fn new(_window: &mut Window, cx: &mut Context<Self>) -> Self {
        Self {
            home: cx.new(|cx| Home::new(cx)),
        }
    }
}

impl Render for Content {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        v_flex().size_full().min_h_0().child(self.home.clone())
    }
}
