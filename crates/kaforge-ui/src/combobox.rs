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

use gpui::{App, Entity, EventEmitter, Subscription, Window, prelude::*};
use gpui_kit::component::{
    IndexPath,
    combobox::{Combobox as KitCombobox, ComboboxEvent as KitComboboxEvent, ComboboxState},
    searchable_list::SearchableVec,
};

pub enum ComboboxEvent {
    Change(usize),
}

pub struct Combobox {
    state: Entity<ComboboxState<SearchableVec<String>>>,
    items: Vec<String>,
    _subscription: Subscription,
}

impl EventEmitter<ComboboxEvent> for Combobox {}

impl Combobox {
    pub fn new(items: Vec<String>, selected_index: Option<usize>, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let selected = selected_index.map(|i| vec![IndexPath::new(i)]).unwrap_or_default();
        let state =
            cx.new(|cx| ComboboxState::new(SearchableVec::new(items.clone()), selected, window, cx).searchable(true));
        let subscription = cx.subscribe_in(
            &state,
            window,
            |this, _state, event: &KitComboboxEvent<SearchableVec<String>>, _window, cx| {
                let KitComboboxEvent::Confirm(values) = event else {
                    return;
                };
                let Some(value) = values.first() else {
                    return;
                };
                if let Some(index) = this.items.iter().position(|item| item == value) {
                    cx.emit(ComboboxEvent::Change(index));
                }
            },
        );
        Self {
            state,
            items,
            _subscription: subscription,
        }
    }

    pub fn selected_index(&self, cx: &App) -> Option<usize> {
        let value = self.state.read(cx).selected_value()?;
        self.items.iter().position(|item| item == &value)
    }

    pub fn selected_value(&self, cx: &App) -> Option<String> {
        self.state.read(cx).selected_value()
    }

    pub fn set_items(&mut self, items: Vec<String>, prefer: Option<&str>, window: &mut Window, cx: &mut Context<Self>) {
        let prefer = prefer
            .map(str::to_string)
            .or_else(|| self.state.read(cx).selected_value());
        self.items = items.clone();
        self.state.update(cx, |state, cx| {
            state.set_items(SearchableVec::new(items.clone()), window, cx);
            if let Some(value) = prefer.filter(|v| items.iter().any(|item| item == v)) {
                state.set_selected_values(&[value], window, cx);
            }
        });
        cx.notify();
    }
}

impl Render for Combobox {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        KitCombobox::new(&self.state)
    }
}
