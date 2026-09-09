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

//! Pre-window startup pieces: version constants and the smoke-test gates.

pub(crate) const VERSION: &str = env!("CARGO_PKG_VERSION");
pub(crate) const GIT_SHA: &str = env!("VERGEN_GIT_SHA");
pub(crate) const BUILD_TIMESTAMP: &str = env!("VERGEN_BUILD_TIMESTAMP");
pub(crate) const BUILD_CHANNEL: &str = match option_env!("KAFORGE_BUILD_CHANNEL") {
    Some(channel) => channel,
    None => "stable",
};

pub(crate) fn is_nightly_build() -> bool {
    BUILD_CHANNEL == "nightly"
}

pub(crate) fn is_smoke_test() -> bool {
    std::env::var("KAFORGE_SMOKE_TEST").is_ok_and(|v| v == "1")
}

pub(crate) fn smoke_gate_is_window() -> bool {
    std::env::var("KAFORGE_SMOKE_GATE").is_ok_and(|v| v == "window")
}
