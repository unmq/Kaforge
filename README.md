# Kaforge

A native Kafka desktop client built in Rust with [GPUI](https://www.gpui.rs/) and [gpui-kit](https://github.com/longbridge/gpui-kit).

Preferences persist to `kaforge.toml`. Saved cluster credentials live in `connections.toml`.

## Run

Rust **1.98.0** is pinned in `rust-toolchain.toml` (rustup applies it). Then:

```bash
make dev          # bacon run
make debug        # RUST_LOG=DEBUG
make test
make fmt && make lint
```

`make lint` is the repo gate: `typos` + `cargo clippy --all-targets --all -- --deny=warnings`.

## Layout

```
src/                      # bin crate `kaforge`
  main.rs, root.rs, …
  states/app.rs           # prefs: theme / locale / fonts / proxy / update / tray / datetime / window
  views/{home,settings,about,title_bar,sidebar,…}
crates/kaforge-ui/        # Card, Dialog, Form, Select, TextTable, …
locales/{en,zh}.toml
```

The sidebar lists currently open Kafka connections. Saved clusters are picked from the Open connection dialog. Settings, About, updates, the command palette, and keyboard shortcuts live on the title bar / palette.

## Release

`.github/workflows/publish.yml` builds macOS / Windows / Linux (deb, rpm, AppImage, tarball, MSI). Smoke, lint, audit, and udeps stay in CI.

## License

Apache-2.0. See [LICENSE](./LICENSE).
