# Flatpak

Files used to ship on Flathub (`flathub/io.github.unmq.kaforge` once accepted):

- `io.github.unmq.kaforge.yml` — flatpak-builder manifest
- `io.github.unmq.kaforge.desktop` — desktop entry (Icon must equal the app-id;
  `assets/kaforge.desktop` is the AppImage variant and stays untouched)
- `io.github.unmq.kaforge.metainfo.xml` — AppStream metadata shown in software
  centers

Regenerate the offline crate mirror after a lockfile change:

```bash
./scripts/gen-flatpak-sources.sh
```

Validate locally:

```bash
appstreamcli validate io.github.unmq.kaforge.metainfo.xml
desktop-file-validate io.github.unmq.kaforge.desktop
```

Build:

```bash
flatpak-builder --user --install --force-clean build-dir io.github.unmq.kaforge.yml
flatpak run io.github.unmq.kaforge
```

First-time Flathub submission: fork `flathub/flathub`, branch off `new-pr`, add
`io.github.unmq.kaforge.yml` + `cargo-sources.json` +
`io.github.unmq.kaforge.metainfo.xml`, open the PR. After acceptance,
releases go to the dedicated `flathub/io.github.unmq.kaforge` repo.
`scripts/submit-flathub.sh` automates pinning the tag and assembling those files.
