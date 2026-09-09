# Security Policy

## Reporting a Vulnerability

Do not open a public issue for a security report.

Instead, report privately via GitHub's [**Report a vulnerability**](https://github.com/unmq/Kaforge/security/advisories/new) (the repo's **Security → Advisories** tab). Please include:

- a description of the issue and its impact
- steps to reproduce, or a proof of concept if you have one
- affected versions / platforms if you know them

This is a **local desktop app** — it stores preferences and Kafka credentials in local TOML files, and makes outbound network calls for the optional update check and to the Kafka clusters you connect to. Reports are especially welcome around:

- path traversal or arbitrary file writes from user-controlled names
- secrets leaking into logs, diagnostics, or crash reports
- local privilege issues around the single-instance socket

---

请通过 GitHub 的 [**Report a vulnerability**](https://github.com/unmq/Kaforge/security/advisories/new)（仓库 **Security → Advisories** 标签）私下报告，并尽量包含：

- 问题描述与影响
- 复现步骤或概念验证
- 已知受影响的版本 / 平台
