# HyperI design notes

Canonical, human-readable notes on HyperI's clickhouse-rs work: design
decisions, behaviour changes, and the reasoning behind them.

For reviewers (Austin and anyone else): cherry-pick what's useful. These
are **not** part of our upstream PRs -- they live on the `hyperi-docs`
branch, separate from the bite-sized feature branches, so PR diffs never
carry them. Read them for context; ignore them if you'd rather.

Short and targeted by design. If a note here turns into a wall of text,
it's wrong -- trim it.

## Index

- [2026-06-03 -- Native format framing + TCP TLS trust](2026-06-03-native-format-and-tcp-tls.md)
