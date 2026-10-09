# Contributing

blocklyd is the node daemon of [Cubepals](https://github.com/cubepals/cubepals), developed under
the codename Blockly. [AGENTS.md](AGENTS.md) holds the rules every change follows, for people and
agents alike; this page is how to work on it.

## Setup and tests

The Rust `rust-toolchain.toml` names, which rustup installs, on Linux (blocklyd uses Linux-only
APIs; on macOS, work in a Linux container or VM).

```sh
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
sudo cargo test --locked --test docker -- --ignored   # against a real Docker daemon
cargo deny --locked check
```

The structure rules are Cubepals' `scripts/check-structure.ts`, run here against
`structure-baseline.json`; CI's `Structure` step shows how to run it. CI's `contract` job runs
Cubepals' control plane against your build. The README's "Build and test" has the rest.

## How changes land

`main` is the trunk. Branch from it, keep the change small, and open a pull request. CI's `check`
must pass. Pull requests are squash-merged, so the title becomes the commit on `main`: a plain
sentence about what changes. `main` keeps a linear history.

A change to the protocol only adds within v1 (a new field, a new endpoint, announced in
`features`), so a control plane one release behind keeps working. Regenerate the specs with
`BLOCKLYD_WRITE_SCHEMA=1 cargo test --lib protocol::schema` and commit `openapi/`.

A release is a pull request that raises `version` in `Cargo.toml`; the README's "Releases" says
what happens when it merges.

## Your contribution's licence

blocklyd is published under [FSL-1.1-ALv2](LICENSE.md), and each version becomes Apache-2.0 two
years later. So that a contribution can follow it there, by opening a pull request you license
your change to The Cubepals Authors under the [Apache License, Version
2.0](https://www.apache.org/licenses/LICENSE-2.0), and you confirm that it is yours to license.
You keep the copyright in it.

## Conduct and security

Everyone here follows the
[code of conduct](https://github.com/cubepals/.github/blob/main/CODE_OF_CONDUCT.md), which every
Cubepals repository shares. Security problems are reported privately, as [SECURITY.md](SECURITY.md)
says, never in an issue. Where to get help is in [SUPPORT.md](SUPPORT.md).
