# blocklyd

The guide for anyone changing this repository, agent or human. [CONTRIBUTING.md](CONTRIBUTING.md)
covers setup and tests; the rules below apply to every change.

blocklyd is the node daemon of Cubepals, whose control plane is
[cubepals/cubepals](https://github.com/cubepals/cubepals). That repository's AGENTS.md holds the
product's core rule (Blockly absorbs complexity so the player doesn't have to); this one holds
what is particular to blocklyd.

## The name

Players know the product as **Cubepals**; **Blockly** is its codename.

- **What a person can see says Cubepals:** the README's opening, release notes, the image's
  description.
- **Everything else stays Blockly:** the crate and binary (`blocklyd`), code comments, config
  keys, labels such as `blocklyd.record`, test names. Don't rename them to match the public name.

## The protocol and the control plane

- **The Rust types are the protocol.** `openapi/` is generated from them (`BLOCKLYD_WRITE_SCHEMA=1
  cargo test --lib protocol::schema`) and committed with the change that moves them.
- **Within v1 a change only adds.** A new field is optional or defaulted, a new request field is
  announced in `features` first, nothing is renamed or removed. A node and a control plane one
  release apart always understand each other. The `contract` job runs Cubepals' main against this
  build and must pass.
- **Comments that name `docs/fleet.md`, `docs/fleet-operations.md` or `scripts/fleet.ts` mean
  Cubepals'.** They stay as they are.
- **blocklyd never links Cubepals' code.** The two talk over the API only; Cubepals is
  AGPL-3.0, blocklyd is FSL-1.1-ALv2.

## Releases

- A release is a pull request that raises `version` in `Cargo.toml`, and nothing else does.
  `release.yml` tags and publishes it once CI passes on that commit of `main`. Never push a tag by hand.
- Cubepals takes a release with `bun scripts/blocklyd.ts bump <version>` in its own pull request.

## Structure

The same rules as Cubepals: a source file at most about 800 lines, an `impl` block or function at
most about 600, split by concern before more is added, in its own `Move: …` change. No file or
module called `utils`, `helpers`, `misc`, `common` or `shared`. A file over 50 lines opens with a
`//!` doc comment saying what it is for. No cycle between modules.

CI's `Structure` step runs Cubepals' `scripts/check-structure.ts` against
`structure-baseline.json`. It is a ratchet: the baseline only shrinks, and nobody adds an entry
without saying why in it.

## CI

A change isn't done when its PR is green. It is done when the runs it starts on `main` are green too, the release included.

- **Workflows are code.** A change to `.github/workflows/` passes `actionlint` before it is pushed, and CI runs it again. A workflow GitHub can't parse never runs and shows only a red run named by its path.
- **After a merge, look at the Actions page,** not only the PR's checks: `gh run list -R cubepals/blocklyd -L 30`. Scheduled runs fail without a PR to show it.
- **A failure outside the code is still a failure.** A registry's rate limit or an outage gets a fix (a mirror, a retry), not a rerun until it passes.

## Commits

Signed, as their author. No attribution lines in commits or pull requests.
