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

## Business internals stay private

Fraud and abuse tuning, costs and margins, competitors, pricing history, strategy and vendor trials
never go in this repository, its commits, PRs or issues. A number like that reaches blocklyd from
its environment or its deploy configuration, and the code's default is a harmless one.

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

## Values from elsewhere

A value that belongs to another system is taken from that system, never typed out again: a
library's error codes and messages, a protocol's numbers (HTTP statuses, Docker Engine API answers,
Minecraft's ports), an API's shapes, another module's enum or limit.

- **Import the owner's constant or type.** An HTTP status is `StatusCode` (`hyper`'s or `axum`'s
  re-export of `http`), never a number. A signal is `libc::SIGKILL`, and the exit it causes
  `runtime::exit_by(libc::SIGKILL)`, never 137. Another module's limit, label or enum is that
  module's item, imported (`labels::LABEL_EPOCH`, `protocol::MAX_STOP_TIMEOUT`).
- **When nothing exports it**, only its documentation gives it, it is one named constant in one
  place, with a comment saying where it comes from: Minecraft's ports in `minecraft.rs`, the
  kernel's cpu.shares bounds and R2's PUT limit in `protocol/`.
- **Docker's answers are read once, in `runtime/docker.rs`,** and turned into `RuntimeError`'s
  variants. The rest of the crate matches variants, never a status or a message's text.
- **Tests take errors and answers from the real thing:** the real library, or a real Docker in
  `tests/docker.rs` where they already can. A shape built by hand keeps passing after the real one
  changes. The fake runtime answers with the same variants in its own words, not Docker's.
- `tests/formats.rs` and `tests/fixtures/` pin the wire and on-disk formats byte for byte, so they
  spell them out. CI's `Values from elsewhere` step greps for what a grep can see.

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

A commit's subject, and a pull request's title (the squash commit on `main`), says the change in
50 characters or fewer, with no closing period; the detail goes in the body. A `Move: …` title
fits too. `scripts/check-subject.sh` enforces it, in lefthook's `commit-msg` hook
(CONTRIBUTING.md installs it) and in CI's `subject` job on the title.

## No accidental mentions

Never put `@` before a name in a commit message or a PR's title or body, not even inside
backticks. Commits do not render markdown, and a squash merge copies the PR body into the commit,
so `@s` becomes a mention of the GitHub user "s". Write "the s selector" or "the player" instead.
`scripts/check-mentions.sh` holds commits and PRs to it, in the same hook and CI job as the
subject. In issues and comments, selectors, crate names and emails go inside backticks closed on
the same line.

## Titles and slips

A PR or commit title says what changed, plainly, the way someone reading the history wants it. It
doesn't state the motive. When something public goes wrong (a title, a mention, a post), stop, say
what happened, and ask; don't close, reopen or force-push to cover it.
