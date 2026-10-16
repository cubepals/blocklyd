# Closing the parity gaps

The work that closes the gaps in [parity.md](parity.md), as packages that sessions can take in
parallel. Each package is one pull request to one repository. Packages in the same wave edit
disjoint files, and a wave starts once the one before it has merged. When a package lands, its
rows in parity.md change too: in the release package for blocklyd's packages, and in the closing
package for cubepals'.

Every package keeps the rules of its repository's AGENTS.md. In blocklyd that means: within v1 a
change only adds, `openapi/` is regenerated with the change that moves it, and `tests/formats.rs`
passes unchanged. Only **B1** changes the protocol, so no two packages touch `src/protocol*` or
`openapi/` at once.

| Wave | Repository | Packages |
|---|---|---|
| 1 | blocklyd | B1 Suspend and resume · B2 Prompt heartbeats · B3 Transfers yield to boots |
| 2 | blocklyd | R Release 0.3.2 |
| 3 | cubepals | U `bun scripts/blocklyd.ts bump 0.3.2` |
| 4 | cubepals | C1 Fleet sleep and wake path · C2 Fleet room and node calls · C3 Fleet docs and endpoint readiness |
| 5 | blocklyd | P Parity refresh |

## Wave 1: blocklyd

### B1 Suspend and resume

Closes the daemon's half of **G1** and **G2** (rows 1, 2, 3, 6).

A workload can be put to sleep with its memory kept, using Docker's cgroup freezer (pause), and
`start` resumes it. A resume that doesn't come back is killed and cold-started on its own data. A
suspended workload holds its memory on the node, as a paused one already does in `room.rs`.

**Protocol additions** (all of this wave's), announced as the feature `suspend`:

- `POST /v1/workloads/{id}/suspend`, with the epoch header (exact): freezes a running workload.
  It answers `200 PowerResponse` (`changed: false` when it is already suspended), or
  `409 not_running` for a workload that isn't running.
- `WorkloadState` gains `suspended`, in the workload view and the heartbeat report. Only a
  workload that was asked to suspend is ever in it, so a control plane that never asks never sees
  it. In Blockly's terms it is asleep with its memory kept.
- `PowerResponse.resumed`, optional: on a `start` of a suspended workload, `true` when it came
  back from memory, and `false` when it was cold-started because the resume didn't come back
  within `workloads.resume_timeout_seconds` (a new config key, default 10). It is absent on any
  other start.
- `WorkloadSpec.resumesOn`, optional, a string of at most 128 characters, left out of the spec
  digest. A `PUT` to a suspended workload whose spec differs but whose `resumesOn` equals the one
  it was suspended on keeps it suspended, and answers `EnsureOutcome::deferred` (new) with
  `WorkloadView.pendingSpecDigest` (new, optional). The pending spec is put in place at the
  workload's next boot: a cold-start fallback, or a start after a stop, a kill, a fence or a host
  reboot. Any other `PUT` replaces it as today.
- The rest needs no new fields: stop, kill, fence and delete work on a suspended workload, and
  exec and export answer it as they answer a running one. After a host reboot the frozen process
  is gone: the workload reads `stopped` and is never resumed by `resume-after-reboot`.

**Owns:**

- `src/protocol.rs`, `src/protocol/*.rs`, `openapi/`, `docs/protocol.md`
- `src/api.rs`, `src/config.rs`
- `src/manager.rs`, `src/manager/power.rs`, `ensure.rs`, `view.rs`, `workload_state.rs`,
  `restarts.rs`, `fence.rs`
- `src/store.rs` (the record's new optional fields)
- `src/runtime.rs`, `src/runtime/docker.rs`, `src/runtime/fake.rs` (pause and unpause)
- `src/metrics.rs` (the state label)
- `tests/lifecycle.rs`, `tests/resume.rs`, `tests/docker.rs`, a new `tests/suspend.rs`

**Proves it:**

- With the fake runtime: suspend, then resume (`resumed: true`, same container, same start time);
  a resume whose unpause hangs is cold-started (`resumed: false`); a deferred `PUT` lands at the
  next boot; fence, stop and delete of a suspended copy; no resume after a reboot.
- `tests/docker.rs` against a real Docker: a suspended Minecraft-like process keeps its memory
  and answers within a second of `start`.
- `tests/formats.rs` passes unchanged, and the regenerated `openapi/` is committed.
- The `contract` job (Cubepals' main against this build) passes: an old control plane never sends
  the new fields.

### B2 Prompt heartbeats

Closes the daemon's half of **G8** (row 9).

The heartbeat loop sends a beat as soon as what it would report changes (a workload's state, its
epoch or its ports), at most once a second, and otherwise every `heartbeat_seconds` as now. A
stop's room is then free in the control plane's ledger about a second later instead of up to five.
It does this by comparing the report it builds against the last one it sent, so the manager
doesn't change. There is no wire change.

**Owns:** `src/fleet/heartbeat.rs`, `src/manager/fleet.rs`, `tests/fleet.rs`. `tests/fleet.rs` is
at the size limit, so its new tests stay small, or go in `heartbeat.rs`'s unit tests.

**Proves it:**

- With the test control plane: with `heartbeat_seconds = 5`, a stop is reported within 1.5 s.
- Under steady churn, beats come no more than once a second.
- A beat that fails is not retried faster than the interval.

### B3 Transfers yield to boots

Closes **G3** (rows 4, 8).

Snapshot copies, archive packing for uploads and exports, and restores' download and unpacking
run on their blocking threads at the lowest CPU priority (nice 19) and in the idle I/O class. Each
thread puts its priority back before it returns to tokio's pool. So a server booting on the node
wins over a restore point being uploaded beside it, as it does where helper machines do that work.

**Owns:** `src/tarball.rs`, `src/tree.rs`, `src/manager/archive.rs`, `src/manager/snapshots.rs`,
`src/store/snapshots.rs`, `src/store/restore.rs`, `src/lib.rs`, a new module for the thread's
priority (it says why the kernel's `ioprio` numbers are typed out, where nothing exports them),
`tests/transfer.rs`, `tests/storage_bench.rs`.

**Proves it:**

- A test reads the working thread's nice value and I/O class while it packs and copies, and again
  after it.
- `tests/storage_bench.rs` reports a boot's time beside a running export, before and after.
- `tests/transfer.rs` passes unchanged.

## Wave 2: blocklyd

### R Release 0.3.2

A pull request that raises `version` to 0.3.2 in `Cargo.toml` (and `Cargo.lock`), as 0.3.1 was
(#14). It also turns parity.md's rows for G1 to G3 and G8 to "the daemon has it; the control plane
next", and adds B2's sentence to `docs/protocol.md`'s heartbeat section. B1 adds a feature, so if
that should be a minor release, it is 0.4.0 instead, and wave 3 bumps to that.

**Owns:** `Cargo.toml`, `Cargo.lock`, `docs/parity.md`, `docs/protocol.md` (that sentence).

**Proves it:** CI on the PR, then `release.yml` tagging v0.3.2 and publishing the image once
main's CI passes on that commit.

## Wave 3: cubepals

### U Pin blocklyd 0.3.2

`bun scripts/blocklyd.ts bump 0.3.2` once the release image exists, in a pull request of its
own. It updates the Dockerfile's line and the copies of `node-reads.openapi.json`,
`node-writes.openapi.json` and `generated/`. Nothing else changes, so the packages after it build
on the new types.

**Proves it:** `bun scripts/blocklyd.ts check`, `openapi:check`, and "FleetRuntime against a real
blocklyd" on the new release.

## Wave 4: cubepals

### C1 Fleet sleep and wake path

Closes the control plane's half of **G1** and **G2**, and **G7** and **G4** (rows 1, 2, 3, 5, 6).

- **Sleep.** `FleetRuntime.sleep(handle)` suspends where the node lists `suspend` and the
  deployment allows it. That is a switch with a harmless default (off), like Fly's suspend switch.
  Otherwise it stops, and it says which it did.
- **Resume.** `ensureProvisioned` resumes a suspended copy and reports the `resuming` step. A
  `resumed: false` answer goes on as a boot. `resumesOn` is passed in `toWorkloadSpec`, and a
  deferred answer is kept as Fly keeps a spec it slept on.
- **No PUT on an unchanged spec.** Every start skips the PUT when the spec's digest, kept per
  placement, is unchanged.
- **Running at once.** `waitRunning` returns as soon as the start's own answer says `running`.
- **Bounded polls.** The poll honours its signal on each call and is bounded by what is left of
  it. A poll that can't reach the node is asked again until the deadline.
- **Wake-sized timeouts.** The PUT and the start on a wake get timeouts sized for a wake, and
  `apply` sends `If-Match` with the stored digest.

**Owns:**

- `apps/control/src/infra/fleet/fleet-runtime.ts`, `fleet-runtime/ledger.ts`,
  `fleet-runtime/workload-api.ts`
- `fleet-runtime.test.ts`, `fleet-runtime.flows.test.ts`, `fleet-runtime.e2e.test.ts`
- the deployment's suspend switch in `config/`
- `docs/waking.md`, `docs/resume-fallback.md` (a fleet column)

**Proves it:**

- "FleetRuntime against a real blocklyd": a sleep suspends, and a wake resumes in place (the same
  container start time, `resuming` reported). A change made while it slept lands at its next boot.
- The flows tests: a resume that doesn't come back goes on as a boot.
- A start whose spec is unchanged sends no PUT.

### C2 Fleet room and node calls

Closes the control plane's half of **G8**, and **G5** (rows 9, 11).

- **Room from the node's own answer.** The ledger frees a copy's memory as soon as the node's own
  stop, kill or start answer says it no longer runs, not only at the next heartbeat.
- **A fresh view before moving.** A start on a node that looks full asks that node for a fresh
  view before it moves the world elsewhere.
- **Suspended copies hold memory.** A `suspended` copy holds its memory in `HOLDS`.
- **Retries.** Node calls that are safe to repeat (GET, PUT, start, stop) are tried again on
  `NodeUnreachable` within their bound.
- **An apply on an unreachable node** leaves the server stopped on what it ran, as `restBack`
  does on Fly.

**Owns:**

- `apps/control/src/infra/fleet/fleet-runtime/admission.ts`, `fleet-runtime/rehoming.ts`,
  `registry/summaries.ts`, `node-client.ts`
- `registry.test.ts`, `node-client.test.ts`, `fleet-two-node.e2e.test.ts`

**Proves it:**

- The two-node e2e test: a wake right after another server's stop on a full node starts in
  place, with no `placement.moved_for_room`.
- `node-client` tests for the retried and the final failures.
- An apply against a node that went away ends stopped, not failed.

### C3 Fleet docs and endpoint readiness

Closes **G6** (rows 12, 13, 25).

- `startNodeEndpoint` resolves once its server is listening, so `endpoint.e2e.ts`'s `listening`
  means bound.
- `docs/fleet.md` §11 and `docs/fleet-operations.md`'s firewall notes say what a server's
  container reaches: the artifact endpoint (`ARTIFACTS_RUNTIME_FACING_URL`), the archive store's
  presigned host, and Mojang and the mod hosts.
- Both docs describe a fleet sleep that keeps the memory, from B1's protocol.

**Owns:** `apps/control/src/infra/fleet/node-endpoint.ts`, `endpoint.e2e.ts`, `docs/fleet.md`,
`docs/fleet-operations.md`.

**Proves it:** "FleetRuntime against a real blocklyd" ten runs in a row with no CA or enrolment
failure. Before the change, a test restarting the endpoint shows the race.

## Wave 5: blocklyd

### P Parity refresh

parity.md's rows for G1 to G8 are set to what main now does in both repositories, each with the
pull request that moved it, and anything still open goes back into this plan.

**Owns:** `docs/parity.md`, `docs/parity-plan.md`.

**Proves it:** each row checked against the code it names.
