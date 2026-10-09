# blocklyd protocol v1

> The Rust types in [`src/protocol.rs`](../src/protocol.rs) and
> [`src/protocol/wire.rs`](../src/protocol/wire.rs) are the source of truth; this page describes them.
> The control plane's side is [`apps/control/src/infra/fleet`](https://github.com/cubepals/cubepals/tree/main/apps/control/src/infra/fleet), whose
> TypeScript types are generated from them ([`src/protocol/schema.rs`](../src/protocol/schema.rs)).
> Within v1 changes are additive: responses only grow, and a new request field is announced in
> `features` before a control plane may send it. A node and a control plane one release apart
> always understand each other.

The control plane talks to one blocklyd per host over **HTTPS with mutual TLS**, JSON bodies,
under `/v1`. The vocabulary is *workloads*: a spec (image, environment, resources, storage,
ports, stop behaviour) that blocklyd keeps on this host, running or stopped. Nothing in the
protocol names Docker.

## Conventions

- **Transport.** TLS 1.3 only, and a client certificate is required. The certificate must chain
  to the host's `client_ca`, carry extended key usage `clientAuth`, and name one of
  `allowed_clients`. A missing or foreign certificate is refused during the handshake. A valid
  certificate for a name that isn't allowed gets `403 forbidden`. There is no 0-RTT.
  - **In fleet mode** (a `[fleet]` section, no `[api.tls]`), the node's server certificate, the CA
    and `allowed_clients` all come from enrollment (below), and its certificate names the node
    (`<nodeId>.nodes.<deployment>.fleet`), not an address. A client dials the node's address and
    verifies that name.
- **Versioning.** The major version is in the path (`/v1`). Every response carries
  `blocklyd-protocol: 1`. `GET /v1/health` lists `protocol.supported` and `features`.
  - A client must ignore response fields it doesn't know. Responses only grow.
  - blocklyd refuses request fields it doesn't know (`422 invalid_request`) rather than
    silently ignoring them. A newer client checks `features` before sending a field an older
    blocklyd lacks.
- **Request ids.** Send `x-request-id` (1–64 of `[A-Za-z0-9_-]`) to correlate. blocklyd echoes
  it, or generates one, and logs it with every line about that request.
- **Caching.** Every response is `cache-control: no-store`.
- **Workload ids.** 1–63 of `a-z 0-9 -`, starting and ending with a letter or digit. A UUID is
  valid. Anything else in the path gets `400 invalid_workload_id` before any handler runs.
- **Errors.** Every error but a `405` (which has no body) and a `stop` body over the size limit
  (a plain-text `413`) has the same shape. The `code` is the contract; the `message` is for people
  and may change.

  ```json
  { "error": { "code": "precondition_failed", "message": "…", "details": { "currentSpecDigest": "sha256:…" } } }
  ```

## Idempotency: every verb is safe to repeat

| Verb | Repeating it |
|---|---|
| `PUT /v1/workloads/{id}` | Same spec → `200 unchanged`, nothing touched. Keyed by id, so a retry never makes two workloads. |
| `POST …/start` | Already running → `200 {changed: false}`. |
| `POST …/stop`, `POST …/kill` | Already stopped → `200 {changed: false}`. |
| `DELETE …` | Already gone → `200 {existed: false}`. |
| `POST …/exec` | Not idempotent by nature. Send `Idempotency-Key`: blocklyd runs the command once per key (10 minutes, in memory) and returns the first answer to repeats. The same key with a different request gets `422 idempotency_key_reused`. Failures aren't cached. |

Operations on one workload are serialized on blocklyd: a concurrent duplicate waits for the
first, then finds the work done. Operations run to completion even when the client disconnects
mid-request, so a retry after a dropped connection finds a finished stop rather than half of
one.

## Placement epochs (fleet mode)

A fleet control plane gives every new home of a workload a **placement epoch**, a number that only
rises: the first placement, each move, restore and recovery. It sends it with every mutating
request in the `blocklyd-epoch` header, and blocklyd stores it with the copy (in its record and as
the container label `blocklyd.epoch`). Each verb checks it against the copy's own:

| Verb | Rule | Why |
|---|---|---|
| `PUT` (ensure) | Its epoch, or a newer one, which takes the copy over | A new placement may land on a node that already holds an older copy |
| `POST …/start`, `…/exec`, `…/export`, `…/snapshots`, `…/restore` | Exactly its epoch | Running a copy, copying it out or filling it is only ever for the current placement (a restore for a new epoch PUTs it first) |
| `POST …/stop`, `…/kill`, `DELETE` | Its epoch or any newer one | A newer placement may always tear an older copy down |
| `…/snapshots/{snapshot}/upload`, `DELETE …/snapshots/{snapshot}` | None | A snapshot never changes, whichever placement made it |

- **Older** than the copy's: `409 stale_epoch` (`details.askedEpoch`, `details.currentEpoch`).
- **Newer** where only exactly its own will do: `409 epoch_ahead`: PUT the spec with that epoch first.
- **No header** for a copy that has one: `428 epoch_required`. A workload made without an epoch keeps
  the single-node protocol: none is asked for, and none is checked.
- **A superseded copy** (fenced: see `…/fence`) refuses everything but teardown with
  `409 superseded`, and is never started again, by anyone, including blocklyd's own restarts. Only a
  PUT for an epoch newer than the one that superseded it brings it back.

## Endpoints

### `GET /v1/health`

`200` → `NodeHealth`:

```json
{
  "status": "ok",
  "docker": true,
  "reconciled": true,
  "daemonVersion": "0.2.0",
  "protocol": { "current": 1, "supported": [1] },
  "features": ["exec-idempotency-key", "logs-follow-across-restarts", "conditional-put",
               "placement-epochs", "data-transfer", "local-snapshots", "export-exclude",
               "execution-lease", "certificate-renewal", "resume-after-reboot", "multipart-upload",
               "self-upgrade"],
  "nodeId": "fra-box-1",
  "deploymentId": "staging"
}
```

`status` is `degraded` while the runtime is unreachable or blocklyd hasn't reconciled with it
yet.

### `GET /v1/node`

Host facts and capacity:
- node and deployment
- blocklyd version, uptime, resident memory and CPU time
- kernel, CPUs, load, memory, disk under the state directory, trash size
- `capacity`: `allocatableMemoryMb`, `runningMemoryMb`, `provisionedMemoryMb`, ports total and
  allocated
- workloads by state
- Docker version, cgroup version and driver, security options, whether live-restore is on
- the last reconciliation
- host-level `issues`: orphaned data (`orphan_data`), a non-isolated network
  (`network_not_isolated`), live-restore off (`live_restore_off`), an unreadable record
  (`unreadable_record`), a container with no valid workload id (`unlabelled_container`), one
  with no record and no readable record label (`unrecoverable_container`), or a snapshot whose
  `snapshot.json` can't be read, or a name under `snapshots/` that isn't a snapshot's
  (`unreadable_snapshot`, found when blocklyd starts and left alone)
- `daemon`: version, start time, uptime, resident memory, CPU time
- `fleet` (fleet mode): node id, control plane, the lifecycle it last said, the last contact and its
  age, the last error and latency, heartbeats answered and failed, the execution lease left
  (`leaseRemainingSeconds`), and when it last renewed its certificates in this run
  (`certificateRenewedAt`)

### `GET /v1/workloads[?changedSince=<RFC 3339>]`

`200 { "workloads": [WorkloadView…], "observedAt": "…" }`. With `changedSince`, it lists only
workloads whose state changed, or whose record was updated, at or after that time. This is the
level-triggered listing Blockly's `observeChanged` needs: one call per host, answered from
blocklyd's own observations.

### `PUT /v1/workloads/{id}` — ensure

Body: `WorkloadSpec`.

```json
{
  "image": "itzg/minecraft-server:2026.9.1-java21",
  "entrypoint": null,
  "env": { "EULA": "TRUE", "TYPE": "VANILLA", "VERSION": "1.21.8", "MEMORY": "2304M" },
  "secrets": { "RCON_PASSWORD": "…" },
  "resources": { "memoryMb": 3072, "cpuMillis": 2000, "cpuWeight": null, "pidsLimit": null },
  "storage": { "mountPath": "/data", "sizeGb": 5 },
  "ports": [
    { "name": "game", "containerPort": 25565, "protocol": "tcp", "audience": ["edge", "control"] },
    { "name": "rcon", "containerPort": 25575, "protocol": "tcp", "audience": ["control"] }
  ],
  "stop": { "signal": "SIGTERM", "timeoutSeconds": 90 },
  "restart": { "policy": "on-failure", "maxRetries": 3 },
  "labels": { "blockly.server": "0b6f1f2e-…" }
}
```

| Field | Rules |
|---|---|
| `image` | `[A-Za-z0-9._-/:@]`, ≤ 255. Must start with one of the host's `allowed_images` prefixes. |
| `entrypoint` | Optional argv, 1–64 arguments of ≤ 64 KiB, no NUL. |
| `env`, `secrets` | Names `[A-Za-z_][A-Za-z0-9_]*` of ≤ 128. Values ≤ 64 KiB, no NUL. ≤ 256 variables and 512 KiB in all. A name is in one or the other. |
| `resources.memoryMb` | Required. Hard limit with no swap, between `min_memory_mb` and what the host allocates. |
| `resources.cpuMillis` | Optional CFS ceiling: 100 to cores × 1000. It is also what the JVM counts as its processors. |
| `resources.cpuWeight` | Optional, 2–262144. Absent means proportional to memory: 1024 per 4 GB. |
| `resources.pidsLimit` | Optional, 64–65536. Absent means the host default (4096). |
| `storage.mountPath` | Absolute path of plain components, not `/`. |
| `storage.sizeGb` | 1–4096. Reported against measured use; **not enforced** in v1. |
| `ports` | ≤ 8. Names `[a-z][a-z0-9-]{0,15}` are unique. `(protocol, containerPort)` pairs are unique. `audience` is a non-empty subset of `edge` and `control`. |
| `stop.timeoutSeconds` | 1–600. The grace between the signal and the kill. |
| `restart` | `no` or `on-failure`. `maxRetries` is 0–10, and 0 means never. blocklyd restarts a failed workload itself, after a short backoff: every container's Docker restart policy is `no`, because Docker's `on-failure` restarts containers after an unclean reboot. It never restarts one after a requested stop, once it is fenced, or, in fleet mode, without the execution lease: the last heartbeat answer's `leaseSeconds`, counted from when that beat was sent (`restart_requires_contact_seconds` if the answer names none). A restart is admitted against memory as a start is: one that doesn't fit isn't made, and the workload stays `crashed` with the issue `insufficient_capacity`. A restart it withholds is not tried later. The retries a run has used are kept in the workload's record, so a restart of blocklyd doesn't hand them out again; a requested start or a new spec begins a new run. After the host restarts, it resumes what was running when the host went down: in fleet mode only once it holds a lease, after applying the fences of the answer that granted it. After a restart of blocklyd alone, in the same boot, it resumes nothing (see "After the host restarts"). |
| `labels` | ≤ 32. Keys `[a-z0-9._/-]`. The `blocklyd.` prefix is reserved. Values ≤ 256. |

Every validation problem comes back at once: `422 invalid_request`, with `details.fields` a
list of `{field, problem}`.

**Semantics.** Ensure means "this workload exists with this spec", as HTTP `PUT` does.
- **Absent:** blocklyd creates it, stopped. `201 Created`, `outcome: "created"`.
- **Present with the same spec and secret values:** nothing happens. `200`,
  `outcome: "unchanged"`.
- **Present with a different spec** (or a rotated secret): blocklyd makes a new generation.
  - It pulls the new image first; if that fails, nothing is touched.
  - It stops the old container gracefully and makes a new one on the **same data** and the
    **same host ports** for port names that remain.
  - It starts the new one if the old one was running.
  - `200`, `outcome: "replaced"`, `restarted: true|false`.
- **A `retained` workload** (compute removed, data kept) gets compute again: `replaced`.

Conditional headers, standard HTTP:

| Header | Meaning | Refused with |
|---|---|---|
| `If-None-Match: *` | Create only. Never touch an existing workload. | `412 precondition_failed` if it exists |
| `If-Match: "<specDigest>"` | Replace only this exact current spec. An apply can't overwrite a newer one. | `412` with `currentSpecDigest` |
| `If-Match: *` | Replace only if it exists. | `412` if absent |

Responses carry `ETag: "<specDigest>"`. The spec digest is `sha256:` over the canonical JSON of
the spec without secret values. Changed secrets are detected by comparing the container's
environment instead, since blocklyd keeps no copy of them.

Refusals:

| Status and code | When |
|---|---|
| `409 no_free_ports` | The host has no free ports. |
| `507 insufficient_disk` | Free disk is under `min_free_disk_mb`. |
| `503 runtime_unavailable` | Docker is down. |
| `504 timeout` | The image pull ran past its limit. |

`EnsureResponse`: `{ "outcome", "restarted", "workload": WorkloadView }`.

### `GET /v1/workloads/{id}`

`200 WorkloadView`, with an `ETag`:

```json
{
  "id": "mc-1",
  "generation": 1,
  "epoch": 3,
  "supersededBy": null,
  "specDigest": "sha256:…",
  "image": "…",
  "state": "running",
  "health": "healthy",
  "exit": null,
  "restartCount": 0,
  "lastFailureAt": null,
  "startedAt": "…",
  "finishedAt": null,
  "changedAt": "…",
  "ports": [{
    "name": "game", "protocol": "tcp", "containerPort": 25565, "hostPort": 42000,
    "endpoints": { "edge": ["10.0.0.5:42000"], "control": ["10.0.0.5:42000"] }
  }],
  "resources": { "memoryMb": 3072, "cpuMillis": 2000, "cpuWeight": null, "pidsLimit": null },
  "storage": { "mountPath": "/data", "sizeGb": 5, "usedBytes": 745123840, "measuredAt": "…" },
  "labels": { "blockly.server": "…" },
  "secretNames": ["RCON_PASSWORD"],
  "issues": [],
  "locate": { "containerName": "blockly-staging-mc-1", "containerId": "…", "dataDir": "/var/lib/blocklyd/workloads/mc-1/data" },
  "createdAt": "…",
  "updatedAt": "…"
}
```

`state`, and how it maps onto Blockly's `ObservedState`:

| `state` | Meaning | Blockly |
|---|---|---|
| `creating` | Accepted; its container isn't made yet (an interrupted create). | `stopped` |
| `created` | Made and not running since. | `stopped` |
| `running` | Running. | `running` |
| `stopping` | A requested stop is in progress. | `stopping` |
| `stopped` | Exited after a requested stop, or with code 0, 130 or 143. | `stopped` |
| `crashed` | Exited with a failure on its own, or `exit.oomKilled`. | `crashed` (with `exit`) |
| `restarting` | The restart policy is bringing it back. | `starting`, plus `lastFailureAt` as `failedAt` |
| `missing` | blocklyd has a record but the runtime lost the container. | `absent` |
| `retained` | Compute removed on request, data kept. | `absent` |
| `unknown` | The runtime can't be asked right now. | `unknown` |
| `fenced` | Superseded by a newer placement: stopped, kept, never started again. | `stopped` (the fleet adapter never reports a stale copy as the server) |

A few fields need a word:
- **`endpoints`** is Blockly's `endpoint(handle, port, audience)`: per audience, the host
  addresses and port where the edge or the control plane reaches it. IPv6 is `[addr]:port`.
- **`restartCount`** counts blocklyd's restarts after failures since the last requested start or
  new spec. It survives restarts of blocklyd.
- **`issues`** lists what reconciliation or an operation found. The codes: `container_missing`,
  `create_incomplete`, `record_rebuilt`, `digest_mismatch`, `port_mismatch`, `port_conflict`,
  `port_out_of_range`, `duplicate_container`, `unexpected_container`, `over_storage`,
  `insufficient_capacity` (a restart after a failure didn't fit in memory; it stays until the
  workload is started or replaced), and `restore_finished` (a restore cut short by a crash after its
  data was complete was finished when blocklyd started: whoever asked for it may not have heard).
- **`locate`** is for operators. It is informational, never a contract.

### `POST /v1/workloads/{id}/start`

`200 PowerResponse { changed, forced: false, workload }`.

| Status and code | When |
|---|---|
| `409 no_compute` | The workload is retained. |
| `409 not_created` | An interrupted create; PUT again. |
| `409 container_missing` | The runtime lost the container; PUT again. |
| `409 insufficient_capacity` | Running memory would exceed the allocatable memory. This is Blockly's `RuntimeFull`. |
| `409 port_conflict` | Something else holds its host port. |

### `POST /v1/workloads/{id}/stop`

The body is optional: `{ "timeoutSeconds": 1–600 }` overrides the spec's grace.
- blocklyd sends `stop.signal`, waits the grace, then kills.
- `200 PowerResponse`. `forced: true` means the grace ran out and the workload was killed
  without saving.
- The request returns once the workload has exited, so it can take up to grace + 30 s.

### `POST /v1/workloads/{id}/kill`

SIGKILL now, with no grace: Blockly's `forceStop`. `200 PowerResponse`.

### `DELETE /v1/workloads/{id}[?data=keep|delete]`

- **`keep`** (the default): blocklyd removes the container (stopping it gracefully first if it
  runs) and releases its ports.
  - The data stays. The workload becomes `retained`, and a later PUT brings its compute back.
  - This is Blockly's `decommission`.
- **`delete`**: the same, and the whole workload directory (record and data) is **moved to the
  host's trash** in one rename.
  - It is purged after `trash_retention_hours`.
  - This is Blockly's `destroy` or `release`.
  - It requires `x-blockly-confirm-delete-data: {id}`, echoing the id; otherwise
    `428 confirmation_required`. Data is never deleted on a single mistake.

`200 DeleteResponse { existed, removedContainer, data: "kept" | "trashed" | "absent", trashPath }`.

### `POST /v1/workloads/{id}/exec`

The body is `{ "command": ["argv", …], "timeoutSeconds": 1–600 }`, with a 30 s default. Limits:
≤ 256 arguments of ≤ 128 KiB each (Linux's own limit), ≤ 1 MiB in all.
- It runs **inside the workload**, as the workload's user, with its limits. It never runs on the
  host.
- blocklyd adds no shell. Send `["sh", "-c", script, name, args…]` for one: Blockly already
  does.
- Output is capped at 1 MiB per stream (`stdoutTruncated`, `stderrTruncated`).
- **On timeout blocklyd kills the command, and every process it started, by host pid.** Docker
  has no API to do that ([moby/moby#35703](https://github.com/moby/moby/issues/35703)).
  - Each pid is checked against the container's cgroup first, in case it was reused.
  - `timedOut: true`, `killed: true`.
- The workload must be running (`409 not_running`). blocklyd checks that, and the epoch, under the
  workload's lock, then runs the command in the container it found, by id, without holding the
  lock: a stop or a fence never waits for an exec. If the workload stops or is replaced in between,
  the command runs nowhere else: `409 not_running`.
- An optional `Idempotency-Key` header is covered above.

`200 ExecResponse { exitCode, stdout, stderr, stdoutTruncated, stderrTruncated, timedOut, killed, durationMs }`.

### `GET /v1/workloads/{id}/logs[?tail=N&since=<unix>&follow=true]`

`application/x-ndjson`, one JSON object per line:

```
{"ts":"2026-09-28T02:16:32.123Z","stream":"stdout","line":"[Server thread/INFO]: Done (9.983s)! For help, type \"help\""}
{"event":"end","reason":"complete"}
```

- `tail` is at most 10 000. Without `follow` it defaults to 200 and the stream ends with
  `{"event":"end","reason":"complete"}`.
- With `follow` the stream continues **across the workload's restarts**:
  - It emits `{"event":"restarted"}` and reads each new run from its start.
  - It ends with `{"event":"end","reason":"removed"}` when the container is removed.
  - It also ends when the client disconnects.
- Lines are capped at 16 KiB.

### `GET /v1/workloads/{id}/stats`

`200 StatsView`:
- `state`, `uptimeSeconds`
- `cpuCores`: cores in use since the previous sample. `cpuSecondsTotal`,
  `cpuThrottledPeriods`, `cpuLimitMillis`
- `memoryBytes`, `memoryWorkingSetBytes` (usage minus inactive page cache), `memoryLimitBytes`
- `pids`, `pidsLimit`
- `networkRxBytes`, `networkTxBytes`
- `dataUsedBytes`, measured every `disk_usage_seconds` by a walk of the data directory that never
  follows a link out of it and leaves out any filesystem mounted inside it

### `POST /v1/workloads/{id}/fence`

Body: `{ "currentEpoch": N }`: the control plane says which epoch is current.
- A copy older than `N` is stopped (gracefully) if it runs, marked superseded by `N`, and kept: its
  data stays where it is, and its Docker restart policy stays `no`.
- A copy at or past `N` is left alone, so a late fence is harmless.
- One older than `N` that is already superseded is fenced again all the same: its Docker restart
  policy is set to `no` again, and it is stopped if it runs. The answer says `changed: false`, so a
  repeated fence is harmless too.
- A fence the runtime couldn't carry out (it didn't answer) is an error, but the mark stands:
  nothing starts the copy again, and blocklyd stops it itself once a resync finds it running.
- Needs no epoch header: it is itself the statement of which epoch is current.
- Heartbeat answers carry the same instruction (below); this endpoint is for fencing at once.

`200 FenceResponse { changed, stopped, workload }`.

### `POST /v1/workloads/{id}/export`

Body: `{ "url": "…", "headers": { … }, "quiesced": false, "exclude": ["libraries"] }`, with the
epoch header (exact).
- blocklyd writes the workload's data directory as a **gzip tarball of `./`-relative paths**,
  Blockly's archive format, to a spool file under its state directory while hashing it, then
  `PUT`s it to the presigned `url` with a known length. Gzip's fastest level: region files are
  compressed already.
- `exclude` names entries at the top of the data to leave out (at most 64 plain names): what the
  image makes again when it is missing, so a move carries the world and not the server jar.
- Only a stopped workload exports, unless `quiesced: true` says the caller paused its saving
  (Minecraft: `save-off`, `save-all flush`). A superseded copy never exports.
- The data belongs to the workload, which can rearrange it while it is read. blocklyd reads it
  relative to directories it already holds open, with `O_NOFOLLOW` at every step: a symlink is
  archived as a symlink and never followed, an entry swapped mid-read is skipped, and a file that
  grows while it is read is cut at the size its header states. Nothing outside the data directory
  can reach the archive.
- The URL is used once and never logged or stored.

**In parts.** One PUT carries at most `transfer.max_put_mb` (default 5115, 5 GiB less 5 MiB:
Cloudflare R2's limit, the strictest of the S3-compatible stores). For a larger archive the body
may also carry `"parts": { "partSize": 67108864, "urls": ["…", "…"], "headers": { … } }`: an S3
multipart upload the control plane has begun, a presigned URL for each part, in order. blocklyd
packs first, then decides:
- An archive no larger than one PUT goes to `url`, whether parts were offered or not; their URLs
  are never called, and the control plane drops the upload.
- A larger one, with enough parts offered (`ceil(size / partSize)` of the URLs, from the first),
  goes in parts: every part `partSize` bytes but the last, each read from the spool with its
  length declared, one after another. A part whose PUT fails on the network or answers 5xx, 408
  or 429 is sent again, up to three times in all; any other answer, or a part with no `ETag`,
  fails the export (`502 transfer_failed`). The response then carries `parts: [{ number, etag
  }]`, from 1, which the control plane completes the upload with. blocklyd holds no credentials
  for the store, so it never completes or aborts an upload itself.
- A larger one without parts, or with too few, is `422 archive_too_large` with
  `details.sizeBytes` (the archive's size) and `details.limitBytes` (one PUT's limit, or what
  the parts offered carry): refused once packed, before anything is sent. Asked again with
  enough parts, it goes.

`partSize` is 5 MiB to 5 GiB less 5 MiB, and `urls` 1 to 10,000 of them, or the request is `422
invalid_request` (`parts.partSize`, `parts.urls`) before anything is packed. The body still has
to fit `max_body_bytes` (2 MiB by default, about 2,000 presigned URLs); Blockly's control plane
offers about 64, larger parts for a larger archive. A node that lists the `multipart-upload`
feature takes `parts`; an older one refuses the field as unknown.

`200 ExportResponse { sizeBytes, sha256, format: "tar.gz", entries, durationMs, parts? }`. Upload
failures are `502 transfer_failed`; too little disk for the spool is `507 insufficient_disk`, up
front or once free disk falls under the floor while the spool is written (blocklyd looks every
256 MiB).

### `POST /v1/workloads/{id}/restore`

Body: `{ "url": "…", "sha256": "…" }` or `{ "snapshot": "<snapshot id>" }`, with the epoch header
(exact). The workload must exist (PUT first) and be stopped.
- **From a URL**, blocklyd downloads the archive to a spool file, and checks its sha256 when one is
  given, before anything changes: `422 checksum_mismatch` leaves the data alone. It unpacks into a
  fresh directory beside the data: regular files and directories only (links, devices and the like
  are skipped and counted); no absolute paths or `..`; files opened with `O_NOFOLLOW`; modes masked
  to 0755; owned by the workload's user; at most 256 GiB and 2 million entries; at most 1 MiB of
  any member's long name, long link and PAX records, or of a skipped entry's data. Anything else is
  `422 invalid_archive`. An archive of several gzip members is read whole, as `gzip -d` reads it.
- **From a snapshot** this node holds of the workload (`404 snapshot_not_found` otherwise), it
  copies the snapshot beside the data, sharing blocks where the filesystem can.
- **Disk.** A restore from a URL is refused up front (`507 insufficient_disk`) when free disk is
  already under `min_free_disk_mb`, or when the archive's announced length (`Content-Length`)
  wouldn't fit above it, and stopped with the same answer if free disk falls under the floor while
  it downloads or unpacks (blocklyd looks every 256 MiB). One from a snapshot needs the snapshot's
  full size free above the floor, shared blocks or not, and is stopped the same way while it
  copies. Either way the data is left as it was.
- Then it swaps the directories in one atomic exchange (`renameat2`), so a crash leaves the old
  data in place or the new, never neither, and the data it replaced goes to the host's trash. On a
  filesystem that can't exchange it renames twice instead. A restore a crash cuts short is settled
  when blocklyd starts: one whose new data was complete is finished (issue `restore_finished`),
  any other is removed, and the data is as it was.

`200 RestoreResponse { sizeBytes, sha256, entries, skipped, unpackedBytes, previousData, durationMs }`
(`sha256` is null for a snapshot).

### `POST /v1/workloads/{id}/snapshots`

Body: `{ "id": "<snapshot id>", "quiesced": false }`, with the epoch header (exact). The id is the
caller's (Blockly's archive id, a UUID; the workload-id rules apply).
- A copy of the data directory into `workloads/<id>/snapshots/<snapshot>/`, beside the data and
  never mounted into the workload. On a filesystem that shares blocks (XFS with reflink, btrfs)
  every file is a `FICLONE`: the snapshot takes moments and no space until the data changes. On
  one that can't (ext4), it is a full copy. Either way it needs the data's full size free above
  the floor (`507 insufficient_disk`): a copy that shares blocks takes as much as a plain one once
  the world moves on. It is stopped with the same answer, and removed, if free disk falls under the
  floor while it copies.
- The same rules as an export: a running workload only with `quiesced: true`, only the current
  copy, and the same walk that never leaves the data directory.
- Asking again with the same id answers `200` with the snapshot already made (`created: false`).
  A snapshot interrupted by a crash has no `snapshot.json` and is redone; what it had copied is
  removed when blocklyd starts, as are spool files a crash left.

`201 SnapshotResponse { created, snapshot: SnapshotView }`, where `SnapshotView` is `{ id,
workload, epoch, createdAt, sizeBytes, files, method: "reflink" | "copy", quiesced, specDigest,
durationMs }`.

### `GET /v1/workloads/{id}/snapshots`

`200 { snapshots: SnapshotView[] }`, oldest first.

### `POST /v1/workloads/{id}/snapshots/{snapshot}/upload`

Body: `{ "url": "…", "headers": { … }, "parts": { … } }`, `parts` optional. The snapshot as an
archive, exactly as an export would write it, PUT to the presigned URL, or in parts when it is
larger than one PUT carries and they are offered, as for an export, which says how. No epoch: a
snapshot doesn't change. It doesn't hold the workload's lock either, so a long upload never
delays a stop. `200 ExportResponse`. A snapshot packs for one upload at a time: another asked for
meanwhile is `409 snapshot_busy`, and goes once the first ends.

### `DELETE /v1/workloads/{id}/snapshots/{snapshot}`

`200 { existed }`. Deleting what isn't there (a workload deleted with its data takes its snapshots
along) is not an error. During an upload of it: `409 snapshot_busy`.

### Ops listener (plain HTTP, loopback by default)

It asks for no certificate, so `ops.listen` is loopback or a private address (10.0.0.0/8,
172.16.0.0/12, 192.168.0.0/16, 100.64.0.0/10, fc00::/7, fe80::/10): blocklyd refuses to start with
0.0.0.0, `::` or a public address.

| Path | |
|---|---|
| `GET /healthz` | `200 ok` while the process runs. For systemd and liveness checks. |
| `GET /readyz` | `200` once Docker answers and blocklyd has reconciled; `503` otherwise. |
| `GET /metrics` | OpenMetrics text: operations, durations, HTTP, auth failures, reconciliations, runtime events, workloads by state, ports, issues, per-workload memory, CPU and disk, snapshot bytes, blocklyd's own RSS and CPU, and in fleet mode heartbeats, contact age, the execution lease left and certificate renewals. |

## Error codes

| Status | `code` |
|---|---|
| 400 | `invalid_request` (malformed JSON or query, both `If-Match` and `If-None-Match`), `invalid_workload_id`, `invalid_snapshot_id` |
| 403 | `forbidden`: a verified certificate for a name that isn't allowed |
| 404 | `not_found`, `snapshot_not_found`, `no_route` |
| 405 | No `code`: a method the path doesn't take is refused with an empty body. |
| 409 | `no_compute`, `not_created`, `container_missing`, `not_running`, `not_stopped`, `not_quiesced`, `insufficient_capacity`, `no_free_ports`, `port_conflict`, `name_taken`, `stale_epoch`, `epoch_ahead`, `superseded`, `snapshot_busy` |
| 412 | `precondition_failed` |
| 413 | `payload_too_large` (bodies over `max_body_bytes`, 2 MiB by default); `POST …/stop` reads its optional body raw, so its 413 is plain text with no `code` |
| 415 | `invalid_request` (a request that takes a JSON body, sent without a JSON `content-type`) |
| 422 | `invalid_request` (spec or exec validation, unknown fields), `idempotency_key_reused`, `invalid_archive`, `checksum_mismatch`, `archive_too_large` (an export or snapshot upload larger than one PUT carries, with no parts or too few offered; `details.sizeBytes`, `details.limitBytes`) |
| 428 | `confirmation_required`, `epoch_required` |
| 500 | `internal` |
| 502 | `runtime_error` (Docker refused something unexpected), `transfer_failed` (an export or restore couldn't move the bytes) |
| 503 | `runtime_unavailable` |
| 504 | `timeout` |
| 507 | `insufficient_disk` |

## The control plane's endpoint (fleet mode)

A node in fleet mode also talks to the control plane: it enrolls once, then beats, and renews its
certificates before they end. All are HTTPS to `fleet.url`, trusting only the fleet CA's
certificate (`fleet.ca`), TLS 1.3. The control plane's side is
[`node-endpoint.ts`](https://github.com/cubepals/cubepals/blob/main/apps/control/src/infra/fleet/node-endpoint.ts) and
[`registry.ts`](https://github.com/cubepals/cubepals/blob/main/apps/control/src/infra/fleet/registry.ts); the wire types are in
[`src/protocol/wire.rs`](../src/protocol/wire.rs).

### What a joining host fetches

Before a host has anything, it fetches four things from the endpoint, with no authentication
([`join-routes.ts`](https://github.com/cubepals/cubepals/blob/main/apps/control/src/infra/fleet/join-routes.ts)). None is secret, and each is
checked before it is used:

- `GET /fleet/v1/ca.pem`: the fleet CA. Fetched without checking the connection, since nothing is
  trusted yet, and then checked against the join token's hash; nothing else is fetched until it
  matches.
- `GET /fleet/v1/join.sh`: [`deploy/join.sh`](../deploy/join.sh), with
  [`deploy/daemon.json`](../deploy/daemon.json) written into it. Fetched over TLS checked against
  that CA.
- `GET /fleet/v1/blocklyd` and `GET /fleet/v1/blocklyd.sha256`: the static blocklyd the control
  plane's image carries, and `<sha256 hex>  blocklyd`, which `sha256sum -c` reads. Without the
  binary both are `404`, in plain text saying why.

**The join token.** `bk1.` and base64url (no padding) of
`{"u": "<endpoint URL>", "h": "<sha256 hex of ca.pem as served>", "d": "<deployment>", "s": "<secret>"}`,
and `"n": "<node id>"` in a token that re-enrolls a node. `blocklyd join` and enrollment read it
([`src/fleet/token.rs`](../src/fleet/token.rs)); only `s` is sent to `enroll`, once `d` is the
configuration's deployment and `h` matches `fleet.ca`. A bare token, the secret alone, is still
accepted everywhere a token is. The region, labels and the node to re-enroll are kept with the
token on the control plane, which decides by them; `n` only lets `join` tell the host it is pasted
on from another node's.

### `POST /fleet/v1/enroll`

No client certificate yet: the one-time token is the authorization: a bare token, or a join
token's secret (the control plane takes a whole join token too, and looks up its secret).

```json
{ "token": "…", "csrPem": "-----BEGIN CERTIFICATE REQUEST-----…", "facts": NodeFacts }
```

- `NodeFacts`: hostname, boot id, sha256 of `/etc/machine-id`, daemon version, protocol and
  features, the API address to dial, edge and control addresses, capacity, labels.
- The key behind the CSR is made on the node and never leaves it. The CSR's subject and names are
  ignored: the control plane names the node.
- **One key until answered.** blocklyd writes the key to `identity/enroll-key.pem` (0600) before
  its first attempt, and every attempt asks for it until one is answered, after a restart too. A
  spent token presented again with a CSR for the key that spent it gets a fresh answer for the
  same node, so an answer lost on the way (a timeout, a connection reset once the control plane
  had answered) is asked for again.
- The answer: `{ nodeId, deploymentId, serverCertPem, clientCertPem, caPem, allowedClients,
  heartbeatSeconds }`. Two certificates for the node's one key: serverAuth for its API, clientAuth
  for its heartbeats. blocklyd ignores this `heartbeatSeconds`: it beats every `heartbeat_seconds`
  of its own configuration until a heartbeat answer names another interval (1–60 seconds).
- `caPem` must be the certificate in `fleet.ca`, compared as DER rather than as PEM text: from then
  on the node trusts that CA for the control plane and for who may call its API. An answer naming
  another is refused, and no identity is written.
- blocklyd writes `identity/` (the enrollment key becomes `node.key`, 0600), then deletes the
  enrollment key and the token file. `node.json`, which says "enrolled", goes last, once both
  certificates are known to be for the key: an answer the node can't use leaves it unenrolled, to
  ask again. An unknown or expired token, or a spent one presented with another key, is
  `403 invalid_token`, one answer for all three.

### `POST /fleet/v1/nodes/{nodeId}/heartbeat`

With the node's clientAuth certificate, every `heartbeat_seconds` (5).

```json
{
  "nodeId": "…", "sessionId": "…", "bootId": "…", "seq": 42,
  "daemonVersion": "0.2.0", "protocol": { "current": 1, "supported": [1] }, "features": ["…"],
  "runtimeUp": true, "reconciled": true, "capacity": NodeCapacity,
  "workloads": [{ "id": "…", "epoch": 3, "supersededBy": null, "state": "running", "specDigest": "…",
                  "generation": 2, "memoryMb": 3072, "restartCount": 0, "exit": null,
                  "lastFailureAt": null, "changedAt": "…", "ports": { "game": 42001 } }],
  "issues": [],
  "addresses": { "api": "10.0.0.5:7443", "edge": "10.0.0.5", "control": "10.0.0.5" },
  "upgradeFailed": { "version": "0.3.0", "reason": "blocklyd 0.3.0 exited with status 1 before it came up" }
}
```

- **The whole state, every time**: every copy the node holds, whatever its epoch. The first beat
  after a restart or a partition is therefore a full resync.
- The session id is new for each daemon run and `seq` counts from 1 within it; the control plane
  uses them to ignore delayed beats and to catch two hosts sharing one identity.
- `issues` beside a workload is what the node found wrong with it and can't mend itself: a restart
  refused for want of room (`insufficient_capacity`), data over its spec's size (`over_storage`),
  each `{ "code", "detail" }`. Left out when there is none. The top-level `issues` are the host's.
- `addresses` is where the node is reached as its configuration says now. The control plane
  follows a change (and records `node.addresses_changed`): the node's identity is its certificate,
  not its IP.
- `upgradeFailed` is the last upgrade this node gave up on, putting back the binary it had: sent in
  every beat until it tries another, and left out when there is none (below).
- `capacity` includes `snapshotBytes` (what local snapshots hold) and `reflink` (whether they share
  blocks with the data). `usedMemoryBytes` and `usedCpuCores` add up the latest samples of the
  workloads running now; they are null only while workloads run and none has been sampled yet.
- The answer:

  ```json
  { "lifecycle": "active", "fences": [{ "workload": "…", "currentEpoch": 4 }],
    "heartbeatSeconds": 5, "leaseSeconds": 120 }
  ```

  blocklyd applies each fence as `POST …/fence` would, **then** takes the lease, so nothing the
  lease allows can be a copy the same answer superseded.
- **The execution lease** (`leaseSeconds`, counted from when the node sent the beat): while it
  lasts, blocklyd may act on its own: restart a workload that failed, per its spec's policy, and
  resume one the host went down under (below). Without one (no answer yet in this run, or none for
  longer than the lease) it does neither. A node the control plane holds `lost` gets `0`. Requests
  carry their own authority (the epoch) and don't need the lease. A control plane that names no
  lease grants `restart_requires_contact_seconds`.
- A control plane that doesn't answer changes nothing on the node: workloads carry on running. It
  only stops what the node would do unasked.
- `renew: true` says the node's certificates end within the control plane's renewal window. The
  answer leaves `renew` out otherwise.
- `upgrade: { "version": "0.3.0", "sha256": "…" }` offers the node the blocklyd the endpoint serves
  at `GET /fleet/v1/blocklyd`, to a node that lists `self-upgrade`; the answer leaves it out
  otherwise, and a node without the feature ignores it. blocklyd downloads it over this same mutual
  TLS and installs it only if its sha256 is the one offered and `--version` prints the version
  offered. It keeps the binary it ran as `blocklyd.prev`, renames the new one into place under the
  state directory (`bin/blocklyd`, which `/usr/local/bin/blocklyd` links to), and exits cleanly
  so systemd starts it; workloads keep running. The new binary is on trial
  (`upgrade/trial.json`) until it has reconciled and a heartbeat is accepted, within 120 seconds;
  otherwise it puts `blocklyd.prev` back and exits. One that can't start at all is put back by
  the unit's `ExecStopPost` (`blocklyd.prev upgrade --exited`), and so is one still on trial 240
  seconds after it started (a startup that hangs), which exits. The binary put back reports
  `upgradeFailed`. A download that fails for want of the control plane is tried again a minute
  later, while the offer stands; any other failure is reported. The control plane offers it to
  one node per region at a time (Cubepals' docs/fleet.md, "Upgrades").
- Refusals: `400 invalid_json` (the body isn't JSON), `400 invalid_request` (a beat for another
  node), `401 certificate_required` (no client certificate from the fleet CA), `403 wrong_identity`
  (not this node's client certificate), `403 certificate_not_current` (not the node's pinned
  certificate), `403 retired`, `404 unknown_node`, `409 duplicate_identity`, `409 session_replaced`
  (a beat from a daemon run that has been replaced), `413 too_large` (a body over 4 MiB), and
  `500 internal`.

### `POST /fleet/v1/nodes/{nodeId}/renew`

With the node's current clientAuth certificate: `{ "nodeId": "…", "csrPem": "…" }` for a **new**
key made on the node. The answer is `{ serverCertPem, clientCertPem, caPem }`.
- blocklyd writes the new key and certificates as the next generation (`node-N.key`, `node-N.pem`,
  `client-N.pem`), checks they load and that both certificates are for the new key, then rewrites
  `node.json` to name generation N. That rename is the switch: a crash anywhere leaves one whole
  generation in use.
- The API's certificate is replaced in place for new connections, and the next heartbeat uses the
  new client certificate. The control plane accepts the old certificate or the new one until the
  first beat that presents the new one, and then only the new one.
- An answer naming another CA (compared as DER, as at enrollment) is refused: rotating the fleet CA
  re-provisions nodes.
- A failed renewal keeps the current certificates and is tried again after ten minutes while the
  control plane still asks. A node whose certificates ended can't renew: it enrolls again with a
  new token (Cubepals' docs/fleet-operations.md).

## After the host restarts

blocklyd records, per workload, the boot of the host (the kernel's boot id) in which it last saw
it running, and clears it once it sees it stop in that same boot. After the host restarts, a
workload still marked with an earlier boot, never asked to stop, not superseded, and with an
`on-failure` restart policy was running when the host went down. It reads `restarting`, and is
started again once blocklyd holds a lease (single-node: at once). A fence in the same answer wins;
a node held `lost` gives the resume up, and forgets it. A restart of blocklyd alone, in the same
boot, resumes nothing: what died while it was down is reported, not restarted.

## How Blockly's `MinecraftRuntime` maps onto it

[`fleet-runtime.ts`](https://github.com/cubepals/cubepals/blob/main/apps/control/src/infra/fleet/fleet-runtime.ts) implements the port
(`apps/control/src/app/ports/runtime.ts`) over this protocol, with the placement, epochs and
fencing described in [docs/fleet.md](https://github.com/cubepals/cubepals/blob/main/docs/fleet.md). Its handle is `fleet:v1:` and
base64url JSON of `{deployment, key, node, nodeName, epoch, region, edgeHost, controlHost, ports}`.

| Port method | Protocol |
|---|---|
| `ensureProvisioned(key, placement, spec)` | Choose the node, record the placement, then `PUT` the spec with the epoch and `POST start`. `RuntimeSpec` maps field for field. |
| `apply(handle, spec)` | `PUT` with the handle's epoch. |
| `start` / `stop` / `forceStop` | `POST start` / `stop` / `kill`, with the epoch. |
| `waitRunning` | Poll `GET` until `running`. |
| `exec` | `POST exec`, with the epoch. |
| `snapshot` | `POST snapshots` on the server's node, then `…/upload` to the archive store in the background. |
| `exportSnapshot` | `…/upload` from the node while it holds the copy, else a copy within the store. |
| `restore` | In place from the node's own snapshot (`{snapshot}`); elsewhere `PUT` with a new epoch on the chosen node, then `POST restore` from the archive, checked against its sha256. |
| `relocate` | `POST export` from the stopped source with `exclude` (what the image remakes), then as a restore elsewhere; the old copy gets `POST fence` and `DELETE ?data=delete`. |
| `goneSnapshots`, `deleteSnapshot` | The archive store and `DELETE …/snapshots/{snapshot}` on the node. |
| `decommission` | `DELETE ?data=keep`. |
| `destroy`, `release` | `DELETE ?data=delete`, confirmed, on every node that holds a copy. |
| `observe`, `observeChanged`, `inventory` | Heartbeats, stored per node and copy: no call per server. |
| `endpoint(handle, port, audience)` | From the handle, which carries the node's addresses and the host ports. |
| `locate` | Node, region, epoch and container name, from the handle. |
| `tag` | A no-op; labels are fixed at creation. |
| `prices` | A share of the node's monthly cost (its `monthly_cost_cents` label), by memory. |
| `LogSource.recent` / `tail` | `GET logs?tail=N` / `?tail=0&follow=true`. |
