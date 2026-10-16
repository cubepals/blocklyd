# Fly and fleet parity

What Cubepals asks of a runtime, and how its Fly runtime and its fleet runtime (blocklyd on Linux
hosts) each do it. A player should not be able to tell which one runs their server. **Update this
page in the same change as anything that moves a row, in either repository.** A gap's fix is in
[parity-plan.md](parity-plan.md).

Paths: **Fly** is in cubepals' `apps/control/src/infra/fly/`, **fleet** in its
`apps/control/src/infra/fleet/`, **app** in its `apps/control/src/app/`, **edge** in its
`apps/edge/`; **blocklyd** is this repository's `src/`. The port both implement is cubepals'
`app/ports/runtime.ts` (`MinecraftRuntime`).

Fleet column: **has it**, **differs** (the same outcome another way), or **lacks it** (a gap,
named G1…G8).

Last checked against cubepals main at 8ad8689 (2026-10-16) and blocklyd 0.3.1.

## Sleep and wake

| # | Capability | Fly | Fleet | Where |
|---|---|---|---|---|
| 1 | Sleep with memory kept | `sleep()` suspends the machine, so the next start resumes it as it was. | **Lacks it (G1).** No `sleep`, so the app stops the server, and every wake boots from disk. blocklyd has no suspend verb. | app: `operations/handlers/winding-down.ts` (`mayDoze`, `memory_kept_at`), `runtimes/router.ts` (`sleeps`). Fly: `fly-runtime.ts` (`#sleep`), `fly-runtime/machines.ts` (`sleepMachine`). Fleet: `fleet-runtime.ts` (none). blocklyd: `manager/power.rs` (start, stop, kill only). |
| 2 | A sleeping server's change keeps its memory (`resumesOn`) | It resumes on the spec it slept on when only `resumesOn` differs (message, icon, plan limits); the change lands at the next boot. | **Lacks it (G2).** `resumesOn` is dropped, and every start PUTs the spec, so an env-only change rebuilds the container before it boots. | app: `servers/specs.ts` (`driftDigest`), `operations/handlers/bringing-up.ts` (`asItSlept`). Fly: `machine-config.ts` (`asleepOn`). Fleet: `fleet-runtime/workload-api.ts` (`toWorkloadSpec`), `fleet-runtime.ts` (`ensureProvisioned`). blocklyd: `manager/ensure.rs`. |
| 3 | A resume that hangs falls back to a cold start | A resume gets 30 s to run and 10 s to answer, then it is killed and cold-started on its own disk. A host that can't run it (`HostFull`, `StuckOnHost`) is rebuilt from the sleep's snapshot. | **Lacks it (G1, G4).** There is no resume. The start's poll loop can overrun its bound, a single unreachable poll fails the start, and the PUT and start before the boot carry 20- and 1-minute timeouts. | app: `operations/resume.ts`, `operations/handlers/host-refused.ts`, `operations/boot.ts`. Fly: `fly-runtime/responses.ts`. Fleet: `fleet-runtime.ts` (`waitRunning`), `fleet-runtime/workload-api.ts`. blocklyd: `runtime/docker.rs` (every Docker call bounded). |
| 4 | Restore point at every sleep | A snapshot after any sleep someone played before, by the app, on any runtime. | **Has it.** A copy on the node at once (reflink where it can), uploaded in the background. The upload shares the node with servers booting there (G3). | app: `operations/handlers/winding-down.ts`, `operations/handlers/backups.ts` (`restorePoint`). Fleet: `fleet-runtime.ts` (`snapshot`), `fleet-runtime/transfers.ts`. blocklyd: `manager/snapshots.rs`, `store/snapshots.rs`, `tarball.rs`. |
| 5 | A ping starts the wake | The edge asks for a wake when a sleeping server is pinged, and only one whose wake is a boot is woken. | **Has it.** Every fleet sleep is a stop, so every ping of a sleeping fleet server starts a wake. | app: `edge/ping-wake.ts`. edge: `ping-wake.ts`. |
| 6 | What a wake costs, to the first answer | From memory, a resume. Stopped, a boot. At rest, a restore from the archive store by a helper machine, then a boot (cubepals `docs/waking.md`). | **Differs (G1, G7).** Always a boot: a PUT, a start, a 500 ms poll for `running` (although the start's answer already says it), then the readiness pings. At rest, a restore from the store onto a node, then a boot. | app: `operations/boot.ts`. Fleet: `fleet-runtime.ts` (`ensureProvisioned`, `waitRunning`), `fleet-runtime/admission.ts`, `fleet-runtime/rehoming.ts`. blocklyd: `manager/power.rs`, `manager/archive.rs`. |

## Starting, room and applying

| # | Capability | Fly | Fleet | Where |
|---|---|---|---|---|
| 7 | Create and start lock | The platform's capacity lock, per-owner locks, and no provider call under the lock. | **Has it.** `hasRoom` is asked before the lock, as one aggregate query. The region's advisory lock covers only database work. | app: `policy/access-policy.ts`, `platform/room.ts`, `runtimes/service.ts`. Fleet: `fleet-runtime.ts` (`hasRoom`), `registry/summaries.ts`, `registry/node.ts` (`lockRegion`). |
| 8 | Start and wake priority | Wakes have a lane of their own ahead of other work, and restores and exports run in helper machines beside the server. | **Differs (G3).** The same lane. On a node, snapshot copies, uploads, exports and restores run in blocklyd at the same CPU and disk priority as the servers booting beside them. | app: cubepals `infra/pg/jobs.ts` (wake lane). Fly: `fly-runtime/helper-machine.ts`. blocklyd: `manager/archive.rs`, `manager/snapshots.rs`, `tarball.rs`, `tree.rs`. |
| 9 | Room once a server stops | A stopped machine holds nothing. | **Lacks it (G8).** The ledger counts a copy as running until a heartbeat says otherwise, up to `heartbeat_seconds` (5) later, so a wake just after another server's stop can be refused room and moved through the archive store. | Fleet: `registry/summaries.ts` (`HOLDS`), `fleet-runtime/admission.ts`, `fleet-runtime/rehoming.ts` (`startElsewhere`). blocklyd: `fleet/heartbeat.rs` (fixed interval). |
| 10 | No room where it sleeps | Fly's limits are waited on (`waiting_for_room`, `RuntimeAtLimit`). A host that can't run it is rebuilt elsewhere. | **Differs.** A start with no room on its node moves the world to a node with room through the archive store (`placement.moved_for_room`). No node with room is `RuntimeFull`. | Fly: `fly-runtime/room.ts`, `refusals.ts`. Fleet: `fleet-runtime/admission.ts`, `fleet-runtime/rehoming.ts`. blocklyd: `manager/room.rs` (`insufficient_capacity`). |
| 11 | Apply when the compute is gone or being replaced | It waits out Fly's replace and finds a machine Fly replaced on the same volume. A failed apply leaves the server stopped on what it ran. | **Differs (G5).** A PUT brings back a missing or retained container (blocklyd). A stale handle is `StaleHandle`. An unreachable node fails the apply on one try, under a 20-minute timeout, and nothing guards against two applies at once (no `If-Match`, which blocklyd takes). | app: `operations/handlers/updating.ts`, `going-back.ts`. Fly: `fly-runtime.ts` (`apply`), `fly-runtime/machines.ts`. Fleet: `fleet-runtime.ts` (`apply`), `fleet-runtime/ledger.ts`, `fleet-runtime/workload-api.ts`. blocklyd: `manager/ensure.rs`. |

## What a server fetches

| # | Capability | Fly | Fleet | Where |
|---|---|---|---|---|
| 12 | A template world from the archive store (the Arcade's) | The server downloads `BLOCKLY_STARTER` through the artifact endpoint on its first start, checked by sha512. | **Has it**, as long as the container reaches the artifact endpoint and the store (G6: not written down for fleet hosts). | app: `artifacts/links.ts`, `artifacts/service.ts`, `setups/starters.ts`; cubepals `minecraft/starter.ts`. blocklyd: `runtime/docker.rs` (`ensure_network`: inter-container traffic off, egress on). |
| 13 | Jars, packs, datapacks and the server's icon | Fetched by the server from `/runtime/v1/…` with its own token. | **Has it**, under the same condition (G6). | app: `artifacts/`, `server-icons/`. |
| 14 | Install seeds (`InstallSeed`) | Ignored. | Ignored: a first start downloads the server. | Fly: `fly-runtime.ts`. Fleet: `fleet-runtime.ts`. |

## The edge

| # | Capability | Fly | Fleet | Where |
|---|---|---|---|---|
| 15 | A sleeping server's own message and icon | The asleep route carries them. | **Has it.** It doesn't depend on the runtime. | app: `edge/service.ts` (`listed`). edge: `asleep.ts`. |
| 16 | The edge's routes for a server | Flycast addresses, which stay while a machine is stopped. | **Has it.** The node's edge address and the host port from the handle; a stopped server keeps both (`stableEndpoints`). A resting one routes asleep with no destination, and a wake answers where it runs. Host ports wait 10 minutes before reuse. | app: `edge/service.ts` (`#destinationOf`). Fleet: `fleet-runtime.ts` (`endpoint`), `fleet-runtime/node-cache.ts`. blocklyd: `ports.rs`. |
| 17 | Running once every edge routes it | Route versions that every edge reports back. | **Has it.** It doesn't depend on the runtime. | app: `edge/routed.ts`. |
| 18 | Per-server limits at the edge | New connections a second and open connections per server, and per-address limits. | **Has it.** It doesn't depend on the runtime. | edge: `limits.ts`, `front.ts`. |
| 19 | Players' addresses (PROXY protocol v2) and the client version check | The edge sends the header and refuses a version the server can't take. | **Has it.** The header passes through Docker's published port as payload. | edge: `front.ts`, `header.ts`. app: `edge/service.ts`. blocklyd: `deploy/daemon.json`. |
| 20 | The console as soon as a server is up | RCON sign-in is tried again while the console closes connections. | **Has it.** It doesn't depend on the runtime. | cubepals `infra/mc-protocol/rcon.ts`. |

## Storage, moves and the node

| # | Capability | Fly | Fleet | Where |
|---|---|---|---|---|
| 21 | Snapshots, exports and restores in parts | Volume snapshots, helper machines, and archives in parts. | **Has it.** Node copies, uploads in parts (`multipart-upload`), restores checked by sha256. | Fly: `fly-runtime/snapshots.ts`, `helper-jobs.ts`. Fleet: `fleet-runtime/transfers.ts`, `archives.ts`. blocklyd: `manager/archive.rs`, `protocol/transfer.rs`, `tarball.rs`. |
| 22 | Moves, rebuilds and moves between runtimes | Relocation, the host-lost rebuild, and `adopt`. | **Has it.** A move through the archive store, a rebuild only after an operator confirms a loss, and `adopt` as a released placement. | Fleet: `fleet-runtime/rehoming.ts`. blocklyd: `manager/fence.rs` (epochs). |
| 23 | Leftover storage | `clearLeftovers` deletes volumes no machine mounts. | **Differs.** Nothing is left beside a world: a replaced or deleted world goes to the node's trash, purged after `trash_retention_hours`. | Fly: `fly-runtime.ts` (`clearLeftovers`). blocklyd: `store.rs`, `store/leftovers.rs`. |
| 24 | Logs | Fly's log stream. | **Has it**, across restarts. | Fly: `fly-logs.ts`. Fleet: `fleet-logs.ts`. blocklyd: `manager/logs.rs`. |
| 25 | A host joining | — (Fly's machines). | **Has it.** `blocklyd join` asks for the fleet CA again for up to 30 s while its endpoint refuses connections. The endpoint's `listening` line comes before its socket is known to be bound (G6). | blocklyd: `cli/join.rs`, `fleet/enroll.rs`. Fleet: `node-endpoint.ts`. |
| 26 | Tags, locate | Fly metadata, a console link. | **Differs.** Tags are a no-op; `locate` names the node and the container. | Fleet: `fleet-runtime.ts` (`tag`, `locate`). |

## Gaps

| Gap | Rows | Belongs to |
|---|---|---|
| G1 Sleep with memory kept, and its resume | 1, 3, 6 | blocklyd (a verb), then cubepals |
| G2 A change while asleep keeps the memory | 2 | blocklyd (a spec field), then cubepals |
| G3 Transfers slow servers booting beside them | 4, 8 | blocklyd |
| G4 A fleet start's waits are bounded loosely | 3 | cubepals |
| G5 Apply on an unreachable node, and applies at once | 11 | cubepals |
| G6 Fleet docs: what a container reaches; the endpoint's readiness | 12, 13, 25 | cubepals |
| G7 A PUT on every start, and a poll after the start already answered | 6 | cubepals |
| G8 Room lags a stop by up to a heartbeat | 9 | blocklyd, then cubepals |
