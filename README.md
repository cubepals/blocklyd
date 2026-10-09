# blocklyd

The node daemon of [Cubepals](https://cubepals.com). One per Linux host: it runs Minecraft
servers there in hardened Docker containers, for a control plane that decides what runs where. The
control plane is [cubepals/cubepals](https://github.com/cubepals/cubepals): its side is the `fleet`
runtime ([docs/fleet.md](https://github.com/cubepals/cubepals/blob/main/docs/fleet.md)), and operating a fleet is
[docs/fleet-operations.md](https://github.com/cubepals/cubepals/blob/main/docs/fleet-operations.md). blocklyd's API is
[docs/protocol.md](docs/protocol.md).

**What it does:** enrolls with the control plane and proves who it is with its own key; runs,
stops, updates and deletes workloads idempotently, refusing anything a newer placement superseded;
keeps each workload's data, snapshots and exports it without ever following a link out of it;
restores archives safely; restarts what fails under it while it holds a lease, and resumes what was
running when its host went down; reports everything it holds every few seconds.

**What it isn't:** a scheduler (it never chooses where anything runs), a database of record, a
provisioning tool (it never buys, powers or deletes machines), or a distributed system (nodes never
talk to each other). If the control plane goes away, servers keep running.

## Build and test

Rust 1.94 (`rust-version` 1.89), one Cargo package, `Cargo.lock` committed and every build
`--locked`.

```sh
cargo build --locked --release                               # target/release/blocklyd
docker build -t blocklyd .                                   # static (musl), as hosts run it, and as each
                                                             # release builds it: the image holds /blocklyd,
                                                             # /openapi, /deploy and /LICENSE.md
cargo test --locked                                          # unit, API, lifecycle, epochs, reconcile, resume, transfer; no root needed
sudo cargo test --locked --test docker -- --ignored          # against a real Docker daemon, as root
cargo test --release --test storage_bench -- --ignored --nocapture   # snapshot/archive costs on this disk
cargo deny --locked check                                    # advisories, licenses, bans, sources (deny.toml)
```

The control plane is tested against a real blocklyd too: CI's `contract` job builds this checkout
and runs Cubepals' `apps/control/src/infra/fleet/fleet-runtime.e2e.test.ts` from its `main`
against it, so a change here that would break the running control plane fails before it merges
([`.github/workflows/ci.yml`](.github/workflows/ci.yml)).

## Run

On a fleet's host, the line Cubepals' `bun scripts/fleet.ts token <region>` prints does all of it,
as root: Docker, blocklyd, its configuration and its service ([fleet-operations.md
§2](https://github.com/cubepals/cubepals/blob/main/docs/fleet-operations.md#2-adding-a-node)). That line ends in `blocklyd join`:

```sh
blocklyd join <bk1. token>               # the fleet CA, checked against the token; the configuration,
                                         # the token and the unit; doctor --preflight, then starts it. Prints what it
                                         # worked out (the address, the memory kept) and how to
                                         # override it. --address <ip> when the host has several.
                                         # A token for the node this host is enrolls it again,
                                         # under a new CA if the token names one
blocklyd doctor                                              # is this host ready? says what to fix
blocklyd doctor --fix                                        # on a host with no servers yet: daemon.json,
                                                             # the state directory; prints how to undo each
blocklyd doctor --json                                       # {checks: [{name, status, detail, fix}]}
blocklyd check-config --config /etc/blocklyd/blocklyd.toml   # the config check alone
blocklyd upgrade                                             # the control plane's blocklyd now, and a restart
                                                             # into it; puts this one back if it doesn't come up
blocklyd serve --config /etc/blocklyd/blocklyd.toml          # as root, beside Docker; refuses to start
                                                             # if it can't give data to the workloads' user
blocklyd --version
```

[`examples/blocklyd.toml`](examples/blocklyd.toml) lists every setting with its default;
[`deploy/`](deploy/) has the systemd unit and Docker's `daemon.json`. The unit runs
`doctor --preflight` before each start: it refuses only an invalid config or a state directory
blocklyd can't own, and logs the rest (Docker down, live-restore off, no reflink) as warnings,
since blocklyd runs through all of them. With a `[fleet]` section the
node enrolls and takes its identity from the control plane, and upgrades itself when a heartbeat's
answer offers it a newer blocklyd ([fleet-operations.md
§9](https://github.com/cubepals/cubepals/blob/main/docs/fleet-operations.md#9-upgrading-blocklyd)); without one it is a standalone daemon
with certificates you give it (`blocklyd dev-certs` makes throwaway ones), which local
development uses.

## Security model

- **It is root-equivalent.** It holds Docker's socket and runs as root to hand data directories to
  the workloads' user. Its API is reachable only with a client certificate from the fleet CA for
  the control plane's name, over TLS 1.3, on the private network. The ops listener (`/healthz`,
  `/readyz`, `/metrics`) asks for no certificate, so it binds only loopback (the default) or a
  private address.
- **Workloads get less than they ask for, never more.** Policy is the host's (`[workloads]`): a
  non-root user and data owner (uid 0 or gid 0 is refused), a read-only root filesystem (on by
  default), no capabilities, no privilege escalation, memory with no swap, a PID limit, an isolated
  network. A request can't relax any of it; it only picks its sizes, within bounds. CPU is shared
  by weight, in proportion to memory by default; there is a ceiling only when a request sets
  `cpuMillis`, which the fleet control plane never sends.
- **A workload's data is untrusted.** Snapshots, exports and the measure of its disk use walk it
  relative to directories already open, with `O_NOFOLLOW` at every step (`src/tree.rs`). Restores
  from an archive accept regular files and directories only, with bounded size and entry count.
  Restores from a local snapshot copy it with the same walk, so a symlink comes back as a symlink,
  never followed. Either way the new data is put together beside the old, within the disk's floor,
  and swapped in with one atomic exchange: a crash leaves the old world or the new, and what it cut
  short is settled at startup.
- **Its key never leaves the host.** Enrollment and renewal send certificate requests; the files
  under `/var/lib/blocklyd/identity` are switched by one atomic rename. Until enrolled, every attempt
  asks for one key, kept on disk, so an answer lost on the way can be asked for again.
- **It trusts one CA**, the fleet CA it was provisioned with (`fleet.ca`): an enrollment or renewal
  answer naming another is refused.
- **Supply chain:** dependencies are pinned by the lockfile, checked against RustSec, limited to
  permissive licenses and crates.io, and OpenSSL is banned (rustls with ring).

## Source

| | |
|---|---|
| `main.rs` | CLI, startup (enroll, reconcile, then serve), shutdown |
| `manager.rs` | every operation, the per-workload lock, epochs, restarts and resumes, the lease, snapshots |
| `api.rs` | the HTTP API, mutual TLS, errors |
| `fleet.rs` | enrollment, heartbeats, identity and renewal |
| `upgrade.rs` | upgrading itself to the control plane's blocklyd, and going back if it fails |
| `runtime.rs` | the container runtime (Docker, and a fake for tests) |
| `store.rs` | the state directory: records, snapshots, trash |
| `tree.rs`, `tarball.rs` | the safe walk, copies (reflink), archives |
| `reconcile.rs` | the Docker event stream and periodic passes |
| `protocol.rs`, `ids.rs` | the wire types and identifiers |
| `http_client.rs`, `tls.rs` | the small HTTPS client, certificates and keys |
| `config.rs`, `host.rs`, `ports.rs`, `metrics.rs`, `certs.rs`, `labels.rs`, `clock.rs` | the rest |

Where a comment or a message names `docs/fleet.md`, `docs/fleet-operations.md` or
`bun scripts/fleet.ts`, it means [Cubepals'](https://github.com/cubepals/cubepals): the fleet's
control plane and its operator tool live there.

## Releases

A pull request that raises `version` in `Cargo.toml` is a release. Once it has merged and CI has
passed on that commit of `main`, [`release.yml`](.github/workflows/release.yml) tags `v<version>`
on it and publishes:

- a GitHub Release, immutable, with the static `blocklyd` binary (x86_64, musl), `SHA256SUMS`,
  `node-reads.openapi.json` and `node-writes.openapi.json`, `deploy/daemon.json`,
  `deploy/blocklyd.service` and `LICENSE.md`, each with a build-provenance attestation;
- `ghcr.io/cubepals/blocklyd:<version>`, a `FROM scratch` image of the same files, attested.

Cubepals pins one release by digest (`bun scripts/blocklyd.ts bump <version>` there) and serves
that binary to the hosts that join its fleet. Check one:

```sh
gh attestation verify blocklyd -R cubepals/blocklyd
gh attestation verify oci://ghcr.io/cubepals/blocklyd:<version> -R cubepals/blocklyd
```

Within protocol v1 a release only adds (see [docs/protocol.md](docs/protocol.md)), so a node and
a control plane one release apart always understand each other.

## License

blocklyd is under the [Functional Source License, Version 1.1, ALv2 Future
License](LICENSE.md) (FSL-1.1-ALv2), from 0.3.0 on. You may use, copy, change and redistribute it
for any purpose except a Competing Use: offering it, or something built from it, in a commercial
product or service that substitutes for blocklyd or for Cubepals' hosting. Running it for
yourself, for your organization, for research or teaching is fine. Two years after each version is
published, that version is also available under the Apache License 2.0.

Versions 0.2.2 and earlier were published under AGPL-3.0-only as part of
[cubepals/cubepals](https://github.com/cubepals/cubepals), and stay under it for whoever has them.

"Cubepals" and its logo are trademarks the licence doesn't grant. For anything else, write to
legal@cubepals.com.
