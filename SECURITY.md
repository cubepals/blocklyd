# Security

Please don't open a public issue for a security problem.

## Reporting

Report it privately on GitHub: **[Report a vulnerability](https://github.com/cubepals/blocklyd/security/advisories/new)**.
Say what is affected, how to reproduce it, and what someone could do with it. You will hear back,
and you'll be told when it is fixed and whether you want to be credited.

blocklyd runs as root beside Docker's socket on every host of a fleet, so a way past its mutual
TLS, out of a workload's data directory, or around the limits it puts on workloads is what matters
most. The README's "Security model" says what it is meant to hold.

## Scope

- This repository: blocklyd, its deploy files and its releases.
- The control plane, the web app and the hosted service at `cubepals.com` are
  [cubepals/cubepals](https://github.com/cubepals/cubepals/security/advisories/new)'s; report
  those there.

Only the latest release is supported; a fix is a new release, which Cubepals' control plane then
hands to its hosts.
