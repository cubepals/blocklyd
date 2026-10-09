# What older blocklyd builds left behind

Each file here is byte for byte what blocklyd 0.2.0 writes:

- `workload-full.json`, `workload-minimal.json`: a workload's record (`workloads/<id>/workload.json`),
  one with every optional field set and one with none
- `snapshot.json`: a local snapshot's description (`workloads/<id>/snapshots/<snapshot>/snapshot.json`)
- `ports.json`: released host ports still in quarantine
- `node.json`: the fleet identity (`identity/node.json`)
- `trial.json`, `failed.json`: self-upgrade's state (`upgrade/`)
- `label-record.json`: the `blocklyd.record` label on a container, on one line as the label holds it

`tests/formats.rs` reads each into today's types and checks it holds the values it was written
from, then writes those values again and checks the bytes match.

They guard upgrades. A node that upgrades reads what the older build left, and one that is put back
reads with the older build what the newer one wrote. If a test here fails, one of those would no
longer work: fix the change, not the fixture. A field added to one of these types is skipped when
absent (`#[serde(default, skip_serializing_if = ...)]`, see `SpecRecord`), so what older builds
wrote still reads, and writes back, the same.

Never rewrite a fixture to make a test pass: nodes have these files on disk. A new shape on purpose
gets a new fixture beside the old ones. Add its case to `tests/formats.rs` and run

    BLOCKLYD_WRITE_FIXTURES=1 cargo test --locked --test formats

which writes only the fixtures that don't exist yet. Read the file before committing it.

The bytes are serde_json's, so `biome.json` leaves this directory unformatted.
