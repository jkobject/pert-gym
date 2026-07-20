# pert-gym agent operating contract

## Durable data and execution boundary

- `laminlabs/pertdata` is the durable system of record. Use
  `tools.lamin_context.connect_pertdata()` on branch `jkobject`; never use the
  global Lamin CLI for this repository.
- Project-owned GCS is temporary staging only, not a durable dependency or a
  deletion target by default. Do not place large payloads or GCS caches on the
  Mac. Perform payload, GCS, and broad Lamin work only on the EU VM
  (`pert-gym-worker-eu`, `europe-west1-b`); keep that VM warm while an active
  migration wave needs it.
- The target for large matrices is adaptive sparse-Zarr with shared-var support.
  Retain legacy triplets until their accepted replacement has parity/readback
  evidence and a documented rollback identity.

## Publication and reproducibility

- One logical dataset has one writer. Publication is append-only and
  crash-recoverable: use the journal/recovery contract; do not bypass stages or
  delete prior representations while publishing.
- Per-dataset processing-decision notebooks are the reconstruction layer. They
  must distinguish accepted facts, current state, and pending work; identify
  immutable upstream checksums or retained Lamin raw inputs, code/lineage,
  validation, and each temporary GCS dependency.
- No data deletion is allowed without accepted artifact UIDs/Collections,
  source-to-Lamin parity and remote readback, an executable dataset notebook,
  and either an immutable upstream checksum or retained Lamin raw source. The
  final GCS exit is gated by the reviewed machine-readable
  `GCS_DECOMMISSION_READY` manifest, not prose.

See [TODO.md](TODO.md) for the live dashboard and
[docs/project/agent-runbook.md](docs/project/agent-runbook.md),
[docs/pert_gym_schema.md](docs/pert_gym_schema.md), and
[docs/adr/0001-logical-sparse-zarr.md](docs/adr/0001-logical-sparse-zarr.md)
for the durable execution, schema, and storage contracts. `wiki/` is obsolete.
