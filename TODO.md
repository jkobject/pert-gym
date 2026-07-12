# pert-gym live migration dashboard

_Last reconciled: 2026-07-12. This is a status dashboard, not a run log. A
running or staged card is not evidence of completion._

## Accepted foundations

- **ACCEPTED:** PR [#50](https://github.com/jkobject/pert-gym/pull/50) defines
the logical sparse-Zarr contract.
- **ACCEPTED:** PR [#51](https://github.com/jkobject/pert-gym/pull/51) adds the
guarded EU-VM ingestion runner.
- **ACCEPTED:** PR [#52](https://github.com/jkobject/pert-gym/pull/52) adds
VM-only adaptive sparse-Zarr/shared-var migration tooling.
- **ACCEPTED:** PR [#53](https://github.com/jkobject/pert-gym/pull/53) merges
  crash-recoverable publication from legacy triplets, including the publication
  journal/recovery and stage-order safeguards.
- **ACCEPTED (tooling/documentation only):** PR
  [#56](https://github.com/jkobject/pert-gym/pull/56) defines the
  migration/reproducibility/GCS-exit contract, and PR
  [#60](https://github.com/jkobject/pert-gym/pull/60) adds the PerturbAI
  sparse-parquet logical-Zarr adapter. Neither proves a production dataset,
  readback, UID, Collection, canonical representation, or final completion.

## Running or staged dataset lanes

- **CURRENT:** large dataset payload/GCS/Lamin operations are EU-VM-only; keep
  `pert-gym-worker-eu` RUNNING and warm while the active wave is in progress.
  The Mac is a control plane and must not cache or materialize large data.
- **BLOCKED:** the HEK293T run cached exactly 4,535 source directories, but
  candidate assembly was kernel-OOM-killed at about 31.9 GiB before candidate
  output or any publication. No dataset/readback/UID completion exists. The
  fresh bounded-streaming repair chain starts at `t_d43e7f87`; do not relaunch
  production from this status note.
- **BLOCKED:** the heavy-runner capacity decision is awaiting human choice A
  (cost/quota packet then non-destructive `pert-gym-worker-eu` expansion) or C
  (pause heavy lanes). No capacity exception, cleanup, promotion, or VM
  decommission is authorized meanwhile.
- **PENDING:** XAtlas/Orion HCT116 and HEK293T are separate logical datasets;
  each needs its own accepted source, migration, parity/readback, and reviewer
gate before it can be called complete.
- **PENDING:** staged or partially represented families remain subject to their
  own completion/review cards. Do not upgrade status from a writer run, a stage,
  or an old GCS object.
- **BLOCKED:** RxRx3 access/EULA remains a real external-access blocker. Do not
  substitute an inferred source, proxy dataset, or metadata-only result; its
  Auth0/EULA decision is separate from the HEK293T and capacity gates.

## Notebook and inventory lane

- **CURRENT:** establish the exact `jkobject`-vs-`main` logical-dataset ledger
  before creating per-dataset notebooks; distinguish datasets from chunks,
  revisions, typed auxiliaries, aliases, and exclusions.
- **PENDING:** every added/revised logical dataset needs an executable,
  metadata-first processing-decisions notebook. It must document source identity,
  transformation choices, Lamin keys/UIDs/Collections, parity/readback, and
  temporary GCS inputs/outputs plus their durable replacement.
- **PENDING:** notebooks are a reconstruction layer, not a claim that a staged
  dataset is accepted. Default execution is Mac-safe metadata only; optional
  live Lamin checks are guarded to the EU VM.

## Collections, tests, and final review

- **PENDING:** retain the distinction between artifact records, Collection
  members/chunks, logical datasets, and model-ready datasets. The denominator
  authority is the reviewed live inventory and its explicit count vocabulary.
- **PENDING:** final Collections, model-ready inclusion, remote source parity,
  and real readback must be independently tested/reviewed per dataset; legacy
  triplets remain until an accepted replacement is proven.
- **PENDING:** a final provenance review must reconcile dataset notebooks,
  Collections, source checksums/retained raw evidence, and the live inventory.

## `GCS_DECOMMISSION_READY` (hard exit gate)

The project is ready to decommission project-owned pert-gym staging only when a
reviewed, machine-readable `GCS_DECOMMISSION_READY` manifest proves for **every**
project-owned prefix either an allowlisted safe removal or a documented retained
exception. Each removable prefix must map to: an accepted Lamin UID/Collection,
source-to-Lamin parity and remote readback, a successfully executed dataset
notebook, immutable upstream checksum or retained Lamin raw source, and no live
consumer. Lamin-managed storage and shared/unrelated buckets are never in scope.

Exit means: completed, independently reviewed Lamin datasets; one executable
notebook for each added logical dataset; and zero unexplained project-owned
pert-gym GCS dependencies. This is not satisfied by a running card, a staged
object, an old cache, or prose.

## Cleanup and VM/disk decommission

- **PENDING:** cleanup may delete only exact `SAFE_DELETE` allowlisted staging
  prefixes after the exit gate; no broad bucket deletion and no deletion of the
  sole irreproducible source.
- **PENDING:** stop/decommission the warm EU VM and its temporary disks only
  after no active migration, parity, notebook, review, or cleanup card requires
  them and all retained data is accounted for in Lamin/upstream provenance.

See [migration/reproducibility/GCS exit contract](wiki/pert-gym/migration-reproducibility-and-gcs-exit.md)
and [current status](wiki/pert-gym/current-status.md) for durable details.
