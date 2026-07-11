# Migration, reproducibility, and GCS exit contract

This page records the durable operating decisions for the current pert-gym
migration wave. It is a contract, not evidence that every dataset has completed.
For the live dashboard see [TODO.md](../../TODO.md); for count definitions see
[lamin-audit-and-branch-model.md](lamin-audit-and-branch-model.md).

## Status vocabulary

- **ACCEPTED** — a reviewed/merged decision or independently accepted dataset
  gate exists.
- **CURRENT** — the active policy or live lane; it is not a completion claim.
- **PENDING** — requires a writer, validation, external access, or review.

## Denominators and logical identity

**ACCEPTED:** describe counts with their authority and level: latest artifact
records, Collection members, triplet prefixes/chunks, logical datasets/families,
and model-ready members are separate denominators. A wide audit count or a
chunk count never proves biological-dataset coverage.

**CURRENT:** a logical dataset may have many chunks, revisions, auxiliary
artifacts, and Collection memberships. The authoritative live `jkobject` vs
`main` inventory must classify each branch-added item exactly once as an added
dataset, branch revision, typed auxiliary, duplicate alias, or exclusion. It
must not promote helper, checkpoint, rollback, or inherited-main artifacts into
a notebook-worthy dataset.

## Storage, format, and adapter policy

**ACCEPTED:** Lamin (`laminlabs/pertdata`) is the durable system of record.
Lamin triplets (`obs.parquet -> X.h5ad -> var.parquet`) remain supported through
a legacy-triplet adapter.

**CURRENT:** the target representation for large data is adaptive sparse-Zarr
with explicitly validated shared-var identity. Chunk sizes are adaptive to
observed resource limits; chunks remain parts of one logical dataset. The
manifest records the logical id, source identity, chunk intervals, X semantics,
var identity/policy, and Collection role.

**PENDING:** retain legacy triplets until their candidate replacement has
accepted source parity, remote readback, Collection/UID evidence, and a
rollback identity. Format migration alone does not complete a dataset.

## Publication correctness lessons

**ACCEPTED:** logical publication is append-only, single-writer, and
crash-recoverable. A journal records stages and recovery reconciles remote
saves before journaling. Never delete the older readable representation while a
replacement is only a candidate.

**CURRENT:** publication must reject orphan or out-of-order promotion. The
journal is not proof that an object exists remotely: save/readback precedes the
journaled stage. Candidate identity is tied to its logical dataset and rollback
identity; cross-key hash deduplication must not make one dataset publish as
another. These safeguards address prior orphan/out-of-order, save-before-journal,
rollback-identity, and cross-key-dedup failure classes.

**PENDING:** every production lane still needs its own source-parity,
readback/denominator, and independent-review evidence before promotion.

## Execution boundary

**CURRENT:** project-owned GCS is temporary staging, not a durable project data
layer. All large payload, GCS inventory, and broad Lamin work runs only on
`pert-gym-worker-eu` in `europe-west1-b`; the Mac is control-plane only and
must not cache/download/materialize large payloads. Keep the VM warm while an
active migration wave, notebook validation, parity/readback, or cleanup needs
it.

**PENDING:** external access is not complete merely because metadata is visible.
RxRx3 remains blocked on real access/EULA resolution. No dataset may be marked
complete from staging presence, a writer log, or an unfinished card.

## Notebook reconstruction layer

**CURRENT:** each added/revised logical dataset receives an executable,
metadata-first processing-decisions notebook. It must distinguish facts,
decisions, and pending work, and record:

1. immutable upstream URL/accession, license, version and checksum — or the
   retained Lamin raw artifact when upstream reacquisition is not possible;
2. exact `jkobject` vs `main` keys/UIDs/revisions/Collections and count
   vocabulary;
3. selected/excluded source payloads, biological unit/readout, transformation,
   filtering/QC, obs identity/control mapping, organism/var normalization, X
   semantics, chunk/shared-var/auxiliary choices, and rejected alternatives;
4. scripts, commit, environment, Kanban lineage, legacy-to-logical mapping,
   source parity/readback evidence, denominator/completeness, limitations, and
   rollback/retention status;
5. every temporary GCS input/output, its durable replacement, and an explicit
   removal decision.

Default notebook execution reads committed local metadata only. Any live Lamin
query is optional and host-guarded to the EU VM; notebooks never cache or fetch
matrix payloads on the Mac. A notebook cannot claim reproducibility when its
only source is an unretained GCS object.

## `GCS_DECOMMISSION_READY` and safe exceptions

**PENDING:** GCS removal is authorized only by a reviewed machine-readable
`GCS_DECOMMISSION_READY` manifest, never prose. For each removable
project-owned pert-gym prefix it must name accepted Lamin artifact UID(s) and
Collection(s), executed notebook, immutable upstream checksum or retained Lamin
raw source, source-to-Lamin parity and remote readback, and proof that no live
consumer remains.

Safe exceptions are explicit, not silent: a prefix may remain when it is the
sole legally/technically irreproducible source, still serves an active
unaccepted lane, is required by a documented consumer, or is not project-owned.
Lamin-managed backing storage and shared/unrelated bucket objects are outside
this deletion scope.

The exit criterion is exact: completed independently reviewed Lamin datasets,
an executable notebook for every added logical dataset, and **zero unexplained
project-owned pert-gym GCS dependencies**. Only then may an allowlisted cleanup
remove exact prefixes, and only afterward may the warm VM and temporary disks be
decommissioned once no active operation needs them.
