# Lamin audit and branch model

## Branches

The working branch is `jkobject` in `laminlabs/pertdata`. It is intended to be
mergeable into `main` once schema, integrity, and duplication checks pass.

Visible branches include:

- `main`
- `jkobject`
- `archive`
- `trash`

Agents must connect with `tools.lamin_context.connect_pertdata()` and must not
use the global Lamin CLI for this repo.

## Counting rules

Count at multiple levels. Never collapse these into one "dataset count". The
live branch-delta inventory is the denominator authority for the current
migration wave: it keeps artifact records, Collection members/chunks, logical
datasets/families, revisions, typed auxiliaries, aliases/exclusions, and
model-ready members separate. For operational and exit rules, see
[migration-reproducibility-and-gcs-exit.md](migration-reproducibility-and-gcs-exit.md).

### Global pertdata scale

The broader public pertdata ecosystem is much larger than this project slice
(2000+ datasets/assets at global scale). Do not use that number when describing
the current pert-gym canonical query surface.

### Artifact level

Raw Lamin artifacts include DataFrame, AnnData, MuData, TileDBSOMA, manifests,
README files, model files, and auxiliary artifacts. This count answers: what
artifact records exist in the visible branch? Current P3R-C branch-visible count:
3407 latest artifact records on `jkobject`.

### Collection-member level

Canonical collection members are `obs.parquet` artifacts in a dated reviewed
Collection. Current P3R surface:

- `pert-gym/base-public/20260621`: 60 members;
- `pert-gym/additions/20260621`: 996 members;
- `pert-gym/canonical/20260621`: 1056 members;
- `pert-gym/model-ready/20260621`: 1 reviewed v0 member.

### Logical dataset level

Logical datasets group shards/chunks/plates together. Examples:

- PRISM chunks under one GSE accession;
- VIPerturbSeq chunks under one component;
- Arc VCC chunks under split;
- Tahoe plates under one release;
- MuData/TileDBSOMA datasets as single logical datasets.

This count answers: how many biological/training datasets exist?

## Read-only audit outputs

`tools/audit_lamin_triplet_schema.py` writes local TSV reports under
`artifacts/schema_audit/`. It must not create, update, delete, or link Lamin
artifacts.

Priority reports:

- artifact inventory;
- logical dataset manifest;
- triplet integrity;
- obs column coverage;
- var alignment;
- x semantics;
- control availability;
- duplicate candidates;
- repair plan.

## Current integrity baseline

Use [current-status.md](current-status.md) for the latest canonical counts. The
current P3R baseline is:

- 1056 canonical members in `pert-gym/canonical/20260621`;
- 1056 members at `triplet-integrity-ok` level;
- 120 logical dataset/family rows in the unified manifest;
- 1 reviewed v0 model-ready member.

The 2026-06-18 metadata-only audit is historical repair context, not the current
canonical database-size statement. It reported 2429 artifacts, 720 triplet-like
prefixes, 711 complete triplets, 9 incomplete/partial prefixes, 110 logical
datasets, 0 true urgent triplet repairs, and 0 same-prefix-var repairs remaining.

The remaining incomplete prefixes from that old audit were auxiliary, reference,
demo/model, or orphan review items. Do not repair them blindly as canonical
triplets; classify or convert them using the typed auxiliary artifact contract
(`X_<name>/var_<name>` or `obsm_<name>`).

## Current publication and retention boundary

Candidate replacements are append-only and must be published by one writer
through the crash-recoverable journal/recovery contract. A journal stage never
substitutes for remote save/readback, and a logical/rollback identity must not
cross-key hash-deduplicate into another dataset. Retain legacy triplets until
accepted source parity, readback, Collection/UID evidence, and the executable
dataset notebook establish an approved replacement. Project-owned GCS is
temporary staging; its removal is gated only by the reviewed
`GCS_DECOMMISSION_READY` manifest described in the migration/reproducibility
contract.
