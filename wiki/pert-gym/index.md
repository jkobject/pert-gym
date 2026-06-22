# pert-gym wiki index

This wiki follows the local project knowledge-base pattern: stable decisions and
compiled context live here; raw data and run logs stay in `artifacts/`, `data/`,
LaminDB, or source notebooks.

## Core pages

- [Current harmonization status](current-status.md) — canonical latest status,
  exact count vocabulary, validated Collection records, temporal/model summaries,
  PRISM/T-cell blockers, and remaining post-P3 work.
- [Dataset status catalogue](dataset-status-catalogue.md) — detailed internal
  source/family status map. Use this for dataset descriptions and blockers instead
  of bloating README or CLAUDE.
- [Schema contract](schema-contract.md) — canonical obs/var/X rules plus the
  unified Collection/query contract for all modalities.
- [Lamin audit and branch model](lamin-audit-and-branch-model.md) — how to count
  artifacts, logical datasets, chunks, branches, and triplet integrity without
  conflating old audit counts with current canonical members.
- [Dataset modalities](dataset-modalities.md) — modality-specific handling for
  scRNA-seq, bulk, screen, temporal, image, multimodal, and auxiliary modality data.
- [Harmonization roadmap](harmonization-roadmap.md) — phased roadmap and current
  post-P3/model-ready execution order.
- [Model and benchmark roadmap](model-roadmap.md) — model-ready criteria,
  benchmark status, environment decisions, and honest interpretation of smoke
  results.
- [Deduplication policy](deduplication-policy.md) — duplicate and subduplicate
  checks before ingesting new data.

## Current validated decisions

- Current canonical query surface: `pert-gym/canonical/20260621` with 1056
  canonical `obs.parquet` collection members and validated `obs -> X -> var`
  links at `triplet-integrity-ok` level.
- Current logical dataset/family count: 120 grouped rows in the unified manifest;
  this is distinct from collection members/chunks and from global pertdata scale.
- Current model-ready surface: `pert-gym/model-ready/20260621` with 1 reviewed v0
  loader-smoke member; do not treat the 1056 canonical members as model-ready.
- Use the existing `laminlabs/pertdata` triplet convention:
  `obs.parquet -> X.h5ad -> var.parquet`.
- Branch `jkobject` is a working branch of the same instance and sees both public
  pertdata families and pert-gym additions.
- New work must be mergeable into `main`, not a parallel format.
- Combination perturbations use repeated `.obs` columns suffixed `_2`, `_3`, ...
  plus `combination_size` and `combination_id`.
- Exclude the legacy GSE150818 mistaken dataset from
  pert-gym; the desired target is PRoPER-seq / ProPer-seq 2026 probe-based
  Perturb-seq once the real scRNA expression source is found.
- Store non-primary modalities as named auxiliary artifacts (`X_<name>/var_<name>`
  or `obsm_<name>`), not vague auxiliary blobs.
- P3 requires a reviewed `ln.Collection` family (`base-public`, `additions`,
  `canonical`, `model-ready`) plus a versioned manifest/query UX; triplet
  existence alone is not complete.
- Do not prioritize methylation/proteomics ingestion in this phase.

## Reports

Schema/audit reports should be written to:

```text
artifacts/schema_audit/
```
