# Harmonization roadmap

## Phase 0 — current execution order

The old urgent-repair and PRISM-first sequence is historical. As of the latest
status, real P3 is complete at the triplet-integrity/query-surface level for
`pert-gym/canonical/20260621`. Current execution order is:

1. Keep count language exact: 3407 latest visible artifacts, 1056 canonical
   collection members, 120 logical dataset/family rows, and 1 model-ready v0
   member are different quantities.
2. Expand `pert-gym/model-ready/<date>` beyond the initial 1-member loader-smoke
   subset only after explicit review and safe loader evidence.
3. Project/revise public/base obs aliases and context fields where needed.
4. Run bounded auxiliary/orphan inspections and convert only confirmed joins to
   typed `X_<name>/var_<name>` or `obsm_<name>` artifacts.
5. Continue targeted var ID/symbol checks and duplicate/subduplicate tiers for
   model-ready coverage decisions.
6. Treat PRISM residuals as source-blocked until authenticated/manual Drive or
   source-owner resolution recovers actual h5ads.
7. Treat T-cell GWPS and XAtlas/Orion-scale files as streaming/chunked or
   high-memory-host work; never full-sync 100GB+ h5ads blindly.

Completed historical items remain below as context, not as active blockers.

## Phase 1 — formalize schema contract

- Maintain `docs/pert_gym_schema.md` as the canonical contract.
- Keep wiki pages in `wiki/pert-gym/` as agent-readable summaries.
- Update ingestion scripts so new datasets emit canonical columns directly.

## Phase 2 — read-only audit

Run `tools/audit_lamin_triplet_schema.py` and inspect:

- artifact inventory;
- logical dataset grouping;
- obs coverage;
- var alignment;
- X semantics;
- control availability;
- duplicate/subduplicate candidates;
- repair plan.

## Phase 3 — unified Collection/query contract

Real P3 is complete for the current triplet-integrity/query surface. The dated
records are:

```text
pert-gym/base-public/20260621
pert-gym/additions/20260621
pert-gym/canonical/20260621
pert-gym/model-ready/20260621
```

The first three records define the 1056-member canonical query surface; the
model-ready record is a separate 1-member v0 loader-smoke subset. Historical
planning steps are kept here because they explain the contract:

1. Build a read-only Collection manifest from `artifact_inventory.tsv`,
   `logical_dataset_manifest.tsv`, `triplet_integrity.tsv`, `obs_column_coverage.tsv`,
   `var_alignment.tsv`, `x_semantics.tsv`, `control_availability.tsv`, and
   `duplicate_candidates.tsv`.
2. Assign stable `dataset_id`, `artifact_role`, `split`, `chunk_id`,
   `harmonization_level`, and duplicate/waiver fields. Count artifacts, triplet
   prefixes, and logical datasets separately.
3. Create/propose dated Collection records only after manifest review. Use
   `obs.parquet` artifacts as canonical members; links define `X` and `var`.
4. Add `pert_gym.collections` helper functions that load a Collection manifest,
   filter by simple columns, and resolve `obs -> X -> var` for selected rows.
5. Add a smoke notebook that demonstrates queries such as human CRISPR scRNA,
   PRISM chunks by accession, screen datasets with baseline expression joins,
   and `model-ready` subsets.
6. Promote members through levels: `present-in-collection` ->
   `triplet-integrity-ok` -> `schema-audited` -> `loader-projectable` ->
   `revised-canonical` -> `model-ready`. Revisions create new artifacts or
   branch-local metadata; never rewrite public `main` payloads destructively.

Required manifest/query fields are defined in `docs/pert_gym_schema.md` under
"Unified Lamin Collection contract".

## Phase 4 — post-audit harmonization

Apply safe metadata aliasing and enrichment by batch:

- `pert_name` -> `perturbation`;
- `pert_type` -> `perturbation_type`;
- `pert_time` -> `timepoint` in minutes;
- `pert_dose` -> `dose`;
- `celltype` -> `cell_type`;
- `self_reported_ethnicity` -> `ethnicity`;
- add modality, assay, bulk/pseudobulk flags, X semantics.

Then enrich where source metadata supports it:

- guide sequences;
- guide library;
- perturbation technology;
- media;
- sequencer/technology;
- cell-line/patient identifiers;
- mutation/CNV auxiliary artifacts (`X_cnv/var_cnv` or equivalent).

## Phase 5 — derived products

- Compute QC fields where possible.
- Compute pseudobulk for scRNA-seq.
- Compute LFC versus matched controls.
- Compute depletion and stress/death proxy scores where justified.
- Record control availability tiers.

## Phase 6 — deduplication gate

Before ingesting any new dataset:

- compare accession/DOI/source IDs;
- compare raw file hashes if available;
- compare obs count, var set, perturbation names, and cell barcodes where
  feasible;
- detect subset relationships;
- write an ingest/skip/merge decision.

## Phase 7 — resume ingestion

Do not use the old PRISM queue as a live ingestion list. Current PRISM residuals
are source-blocked after P5D/P5E Mac retries; see
[dataset-status-catalogue.md](dataset-status-catalogue.md) and
[current-status.md](current-status.md). Resume PRISM only after an actual h5ad is
recovered and staged:

1. verify staged GCS object and source provenance;
2. run duplicate/subduplicate checks against public `main` plus `jkobject`;
3. run a smoke-first chunked ingestion;
4. verify `obs -> X -> var` links and control/treated metadata;
5. update the status artifacts instead of editing generated tables by hand.

Handle T-cell GWPS follow-ons and XAtlas/Orion-scale datasets with remote
streaming/chunked readers or high-memory conversion hosts; never blindly sync
100GB+ h5ads to the Mac workspace.

## Phase 8 — PR readiness

Before proposing `jkobject -> main`:

- zero broken triplets;
- no uncontrolled duplicates;
- schema audit reports committed/available;
- loaders can reconstruct AnnData for representative datasets;
- cache cleaned;
- branch diff summarized.
