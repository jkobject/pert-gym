# Broad PRISM Repurposing LFC source-row contract — 2026-07-11

## Scope and non-claims

This is a bounded source-row audit/design for the **Broad PRISM Repurposing Public 24Q2** viability/LFC source. It does not write Lamin, revise `broad_prism_repurposing`, promote a Collection, or claim global dataset completion.

Large source inspection was performed only on `pert-gym-worker-eu` (`europe-west1-b`). The Mac performed only local documentation and JSON validation.

## Evidence and exact inputs

| Role | Exact path / identifier | Bounded inspection result |
|---|---|---|
| Current Lamin obs (diagnostic only; not a candidate label source) | `broad_prism_repurposing/obs.parquet`, UID `eKrJkcFDb9TEDbte0003`, 594,094,912 bytes | 22,316,860 rows / 90 Parquet row groups. Sampled only row groups 0, 45, and 89 (20 rows each) on EU. Later groups encode `broad_id`/`perturbation` as `profile_id`, `LFC`, `LFC_cb`, and `PASS`; compound identity is absent or malformed. |
| Authoritative staged LFC table | `gs://scperturb/pert-gym/staging/data/main/broad_prism/Repurposing_Public_24Q2_LFC.csv` | 479,352,899 bytes; SHA-256 `824149f9b9f3821eb520b385a5976e1a9977d86b21caf5d22171763800a40523`; 4,463,372 rows streamed in 250,000-row chunks on EU. |
| Authoritative staged treatment metadata | `gs://scperturb/pert-gym/staging/data/main/broad_prism/Repurposing_Public_24Q2_Treatment_Meta_Data.csv` | 1,510,417 bytes; SHA-256 `6be6422ba804ad0775e78b457677bdf088707b9354746a03e110ae63f5eb2061`; 11,502 rows. |
| Existing bounded real-source projection | `artifacts/schema_audit/model_ready_v2_prism_response_rows_phase2_20260710.{tsv,json}` on `pert-gym-worker-eu` | 128 direct finite LFC rows, no Lamin write, `loader_projectable_only`; source projection smoke passed. |
| Historical diagnostic | `artifacts/schema_audit/broad_prism_response_repair_probe_20260703.json` on `pert-gym-worker-eu` | Explicitly holds the current Lamin obs out of response projection until the raw LFC table is joined to treatment metadata. |

The raw source release is `PRISM Primary Repurposing DepMap Public 24Q2`. Required raw LFC fields are `row_id`, `profile_id`, `LFC`, and `PASS`; required metadata fields are `profile_id`, `perturbation_type`, `dose`, `broad_id`, and `name`. `profile_id` must be non-empty and unique in metadata.

## Bounded denominator and row-selection contract

The established EU-worker streaming pass reports:

- source denominator: **4,463,372** raw LFC rows;
- non-PASS exclusions: **171,944**;
- real finite, PASS, metadata-joined eligible rows: **4,291,428**;
- bounded deterministic materialization: **128** rows, selected by the smallest SHA-256-derived score over `(row_id, profile_id)` without materializing the full source table;
- selected evidence: 124 unique `broad_id` compounds and 120 context IDs; split counts train/validation/test = 89/10/29.

A source row is eligible for the response-table contract only when all conditions hold:

1. it comes from the exact 24Q2 LFC file above;
2. `LFC` parses as a finite IEEE numeric value (exclude missing, malformed, `NaN`, `+/-Inf`);
3. `PASS` is case-insensitively `true`;
4. `profile_id` joins exactly once to treatment metadata;
5. metadata has non-empty `broad_id` and `name`;
6. metadata treatment type is a drug-treatment class. The prior projection admitted `trt_cp` and `trt_poscon`; its actual 128 selected rows were all `trt_cp`. **Default next projection policy is `trt_cp` only until review explicitly admits `trt_poscon`.**

The canonical source-row identifier is `row_id + "|" + profile_id`. It is immutable source provenance and must not be replaced by a generated obs UUID.

## Required output provenance and response fields

Every projected row must carry at least:

```text
source_release = "DepMap PRISM Repurposing Public 24Q2"
source_lfc_uri, source_lfc_sha256
source_treatment_metadata_uri, source_treatment_metadata_sha256
source_row_id = raw row_id
source_profile_id = raw profile_id
source_row_identifier = row_id|profile_id
source_row_chunk_index and source_row_offset_in_chunk (for the generating pass)
source_file_row_number (1-based data row number, excluding CSV header)

perturbation = metadata.name
perturbation_id = metadata.broad_id
perturbation_type = "drug"
context_id = "Homo sapiens|" + depmap_id
cell_line / depmap_id = first `::` component of row_id only after preserving row_id
source_profile_id remains separate; do not treat the composite profile string as a cell-line identifier

dose = metadata.dose
dose_unit = "source_unit_not_specified" unless a release-bound source field proves units
response_metric = "lfc"
response_value = raw finite LFC
response_source = "Repurposing_Public_24Q2_LFC.csv:LFC joined to Treatment_Meta_Data by profile_id"
response_transform = "source_lfc_vs_vehicle"
response_direction = "lower_more_sensitive"
target_is_direct = true
has_expression_X = false
x_semantics = "response_table"
model_ready_status = "loader_projectable_only"
```

`LFC_cb` and `PASS` are not alternative response metrics. `PASS` is a quality eligibility field; `LFC_cb` may be retained only as an explicitly named source sidecar after a separate semantics review.

## Duplicate, denominator, and no-overwrite gate

The response denominator must report counts for every rejection reason: non-finite LFC, non-PASS, missing/unmatched profile metadata, non-drug treatment type, and missing compound identity. The denominator must state file SHA-256 and chunk size.

- Exact source duplicate key: `(source_row_identifier)`; duplicate rows are invalid and must be reported before selection.
- Biological/measurement duplicate review key: `(depmap_id, broad_id, profile_id)`; retain separate source rows only with an explicit replicate policy.
- Never deduplicate solely by compound name: `broad_id` is the split identity; name is display/provenance.
- No overwrite: emit a new local projection/manifest first. Do not revise `broad_prism_repurposing/obs.parquet` unless row-order and existing `X` links are independently proven compatible and a reviewer approves it. No Lamin write is authorized by this audit.

## Compound-holdout leakage contract

Assign splits from the normalized `broad_id`, not display name or source row:

```text
bucket = sha256("phase2-prism-compound-holdout-v1|" + broad_id) % 10
bucket 0-1 -> test; bucket 2 -> validation; bucket 3-9 -> train
```

Before publishing any bounded manifest, fail if any `broad_id` / `perturbation_id` overlaps between split pairs, or if any `source_row_identifier` overlaps between split pairs. Report `context_id` overlap explicitly but do not label it context holdout. Compound aliases that map to the same `broad_id` cannot cross splits; distinct Broad IDs with identical display names require review rather than automatic merging.

## Review-required issues

1. Confirm whether `trt_poscon` is admissible as a repurposing compound response. The existing builder allows it, but the existing selected subset happened to contain only `trt_cp`; default new contract excludes it pending approval.
2. Confirm whether historical `LFC_cb` is an analyzable secondary response or only a source sidecar. It must not mix with `LFC` in `response_value`.
3. Confirm release-bound dose units. The metadata exposes numeric dose but no explicit unit, so this contract preserves `source_unit_not_specified` rather than inferring units.
4. The remote worker's `tools/ingest_phase3_bulk.py` import currently fails due a malformed local Lamin branch-settings file. This audit used direct artifact metadata / existing bounded source evidence and did not write or repair that worker state; a future projection should repair or isolate that environment before execution.

## Reproducibility boundary

Source inspection command family:

```bash
gcloud compute ssh pert-gym-worker-eu --zone europe-west1-b --project jkobject-1549353370965 --command \
  'cd ~/work/pert-gym && PYTHONPATH=src uv run python artifacts/model_benchmarks/t_faf8f2dc_build_prism_phase2_response_rows.py'
```

The existing projection uses 250,000-row CSV chunks and a deterministic 128-row source-id-hash selection. It is a bounded real-label plumbing artifact, not a request to scan or rewrite the complete dataset on the Mac.
