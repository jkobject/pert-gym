# OBS_COMPLETED contract

`OBS_COMPLETED` is a logical-dataset-level verdict about observation metadata. It is not a storage, `X`, or `var` completion verdict.

The executable contract is `config/obs_completed_contract_v1.json`; the read-only scorer is `tools/score_obs_completed.py`.

## Recovered contract

The following requirements are recovered from the 17 June definition and the later schema/identity audits:

1. The canonical observation surface has exactly 44 fields, from `dataset` through `is_low_quality`, listed in the machine contract. Combination perturbations repeat the ten perturbation/dose fields as `_2..._N` and carry `combination_size` and `combination_id`.
2. A column name alone is not completion. Evidence records state, named source/alias/provenance, and non-null numerator/denominator coverage.
3. Every field has one of `present`, `alias_only`, `manifest_only`, `missing`, or `not_applicable`. `missing` and `not_applicable` are never interchangeable.
4. Every member preserves row count/order and `original_obs_index`; `obs_uuid` is present and unique within the member and globally.
5. `modality`, `assay`, `x_semantics`, and modality-required observation fields are explicit. The scorer records semantics only; it does not inspect `X`.
6. Control evidence distinguishes strict, relaxed, dataset-level, and no-control cases in the relevant biological context.
7. Applicability and completion are explicit for pseudobulk, LFC versus control, QC (`n_counts`, `n_genes`, `pct_mito`, `pct_ribo`, `is_low_quality`), temporal (`timepoint` in minutes, trajectory, pseudotime, baseline), response/sensitivity, and combination outputs.
8. Duplicate/subduplicate status, `loader_projectable`, `model_ready`, and quality status are explicit.
9. Citations and provenance are present, and evidence explicitly asserts that values were not fabricated.

A proven failed requirement yields `OBS_COMPLETED=false`. If there is no proven failure but required evidence is absent or inconclusive, the result is `blocked`. Only complete passing evidence yields `true`.

## Proposed fail-closed rules

No historical numeric coverage threshold or manifest-only waiver policy was recovered. Version 1 therefore labels these choices as proposed rather than historical:

- applicable `present` and `alias_only` fields require exact 100% non-null coverage;
- `manifest_only` blocks completion until a reviewed dataset-level waiver or row projection proves coverage;
- every member of a logical dataset must pass member-level field and identity checks.

Changing any of these choices requires a reviewed contract version, not a silent scorer edit.

## Explicit exclusions

The following are never OBS_COMPLETED criteria:

- Zarr presence or format;
- number or size of chunks;
- duplicated `var` payloads;
- completeness of the `X` payload;
- source identity of `X`.

Those belong to storage, triplet, shared-var, or matrix-semantics contracts. The scorer neither reads nor resolves `X` or `var`.

## Evidence input

The canonical manifest is a TSV with one member per row and `logical_dataset`, `artifact_key`, and `n_obs`. Evidence is optional JSON, either a list or `{ "datasets": [...] }`. Each dataset record has this shape:

```json
{
  "logical_dataset": "dataset/id",
  "members": {
    "dataset/id/obs.parquet": {
      "fields": {
        "dataset": {
          "state": "present",
          "source": "dataset",
          "non_null_rows": 100,
          "total_rows": 100
        },
        "dose": {
          "state": "not_applicable",
          "source": "curation:no chemical dose"
        }
      },
      "identity": {
        "obs_uuid_present": true,
        "obs_uuid_unique_within_member": true,
        "obs_uuid_global_unique": true,
        "original_obs_index_preserved": true,
        "row_count_preserved": true,
        "row_order_preserved": true
      }
    }
  },
  "dataset_checks": {
    "modality_assay_x_semantics": true,
    "modality_required_fields": true,
    "control_semantics": true,
    "derived_applicability_declared": true,
    "derived_outputs_complete": true,
    "combination_semantics": true,
    "duplicate_status": "unique",
    "loader_projectable": true,
    "model_ready": true,
    "quality_flag": "ok",
    "citations": ["doi:..."],
    "provenance": ["source release + transformation report"],
    "fabricated_values": false
  }
}
```

States are evidence, not inference. In particular, the scorer does not infer that a field is inapplicable from modality and does not turn an absent report into `missing`.

## Usage

```bash
uv run python tools/score_obs_completed.py \
  --manifest artifacts/schema_audit/unified_collection_manifest_20260621.tsv \
  --evidence path/to/reviewed_obs_evidence.json \
  --output artifacts/schema_audit/obs_completed_score.json
```

Omit `--evidence` for an inventory-only fail-closed run. This proves the denominator and emits `blocked` rather than pretending that absent evidence passed or failed.

The checked 2026-06-21 manifest run produced `artifacts/schema_audit/obs_completed_score_20260715.json`: 1,056 members aggregate to exactly 120 logical datasets; all 120 are `blocked` because no per-member evidence packet was supplied. That output is a baseline evidence-gap inventory, not a claim that observation metadata is absent.

Each dataset output contains:

- `OBS_COMPLETED`: `true`, `false`, or `blocked`;
- `failed_checks` and `blocked_checks`;
- denominators for manifest members/rows, canonical fields, applicable fields, covered fields, and identity-passing members;
- the status of each dataset-level check.

## Safety

The scorer uses only local JSON/TSV inputs. It imports no Lamin or GCS client, performs no network calls, and reads no obs, `X`, or `var` payload. It writes only the requested local JSON output.
