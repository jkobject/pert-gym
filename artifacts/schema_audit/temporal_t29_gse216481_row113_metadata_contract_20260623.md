# T29G row113 GSE216481 metadata contract — 2026-06-23

Scope: row113 / PerturBase repository 1 / GEO `GSE216481` / BioProject `PRJNA893678`, following the 2026-06-23 user decision to continue row113 and exclude row114 from the active path.

This is a metadata/converter contract, not a completed Lamin ingestion. No canonical Lamin write should be made until the missing perturbation-map input below is recovered or supplied.

## Source and staged inputs

- Staged RAW tar: `gs://scperturb/pert-gym/staging/data/main/temporal_pretraining/perturbase_t29/GSE216481_RAW.tar`
- Verified staged byte size from the prior probe: `17,908,162,560` bytes.
- Filelist artifact: `artifacts/schema_audit/temporal_t29_gse216481_filelist_20260622.txt`.
- Probe artifacts:
  - `artifacts/schema_audit/temporal_t29_gse216481_row113_probe_20260622.md`
  - `artifacts/schema_audit/temporal_t29_gse216481_row113_probe_20260622.json` (full local probe output; not committed if too large)
- `artifacts/schema_audit/temporal_t29_gse216481_row113_contract_input_20260623.json` (compact validation input)
- Contract validator/smoke: `artifacts/scripts/validate_temporal_perturbase_t29_gse216481_contract_20260623.py`

## Component inclusion contract

Canonical RNA `X.h5ad` may include only QC-pass RNA expression components:

| component | source files | PerturBase filtered shape | status | canonical handling |
| --- | ---: | ---: | --- | --- |
| `201218_RNA` | 16 dense gene×encoded-cell CSV.gz + 4 TFmap CSV.gz | `56,857 × 36,844`, 139 perturbations | active, not ingested | eligible for canonical RNA after perturbation/filter contract is supplied |
| `210322_TFAtlas` | 20 dense gene×encoded-cell CSV.gz + 24 TFmap CSV.gz | `527,594 × 16,873`, 1,183 perturbations | active, not ingested | eligible for canonical RNA after perturbation/filter contract is supplied; must be chunked/streamed |
| `PRJNA893678_ATAC` | ATAC payloads inside same archive | `69,085 × 865,996` | excluded from canonical RNA | future typed auxiliary only (`X_atac`/`var_atac`) after review |
| `180124_perturb` | MatrixMarket RNA-like files | PerturBase QC failed, filtered `0 × 0` | excluded | do not ingest canonically |
| `210715_combinatorial` | dense CSV.gz | PerturBase QC failed, filtered `0 × 0` | excluded | do not ingest canonically |
| other older small 10x-style TF/DS/EB/EOMES/etc. members | barcodes/features/matrix files | no active row113 contract | excluded from row113 canonical path | source-level historical/auxiliary only unless separately contracted |

## Perturbation label contract

A safe converter must not infer TF perturbation labels from filename prefixes or TFmap numeric values alone.

Required missing input:

```text
encoded_barcode_or_sequence_to_orf_or_tf_symbol map
```

Minimum accepted form:

- maps each TFmap 24 nt barcode/sequence and/or numeric id to an ORF identifier and a human TF symbol;
- declares which TFmap file(s) correspond to which expression sample(s);
- defines how to join expression encoded cell ids such as `R1.01,R2.01,R3.01,P1.27` to TFmap coordinates such as `R1.54,R2.54,R3.53`;
- distinguishes true controls/non-targeting/empty-vector cells from unresolved or failed mappings;
- supports both active components (`201218_RNA` and `210322_TFAtlas`) or explicitly scopes itself to one component at a time.

Until this input exists, write `perturbation`, `guide_id`, `orf_id`, and `tf_symbol` only in a dry-run/probe artifact. Do not register a canonical obs table with guessed labels.

Recommended recovery source: PerturBase repository 1 filtered object/metadata export, or the original TF atlas supplementary/library table that maps the TFmap sequence/numeric ids to ORF/TF symbols. The GEO RAW tar and SOFT metadata probed so far do not contain this lookup table.

## Filtered-cell inclusion contract

A safe converter must reproduce or explicitly explain PerturBase filtered-cell counts:

- `201218_RNA`: target `56,857` cells from raw `69,085` cells.
- `210322_TFAtlas`: target `527,594` cells from raw `623,153` cells.

The expression CSV headers contain many more encoded cell columns because they are dense gene×cell source tables across all sample files (`2,608,231` summed header columns for `201218_RNA`, `16,012,964` for `210322_TFAtlas` in the range probe). Therefore the converter must not treat header-column count as the final obs row count.

Minimum accepted filtering rule:

1. deterministic per-cell inclusion table or predicate;
2. component-specific target count check against the PerturBase filtered counts above;
3. rejection report for excluded raw encoded cells;
4. no duplicate obs names after sample/component prefixing.

If the PerturBase filtered object is recovered, prefer its filtered-cell list over reconstructing filters from the raw tar.

## Required obs fields after the missing contract is supplied

Every canonical row113 obs table must include at least:

- `dataset_id`: `GSE216481`
- `source`: `PerturBase`
- `source_repository`: `PerturBase repository 1`
- `geo_accession`: sample GSM id
- `bioproject`: `PRJNA893678`
- `component`: `201218_RNA` or `210322_TFAtlas`
- `sample_title`
- `sample_id`
- `encoded_cell_id`
- `r1_coordinate`, `r2_coordinate`, `r3_coordinate`, `plate_coordinate` parsed from encoded cell id where available
- `tfmap_coordinate` or equivalent raw TFmap key
- `tfmap_sequence` where available
- `tfmap_numeric_id` where available
- `orf_id` if supplied by the recovered contract
- `tf_symbol` if supplied by the recovered contract
- `perturbation`: canonical TF symbol or control label, never a guessed numeric id
- `perturbation_type`: `ORF_overexpression`
- `organism`: `Homo sapiens`
- `cell_type`: `H1 human embryonic stem cells`
- `assay`: `scRNA-seq`
- `modality`: `scRNA-seq`
- `timepoint_raw`: e.g. `4 days`, `7 days`, or component-provided label
- `timepoint`: numeric minutes where unambiguous (`4 days` = `5760`, `7 days` = `10080`)
- `timepoint_unit`: `minutes`
- `is_control`
- `is_baseline`
- `filter_contract_source`: path/URI of the recovered filtered-cell metadata
- `label_contract_source`: path/URI of the recovered ORF/TF-symbol map
- `qc_note`: include any mismatch between reconstructed counts and PerturBase counts

## Converter plan

Once the missing label/filter contract is supplied:

1. Re-run `artifacts/scripts/validate_temporal_perturbase_t29_gse216481_contract_20260623.py` with paths to the recovered label/filter files and require zero `missing_required_inputs`.
2. Materialize only the needed tar members from the staged GCS object into an ignored local cache or range-read them; do not copy the full 17.9 GB tar to Git or `data/main/`.
3. Build per-component, per-sample chunks from dense gene×cell CSVs using streaming transpose/chunking. Start with `201218_RNA` because it is smaller.
4. For each chunk, write same-prefix triplets:
   - `temporal_pretraining/perturbase/gse216481_<component>/chunk_####/obs.parquet`
   - `.../X.h5ad`
   - `.../var.parquet`
5. Register with `tools.lamin_context.connect_pertdata()` on `laminlabs/pertdata`, branch `jkobject`; set obs→X and X→var links.
6. Verify each chunk by checking payload existence, row counts, obs required columns, same-prefix var, and obs→X→var links. Do not full-load the 527k-cell TFAtlas component.
7. Clean `.lamin-cache/` after verified chunks.

## Current decision

Row113 is active but `contract_incomplete_missing_label_filter_metadata`. The exact missing artifact is the barcode/sequence/numeric-id to ORF/TF-symbol lookup plus the filtered-cell inclusion table/predicate. Row114 is excluded from the active path by user decision, and row115 remains accepted/verified with its PerturBase count discrepancy recorded in obs `qc_note`.
