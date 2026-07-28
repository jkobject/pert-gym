# GSM5901230 scientific audit and correction plan

Verdict: **correction_plan_ready_no_mutation**. This packet is read-only; publication belongs to the downstream single writer.

## Exact source and current identity

- GEO child: [GSM5901230](https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSM5901230&targ=self&form=text&view=full) of [GSE196799](https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSE196799&targ=self&form=text&view=full).
- Source relation confidence: `exact`.
- Accepted family manifest: `gs://scperturb/pert-gym/staging/pert-gym/logical/temporal/organoiddb_odd001155_gse196799/revisions/temporal-v4-089-wave09-e1265bc274817bb7/manifest.json#1784257733140715`; SHA-256 `3f73fdc9e405279ebbd5e5a4d67ee8b6d32cd0a031b73d5297184f95b6bb7eb3`.
- Frozen evidence is hash-bound and all nine OBS/X/VAR GCS generations were freshly rechecked at `2026-07-28T11:10:30.728024+00:00` with no generation or size drift.
- OBS: Artifact `770vKgYITsWgwsQ50000` / `data/cleaned/GSM5901230/obs.parquet` / Lamin hash `j7h38e6PqY1FPQlSZfd9DQ` / GCS generation `1785154234703221` / payload SHA-256 `f35de6ba53b5ead8e65ee8ca7cd1419f61dbd8b476b2ab2c6ee4017dc96b0be6`.
- X: Artifact `vn537Bt2uFhZncXo0000` / `data/cleaned/GSM5901230/X.h5ad` / Lamin hash `NJbcmYRJByidha4aC-gltA` / GCS generation `1785154234743869` / payload SHA-256 `ae9c968e0c7ceb09a2ac80c1b3518ba4626dc13106aece29214a330e5f049047`.
- VAR: Artifact `NCuDtw4vtWZpliBU0000` / `data/cleaned/GSM5901230/var.parquet` / Lamin hash `5IxJsE91crLE_PZZ7Ccq1w` / GCS generation `1785154234729370` / payload SHA-256 `1d74fc35a8ed61c7f487b5b3b955fd91e9208e6e42e7943ebf0e8442ec83c134`.

## OBS column audit

Current OBS has 4,807 rows, unique ordered index SHA-256 `0183ccf12a871b94c22676fd6768c1e9f7b33277c85952b86ac3e7c55b281df2`, and no nulls in any current column. It lacks required `obs_uuid` and `original_obs_index`.

| column | dtype | cardinality | missing | decision | target | rationale |
|---|---|---:|---:|---|---|---|
| `cell_id` | `object` | 4807 | 0 | `keep` | `cell_id` | unique source-backed cell identity |
| `barcode` | `object` | 4807 | 0 | `keep` | `barcode` | unique source 10x barcode; useful for replay |
| `source_accession` | `object` | 1 | 0 | `move_to_dataset_metadata` | `source_accession` | exact but dataset-wide provenance |
| `sample_accession` | `object` | 1 | 0 | `move_to_dataset_metadata` | `sample_accession` | exact but dataset-wide provenance |
| `sample_title` | `object` | 1 | 0 | `move_to_dataset_metadata` | `sample_title` | descriptive source constant |
| `source_name` | `object` | 1 | 0 | `map` | `source_material` | scientific source material; not a current cell-type annotation |
| `experiment` | `int64` | 1 | 0 | `map` | `source_experiment,batch` | source-backed experimental stratum |
| `timepoint` | `float64` | 1 | 0 | `move_to_dataset_metadata` | `timepoint_days,timepoint_minutes` | single constant snapshot in this GSM child; preserve family time-course relation separately |
| `timepoint_unit` | `object` | 1 | 0 | `move_to_dataset_metadata` | `timepoint unit/day` | paired with child-level constant timepoint |
| `culture_method` | `object` | 1 | 0 | `keep` | `culture_method` | inference-relevant experimental conditioning field |
| `ascorbic_acid_from_day_12` | `bool` | 1 | 0 | `keep` | `ascorbic_acid_from_day_12` | inference-relevant family condition; false for this child |
| `biosample` | `object` | 1 | 0 | `move_to_dataset_metadata` | `biosample` | exact sample-level provenance |
| `sra_experiment` | `object` | 1 | 0 | `move_to_dataset_metadata` | `sra_experiment` | exact sample-level provenance |
| `organism` | `object` | 1 | 0 | `keep_and_map` | `organism,organism_ontology_id` | scientific conditioning field; GEO taxid 9606 |
| `assay` | `object` | 1 | 0 | `map` | `assay,assay_ontology_id,technology,technology_ontology_id` | source protocol specifies 10x 3' v3.1 |
| `n_genes_by_counts` | `int32` | 2560 | 0 | `map` | `n_genes` | cell-level QC covariate |
| `total_counts` | `int64` | 4093 | 0 | `map` | `n_counts` | cell-level QC covariate |
| `pct_counts_mt` | `float32` | 4794 | 0 | `map` | `pct_mito` | cell-level QC covariate |
| `source_cell_call_flag_missing` | `bool` | 1 | 0 | `move_to_dataset_metadata` | `source_cell_call_flag_missing` | ingestion/source-quality flag, not a biological cell variable |

### Temporal and metadata verdict

This child is one snapshot at day 6 (8640 minutes), so its child verdict is `non_temporal_single_snapshot` and the redundant per-cell time fields move to dataset metadata. GSE196799 remains `temporal_time_course` across days [0, 3, 6, 9, 12, 18]; the family relation is not erased.

Scientifically useful constants remain conditioning columns (organism, culture method, ascorbic-acid condition, assay/technology, source material). Pure source/descriptive constants move to dataset metadata. `hiPSCs` maps only to source material EFO:0004905, not to current cell type. Unsupported fields remain `unknown`; in-vitro tissue is `not_applicable`.

## VAR, X, links, and layout

- VAR: 20,631 rows; all 20,631 IDs are unique unversioned human ENSG identifiers; zero control-character rows. Gene symbols are complete but only 20,616 unique (30 rows participate in duplicate symbols), so symbol duplicates are retained and Ensembl IDs remain identity.
- X: `csr_matrix` `int32` raw integer counts, shape [4807, 20631], nnz 17,517,151, sum 58,069,115; exact OBS/X and X/VAR axis parity passes.
- Links: OBS→X `True` targeting `vn537Bt2uFhZncXo0000`; X→VAR `True` targeting `NCuDtw4vtWZpliBU0000`.
- Layout: appropriate: one 4.4k-6.4k-cell CSR member of 41-60 MB; rechunking would add overhead without a failed invariant
- VAR policy: exactly one same-prefix VAR for this unchunked child; identical sibling hashes prove family axis parity but do not justify violating same-prefix contract

## Executable remediation and validators

Run `remediate_local.py` against the exact current local payloads. It emits revised local OBS/VAR plus dataset metadata and a receipt; it never connects to LaminDB or writes GCS. The downstream writer must publish append-only revisions, restore exact links, build a successor Collection, perform bounded readback, and prove zero-write replay.

- source/current hashes and artifact UIDs unchanged before write
- OBS row count/order/index conserved; obs_uuid present, valid and unique
- all current OBS columns have an explicit decision
- VAR exactly 20,631 unique ENSG IDs, no controls, exact X feature order
- X shape/CSR integer sum/nnz/hash unchanged
- obs->X and X->var exact UID links resolve
- one same-prefix VAR; no chunks introduced
- child temporal status non_temporal_single_snapshot and GSE196799 family relation retained
- unknown and not_applicable states remain distinct

## Residual unknowns

- cell-level differentiated cell_type labels are absent from bounded GEO/current OBS evidence
- exact cell-line/donor/sex/age/ethnicity/disease metadata are absent from bounded evidence
- GEO text contains a processing inconsistency: alignment says GRCh38/Ensembl 99 while a separate Genome_build line says hg19; the deposited ordered ENSG feature axis is accepted, but the writer must preserve this provenance note rather than relabel the build
