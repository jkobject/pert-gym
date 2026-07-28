# E-MTAB-9304 — read-only scientific audit and correction plan

Live audit: `artifacts/first10_audit/cohort_a/E-MTAB-9304.audit.json` at 2026-07-28T11:49:37.105081+00:00
Canonical prefix: `pert-gym/logical/temporal/drosophila_embryo_dorsal_ventral_patterning_scrna_seq`

## Verdict

- Temporal: **non_temporal_single_stage** — Remove cell-level timepoint/developmental-time fields; retain age and stage once in dataset metadata. Do not label developmental stage as a perturbation.
- Chunking: One 119362x16936 CSR triplet is appropriate (accepted X was 189224969 bytes); the 1000-row chunk_0000 is a partial legacy fallback, not a complete dataset and must not enter the canonical Collection.
- VAR: **malformed_authoritative_row_pair_in_legacy_payload_and_canonical_payload_unavailable**

## Current triplet

- obs: `pert-gym/logical/temporal/drosophila_embryo_dorsal_ventral_patterning_scrna_seq/obs.parquet` / `rt5eRz8opcJXtybp0000`; payload exists=False; SHA-256=`unavailable`
- X: `pert-gym/logical/temporal/drosophila_embryo_dorsal_ventral_patterning_scrna_seq/X.h5ad` / `At3j5L0or4eqfgAD0000`; payload exists=False; SHA-256=`unavailable`
- var: `pert-gym/logical/temporal/drosophila_embryo_dorsal_ventral_patterning_scrna_seq/var.parquet` / `cvoiSPVFrjufRvVu0000`; payload exists=False; SHA-256=`unavailable`

## Main corrections

1. Fail closed on the live defect: accepted canonical obs/X/var records exist but all three GCS payloads are absent; the accepted manifest generation URI is also the required provenance anchor.
2. Rebuild/restore from the authoritative GXA design and raw MatrixMarket source, preserving the recorded accepted axis hashes where parity is possible.
3. Create revised obs/var/X only after full source row/feature parity; do not revise the 1000-row fallback into the canonical key.
4. Apply the OBS and VAR decisions above, re-link obs->X->var, and validate payload existence by generation-pinned readback.

## OBS decisions

| Current column | Action | Target | Reason |
|---|---|---|---|
| `assay` | `keep` | `assay` | Inference-relevant assay constant. |
| `dataset` | `map` | `dataset` | Use the stable logical dataset key. |
| `design_factor_value_inferred_cell_type_authors_labels` | `map_preserve` | `source_cell_type_author` | Author labels are source evidence for 16786 cells; missing rows stay unknown. |
| `design_factor_value_inferred_cell_type_ontology_labels` | `map_preserve` | `cell_type` | Use source ontology labels as canonical text where present; do not fill absent cells. |
| `design_factor_value_ontology_term_inferred_cell_type_authors_labels` | `drop` | `` | Entirely missing in the inspected payload. |
| `design_factor_value_ontology_term_inferred_cell_type_ontology_labels` | `map_preserve` | `cell_type_ontology_term` | Retain the source term only where present; absence remains unknown. |
| `design_factor_value_ontology_term_strain` | `drop` | `` | Entirely missing. |
| `design_factor_value_strain` | `map_preserve` | `source_strain` | Variable experimental genotype/strain evidence; keep even where canonical genotype differs textually. |
| `design_ontology_term_age` | `drop` | `` | Entirely missing and age is dataset-wide. |
| `design_ontology_term_developmental_stage` | `drop` | `` | Entirely missing and stage is dataset-wide. |
| `design_ontology_term_genotype` | `drop` | `` | Entirely missing; do not invent ontology mappings. |
| `design_ontology_term_organism_part` | `drop` | `` | Entirely missing and organism part is dataset-wide. |
| `design_ontology_term_strain` | `drop` | `` | Entirely missing. |
| `design_sample_characteristic_age` | `move_to_dataset_metadata` | `age_original` | Constant single stage; not a temporal observation axis. |
| `design_sample_characteristic_developmental_stage` | `move_to_dataset_metadata` | `developmental_stage` | Constant single stage. |
| `design_sample_characteristic_genotype` | `map_preserve` | `genotype` | Variable in the full source and required to distinguish control from maternal pathway mutants. |
| `design_sample_characteristic_organism_part` | `move_to_dataset_metadata` | `organism_part` | Constant whole embryo. |
| `design_sample_characteristic_strain` | `map_preserve` | `source_strain` | Variable in the full source and useful conditioning/provenance. |
| `developmental_time_label` | `move_to_dataset_metadata` | `age_original` | Constant and therefore not a temporal cell covariate. |
| `developmental_time_label_source` | `drop` | `` | Internal harness lineage; source citation belongs in dataset provenance. |
| `modality` | `keep` | `modality` | Inference-relevant modality constant. |
| `organism` | `normalize_keep` | `organism` | Normalize capitalization and retain as inference-relevant constant. |
| `perturbation` | `replace` | `perturbation` | Current developmental_time value is biologically wrong; derive from genotype. |
| `perturbation_type` | `replace` | `perturbation_type` | Use none for controls and maternal_genetic for gd7/Toll mutants. |
| `source_accession` | `move_to_dataset_metadata` | `source_accession` | Dataset-wide provenance. |
| `source_experiment_design_path` | `drop` | `` | Host-local non-portable path; replace with immutable URL and checksum. |
| `source_raw_zip_path` | `drop` | `` | Host-local non-portable path; replace with immutable source/receipt identity. |
| `source_title` | `move_to_dataset_metadata` | `source_title` | Dataset-wide description. |
| `timepoint_source_hint` | `drop` | `` | Vague harness hint contradicted by the single-stage source distribution. |

## Proposed OBS schema

`obs_uuid`, `original_obs_index`, `dataset`, `sample`, `cell_id`, `organism`, `assay`, `modality`, `genotype`, `source_strain`, `cell_type`, `cell_type_ontology_term`, `source_cell_type_author`, `perturbation`, `perturbation_type`, `is_control`, `is_baseline`

## Residual risks

- The canonical payloads are missing and the accepted manifest was not discovered by the bounded Lamin query, so the full accepted canonical OBS schema and matrix cannot be read back in this audit.
- Only source-backed cell labels may be restored; 102576 source rows without labels remain unknown.

No LaminDB, GCS, or Collection mutation was performed by this audit.
