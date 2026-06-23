# Temporal scRNA/spatial/microscopy pretraining dataset search

Working directory for the deep research run on temporal single-cell, spatial transcriptomics, metabolic-labeling, perturbation, regeneration, organoid, developmental trajectory, and temporal microscopy datasets.

## Current Best Files

- Main current table: `temporal_pretraining_datasets_v4.tsv`
- Human-readable current table: `temporal_pretraining_datasets_v4.md`
- JSON current table: `temporal_pretraining_datasets_v4.json`
- Validation summary: `validation_v4.json`
- SRA/BioProject mapping: `ncbi_sra_bioproject_mapping_v0.tsv`
- Original DeepResearch ledger and raw scrape artifacts remain in the OpenClaw
  workspace at `.deepresearch/temporal-scrna-development-pretraining/`.

Current count after the 1-6 exhaustive pass:

- 150 kept datasets/families in v4.
- 0 `unclear`.
- 0 primary `dataset_url` detected as a paper URL.
- 0 non-`temporal_yes` rows.
- 8 duplicate rows merged during v4 consolidation.
- 1 prior `temporal_no` removed: `Developmental cell programs are co-opted in inflammatory skin disease`.

## Inclusion Rules

Keep only entries with at least one of:

- multiple sampled biological timepoints/stages;
- perturbation, infection, injury, or regeneration timepoints;
- organoid/differentiation/reprogramming days or ordered stages;
- spatial transcriptomics across developmental/regeneration stages;
- metabolic-labeling time-resolved RNA dynamics;
- microscopy/live-imaging temporal developmental trajectories;
- snapshot datasets with clear user-defined ordered developmental trajectory annotation.

Remove:

- static adult/disease/case-control atlases with no ordered time/stage axis;
- donor age bins only, unless explicitly developmental/postnatal/aging-series is useful and marked as such;
- catalogues as datasets. Example: scPerturb/PerturBase are sources to mine, not rows to ingest wholesale.

## Source Coverage Status

The user asked to complete these in order:

1. BioStudies/ArrayExpress API - done with fallback: direct API timed out from VPS; web/SCEA/ArrayExpress search added 9 explicit datasets.
2. Single Cell Portal search/export metadata - done: API search with JSON headers, 19 retained studies.
3. STOmicsDB/STellaris scrape/API - done: web/database search retained 3 spatial-temporal datasets/families and rejected 1 static disease dataset.
4. OrganoidDB table extraction - done: parsed the HTTP `browse` page JS table (`showdata`, 17,287 rows), filtered 11 dataset-level `Odd` entries with `study_type=Development` and `platform=scRNA-Seq`, downloaded detail pages, and extracted GEO/sample accessions.
5. PerturBase entry-level scrape/API - done: inspected the Umi SPA chunks, found `/api/dataset/list` and `/api/dataset/getParam`, dumped all 122 records, filtered 13 strict temporal/developmental/reprogramming entries, and compacted them into 6 dataset families.
6. SRA/BioProject fallback from accessions - done: extracted 50 accessions overall; queried 40 NCBI GEO/BioProject accessions with E-utilities; wrote 6 GEO entries with SRA hits and 11 entries with BioProject IDs. E-MTAB/E-GEOD are listed but remain ArrayExpress/ENA-side rather than NCBI-resolved.

Merged v4 source counts:

- CELLxGENE: 66
- CELLxGENE plus extra web: 1
- Single Cell Portal: 19
- initial seed rows: 13
- BioStudies/ArrayExpress step 1: 9
- GEO/database second pass: 16
- extra web additions: 5
- OrganoidDB step 4: 11
- PerturBase step 5: 6
- scPerturb strict timecourse: 1
- STOmicsDB/STellaris step 3: 3

Previous work already done:

- CELLxGENE public collections mined through `https://api.cellxgene.cziscience.com/dp/v1`.
- CELLxGENE ambiguous candidates resolved through collection API metadata.
- scPerturb repo tables downloaded and filtered by `multiple_time_points`.
- NCBI GEO/GDS second pass through E-utilities with a manually retained high-confidence subset.
- Several CNGB/STOmics datasets added manually: HESTA, ZESTA, ARTISTA, MOSTA, STAPR and related candidates.

## Output Schema

The current final tables contain:

- `confidence`
- `source`
- `title`
- `organism`
- `cells`
- `modality`
- `time_axis`
- `category`
- `dataset_url`
- `url_type`
- `paper_url`
- `temporal_status_verified`
- `verification_reason`
- `development_stage_evidence`
- `alternate_accessions`
- `merge_sources`
- `merge_notes`

Future agents should preserve these columns when making v5+.

## Next Step Protocol

For future follow-up passes:

1. Save raw API/scrape output under `artifacts/<source_name>/`.
2. Save filtered source-specific candidates as `<source>_temporal_candidates_v*.tsv`.
3. Add only candidate rows with dataset/collection/download URLs, not paper-only URLs.
4. Mark uncertain rows as `unclear_source_specific`, not final.
5. Merge into a new final table after deduplication.
6. Update this README and add a ledger note with `scripts/deepresearch.py note add`.

## Dedup Strategy

Deduplicate by:

- exact dataset accession or collection ID;
- DOI/GEO/BioProject accession when present;
- normalized title prefix;
- known aliases such as CELLxGENE collection plus original paper/dataset page.

When two rows refer to the same dataset family, keep the dataset URL with best download/access metadata and preserve alternate URLs in notes or `paper_url`.

## Known Caveats

- v4 contains some high-confidence second-pass additions from GEO/database searches that should eventually be validated at sample/metadata level.
- Some database pages are portals rather than direct file downloads; acceptable for current manifest if they are dataset/collection pages, but ingestion will need file-resolution later.
- Plant and microscopy-only datasets are included because the user explicitly allowed derivatives and microscopy, but they may deserve a separate modality class during ingestion.
- OrganoidDB and PerturBase rows are now entry-level, but exact per-sample timepoint values may still require parsing GEO/SRA sample metadata during ingestion.
- NCBI E-utilities were used for the SRA/BioProject fallback. The ArrayExpress/E-MTAB records should be resolved through ENA/ArrayExpress-specific endpoints in a later ingestion pass.

## PerturBase T29 directed differentiation status — 2026-06-23

- Row 115 / `GSE142078` remains accepted as ingested and verified; preserve the PerturBase filtered-cell discrepancy in obs `qc_note`.
- Row 114 / `GSE156170` is `excluded_with_reason` from the active path by user decision due perturbation-label/QC ambiguity; do not treat it as the current blocker.
- Row 113 / `GSE216481` is active with an explicit metadata contract. Existing staged/probe artifacts identify QC-pass RNA components `201218_RNA` and `210322_TFAtlas`, and exclude ATAC plus failed/combinatorial components from canonical RNA `X.h5ad`. The exact missing inputs before canonical write are the barcode/sequence/numeric-id-to-ORF/TF-symbol lookup and component-specific filtered-cell inclusion table/predicate.

Artifacts:

- `artifacts/schema_audit/temporal_t29_gse216481_row113_probe_20260622.md`
- `artifacts/schema_audit/temporal_t29_gse216481_row113_metadata_contract_20260623.md`
- `artifacts/scripts/validate_temporal_perturbase_t29_gse216481_contract_20260623.py`
- `artifacts/schema_audit/temporal_t29_gse216481_row113_contract_validation_20260623.json`

- `artifacts/schema_audit/temporal_t29_gse216481_row113_contract_input_20260623.json` (compact validation input)
