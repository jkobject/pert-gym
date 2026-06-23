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

## Ingestion prioritization plan

A read-only Mac-side validation/prioritization pass was completed on 2026-06-21:

- `artifacts/schema_audit/temporal_pretraining_dataset_plan_20260621.md`
- `artifacts/schema_audit/temporal_pretraining_dataset_plan_20260621.json`

It validates TSV/JSON/Markdown consistency, summarizes counts, checks live Lamin duplicate hits for the shortlist, and recommends a v0 execution order: human embryonic limb, Zebrahub, mouse gastrulation/early organogenesis, rat kidney postnatal, reproducible brain organoids, then remaining source-blocked metabolic-labeling/SCP regeneration rows after blockers are cleared. Schiebinger/Optimal-transport-style reprogramming should be treated as duplicate-review because `SchiebingerLander2019` triplets already exist in Lamin.

The current batched temporal roadmap supersedes any one-card-per-dataset planning:

- `artifacts/schema_audit/temporal_ingestion_batches_20260622.md`
- `artifacts/schema_audit/temporal_ingestion_batches_20260622.json`

It covers all 84 `A_explicit` catalogue rows, marks already ingested/smoked rows separately, and groups pending work into source/tooling-oriented batches of mostly 2–3 datasets each. Future Kanban cards should be created from these batch sections, not from individual catalogue rows.

T22R scNT-seq repair/finish status (2026-06-22): row 84 (`GSE141851`) is now
represented by 19 verified same-prefix triplets under
`temporal_pretraining/metabolic_labeling/scnt_seq/GSE141851/`, totaling 56,500
obs rows. Canonical `X.h5ad` stores C_new + T_old total RNA counts; linked
`X_new_counts.h5ad` and `X_old_counts.h5ad` preserve new/pre-existing RNA
semantics. The run verified nine pre-existing prefixes and wrote/verified ten
remaining prefixes after workspace consolidation. scSLAM-seq `GSE115612` remains
blocked until explicit multi-timepoint/labeling-window semantics and converter
mapping are established. Status artifact:
`artifacts/schema_audit/temporal_scnt_seq_gse141851_t22r_20260622.md`.

T14 developmental small-animal SCP probe status (2026-06-22): rows 56
(`SCP667`), 58 (`SCP454`), and 76 (`SCP162`) were source/duplicate-probed
without bulk download or Lamin writes. SCP exposes file manifests for all three
and planned prefixes/accessions/file names have no duplicate hits, but every
tested `download`/`stream` Range probe returned HTTP 401. `SCP454` includes a
2.1 GB all-stage expression matrix, so once authenticated export is available it
should be staged/chunked rather than blindly downloaded. Status artifact:
`artifacts/schema_audit/temporal_t14_scp_small_animal_source_probe_20260622.md`.

T15 developmental neural/eye/heart SCP status (2026-06-22/23): rows 65
(`SCP1290`), 75 (`SCP3301`), and 79 (`SCP1467`) were source/duplicate-probed.
SCP file manifests are public but anonymous payload downloads return HTTP 401;
logged-in browser recovery later staged the SCP1467 heart exports on GCS.
`SCP1290` was safely ingested through its bounded GEO scRNA alternate
`GSE153162`: 20 per-sample 10x H5 triplets under
`temporal_pretraining/scp1290_gse153162_mammalian_cerebral_cortex/`, totaling
128,746 obs rows with 27,998 var rows per sample and verified obs→X→var links.
`SCP1467` is now ingested and verified under
`temporal_pretraining/scp1467_drosophila_embryonic_heart/` (`2,857 × 9,034`):
canonical `X.h5ad` is raw counts from `Heart_counts.tsv`, with normalized
expression preserved as `X_normalized_expression.h5ad`. `SCP3301`/`GSE315712`
is deferred for processed-vs-raw matrix-family selection and staged/chunked
conversion; avoid the 33 GB GEO RAW tar as the first path. Status artifacts:
`artifacts/schema_audit/temporal_t15_scp_gse153162_status_20260622.md` and
`artifacts/schema_audit/temporal_scp1467_heart_ingestion_20260623.md`.

T20 GEO developmental batch status (2026-06-22): rows 62 (`GSE334273` sea
lamprey), 73 (`GSE280655` mouse cortical inhibitory neurons), and 78
(`GSE325829` human embryos) were source/duplicate-probed. `GSE280655` was safely
ingested and verified as
`temporal_pretraining/developing_mouse_cortical_inhibitory_neurons/GSE280655_E18_5`
(`8,015 × 21,909`; single E18.5 developmental snapshot). `GSE334273` needs a
staged/streamed converter for a 1.19 GB RAW tar with four 10x MTX samples;
`GSE325829` needs GCS staging + backed/chunked ingestion for an 11.08 GB h5ad.
Status artifact:
`artifacts/schema_audit/temporal_t20_geo_developmental_status_20260622.md`.

T28 organoid follow-up probe status (2026-06-22): row 107 (`SCP282`), row 109
(`GSE269572`), and row 112 (`GSE329346`) were source/duplicate-probed without
bulk download or Lamin writes. Planned prefixes have no duplicate hits. `SCP282`
remains `blocked_source_auth_needed` because SCP expression downloads return
HTTP 401 without a session; `GSE269572` exposes no GEO supplementary processed
matrix; `GSE329346` exposes only a bulk RNA-seq gene-expression supplement, not
scRNA. Status artifact:
`artifacts/schema_audit/temporal_t28_organoid_followup_status_20260622.md`.

T26 OrganoidDB/Human Organoid Bank status (2026-06-22): row 97 (`Odd001111 /
GSE130238`) was ingested and verified as
`temporal_pretraining/organoiddb/GSE130238_cortical_organoid_months` (`16,086 ×
33,694`, months 1/3/6/10, obs→X→var verified). Row 91 (`Odd001138 / GSE162547`)
was source-resolved but not ingested because the GEO RAW archive contains raw
Cell Ranger barcode-universe matrices (`33,939 × 6,794,880` per sample), not
filtered cell matrices. Row 108 scHOB static crawl found no concrete canonical
matrix payload URL. Status artifact:
`artifacts/schema_audit/temporal_organoiddb_t26_status_20260622.md`.

T13 Zebrahub/mouse gastrulation status (2026-06-22): Zebrahub stage
continuation is complete under `temporal_pretraining/zebrahub/`. All 11 stages
(`10hpf`, `12hpf`, `14hpf`, `15hpf`, `16hpf`, `19hpf`, `24hpf`, `2dpf`, `3dpf`,
`5dpf`, `10dpf`) verify with same-prefix obs→X→var links, payloads, and temporal
fields; total obs rows: 124,306. The run repaired a partial `3dpf` var/link write
and a pre-existing `14hpf` X→`15hpf/var.parquet` link. Mouse gastrulation HCA is
now fully ingested under `temporal_pretraining/mouse_gastrulation_hca/`: 141
same-prefix triplet prefixes cover all 139,331 cells with 29,452 var rows per
prefix. The production run streamed 483,512,215 MatrixMarket entries from the
staged archive without `mmread` or full `raw_counts.mtx` extraction, and final
verification found 0 payload/link/schema errors. Status artifacts:
`artifacts/schema_audit/temporal_t13_final_status_20260622.md` and
`artifacts/schema_audit/temporal_t13g_mouse_gastrulation_full_status_20260622.md`.

T17 BioStudies/ArrayExpress embryo batch status (2026-06-22): rows 59
(`E-MTAB-9304`), 64 (`E-MTAB-3929`), and 72 (`E-MTAB-8060`) were source/duplicate
probed against BioStudies plus SCEA processed-export directories. `E-MTAB-3929`
was ingested and verified as
`temporal_biostudies/E-MTAB-3929_preimplantation_embryo` (`1,519 × 34,570`,
timepoints 3–7 day). `E-MTAB-9304` and `E-MTAB-8060` have source-resolved SCEA
`.project.h5ad`/MTX exports, but need backed/MatrixMarket conversion rather than
busy-host full-load conversion. Status artifact:
`artifacts/schema_audit/temporal_t17_biostudies_embryo_batch_status_20260622.md`.

T18 BioStudies/ArrayExpress embryo batch B status (2026-06-22): rows 63
(`E-MTAB-8894`), 69 (`E-GEOD-234602`), and 70 (`E-MTAB-10894`) were
source/duplicate-probed. `E-MTAB-10894` rabbit embryo PGC was safely ingested from
bounded GXA MatrixMarket exports and verified as
`temporal_pretraining/gxa/E-MTAB-10894_rabbit_embryo_pgc` (`381 × 16,587`, ages
4/5/6/7 day, obs→X→var verified). `E-MTAB-8894` and `E-GEOD-234602` are
source-resolved through GXA MatrixMarket endpoints but need staged/backed
conversion; do not ingest SDRF-only metadata or blindly download their larger
generated archives. Status artifact:
`artifacts/schema_audit/temporal_t18_biostudies_batch_b_status_20260622.md`.

T16 SCP chick/Drosophila/retinal ganglion probe status (2026-06-22): rows 68
(`SCP1469`), 74 (`SCP1570`), and 132 (`SCP1846`) were source/duplicate-probed
without bulk download or Lamin writes. SCP exposes processed matrix/metadata file
listings for all three and exact planned prefixes have no duplicate hits, but
representative `download_url` probes returned HTTP 401 even after a public-page
cookie-jar visit. Treat these as `blocked_source_auth_needed` until a
browser/session-aware SCP export is available. Status artifact:
`artifacts/schema_audit/temporal_t16_scp_probe_20260622.md`.

T32 zebrafish regeneration SCP probe status (2026-06-22): rows 129 (`SCP1549`),
130 (`SCP1674`), and 133 (`SCP1973`) were source/duplicate-probed without bulk
download or Lamin writes. SCP exposes expression matrices plus metadata for all
three and exact planned prefixes have no duplicate hits, but every SCP
`download_url` Range probe returned HTTP 401. Treat these as
`blocked_source_auth_needed` until a browser/session-aware SCP export is
available. Status artifact:
`artifacts/schema_audit/temporal_t32_zebrafish_regen_scp_20260622.md`.

T33 axolotl regeneration probe status (2026-06-22): rows 134, 135, and 136 were
source/duplicate-probed without bulk download or Lamin writes. Row 134 resolves
to `GSE165901`, whose axolotl-only MatrixMarket members are bounded but only
available inside a 1.97 GB GEO RAW tar; row 135 resolves to a SciLifeLab
ShinyCell/GitHub app without published original Seurat RDS payloads; row 136
`SCP499` exposes a bounded public manifest (`EB.matrix.txt.gz` 40.4 MB plus
sidecars) but SCP downloads return HTTP 401. Status artifact:
`artifacts/schema_audit/temporal_t33_axolotl_status_20260622.md`.

T29 PerturBase directed differentiation status (2026-06-22): row 115
(`GSE142078`) is ingested and verified as
`temporal_pretraining/perturbase/gse142078_luhmes_crispri_day8` (`8,843 × 33,694`,
obs→X→var verified). This follows the user decision to keep reconstructed
guide-joined cells despite PerturBase's 7,684 filtered-cell count, with the note
stored in obs `qc_note`. Row 114 (`GSE156170`) remains blocked on perturbation-label
semantics after archive/pheno inspection: pheno dicts assign many cells to multiple
guides and human∩pheno filters do not reproduce PerturBase filtered counts. Row
113 (`GSE216481`) has now been staged to GCS and filelist/range-probed without Lamin writes. The
QC-pass RNA components are identifiable (`201218_RNA` and `210322_TFAtlas`), while
ATAC and failed/combinatorial components should stay out of canonical RNA `X.h5ad`.
Row 113 remains blocked on a verified encoded-barcode/ORF-to-TF-symbol and
filtered-cell contract before any safe `perturbation` labels can be written. Status
artifacts:
`artifacts/schema_audit/temporal_t29_perturbase_directed_differentiation_status_20260622.md`
and `artifacts/schema_audit/temporal_t29_gse216481_row113_probe_20260622.md`.

T31 Tian/Kampmann 2019 scPerturb status (2026-06-22): row 124 was resolved
inside broad Zenodo record `10044268` without bulk bundle download. The target
files are already present as verified triplets: `scperturb/tian19_iPSC` and
`scperturb/tian19_day7neuron`. Status artifact:
`artifacts/schema_audit/temporal_t31_tian_kampmann_status_20260622.md`. Do not
redownload the Zenodo bundle for this row; if needed, perform an obs-only schema
harmonization for old `pert_name`/`pert_type`/`pert_time` fields.

T30 PerturBase reprogramming/endoderm status (2026-06-22): rows 116–118 were
source/duplicate-audited without Lamin writes. Row 117 (`GSE122662`) overlaps the
existing `SchiebingerLander2019` triplet enough to require a dedicated equivalence
audit before any new ingestion; rows 116 (`GSE107185`) and 118 (`GSE127202`) have
accessible GEO payloads but not direct PerturBase-filtered matrices, so they need
source-specific converters/contracts before triplets are safe. Status artifact:
`artifacts/schema_audit/temporal_t30_perturbase_status_20260622.md`.

T43 Drosophila/human gastrulation status (2026-06-22): row 82 `E-MTAB-9388`
human gastrulation was ingested and verified as
`temporal_pretraining/human_gastrulation/E-MTAB-9388` (`1195 × 25330`, CS7,
`obs -> X -> var` links/payloads OK). Row 60 `GSE190149` Drosophila continuum is
source-resolved but deferred: the RNA subseries `GSE190147` has direct
MatrixMarket files (`23932 × 547805`, 212,703,167 nnz, ~698 MB gz) and needs
GCS-staged/chunked ingestion; the ATAC subseries `GSE190130` should only be
represented as typed auxiliary `X_atac/var_atac` after review. Status artifact:
`artifacts/schema_audit/temporal_t43_drosophila_human_gastrulation_status_20260622.md`.


T41 CELLxGENE B-candidate continuation status (2026-06-22): rows 2, 3, and 8
were source/duplicate-probed. Row 2 wrote the bounded non-Visium snRNA-seq
outflow/aortic valve triplet (`30,125 × 31,008`) as
`temporal_pretraining/human_outflow_tract_aortic_valve_derivatives/cellxgene_single_nuclei_sn_rna_seq_of_the_human_outflow_tract_and_aortic_valve_tissue_cs16`.
Row 8 wrote the smallest mouse gastrula-to-pup cluster (`Testis and adrenal`,
`3,342 × 45,525`) as
`temporal_pretraining/mouse_embryonic_timelapse_gastrula_to_pup/cellxgene_major_cell_cluster_testis_and_adrenal`.
Both verified obs→X→var links and payloads; row 2 has an obs-only repair artifact preserving Carnegie/adult as raw labels while keeping `timepoint` minute-valued only for 13th-week cells. Row 3 remains streaming/chunked-only
(~4.94 GB subset / ~20.03 GB full H5AD); row 8 remaining assets and row 2 Visium
assets are deferred. Status artifact:
`artifacts/schema_audit/temporal_cellxgene_t41_ingestion_20260622.md`.

T42 CELLxGENE small/partial B-candidate continuation status (2026-06-22): rows 5,
10, and 12 were source/duplicate-probed through CELLxGENE collection metadata.
Nine bounded sub-datasets are now verified triplets (203,296 obs total): fetal lung
fibroblast/smooth muscle, epithelium/no-cilium, cilium, endothelium, B cells,
T/NK/ILC, and myeloid; fetal bone marrow Down syndrome 10x; and mouse cerebellum
developmental snRNA-seq. Final read-back verified payload existence and obs→X→var
links for all nine. This is explicitly a small/partial continuation, not full
collection completion: fetal lung Visium/Organoid/All-cells, fetal BM CD34+/large
all-cells, and human cerebellum are deferred for spatial/chunked/backed follow-up.
Status artifact: `artifacts/schema_audit/temporal_cellxgene_t42_final_status_20260622.md`.

T25 OrganoidDB mesoderm/heart/intestine status (2026-06-22): all three rows were
resolved through OrganoidDB detail pages to GEO payloads and ingested as verified
triplets. `Odd001154 / GSE194214` paraxial mesoderm somitoids: `18,716 × 33,694`,
days 1/2/3/5. `Odd001137 / GSE158999` mouse gastruloid cardiogenesis: `30,496 ×
23,961`, days 4/5/6/7. `Odd001151 / GSE148093` mouse intestinal organoids: `5,212
× 24,870`, sample days 0/1/2/3, restricted to the OrganoidDB-listed
GSM4453981–GSM4454011 samples. All three have verified payloads and obs→X→var
links. Status artifact: `artifacts/schema_audit/temporal_organoiddb_t25_ingestion_20260622.md`.

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
- Plant and microscopy-only datasets are included because the user explicitly allowed derivatives and microscopy, but they may deserve a separate modality class during ingestion. For row 86 (`Ceratitis capitata` LSFM embryogenesis), see `artifacts/schema_audit/temporal_ceratitis_lsfm_representation_probe_20260622.{md,json}`: metadata-only probe complete, no Lamin write, blocked until an image/non-expression representation contract is reviewed.
- OrganoidDB and PerturBase rows are now entry-level, but exact per-sample timepoint values may still require parsing GEO/SRA sample metadata during ingestion.
- NCBI E-utilities were used for the SRA/BioProject fallback. The ArrayExpress/E-MTAB records should be resolved through ENA/ArrayExpress-specific endpoints in a later ingestion pass.
