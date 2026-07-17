# Dataset status catalogue

_Last updated: 2026-06-26 17:07 CEST — patched after live board/artifact correction._

This page is the internal dataset/status map for agents. Keep README and CLAUDE
short; put detailed source status, blockers, and artifact pointers here. For exact
counts, always cross-check [current-status.md](current-status.md).

## Count vocabulary

Do not use a naked "dataset count" for this project. Use the current vocabulary:

- latest visible Lamin artifacts: 3407 artifact records visible on branch
  `jkobject` at the P3R-C build time;
- canonical collection members: 1056 `obs.parquet` artifacts in
  `pert-gym/canonical/20260621`;
- base-public collection members: 60 public/base members in the canonical union;
- pert-gym addition members: 996 branch-added members in the canonical union;
- logical datasets / families: 120 grouped biological/source dataset rows in the
  unified manifest;
- triplet prefixes / chunks: 1056 canonical obs/X/var prefixes represented by
  collection members;
- model-ready members: 1 reviewed v0 loader-smoke member.

Historical `110`/`111` counts were logical dataset/audit-subset counts, not
Lamin database size. Historical `720`/`721` counts were metadata-only
triplet-prefix counts, not current database size. The global pertdata instance is
much larger: public pertdata has 2000+ datasets/assets at global scale. The
current pert-gym query surface is the curated dated subset above, not the whole
pertdata universe.

## Canonical query surface

Current canonical surface:

- `pert-gym/base-public/20260621` — 60 members;
- `pert-gym/additions/20260621` — 996 members;
- `pert-gym/canonical/20260621` — 1056 canonical obs-triplet members;
- `pert-gym/model-ready/20260621` — 1 reviewed v0 loader-smoke member.

Artifacts to link, not copy:

- `artifacts/schema_audit/unified_collection_build_20260621.md`
- `artifacts/schema_audit/unified_collection_build_20260621.json`
- `artifacts/schema_audit/unified_collection_manifest_20260621.tsv`
- `artifacts/schema_audit/unified_collection_query_validation_20260621.md`
- `artifacts/schema_audit/explore_unified_collection_notebook_20260621.md`

## Live correction note — 2026-06-26

This page was stale relative to live Kanban/artifacts. Treat older statements about SCP residuals, STT0000071, and GSE216481 row113 as superseded by this note unless a newer artifact says otherwise.

- SCP browser-auth residuals are not broadly pending anymore: `SCP1467`, `SCP211`, `SCP282`, `SCP3301`, and `SCP499` have completed follow-up Kanban/artifacts. Do not ask Jérémie to redownload those as a generic blocker; inspect the specific ingestion artifact first.
- `GSE216481` row113 MORF/TFmap label mapping was recovered from Data S1 / Elsevier CDN and staged. Remaining blocker is narrower: an explicit filtered-cell inclusion table/predicate for `201218_RNA` and `210322_TFAtlas`, not the ORF/TF-symbol map.
- `STT0000071` is not source-empty: CNGB dataset is rooted at `https://ftp.cngb.org/pub/stomics/STT0000071/Analysis/`.
  - A confirmed non-TIFF subset is `https://ftp.cngb.org/pub/stomics/STT0000071/Analysis/STSA0000734/STTS0001152/` with payloads `T1_C1.70.gem.gz`, `T1_C1.70.tsv.gz`, `T1_C1.gem.gz`, `readme.en.txt`, `readme.zh.txt`, `event.tar`.
  - TIFFs (e.g. `*.tif*`/`*.tif.gz`) exist and should be skipped for the first pilot.
- STAPR and Arabidopsis rows are still source/action-resolution work, but should be handled as concrete source-resolver cards, not vague “blocked” prose.

## Dataset family status

| family/source | current status | notes / blockers | primary artifact pointers |
| --- | --- | --- | --- |
| Public/base pertdata examples | included in canonical query surface | 60 members; some need alias projection before model-ready promotion | `unified_collection_manifest_20260621.tsv`; `model_ready_subset_20260621.md` |
| PRISM Perturb-seq collection | accessible subset ingested/chunked; residual Google Drive files now staged for resumed ingestion | canonical surface has 925 PRISM members. P5D/P5E originally classified 36 rows as Drive quota/permission blocked, but a later manual/Chrome recovery succeeded and the files were staged to `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/` (54 objects, ~92.33 GiB). Next step is duplicate-gated, smoke-first chunked ingestion from GCS. `GSE90063_human-004` remains excluded by user decision due duplicate/subset ambiguity | `prism_residual_cleanup_20260622.md`; `prism_drive_residual_resolution_20260622.md` (historical failed CLI/gdown pass); GCS prefix `prism_google_drive_datasets_20260622`; `phase3_ingestion_progress.json` |
| VIPerturbSeq | represented in canonical, including v0 model-ready smoke member | current model-ready v0 member is `viperturb/vimentin_screen_chunk_smoke/chunk_0000/obs.parquet`; broader biological model-ready promotion still needs review | `model_ready_subset_20260621.md`; `unified_collection_manifest_20260621.tsv` |
| T-cell GWPS / `D4_Rest.assigned_guide` | final-verified chunked canonical triplets on `jkobject` | 539 chunks; remote GCS range-read chunking path avoids full local materialization. Do not start duplicate writers. Future T-cell files should reuse the remote chunker with checkpointed resume | `tcell_gwps_D4_Rest_final_verification.json`; `tcell_gwps_D4_Rest_inflight_status.json`; `tcell_gwps_remote_chunk_ingestion_status.json` |
| XAtlas / Orion | staged but not triplet-ingested | two raw h5ads staged on GCS, ~195 GiB and ~326 GiB. 2026-06-23 mount-backed read prototype succeeded via `/Users/jkobject/mnt/gcs/scperturb`; production still needs a patched/reviewed `connect_pertdata()` chunker and an idle single-writer window | `post_prism_huge_dataset_plan_refresh_20260623.md`; `post_prism_huge_dataset_plan_refresh_20260623.json` |
| Sanger dual-guide CRC | no longer missing from Lamin | `sanger_dual_guide_crc/mapping_counts/{obs.parquet,X.h5ad,var.parquet}` and `mapping_manifest.parquet` exist | `current-status.md` P6B section |
| Broad PRISM repurposing | present as bulk/screen-style response data | hybrid obs model with `obs["lfc"]`; X empty pending CCLE baseline-expression join | `data/README.md` |
| Sanger GDSC / SCORE / DepMap CCLE | present as screen/baseline resources | useful for response/baseline joins, not equivalent to scRNA perturbation triplets | `data/README.md`; `unified_collection_manifest_20260621.tsv` |
| Temporal pretraining catalogue | validated source catalogue plus bounded per-source ingestions/contracts; docs are being corrected against live board/artifacts | v4 catalogue has 150 rows: 84 `A_explicit`, 66 `B_candidate_needs_timepoint_metadata`. T29 row115/GSE142078 is verified; row114/GSE156170 is `excluded_with_reason`; row113/GSE216481 has recovered MORF/TFmap label mapping (`gse216481_morf_supplement_validation_20260625.*`) and now waits specifically on filtered-cell inclusion/predicate before canonical writes. Ingestions proceed through bounded per-source cards | `temporal_pretraining_dataset_plan_20260621.md`; `data/temporal_pretraining_datasets/README.md`; `gse216481_morf_supplement_validation_20260625.md` |
| CELLxGENE temporal/dev batches | several conservative triplets written and verified | larger CELLxGENE assets remain deferred for backed/chunked handling; review-required Kanban rows may still need board closure even after supervisor acceptance | `temporal_cellxgene_limb_full_20260622.md`; `temporal_cellxgene_development_batch1_20260622.md` |
| SCP/BioStudies/organoid candidates | SCP browser-auth residuals mostly completed; non-SCP rows still source/representation-gated as documented | Live board/artifacts show follow-ups completed for `SCP1467`, `SCP211`, `SCP282`, `SCP3301`, and `SCP499`. Do not report these as generic “needs download” blockers; inspect their ingestion/status artifacts. Remaining blockers are specific matrix-family choices, missing sidecars, or non-SCP raw/source issues, not the old headless HTTP 401 state | `scp211_kidney_organoid_ingestion_t_8ed1ea12_20260623.md`; `scp282_brain_organoid_ingestion_20260623.md`; `sc*p*` artifacts; temporal batch artifacts under `artifacts/schema_audit/` |
| NASC-seq / metabolic-labeling | accepted for NASC-seq GSE128273 and scNT-seq GSE141851 explicit timecourse | scNT has 19 verified triplets under `temporal_pretraining/metabolic_labeling/scnt_seq/GSE141851/` with `X_new_counts`/`X_old_counts` side matrices; scSLAM GSE115612 remains blocked on explicit multi-timepoint/labeling-window semantics | `current-status.md`; `temporal_scnt_seq_gse141851_t22r_20260622.md`; T11/T22R Kanban handoffs |
| ZESTA / MOSTA / STOmics spatial-temporal | ZESTA 6/6 verified; STDS0000060 staged; STT0000071 source path corrected; MOSTA/GSE326326 still staged/chunked work | ZESTA continuation is complete. STDS0000060 five processed H5ADs were staged to GCS via continuation chain. `STT0000071` old “no downloadable files” probe is stale: the CNGB FTP analysis path for sample `STSA0000734/STTS0001152` lists GEM/TSV/readme/event payloads; avoid TIFFs and stage expression/metadata only. MOSTA has endpoint/range probe but one-file pilot still needs a clean retry. GSE326326 has a bounded 3-H5AD pilot staged; full 77-H5AD/large archive surface remains staged/chunked work | `temporal_t38_heart_source_probe_20260622.md` plus 2026-06-26 correction note; `temporal_t8a45bdc8_source_staging_probe_20260625.*`; STDS/GSE326326 Kanban handoffs |
| Plant spatial/development T39 | probe-only; needs concrete source resolver cards | Tomato GSE293948 has a 12 GB processed RDS requiring staged/high-memory Seurat inspection. Arabidopsis life-cycle row points to `https://travislee.science/` and paper `PMC12416547`; next action is repository/download accession resolution for processed snRNA/spatial matrices. Arabidopsis STOmics row still has only generic CNGB datasets URL; next action is exact STDS/project accession discovery. No Lamin writes until exact sources + duplicate check + bounded matrix plan | `temporal_t39_plant_spatial_probe_20260622.md/json`; catalogue rows 127–128 in `temporal_pretraining_datasets_v4.*` |
| PRoPER-seq / ProPer-seq 2026 | wanted but source-TBD | actual scRNA expression source not found; legacy GSE150818 is explicitly excluded and must not be substituted | [dataset-modalities.md](dataset-modalities.md) |
| RxRx / Recursion / JUMP image datasets | image contract defined; implementation pending | use external image URIs plus rich `obs` and typed image-derived payloads; do not force into canonical scRNA `X.h5ad`. Preferred implementation path is scPortrait (`https://mannlabs.github.io/scPortrait/pages/workflow.html`) to segment phenotypic microscopy images into single-cell image datasets and produce single-cell embeddings/features | `post_prism_huge_dataset_plan_20260621.md`; [dataset-modalities.md](dataset-modalities.md); scPortrait workflow |

## PRISM blockers

Current PRISM residual decision has changed: source download is no longer the blocker for the Google Drive residual batch.

- `GSE208240` and `GSE220974` are already present as exact chunked triplets on
  `jkobject` and were moved to ingested during P5D cleanup.
- `D4_Rest.assigned_guide` belongs to T-cell GWPS, not PRISM.
- Historical P5E CLI/gdown retries classified 36 rows as quota/rate-limit or
  permission/private-link blocked, but this was superseded by a later manual/Chrome
  recovery. The recovered h5ads are staged at
  `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/`
  with 54 objects / ~92.33 GiB and byte-size verified upload manifests.
- `GSE90063_human-004` is excluded/skipped by user decision because of
  duplicate/subset ambiguity.

Next safe action: run the duplicate gate against public `main` plus `jkobject`
for the staged h5ads, then smoke-first chunked ingestion from GCS. Do not ingest
directly from Downloads and do not treat historical Drive quota reports as current.

## T-cell and other huge dataset blockers

T-cell GWPS `D4_Rest.assigned_guide` is done enough for the current canonical
surface, but other huge files are still resource/strategy work:

- assigned-guide raw h5ads total ~1.74 TB under GCS staging;
- current Mac local free space is far below the smallest full h5ad plus outputs;
- use fsspec/gcsfs range reads and chunked writers, not blind local sync;
- checkpoint via `artifacts/tcell_gwps_remote_chunk_ingestion_status.json` and
  prefer timestamped tmux runners for multi-hour chunking.

For XAtlas/Orion, first build a safe chunker using `connect_pertdata()` and avoid
existing reference-code pitfalls: global `ln.connect`, local full downloads, and
backed-view `.to_memory()` on huge matrices.

## Temporal catalogue and ingestion status

The source catalogue lives in `data/temporal_pretraining_datasets/`. The wiki
source-of-truth summary is in [current-status.md](current-status.md), with artifact
reports linked there. Practical rules:

- the catalogue is a source-prioritization table, not a claim that all 150 rows
  are ingested;
- temporal triplets need `timepoint` in minutes where possible,
  `trajectory_id`, `pseudotime`, `is_baseline`, and preserved raw labels;
- small/medium CELLxGENE datasets can be ingested directly after backed probes and
  duplicate checks;
- large spatial/Visium/MOSTA-like assets need chunked/staged handling;
- SCP/auth/raw-only blockers stay blockers until processed matrices or access are
  recovered.

## Model-ready implications

The 1056-member canonical surface is `triplet-integrity-ok`, not fully model-ready.
For model work, read [model-roadmap.md](model-roadmap.md) and promote reviewed
subsets with explicit loader evidence before making benchmark claims.
