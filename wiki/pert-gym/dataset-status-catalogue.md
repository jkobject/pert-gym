# Dataset status catalogue

_Last updated: 2026-06-22 12:08 CEST_

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

## Dataset family status

| family/source | current status | notes / blockers | primary artifact pointers |
| --- | --- | --- | --- |
| Public/base pertdata examples | included in canonical query surface | 60 members; some need alias projection before model-ready promotion | `unified_collection_manifest_20260621.tsv`; `model_ready_subset_20260621.md` |
| PRISM Perturb-seq collection | accessible subset ingested/chunked; residual queue source-blocked | canonical surface has 925 PRISM members. P5D/P5E found no new unique staged residual; 36 rows remain blocked by Google Drive quota/permissions, and `GSE90063_human-004` is excluded by user decision due duplicate/subset ambiguity | `prism_residual_cleanup_20260622.md`; `prism_drive_residual_resolution_20260622.md`; `phase3_ingestion_progress.json` |
| VIPerturbSeq | represented in canonical, including v0 model-ready smoke member | current model-ready v0 member is `viperturb/vimentin_screen_chunk_smoke/chunk_0000/obs.parquet`; broader biological model-ready promotion still needs review | `model_ready_subset_20260621.md`; `unified_collection_manifest_20260621.tsv` |
| T-cell GWPS / `D4_Rest.assigned_guide` | final-verified chunked canonical triplets on `jkobject` | 539 chunks; remote GCS range-read chunking path avoids full local materialization. Do not start duplicate writers. Future T-cell files should reuse the remote chunker with checkpointed resume | `tcell_gwps_D4_Rest_final_verification.json`; `tcell_gwps_D4_Rest_inflight_status.json`; `tcell_gwps_remote_chunk_ingestion_status.json` |
| XAtlas / Orion | staged but not triplet-ingested | two raw h5ads staged on GCS, ~195 GiB and ~326 GiB. Mac free space is too small for local backed smoke; needs patched chunker and large-disk/high-memory or robust remote strategy | `post_prism_huge_dataset_plan_20260621.md`; `post_prism_phase3_manifest_20260621.json` |
| Sanger dual-guide CRC | no longer missing from Lamin | `sanger_dual_guide_crc/mapping_counts/{obs.parquet,X.h5ad,var.parquet}` and `mapping_manifest.parquet` exist | `current-status.md` P6B section |
| Broad PRISM repurposing | present as bulk/screen-style response data | hybrid obs model with `obs["lfc"]`; X empty pending CCLE baseline-expression join | `data/README.md` |
| Sanger GDSC / SCORE / DepMap CCLE | present as screen/baseline resources | useful for response/baseline joins, not equivalent to scRNA perturbation triplets | `data/README.md`; `unified_collection_manifest_20260621.tsv` |
| Temporal pretraining catalogue | validated source catalogue, not a Lamin collection by itself | v4 catalogue has 150 rows and 0 unclear rows after validation. Ingestions proceed through bounded per-source cards | `temporal_pretraining_dataset_plan_20260621.md`; `data/temporal_pretraining_datasets/README.md` |
| CELLxGENE temporal/dev batches | several conservative triplets written and verified | larger CELLxGENE assets remain deferred for backed/chunked handling; review-required Kanban rows may still need board closure even after supervisor acceptance | `temporal_cellxgene_limb_full_20260622.md`; `temporal_cellxgene_development_batch1_20260622.md` |
| SCP/BioStudies/organoid candidates | browser-auth retry still blocked by local OMX tooling | previous headless HTTP 401 probes should be retried with logged-in Chrome, but T-SCPAUTH-Mac (`t_a469ca1d`) could not reach Chrome because OMX/Codex `node_repl/js` failed with a sandbox cwd bootstrap error. No SCP files were recovered or staged in that run. Residual targets include `SCP3301`, `SCP1467`, `SCP211`, `SCP3697`, `SCP282`, and `SCP499`; fix browser control before retrying downloads and continue to avoid blind giant/ambiguous payload clicks | current temporal batch artifacts under `artifacts/schema_audit/`; `temporal_t32_zebrafish_regen_scp_20260622.md`; `scp_browser_auth_recovery_20260622.{md,json}` |
| NASC-seq / metabolic-labeling | accepted for NASC-seq GSE128273 and scNT-seq GSE141851 explicit timecourse | scNT has 19 verified triplets under `temporal_pretraining/metabolic_labeling/scnt_seq/GSE141851/` with `X_new_counts`/`X_old_counts` side matrices; scSLAM GSE115612 remains blocked on explicit multi-timepoint/labeling-window semantics | `current-status.md`; `temporal_scnt_seq_gse141851_t22r_20260622.md`; T11/T22R Kanban handoffs |
| ZESTA / MOSTA / STOmics spatial-temporal | ZESTA 6/6 timepoint prefixes verified (zf3, zf5, zf10, zf12, zf18, zf24); T35 source-probed only | ZESTA continuation is complete for bounded STDS0000057 timepoint h5ads. STT0000071 zebrafish heart is source-blocked: metadata + 9 sample records are public, but samples are not downloadable and file-table probes are empty. HESTA needs SPA/source-manifest resolver; STDS0000060 has only large processed H5ADs (smallest ~1.44 GB); MOSTA needs chunked/staged ingestion | `current-status.md`; T12/T35/T38 Kanban handoffs; `temporal_t38_zesta_heart_partial_status_20260622.md`; `temporal_t38_heart_source_probe_20260622.md`; `temporal_t35_stomics_spatial_probe_20260622.md` |
| Plant spatial/development T39 | probe-only/source-blocked | tomato GSE293948 has a 12 GB processed RDS requiring staged/high-memory Seurat inspection; Arabidopsis life-cycle needs exact repository/accessions; Arabidopsis STOmics needs exact STDS/project accession; no Lamin writes | `temporal_t39_plant_spatial_probe_20260622.md/json`; T39 Kanban handoff |
| PRoPER-seq / ProPer-seq 2026 | wanted but source-TBD | actual scRNA expression source not found; legacy GSE150818 is explicitly excluded and must not be substituted | [dataset-modalities.md](dataset-modalities.md) |
| RxRx / Recursion image datasets | not expression-triplet work yet | needs representation contract for images/embeddings before ingestion; do not force into canonical `X.h5ad` expression triplets | `post_prism_huge_dataset_plan_20260621.md`; [dataset-modalities.md](dataset-modalities.md) |

## PRISM blockers

Current PRISM residual decision is staged-ingestion gated for accessible h5ads, with explicit source/payload exceptions:

- P5D cleanup verified `GSE208240` and `GSE220974` were already present as exact chunked triplets on `jkobject` and moved them to ingested.
- P5F continuation has ingested and verified 16 datasets through `GSE269596`; latest verification: 75 chunks, 74,312 rows, 36,601 vars, 7,394 controls.
- 14 accessible staged h5ads remain smoke-first candidates; next by current size inventory is `GSE281048_TGFB_Perturb_seq`.
- `GSE274751` remains `staged_h5ad_truncated` until the source is re-staged.
- `GSE247598`, `GSE261157`, `GSE272093`, `GSE272457`, and `GSE282731` remain missing/source-blocked.
- `GSE90063_human-004` is excluded/skipped by user decision because of duplicate/subset ambiguity.
- `D4_Rest.assigned_guide` belongs to T-cell GWPS, not PRISM.

Next safe action for accessible staged h5ads: verify GCS bytes, run backed dry-run/shape inspection, smoke-first chunked ingestion, targeted verification, and update P5F status artifacts. For missing/source rows, after any h5ad recovery: stage to GCS, run duplicate gate against public `main` plus `jkobject`, then smoke-first chunked ingestion. Do not mark a PRISM residual done from metadata-only access probes.

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
  recovered. The 2026-06-22 logged-in Chrome retry is blocked by local OMX
  Chrome-control bootstrap, not by a renewed SCP source/access verdict.

## Model-ready implications

The 1056-member canonical surface is `triplet-integrity-ok`, not fully model-ready.
For model work, read [model-roadmap.md](model-roadmap.md) and promote reviewed
subsets with explicit loader evidence before making benchmark claims.
